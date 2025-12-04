/*
 * MC/DC - Memory Cache with Dictionary Compression
 * Copyright (c) 2025 Carrot Data Inc.
 *
 * Licensed under the MC/DC Community License.
 * You may use, modify, and distribute this file, except that neither MC/DC
 * nor any derivative work may be used in any third-party
 * Redis/Valkey/Memcached-as-a-Service offering.
 *
 * See LICENSE-COMMUNITY.txt for details.
 */

#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <zstd.h>

#include "redismodule.h"
#include "mcdc_string_cmd.h"
#include "mcdc_role.h"
#include "mcdc_compression.h"
#include "mcdc_module_utils.h"


/* ------------------------------------------------------------------------- */
/* mcdc.set key value  (full set of options is supported)                                                      */
/* ------------------------------------------------------------------------- */

int MCDC_SetCommand(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC set: wrong number of arguments (expected: mcdc.set key value [options])");
    }

    /* Get key + value bytes */
    size_t klen, vlen;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);
    const char *vptr = RedisModule_StringPtrLen(argv[2], &vlen);

    if (!kptr || !vptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC set: failed to read arguments");
    }

    /* ---------------------------------------------------------
      * Replica or replicated/AOF command → NO compression.
      * Just forward all args (key, value, options…) to SET.
      * --------------------------------------------------------- */
    if (!MCDC_ShouldCompress(ctx)) {
         /* argv[1..argc-1] are exactly: key, value, [options…] */
         RedisModuleCallReply *reply =
             RedisModule_Call(ctx, "SET", "v", argv + 1, (size_t)(argc - 1));
         if (!reply) {
             return RedisModule_ReplyWithError(
                 ctx, "ERR MCDC set: underlying SET failed");
         }
         return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    /* Compress + wrap value with MC/DC header */
    char *stored = NULL;
    ssize_t slen = mcdc_encode_value(kptr, klen, vptr, vlen, &stored);
    if (slen < 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC set: compression failed");
    }

    bool need_dealloc = false;
    if (!stored) {
        /* value smaller than min or bigger than max to compress
         * store as: [raw bytes...]
         */
        need_dealloc = true;
        slen = (int)(vlen);
        stored = RedisModule_Alloc(slen);
        if (!stored) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC set: memory allocation failed");
        }
        memcpy(stored, vptr, vlen);
    }

    RedisModuleString *encoded =
        RedisModule_CreateString(ctx, stored, slen);

    if (need_dealloc) {
        RedisModule_Free(stored);
    }

    /* Detect if user passed GET option, case-insensitive.
     * mcdc.set key value [options...]
     * options start at argv[3]
     */
    int has_get = 0;
    for (int i = 3; i < argc; i++) {
        size_t optlen;
        const char *opt = RedisModule_StringPtrLen(argv[i], &optlen);
        if (optlen == 3 &&
            (opt[0] == 'G' || opt[0] == 'g') &&
            (opt[1] == 'E' || opt[1] == 'e') &&
            (opt[2] == 'T' || opt[2] == 't')) {
            has_get = 1;
            break;
        }
    }

    /* Build argv for underlying SET:
     *
     * mcdc.set key value [opts...]
     * -> SET key encoded_value [opts...]
     *
     * Number of args to SET = (argc - 1):
     *   0: key            (argv[1])
     *   1: encoded value  (encoded)
     *   2..N: options     (argv[3..argc-1])
     */
    int set_argc = argc - 1;
    RedisModuleString **set_argv =
        RedisModule_PoolAlloc(ctx, sizeof(RedisModuleString *) * set_argc);

    set_argv[0] = argv[1];
    set_argv[1] = encoded;

    for (int i = 3; i < argc; i++) {
        set_argv[i - 1] = argv[i];  /* copy all options as-is */
    }

    /* Call underlying Redis SET command (! - for replication if enabled) with full options preserved */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "SET", "!v", set_argv, set_argc);

    if (reply == NULL) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC set: underlying SET failed");
    }

    /* If there is no GET option, pass reply through unchanged:
     * - "OK"
     * - (nil) for NX/XX miss
     * - error, etc.
     */
    if (!has_get) {
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    /* With GET option:
     *   - If reply is NULL -> (nil), like SET GET semantics.
     *   - If reply is an error -> pass through.
     *   - If reply is a bulk string:
     *       - It is the *previous* stored value (likely MC/DC encoded)
     *       - Decode/decompress it and return plain value.
     */

    int rtype = RedisModule_CallReplyType(reply);

    if (rtype == REDISMODULE_REPLY_NULL) {
        /* No previous value */
        return RedisModule_ReplyWithNull(ctx);
    }

    if (rtype == REDISMODULE_REPLY_ERROR) {
        /* Underlying SET failed with a protocol-level error */
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    if (rtype != REDISMODULE_REPLY_STRING) {
        /* Should not happen, but be defensive */
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    /* Extract old encoded value */
    size_t enc_len;
    const char *enc_ptr = RedisModule_CallReplyStringPtr(reply, &enc_len);
    if (!enc_ptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC set: failed to read GET reply");
    }
    if (vlen <= sizeof(uint16_t) || !mcdc_is_compressed(enc_ptr + sizeof(uint16_t), enc_len - sizeof(uint16_t))){
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }
    /* Decode / decompress previous value. This must:
     *   - handle MC/DC encoded values (header + compressed payload)
     *   - gracefully fall back to "raw" if the value is not encoded
     *     (e.g. key was set by plain SET, not mcdc.set)
     */

    char *out = NULL;
    ssize_t outlen = mcdc_decode_value(kptr, klen,
                                enc_ptr, enc_len,
                                &out);
    if (outlen < 0 || !out) {
        /* MC/DC encoded (can be false positive) and decompression failed:
         * delete key and retun null
         */
        if (out) free(out);
        MCDC_DelKey(ctx, argv[1]);
        /* Behave like GET: return null bulk string */
        return RedisModule_ReplyWithNull(ctx);
    }

    RedisModule_ReplyWithStringBuffer(ctx, out, outlen);
    free(out);

    return REDISMODULE_OK;
}

/* ------------------------------------------------------------------------- */
/* mcdc.setex key expire value  (full set of options is supported)           */
/* ------------------------------------------------------------------------- */

int MCDC_SetExCommand(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 4) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC setex: wrong number of arguments (expected: mcdc.setex key expire value)");
    }

    /* Get key + value bytes */
    size_t klen, vlen;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);
    const char *vptr = RedisModule_StringPtrLen(argv[3], &vlen);

    if (!kptr || !vptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC setex: failed to read arguments");
    }
    /* ---------------------------------------------------------
      * Replica or replicated/AOF command → NO compression.
      * Just forward all args (key, value, options…) to SET.
      * --------------------------------------------------------- */
    if (!MCDC_ShouldCompress(ctx)) {
        /* argv[1..argc-1] are exactly: key, value, [options…] */
        RedisModuleCallReply *reply =
            RedisModule_Call(ctx, "SETEX", "v", argv + 1, (size_t)(argc - 1));
        if (!reply) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC set: underlying SETEX failed");
        }
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    /* Compress + wrap value with MC/DC header */
    char *stored = NULL;
    ssize_t slen = mcdc_encode_value(kptr, klen, vptr, vlen, &stored);
    if (slen < 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC setex: compression failed");
    }

    bool need_dealloc = false;
    if (!stored) {
        /* value smaller than min or bigger than max to compress
         * store as: [raw bytes...]
         */
        need_dealloc = true;
        slen = (int)(vlen);
        stored = RedisModule_Alloc(slen);
        if (!stored) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC setex: memory allocation failed");
        }
        memcpy(stored, vptr, vlen);
    }

    RedisModuleString *encoded =
        RedisModule_CreateString(ctx, stored, slen);

    if (need_dealloc) {
        RedisModule_Free(stored);
    }

    int set_argc = 3;
    RedisModuleString **set_argv =
        RedisModule_PoolAlloc(ctx, sizeof(RedisModuleString *) * set_argc);

    set_argv[0] = argv[1];      /* key */
    set_argv[1] = argv[2];      /* expiration */
    set_argv[2] = encoded;      /* compressed value */

    /* Call underlying Redis SETEX command with full options preserved */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "SETEX", "!v", set_argv, set_argc);

    if (reply == NULL) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC setex: underlying SETEX failed");
    }

    return RedisModule_ReplyWithCallReply(ctx, reply);

}

/* ------------------------------------------------------------------------- */
/* mcdc.psetex key expire value                                              */
/* ------------------------------------------------------------------------- */

int MCDC_PsetExCommand(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 4) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC psetex: wrong number of arguments (expected: mcdc.psetex key expire value)");
    }

    /* Get key + value bytes */
    size_t klen, vlen;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);
    const char *vptr = RedisModule_StringPtrLen(argv[3], &vlen);

    if (!kptr || !vptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC psetx: failed to read arguments");
    }
    /* ---------------------------------------------------------
      * Replica or replicated/AOF command → NO compression.
      * Just forward all args (key, value, options…) to SET.
      * --------------------------------------------------------- */
    if (!MCDC_ShouldCompress(ctx)) {
        /* argv[1..argc-1] are exactly: key, value, [options…] */
        RedisModuleCallReply *reply =
            RedisModule_Call(ctx, "PSETEX", "v", argv + 1, (size_t)(argc - 1));
        if (!reply) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC set: underlying PSETEX failed");
        }
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    /* Compress + wrap value with MC/DC header */
    char *stored = NULL;
    ssize_t slen = mcdc_encode_value(kptr, klen, vptr, vlen, &stored);
    if (slen < 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC psetex: compression failed");
    }

    bool need_dealloc = false;
    if (!stored) {
        /* value smaller than min or bigger than max to compress
         * store as: [raw bytes...]
         */
        need_dealloc = true;
        slen = (int)(vlen);
        stored = RedisModule_Alloc(slen);
        if (!stored) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC psetex: memory allocation failed");
        }
        memcpy(stored, vptr, vlen);
    }

    RedisModuleString *encoded =
        RedisModule_CreateString(ctx, stored, slen);

    if (need_dealloc) {
        RedisModule_Free(stored);
    }

    int set_argc = 3;
    RedisModuleString **set_argv =
        RedisModule_PoolAlloc(ctx, sizeof(RedisModuleString *) * set_argc);

    set_argv[0] = argv[1];      /* key */
    set_argv[1] = argv[2];      /* expiration */
    set_argv[2] = encoded;      /* compressed value */

    /* Call underlying Redis SETEX command with full options preserved */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "PSETEX", "!v", set_argv, set_argc);

    if (reply == NULL) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC psetex: underlying PSETEX failed");
    }

    return RedisModule_ReplyWithCallReply(ctx, reply);

}

/* ------------------------------------------------------------------------- */
/* mcdc.setnx key value                                                      */
/* ------------------------------------------------------------------------- */

int MCDC_SetNxCommand(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC setnx: wrong number of arguments (expected: mcdc.setnx)");
    }

    /* Get key + value bytes */
    size_t klen, vlen;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);
    const char *vptr = RedisModule_StringPtrLen(argv[2], &vlen);

    if (!kptr || !vptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC setnx: failed to read arguments");
    }
    /* ---------------------------------------------------------
      * Replica or replicated/AOF command → NO compression.
      * Just forward all args (key, value, options…) to SET.
      * --------------------------------------------------------- */
    if (!MCDC_ShouldCompress(ctx)) {
        /* argv[1..argc-1] are exactly: key, value, [options…] */
        RedisModuleCallReply *reply =
            RedisModule_Call(ctx, "SETNX", "v", argv + 1, (size_t)(argc - 1));
        if (!reply) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC set: underlying SETNX failed");
        }
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    /* Compress + wrap value with MC/DC header */
    char *stored = NULL;
    ssize_t slen = mcdc_encode_value(kptr, klen, vptr, vlen, &stored);
    if (slen < 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC setnx: compression failed");
    }

    bool need_dealloc = false;
    if (!stored) {
        /* value smaller than min or bigger than max to compress
         * store as: [u16 = -1][raw bytes...]
         */
        need_dealloc = true;
        slen = (int)(vlen);
        stored = RedisModule_Alloc(slen);
        if (!stored) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC setnx: memory allocation failed");
        }
        memcpy(stored, vptr, vlen);
    }

    RedisModuleString *encoded =
        RedisModule_CreateString(ctx, stored, slen);

    if (need_dealloc) {
        RedisModule_Free(stored);
    }

    /* Call underlying Redis SETNX command */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "SETNX", "!ss", argv[1], encoded);

    if (reply == NULL) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC set: underlying SET failed");
    }

    return RedisModule_ReplyWithCallReply(ctx, reply);

}

/* ------------------------------------------------------------------------- */
/* mcdc.get key                                                              */
/* ------------------------------------------------------------------------- */

int MCDC_GetCommand(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC get: wrong number of arguments (expected: mcdc.get key)");
    }

    /* Call underlying Redis GET:
     *   GET key
     */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "GET", "s", argv[1]);

    if (reply == NULL) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC get: underlying GET failed");
    }

    int rtype = RedisModule_CallReplyType(reply);

    if (rtype == REDISMODULE_REPLY_NULL) {
        /* Behave like GET: return null bulk string */
        return RedisModule_ReplyWithNull(ctx);
    }

    if (rtype != REDISMODULE_REPLY_STRING) {
        /* This should not happen with GET, but be defensive */
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC get: unexpected reply type from GET");
    }

    /* Extract blob returned by GET */
    size_t rlen;
    const char *rptr = RedisModule_CallReplyStringPtr(reply, &rlen);

    if (!rptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC get: failed to read GET reply");
    }
    
    if (rlen <= sizeof(uint16_t) || !mcdc_is_compressed(rptr + sizeof(uint16_t), rlen - sizeof(uint16_t))){
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }
    /* Decompress if needed, based on MC/DC header */
    size_t klen;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);
    char *out = NULL;
    ssize_t outlen = mcdc_decode_value(kptr, klen, rptr, rlen, &out);
    if (outlen < 0 || !out) {
        /* MC/DC encoded (can be false positive) and decompression failed:
         * delete key and retun null
         */
        if (out) free(out);
        MCDC_DelKey(ctx, argv[1]);
        /* Behave like GET: return null bulk string */
        return RedisModule_ReplyWithNull(ctx);
    }

    /* Return uncompressed payload as bulk string */
    RedisModule_ReplyWithStringBuffer(ctx, out, outlen);
    // free after malloc
    free(out);
    return REDISMODULE_OK;
}

/* ------------------------------------------------------------------------- */
/* mcdc.getdel key                                                           */
/* ------------------------------------------------------------------------- */
int MCDC_GetDelCommand(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getdel: wrong number of arguments (expected: mcdc.getdel key)");
    }

    /* Call underlying Redis GET:
     *   GETDEL key
     */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "GETDEL", "!s", argv[1]);

    if (reply == NULL) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getdel: underlying GETDEL failed");
    }

    int rtype = RedisModule_CallReplyType(reply);

    if (rtype == REDISMODULE_REPLY_NULL) {
        /* Behave like GET: return null bulk string */
        return RedisModule_ReplyWithNull(ctx);
    }

    if (rtype != REDISMODULE_REPLY_STRING) {
        /* This should not happen with GET, but be defensive */
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getdel: unexpected reply type from GETDEL");
    }

    /* Extract blob returned by GET */
    size_t rlen;
    const char *rptr = RedisModule_CallReplyStringPtr(reply, &rlen);

    if (!rptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getdel: failed to read GETDEL reply");
    }
    if (rlen <= sizeof(uint16_t) || !mcdc_is_compressed(rptr + sizeof(uint16_t), rlen - sizeof(uint16_t))){
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }
    /* Decompress if needed, based on MC/DC header */
    size_t klen;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);

    char *out = NULL;

    ssize_t outlen = mcdc_decode_value(kptr, klen, rptr, rlen, &out);
    if (outlen < 0 || !out) {
        /* MC/DC encoded (can be false positive) and decompression failed:
         * delete key and retun null
         */
        if (out) free(out);
        MCDC_DelKey(ctx, argv[1]);
        /* Behave like GET: return null bulk string */
        return RedisModule_ReplyWithNull(ctx);
    }

    /* Return uncompressed payload as bulk string */
    RedisModule_ReplyWithStringBuffer(ctx, out, outlen);
    // free after malloc
    free(out);
    return REDISMODULE_OK;
}

/* ------------------------------------------------------------------------- */
/* mcdc.getex key                                                              */
/* ------------------------------------------------------------------------- */
int MCDC_GetExCommand(RedisModuleCtx *ctx,
                      RedisModuleString **argv,
                      int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getex: wrong number of arguments (expected: mcdc.getex key [options])");
    }

    /* Call underlying Redis GETEX:
     *   GETEX key [EX seconds | PX ms | EXAT ts | PXAT ts_ms | PERSIST]
     *
     * We forward everything except the module command name:
     *   argv[1..argc-1] -> GETEX args
     */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "GETEX", "!v", argv + 1, argc - 1);

    if (reply == NULL) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getex: underlying GETEX failed");
    }

    int rtype = RedisModule_CallReplyType(reply);

    if (rtype == REDISMODULE_REPLY_NULL) {
        /* Behave like GETEX: return null bulk string if key missing */
        return RedisModule_ReplyWithNull(ctx);
    }

    if (rtype != REDISMODULE_REPLY_STRING) {
        /* Should not happen for GETEX on a string key, but be defensive */
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getex: unexpected reply type from GETEX");
    }

    /* Extract blob returned by GETEX */
    size_t rlen;
    const char *rptr = RedisModule_CallReplyStringPtr(reply, &rlen);

    if (!rptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getex: failed to read GETEX reply");
    }
    if (rlen <= sizeof(uint16_t) || !mcdc_is_compressed(rptr + sizeof(uint16_t), rlen - sizeof(uint16_t))){
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }
    /* Decompress / decode if needed, based on MC/DC header */
    size_t klen;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);

    char *out = NULL;
    ssize_t outlen = mcdc_decode_value(kptr, klen, rptr, rlen, &out);
    if (outlen < 0 || !out) {
        /* MC/DC encoded (can be false positive) and decompression failed:
         * delete key and retun null
         */
        if (out) free(out);
        MCDC_DelKey(ctx, argv[1]);
        /* Behave like GET: return null bulk string */
        return RedisModule_ReplyWithNull(ctx);
    }

    /* Return uncompressed payload as bulk string */
    RedisModule_ReplyWithStringBuffer(ctx, out, outlen);
    free(out);
    return REDISMODULE_OK;
}
/* ------------------------------------------------------------------------- */
/* mcdc.getset key                                                              */
/* ------------------------------------------------------------------------- */
int MCDC_GetSetCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getset: wrong number of arguments (expected: mcdc.getset key value)");
    }

    /* Get key + value bytes */
    size_t klen, vlen;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);
    const char *vptr = RedisModule_StringPtrLen(argv[2], &vlen);

    if (!kptr || !vptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getset: failed to read arguments");
    }
    /* ---------------------------------------------------------
      * Replica or replicated/AOF command → NO compression.
      * Just forward all args (key, value, options…) to SET.
      * --------------------------------------------------------- */
    if (!MCDC_ShouldCompress(ctx)) {
        /* argv[1..argc-1] are exactly: key, value, [options…] */
        RedisModuleCallReply *reply =
            RedisModule_Call(ctx, "GETSET", "v", argv + 1, (size_t)(argc - 1));
        if (!reply) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC set: underlying GETSET failed");
        }
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    /* Compress + wrap value with MC/DC header */
    char *stored = NULL;
    ssize_t slen = mcdc_encode_value(kptr, klen, vptr, vlen, &stored);
    if (slen < 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getset: compression failed");
    }

    bool need_dealloc = false;
    if (!stored) {
        /* Value is too small/too large to compress, store as:
         *   [raw bytes...]
         */
        need_dealloc = true;
        slen = (int)(vlen);
        stored = RedisModule_Alloc(slen);
        if (!stored) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC getset: memory allocation failed");
        }
        memcpy(stored, vptr, vlen);
    }

    /* Create Redis string from encoded value */
    RedisModuleString *encoded =
        RedisModule_CreateString(ctx, stored, slen);

    /* Free temp buffer if it was heap-allocated just for this call */
    if (need_dealloc) {
        RedisModule_Free(stored);
    }

    /* Call underlying Redis GETSET:
     *   GETSET key encoded_value
     */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "GETSET", "!ss", argv[1], encoded);

    if (reply == NULL) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getset: underlying GETSET failed");
    }

    int rtype = RedisModule_CallReplyType(reply);

    if (rtype == REDISMODULE_REPLY_NULL) {
        /* Behave like GETSET: return (nil) if key didn't exist */
        return RedisModule_ReplyWithNull(ctx);
    }

    if (rtype != REDISMODULE_REPLY_STRING) {
        /* Should not happen, but be defensive */
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getset: unexpected reply type from GETSET");
    }

    /* Extract old (encoded) value */
    size_t rlen;
    const char *rptr = RedisModule_CallReplyStringPtr(reply, &rlen);

    if (!rptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getset: failed to read GETSET reply");
    }
    if (rlen <= sizeof(uint16_t) || !mcdc_is_compressed(rptr + sizeof(uint16_t), rlen - sizeof(uint16_t))){
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }
    /* Decode/decompress previous value.
     * This should gracefully handle:
     *  - MC/DC-encoded values, and
     *  - plain raw values (if key was written by normal SET).
     */

    char *out = NULL;
    ssize_t outlen = mcdc_decode_value(kptr, klen, rptr, rlen, &out);
    if (outlen < 0 || !out) {
        /* MC/DC encoded (can be false positive) and decompression failed:
         * delete key and retun null
         */
        if (out) free(out);
        MCDC_DelKey(ctx, argv[1]);
        /* Behave like GET: return null bulk string */
        return RedisModule_ReplyWithNull(ctx);
    }

    /* Return decoded old value */
    RedisModule_ReplyWithStringBuffer(ctx, out, outlen);
    free(out);
    return REDISMODULE_OK;
}

/* ------------------------------------------------------------------------- */
/* mcdc.cstrlen key  - compressed value length   (one can use STRLEN)        */
/* ------------------------------------------------------------------------- */

int MCDC_CstrlenCommand(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC cstrlen: wrong number of arguments (expected: mcdc.cstrlen key)");
    }
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "STRLEN", "s", argv[1]);

    if (reply == NULL) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC cstrlen: underlying STRLEN failed");
    }
    return RedisModule_ReplyWithCallReply(ctx, reply);
}

/* ------------------------------------------------------------------------- */
/* mcdc.strlen key  - value length                                           */
/* ------------------------------------------------------------------------- */
int MCDC_StrlenCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC strlen: wrong number of arguments (expected: mcdc.strlen key)");
    }

    /* Call underlying Redis GET:
     *   GET key
     *
     * We want the logical length of the *decoded* value, but for
     * compressed values we can get it from the Zstd frame header
     * without actually decompressing.
     */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "GET", "s", argv[1]);

    if (reply == NULL) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC strlen: underlying GET failed");
    }

    int rtype = RedisModule_CallReplyType(reply);

    if (rtype == REDISMODULE_REPLY_NULL) {
        /* Behave like STRLEN: non-existing key -> 0 */
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }

    if (rtype == REDISMODULE_REPLY_ERROR) {
        /* WRONGTYPE or other error from GET: pass it through */
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    if (rtype != REDISMODULE_REPLY_STRING) {
        /* Should not happen, but be defensive */
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC strlen: unexpected reply type from GET");
    }

    /* Extract stored (possibly MC/DC-encoded) value */
    size_t rlen;
    const char *rptr = RedisModule_CallReplyStringPtr(reply, &rlen);
    if (!rptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC strlen: failed to read GET reply");
    }

    /* Case 1: too short to have a MC/DC header -> treat as raw. */
    if (rlen < sizeof(uint16_t)) {
        return RedisModule_ReplyWithLongLong(ctx, (long long)rlen);
    }
    if (rlen <= sizeof(uint16_t) || !mcdc_is_compressed(rptr + sizeof(uint16_t), rlen - sizeof(uint16_t))){
        return RedisModule_ReplyWithLongLong(ctx, rlen);
    }
    /* Read dict_id from header */
    const char *payload = rptr + sizeof(uint16_t);
    size_t plen = rlen - sizeof(uint16_t);

    /* Case 3: did >= 0 => compressed Zstd frame in payload.
     * Use ZSTD_getFrameContentSize() to get original size without
     * decompressing.
     *
     * NOTE: This function may return:
     *   - exact size
     *   - ZSTD_CONTENTSIZE_UNKNOWN
     *   - ZSTD_CONTENTSIZE_ERROR
     */
    unsigned long long rawSize =
        ZSTD_getFrameContentSize(payload, plen);

    if (rawSize != ZSTD_CONTENTSIZE_ERROR &&
        rawSize != ZSTD_CONTENTSIZE_UNKNOWN)
    {
        /* Valid Zstd frame with known uncompressed size */
        return RedisModule_ReplyWithLongLong(ctx, (long long)rawSize);
    } else {
        MCDC_DelKey(ctx, argv[1]);
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }

}
/* ------------------------------------------------------------------------- */
/* mcdc.mget key [key ...] - multi get                                       */
/* ------------------------------------------------------------------------- */
int MCDC_MGetCommand(RedisModuleCtx *ctx,
                     RedisModuleString **argv,
                     int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC mget: wrong number of arguments (expected: mcdc.mget key [key ...])");
    }

    int nkeys = argc - 1;

    /* Call underlying Redis MGET:
     *   MGET key1 key2 ... keyN
     *
     * We forward all keys from argv[1..argc-1].
     */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "MGET", "v", argv + 1, nkeys);

    if (reply == NULL) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC mget: underlying MGET failed");
    }

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC mget: unexpected reply type from MGET");
    }

    size_t arrlen = RedisModule_CallReplyLength(reply);
    if (arrlen != (size_t)nkeys) {
        /* Should not happen, but be defensive */
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC mget: unexpected array length from MGET");
    }

    /* Build the top-level reply array: one element per key */
    RedisModule_ReplyWithArray(ctx, nkeys);

    for (int i = 0; i < nkeys; i++) {
        RedisModuleCallReply *elem =
            RedisModule_CallReplyArrayElement(reply, i);

        int etype = RedisModule_CallReplyType(elem);

        if (etype == REDISMODULE_REPLY_NULL) {
            /* Key does not exist -> (nil) */
            RedisModule_ReplyWithNull(ctx);
            continue;
        }

        if (etype == REDISMODULE_REPLY_ERROR) {
            /* MGET normally doesn't return errors per element, but in case it does,
             * just pass the error object through as the array element.
             */
            RedisModule_ReplyWithCallReply(ctx, elem);
            continue;
        }

        if (etype != REDISMODULE_REPLY_STRING) {
            /* Very unexpected; still pass through whatever Redis gave us */
            RedisModule_ReplyWithCallReply(ctx, elem);
            continue;
        }

        /* Extract stored (possibly MC/DC-encoded) value */
        size_t rlen;
        const char *rptr = RedisModule_CallReplyStringPtr(elem, &rlen);
        if (!rptr) {
            /* Treat as nil if we can't read it */
            RedisModule_ReplyWithNull(ctx);
            continue;
        }
        if (rlen <= sizeof(uint16_t) || !mcdc_is_compressed(rptr + sizeof(uint16_t), rlen - sizeof(uint16_t))){
            RedisModule_ReplyWithCallReply(ctx, elem);
            continue;
        }
        /* Key name for this element is argv[i+1] */
        size_t klen;
        const char *kptr = RedisModule_StringPtrLen(argv[i + 1], &klen);

        char *out = NULL;
        ssize_t outlen = mcdc_decode_value(kptr, klen, rptr, rlen, &out);

        if (outlen < 0 || !out) {
            /* MC/DC encoded (can be false positive) and decompression failed:
             * delete key and return null
             */
            if (out) free(out);
            RedisModule_Log(ctx, "warning",
                    "<mcdc> compression FAILED key='%.*s' value='%.*s' value-length=%zu rc=%zd",
                    (int)klen, kptr,
                    (int)rlen, rptr, rlen,
                    outlen);
            MCDC_DelKey(ctx, argv[1]);
            /* Behave like GET: return null bulk string */
            RedisModule_ReplyWithNull(ctx);
            continue;
        }
        /* Return decoded value for this element */
        RedisModule_ReplyWithStringBuffer(ctx, out, (size_t)outlen);
        free(out);
    }

    return REDISMODULE_OK;
}

/* ------------------------------------------------------------------------- */
/* mcdc.mset key value [key value ...] - multi set                           */
/* ------------------------------------------------------------------------- */
int MCDC_MSetFamilyCommand(RedisModuleCtx *ctx,
                     RedisModuleString **argv,
                     int argc, const char *cmd)
{
    RedisModule_AutoMemory(ctx);

    /* mcdc.mset key1 value1 [key2 value2 ...] */
    if (argc < 3 || (argc % 2) == 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC mset(nx): wrong number of arguments (expected: mcdc.mset(nx) key value [key value ...])");
    }

    int n_pairs   = (argc - 1) / 2;
    int mset_argc = argc - 1;  /* everything except module command name */

    /* ---------------------------------------------------------
      * Replica or replicated/AOF command → NO compression.
      * Just forward all args (key, value, options…) to MSET.
      * --------------------------------------------------------- */
    if (!MCDC_ShouldCompress(ctx)) {
        /* argv[1..argc-1] are exactly: key, value, [options…] */
        RedisModuleCallReply *reply =
            RedisModule_Call(ctx, cmd, "v", argv + 1, (size_t)(argc - 1));
        if (!reply) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC mset(nx): underlying cmd failed");
        }
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }
    /* We’ll build argv for underlying MSET:
     *   MSET key1 enc_v1 key2 enc_v2 ...
     */
    RedisModuleString **mset_argv =
        RedisModule_PoolAlloc(ctx, sizeof(RedisModuleString *) * mset_argc);

    /* For each (key, value) pair */
    for (int i = 0; i < n_pairs; i++) {
        int key_index = 1 + 2 * i;
        int val_index = key_index + 1;

        RedisModuleString *key   = argv[key_index];
        RedisModuleString *value = argv[val_index];

        /* Put key into the MSET argv */
        mset_argv[2 * i] = key;

        /* Get raw key/value bytes */
        size_t klen, vlen;
        const char *kptr = RedisModule_StringPtrLen(key, &klen);
        const char *vptr = RedisModule_StringPtrLen(value, &vlen);

        if (!kptr || !vptr) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC mset(nx): failed to read arguments");
        }

        /* Compress + wrap value with MC/DC header */
        char *stored = NULL;
        ssize_t slen = mcdc_encode_value(kptr, klen, vptr, vlen, &stored);
        if (slen < 0) {
            RedisModule_Log(ctx, "warning",
                    "<mcdc> compression FAILED key='%.*s' value='%.*s' value-length=%zu rc=%zd",
                    (int)klen, kptr,
                    (int)vlen, vptr, vlen,
                    slen);
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC mset(nx): compression failed");
        }

        bool need_dealloc = false;
        if (!stored) {
            /* value smaller than min or bigger than max to compress
             * We store as:
             *   [u16 = -1][raw bytes...]
             */
            need_dealloc = true;
            slen = (int)(vlen);
            stored = RedisModule_Alloc(slen);
            if (!stored) {
                return RedisModule_ReplyWithError(
                    ctx, "ERR MCDC mset(nx): memory allocation failed");
            }
            memcpy(stored, vptr, vlen);
        }

        /* Create Redis string from encoded value */
        RedisModuleString *encoded =
            RedisModule_CreateString(ctx, stored, slen);

        if (need_dealloc) {
            RedisModule_Free(stored);
        }

        /* Put encoded value into the MSET argv */
        mset_argv[2 * i + 1] = encoded;
    }

    /* Call underlying Redis MSET with all encoded pairs */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, cmd, "!v", mset_argv, mset_argc);

    if (reply == NULL) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC mset(nx): underlying MSET failed");
    }

    /* Redis MSET normally returns "OK" */
    return RedisModule_ReplyWithCallReply(ctx, reply);
}

int MCDC_MSetCommand(RedisModuleCtx *ctx,
                     RedisModuleString **argv,
                     int argc) {
    return MCDC_MSetFamilyCommand(ctx, argv, argc, "MSET");
}

int MCDC_MSetNXCommand(RedisModuleCtx *ctx,
                     RedisModuleString **argv,
                     int argc) {
    return MCDC_MSetFamilyCommand(ctx, argv, argc, "MSETNX");
}

/* ------------------------------------------------------------------------- */
/* mcdc.setraw key value - set key value bypassing compression               */
/* ------------------------------------------------------------------------- */
int MCDC_SetRawCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc)
{
    RedisModule_AutoMemory(ctx);

    /* mcdc.setraw key value */
    if (argc != 3) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC setraw: wrong number of arguments "
                 "(expected: mcdc.setraw key value)");
    }

    /* Just forward to plain SET and bypass MC/DC compression logic.
     *
     * Important details:
     * - We use "!v" so this is treated as a write command and is
     *   replicated / written to AOF.
     * - The command filter was registered with REDISMODULE_CMDFILTER_NOSELF,
     *   so this internal SET will NOT be rewritten to mcdc.set.
     */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "SET", "!v", argv + 1, (size_t)2);

    if (!reply) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC setraw: underlying SET failed");
    }

    return RedisModule_ReplyWithCallReply(ctx, reply);
}

/* ------------------------------------------------------------------------- */
/* Registration helper                                                       */
/* ------------------------------------------------------------------------- */

int MCDC_RegisterStringCommands(RedisModuleCtx *ctx)
{
    if (RedisModule_CreateCommand(ctx, "mcdc.set", MCDC_SetCommand, "write deny-oom",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "mcdc.setnx", MCDC_SetNxCommand, "write deny-oom",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx, "mcdc.setex", MCDC_SetExCommand, "write deny-oom",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "mcdc.psetex", MCDC_PsetExCommand, "write deny-oom",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx, "mcdc.get", MCDC_GetCommand, "readonly",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    if (RedisModule_CreateCommand(ctx, "mcdc.getdel", MCDC_GetDelCommand, "write deny-oom",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx, "mcdc.getex", MCDC_GetExCommand, "write deny-oom",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx, "mcdc.getset", MCDC_GetSetCommand, "write deny-oom",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx, "mcdc.cstrlen", MCDC_CstrlenCommand, "readonly",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx, "mcdc.strlen", MCDC_StrlenCommand, "readonly",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    if (RedisModule_CreateCommand(ctx, "mcdc.mget", MCDC_MGetCommand, "readonly",
            1, -1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx, "mcdc.mset", MCDC_MSetCommand, "write deny-oom",
            1, -1, 2) == REDISMODULE_ERR)  /* keys: argv[1], argv[3], ..., step=2 */
    {
        return REDISMODULE_ERR;
    }
   
    if (RedisModule_CreateCommand(ctx, "mcdc.msetnx", MCDC_MSetNXCommand, "write deny-oom",
            1, -1, 2) == REDISMODULE_ERR)  /* keys: argv[1], argv[3], ..., step=2 */
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx, "mcdc.setraw", MCDC_SetRawCommand, "write",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
