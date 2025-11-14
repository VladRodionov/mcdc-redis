#include <string.h>
#include <stdint.h>
#include <stdlib.h>

#include "redismodule.h"
#include "mcdc_string_cmd.h"
#include "mcdc_compression.h"

static inline void write_u16(char *dst, int v)
{
    /* Map -1 to 0xFFFF, otherwise mask to uint16_t range */
    unsigned int u = (v == -1) ? 0xFFFF : ((unsigned int)v & 0xFFFF);

    dst[0] = (uint8_t)(u >> 8);     // high byte
    dst[1] = (uint8_t)(u & 0xFF);   // low byte
}

static inline int read_u16(const char *src)
{
    unsigned int hi = (unsigned char)src[0];
    unsigned int lo = (unsigned char)src[1];

    unsigned int u = (hi << 8) | lo;

    /* Interpret sentinel 0xFFFF as -1 */
    if (u == 0xFFFF)
        return -1;

    return (int)u;
}

/* ------------------------------------------------------------------------- */
/* MC/DC value layout                                                        */
/* ------------------------------------------------------------------------- */
/*
 * For now we assume a simple layout:
 *
 *   [2 bytes dict_id in network order][payload bytes...]
 *
 * dict_id == -1  => value is stored uncompressed (payload = original data)
 * dict_id == 0  => value is stored compressed w/o dictionary (payload = zstd compressed)
 * dict_id > 0  => payload is compressed with MC/DC using that dictionary (payload = zstd with dictionary compressed)
 *
 */

static size_t
mcdc_encode_value(const char *key, size_t klen,
                  const char *value, size_t vlen,
                  char **outbuf)
{
    // 0xFFFF is -1 (max dict_id is 65534)
    uint16_t dict_id = 0;
    /* Call into your existing MC/DC compressor. */
    int csz = mcdc_maybe_compress(value, vlen, key, klen,
                           (void **) outbuf, &dict_id);
    if (csz < 0) {
        /* error */
        return csz;
    }
    if (csz == 0 && !outbuf) {
        /* Store uncompressed; still add header with dict_id=-1 */
        write_u16(*outbuf, -1);
        memcpy(*outbuf + sizeof(uint16_t), value, vlen);
        return vlen + sizeof(uint16_t);
    } else if (csz > 0) {
        /* Prepend dict_id header */
        write_u16(*outbuf, dict_id);
        return csz + sizeof(uint16_t);
    } else {
        return vlen + sizeof(uint16_t);
    }
}

static size_t
mcdc_decode_value(const char *key, size_t klen,
                  const char *input, size_t ilen,
                  char **outbuf)
{
 
    int dict_id = read_u16(input);
    const char *payload = input + sizeof(uint16_t);
    size_t      plen    = ilen  - sizeof(uint16_t);

    if (dict_id < 0) {
        /* Uncompressed payload */
        *outbuf = malloc(plen);
        if (!*outbuf) return -1;
        memcpy(*outbuf, payload, plen);
        return plen;
    }

    /* Compressed: call into your MC/DC decompressor */
    size_t dsz = mcdc_maybe_decompress(payload, plen, key, klen,
                             (void **) outbuf, (uint16_t) dict_id);
    return dsz;
}

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

    /* MC/DC sampling hook */
    mcdc_sample(kptr, klen, vptr, vlen);

    /* Compress + wrap value with MC/DC header */
    char *stored = NULL;
    int slen = mcdc_encode_value(kptr, klen, vptr, vlen, &stored);
    if (slen < 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC set: compression failed");
    }

    bool need_dealloc = false;
    if (!stored) {
        /* value smaller than min or bigger than max to compress
         * store as: [u16 = -1][raw bytes...]
         */
        need_dealloc = true;
        slen = (int)(sizeof(uint16_t) + vlen);
        stored = RedisModule_Alloc(slen);
        if (!stored) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC set: memory allocation failed");
        }
        write_u16(stored, -1);
        memcpy(stored + sizeof(uint16_t), vptr, vlen);
    }

    RedisModuleString *encoded =
        RedisModule_CreateString(ctx, stored, slen);

    /* If this buffer was allocated just for this call, free it.
     * If mcdc_encode_value uses TLS, it can keep its own buffer.
     */
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

    set_argv[0] = argv[1];      /* key */
    set_argv[1] = encoded;      /* compressed value */

    for (int i = 3; i < argc; i++) {
        set_argv[i - 1] = argv[i];  /* copy all options as-is */
    }

    /* Call underlying Redis SET command with full options preserved */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "SET", "v", set_argv, set_argc);

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

    /* Decode / decompress previous value. This must:
     *   - handle MC/DC encoded values (header + compressed payload)
     *   - gracefully fall back to "raw" if the value is not encoded
     *     (e.g. key was set by plain SET, not mcdc.set)
     */

    char *out = NULL;
    size_t outlen = mcdc_decode_value(kptr, klen,
                                enc_ptr, enc_len,
                                &out);
    if (outlen < 0 || !out) {
        if (out) free(out);
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC set: failed to decode previous value");
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

    /* MC/DC sampling hook */
    mcdc_sample(kptr, klen, vptr, vlen);

    /* Compress + wrap value with MC/DC header */
    char *stored = NULL;
    int slen = mcdc_encode_value(kptr, klen, vptr, vlen, &stored);
    if (slen < 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC setex: compression failed");
    }

    bool need_dealloc = false;
    if (!stored) {
        /* value smaller than min or bigger than max to compress
         * store as: [u16 = -1][raw bytes...]
         */
        need_dealloc = true;
        slen = (int)(sizeof(uint16_t) + vlen);
        stored = RedisModule_Alloc(slen);
        if (!stored) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC setex: memory allocation failed");
        }
        write_u16(stored, -1);
        memcpy(stored + sizeof(uint16_t), vptr, vlen);
    }

    RedisModuleString *encoded =
        RedisModule_CreateString(ctx, stored, slen);

    /* If this buffer was allocated just for this call, free it.
     * If mcdc_encode_value uses TLS, it can keep its own buffer.
     */
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
        RedisModule_Call(ctx, "SETEX", "v", set_argv, set_argc);

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

    /* MC/DC sampling hook */
    mcdc_sample(kptr, klen, vptr, vlen);

    /* Compress + wrap value with MC/DC header */
    char *stored = NULL;
    int slen = mcdc_encode_value(kptr, klen, vptr, vlen, &stored);
    if (slen < 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC psetex: compression failed");
    }

    bool need_dealloc = false;
    if (!stored) {
        /* value smaller than min or bigger than max to compress
         * store as: [u16 = -1][raw bytes...]
         */
        need_dealloc = true;
        slen = (int)(sizeof(uint16_t) + vlen);
        stored = RedisModule_Alloc(slen);
        if (!stored) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC psetex: memory allocation failed");
        }
        write_u16(stored, -1);
        memcpy(stored + sizeof(uint16_t), vptr, vlen);
    }

    RedisModuleString *encoded =
        RedisModule_CreateString(ctx, stored, slen);

    /* If this buffer was allocated just for this call, free it.
     * If mcdc_encode_value uses TLS, it can keep its own buffer.
     */
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
        RedisModule_Call(ctx, "PSETEX", "v", set_argv, set_argc);

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

    /* MC/DC sampling hook */
    mcdc_sample(kptr, klen, vptr, vlen);

    /* Compress + wrap value with MC/DC header */
    char *stored = NULL;
    int slen = mcdc_encode_value(kptr, klen, vptr, vlen, &stored);
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
        slen = (int)(sizeof(uint16_t) + vlen);
        stored = RedisModule_Alloc(slen);
        if (!stored) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC setnx: memory allocation failed");
        }
        write_u16(stored, -1);
        memcpy(stored + sizeof(uint16_t), vptr, vlen);
    }

    RedisModuleString *encoded =
        RedisModule_CreateString(ctx, stored, slen);

    /* If this buffer was allocated just for this call, free it.
     * If mcdc_encode_value uses TLS, it can keep its own buffer.
     */
    if (need_dealloc) {
        RedisModule_Free(stored);
    }

    /* Call underlying Redis SETNX command */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "SETNX", "ss", argv[1], encoded);

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

    /* Decompress if needed, based on MC/DC header */
    size_t klen;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);

    char *out = NULL;

    size_t outlen = mcdc_decode_value(kptr, klen, rptr, rlen, &out);
    if (outlen < 0 || !out) {
        /* Not MC/DC encoded or decompression failed:
         * fall back to returning the raw Redis value.
         * This is important for keys created by plain SET.
         */
        if (out) free(out);
        RedisModule_ReplyWithStringBuffer(ctx, rptr, rlen);
        return REDISMODULE_OK;
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
        RedisModule_Call(ctx, "GETDEL", "s", argv[1]);

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

    /* Decompress if needed, based on MC/DC header */
    size_t klen;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);

    char *out = NULL;

    size_t outlen = mcdc_decode_value(kptr, klen, rptr, rlen, &out);
    if (outlen < 0 || !out) {
        if (out) free(out);
        RedisModule_ReplyWithStringBuffer(ctx, rptr, rlen);
        return REDISMODULE_OK;
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
        RedisModule_Call(ctx, "GETEX", "v", argv + 1, argc - 1);

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

    /* Decompress / decode if needed, based on MC/DC header */
    size_t klen;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);

    char *out = NULL;
    size_t outlen = mcdc_decode_value(kptr, klen, rptr, rlen, &out);
    if (outlen < 0 || !out) {
        if (out) free(out);
        RedisModule_ReplyWithStringBuffer(ctx, rptr, rlen);
        return REDISMODULE_OK;
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

    /* MC/DC sampling hook */
    mcdc_sample(kptr, klen, vptr, vlen);

    /* Compress + wrap value with MC/DC header */
    char *stored = NULL;
    int slen = mcdc_encode_value(kptr, klen, vptr, vlen, &stored);
    if (slen < 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getset: compression failed");
    }

    bool need_dealloc = false;
    if (!stored) {
        /* Value is too small/too large to compress, store as:
         *   [u16 = -1][raw bytes...]
         */
        need_dealloc = true;
        slen = (int)(sizeof(uint16_t) + vlen);
        stored = RedisModule_Alloc(slen);
        if (!stored) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC getset: memory allocation failed");
        }
        write_u16(stored, -1);
        memcpy(stored + sizeof(uint16_t), vptr, vlen);
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
        RedisModule_Call(ctx, "GETSET", "ss", argv[1], encoded);

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

    /* Decode/decompress previous value.
     * This should gracefully handle:
     *  - MC/DC-encoded values, and
     *  - plain raw values (if key was written by normal SET).
     */

    char *out = NULL;
    size_t outlen = mcdc_decode_value(kptr, klen, rptr, rlen, &out);
    if (outlen < 0 || !out) {
        if (out) free(out);
        RedisModule_ReplyWithStringBuffer(ctx, rptr, rlen);
        return REDISMODULE_OK;
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
/* Registration helper                                                       */
/* ------------------------------------------------------------------------- */

int MCDC_RegisterStringCommands(RedisModuleCtx *ctx)
{
    if (RedisModule_CreateCommand(ctx,
            "mcdc.set",
            MCDC_SetCommand,
            "write",   /* modifies keyspace */
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx,
            "mcdc.setnx",
            MCDC_SetNxCommand,
            "write",   /* modifies keyspace */
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx,
            "mcdc.setex",
            MCDC_SetExCommand,
            "write",   /* modifies keyspace */
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx,
            "mcdc.psetex",
            MCDC_PsetExCommand,
            "write",   /* modifies keyspace */
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx,
            "mcdc.get",
            MCDC_GetCommand,
            "readonly",  /* does not modify keyspace */
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    if (RedisModule_CreateCommand(ctx,
            "mcdc.getdel",
            MCDC_GetDelCommand,
            "write",   /* modifies keyspace */
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx,
            "mcdc.getex",
            MCDC_GetExCommand,
            "write",   /* modifies keyspace */
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx,
            "mcdc.getset",
            MCDC_GetSetCommand,
            "write",   /* GETSET writes the key */
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx,
            "mcdc.cstrlen",
            MCDC_CstrlenCommand,
            "readonly",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    return REDISMODULE_OK;
}
