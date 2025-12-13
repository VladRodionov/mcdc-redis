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
/*
 * mcdc_hash_cmd.c
 *
 * Synchronous Hash command wrappers for MC/DC (Redis/Valkey module layer).
 *
 * Key duties:
 *   - Provide drop-in hash commands that transparently decode MC/DC-compressed
 *     hash values on reads and encode/compress values on writes.
 *   - Preserve Redis-compatible semantics while adding corruption handling
 *     (delete bad fields when possible).
 *
 * Commands implemented/registered here:
 *   - Read wrappers (decode when needed):
 *       mcdc.hget, mcdc.hgetex, mcdc.hmget, mcdc.hvals, mcdc.hgetall,
 *       mcdc.hrandfield (WITHVALUES), mcdc.hgetdel
 *   - Write wrappers (encode when allowed):
 *       mcdc.hset, mcdc.hsetex, mcdc.hsetnx
 *   - Length helpers:
 *       mcdc.hstrlen  (logical length; uses frame content size when compressed)
 *       mcdc.chstrlen (compressed length passthrough via HSTRLEN)
 *
 * Design notes:
 *   - Compression context is the hash key name (stable per hash) and is passed
 *     into the compression layer for both encode/decode.
 *   - Hot path avoids decoding when the value is not MC/DC-compressed.
 *   - On decode failure (corrupt compressed blob):
 *       - where the field name is known, the field is deleted (HDEL)
 *       - on replicas, deletions are skipped to avoid mutating state
 *       - where the field name is unknown (e.g., HVALS), the command returns
 *         NULL for that element without attempting repair.
 *   - Write wrappers respect role / replay state via MCDC_ShouldCompress():
 *       - replica or AOF replay => forward raw values (no compression)
 *       - master normal traffic => encode/compress before calling Redis HSET/HSETEX.
 *
 * Compatibility:
 *   - Uses RedisModule_Call wrappers around native Redis hash commands.
 *   - Uses ZSTD_getFrameContentSize() for logical length when values are stored
 *     compressed, avoiding full decompression for HSTRLEN semantics.
 */
#include "mcdc_hash_cmd.h"

#include "mcdc_compression.h"
#include "mcdc_module_utils.h"
#include "mcdc_role.h"

#include "redismodule.h"
#include "mcdc_capabilities.h"

#include <stdlib.h>
#include <string.h>

/* -------------------------------------------------------------------------
 * Small helpers
 * ------------------------------------------------------------------------- */

/* Check if blob (bptr, blen) is MC/DC-compressed. */
inline static int
mcdc_val_is_compressed(const char *bptr, size_t blen)
{
    if (!bptr || blen <= 6) {
        return 0;
    }
    return mcdc_is_compressed(bptr + sizeof(uint16_t),
                              blen  - sizeof(uint16_t));
}

/* Decode hash value stored under key_name (used as compression context). */
inline static int
mcdc_hash_decode_value(const RedisModuleString *key_name,
                       const char *bptr,
                       size_t blen,
                       char **decoded_out,
                       size_t *decoded_len_out)
{
    *decoded_out     = NULL;
    *decoded_len_out = 0;

    size_t klen = 0;
    const char *kptr = RedisModule_StringPtrLen(key_name, &klen);

    char   *decoded = NULL;
    ssize_t outlen  = mcdc_decode_value(kptr, klen,
                                        bptr, blen,
                                        &decoded);
    if (outlen < 0 || !decoded) {
        if (decoded) {
            free(decoded);
        }
        return REDISMODULE_ERR;
    }

    *decoded_out     = decoded;
    *decoded_len_out = (size_t)outlen;
    return REDISMODULE_OK;
}

/* HDEL helper for corrupted field. We should not log errors here? */
inline static void
mcdc_hash_del_field(RedisModuleCtx *ctx,
                    RedisModuleString *key,
                    RedisModuleString *field)
{

    if (MCDC_IsReplica(ctx)) {
        RedisModule_Log(ctx, "warning",
                        "MC/DC: skip DEL on replica (key not deleted)");
    }
    RedisModule_Call(ctx, "HDEL", "!ss", key, field);
}

/* Compute logical length for HSTRLEN:
 *  - If not compressed: return raw length.
 *  - If compressed: use mcdc_get_uncompressed_length() (no full decode).
 */
inline static ssize_t
mcdc_hash_logical_strlen(const char *bptr, size_t blen)
{
    if (!bptr) {
        return -1;
    }

    if (!mcdc_val_is_compressed(bptr, blen)) {
        return (ssize_t)blen;
    }

    /* val is MC/DC-compressed; ask compression layer for original size
     * without fully decoding. Implement this using ZSTD_getFrameContentSize()
     */
    return ZSTD_getFrameContentSize(bptr + sizeof(uint16_t),
                                        blen  - sizeof(uint16_t));
}

/* -------------------------------------------------------------------------
 * mcdc.hget
 * ------------------------------------------------------------------------- */
/* mcdc.hget key field */
static int
MCDC_HGetCommand(RedisModuleCtx *ctx,
                 RedisModuleString **argv,
                 int argc)
{
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModule_AutoMemory(ctx);

    RedisModuleKey *key =
        RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    if (!key || RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH) {
        return RedisModule_ReplyWithNull(ctx);
    }

    RedisModuleString *field = argv[2];
    RedisModuleString *raw   = NULL;

    if (RedisModule_HashGet(key,
                            REDISMODULE_HASH_NONE,
                            field, &raw,
                            NULL) != REDISMODULE_OK || raw == NULL)
    {
        return RedisModule_ReplyWithNull(ctx);
    }

    size_t blen = 0;
    const char *bptr = RedisModule_StringPtrLen(raw, &blen);
    if (!bptr || blen == 0) {
        return RedisModule_ReplyWithNull(ctx);
    }

    /* Fast path: not compressed → pass through */
    if (!mcdc_val_is_compressed(bptr, blen)) {
        return RedisModule_ReplyWithString(ctx, raw);
    }

    /* Compressed path: decode */
    char   *decoded = NULL;
    size_t  dlen    = 0;
    if (mcdc_hash_decode_value(argv[1], bptr, blen,
                               &decoded, &dlen) != REDISMODULE_OK)
    {
        /* Corrupt compressed blob: delete field and return NULL,
         * same semantics as string GET.
         */
        mcdc_hash_del_field(ctx, argv[1], field);
        return RedisModule_ReplyWithNull(ctx);
    }

    /* Return uncompressed payload as bulk string */
    RedisModule_ReplyWithStringBuffer(ctx, decoded, dlen);

    free(decoded);
    return REDISMODULE_OK;
}

/* ------------------------------------------------------------------------- */
/* mcdc.hgetex key field [EX seconds | PX ms | EXAT ts | PXAT ts_ms | PERSIST]
 *  - HGETEX wrapper with MC/DC decompression and corruption cleanup         */
/* ------------------------------------------------------------------------- */

static int
MCDC_HGetExCommand(RedisModuleCtx *ctx,
                   RedisModuleString **argv,
                   int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hgetex: wrong number of arguments "
                 "(expected: mcdc.hgetex key field [options])");
    }

    /* Underlying: HGETEX key field [options...] */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "HGETEX", "!v", argv + 1, (size_t)(argc - 1));

    if (!reply) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hgetex: underlying HGETEX failed");
    }

    int rtype = RedisModule_CallReplyType(reply);

    if (rtype == REDISMODULE_REPLY_NULL) {
        /* Behave like HGETEX: missing field -> NULL */
        return RedisModule_ReplyWithNull(ctx);
    }

    if (rtype != REDISMODULE_REPLY_STRING) {
        /* Should not happen for HGETEX on hash field, but be defensive */
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hgetex: unexpected reply type from HGETEX");
    }

    size_t blen = 0;
    const char *bptr = RedisModule_CallReplyStringPtr(reply, &blen);
    if (!bptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hgetex: failed to read HGETEX reply");
    }

    /* Not compressed? Just pass it through */
    if (!mcdc_val_is_compressed(bptr, blen)) {
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    /* Decode using hash key as compression context */
    char   *decoded = NULL;
    size_t  dlen    = 0;
    if (mcdc_hash_decode_value(argv[1], bptr, blen,
                               &decoded, &dlen) != REDISMODULE_OK)
    {
        /* Corrupt compressed blob:
         *  - delete this field
         *  - return NULL (like HGETEX on missing field)
         */
        mcdc_hash_del_field(ctx, argv[1], argv[2]);
        return RedisModule_ReplyWithNull(ctx);
    }

    RedisModule_ReplyWithStringBuffer(ctx, decoded, dlen);
    free(decoded);
    return REDISMODULE_OK;
}

/* ------------------------------------------------------------------------- */
/* mcdc.hmget key field [field ...]                                          */
/*  - HMGET wrapper with per-field decompression                             */
/* ------------------------------------------------------------------------- */

static int
MCDC_HMGetCommand(RedisModuleCtx *ctx,
                  RedisModuleString **argv,
                  int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hmget: wrong number of arguments (expected: mcdc.hmget key field [field ...])");
    }

    int nfields = argc - 2;

    /* HMGET key field1 field2 ... */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "HMGET", "v", argv + 1, (size_t)(argc - 1));

    if (!reply) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hmget: underlying HMGET failed");
    }

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hmget: unexpected reply type from HMGET");
    }

    size_t arrlen = RedisModule_CallReplyLength(reply);
    if (arrlen != (size_t)nfields) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hmget: unexpected array length from HMGET");
    }

    RedisModule_ReplyWithArray(ctx, nfields);

    for (int i = 0; i < nfields; i++) {
        RedisModuleCallReply *elem =
            RedisModule_CallReplyArrayElement(reply, (size_t)i);

        int etype = RedisModule_CallReplyType(elem);
        if (etype == REDISMODULE_REPLY_NULL) {
            RedisModule_ReplyWithNull(ctx);
            continue;
        }

        if (etype != REDISMODULE_REPLY_STRING) {
            /* Just forward weird replies as-is */
            RedisModule_ReplyWithCallReply(ctx, elem);
            continue;
        }

        size_t blen = 0;
        const char *bptr = RedisModule_CallReplyStringPtr(elem, &blen);
        if (!bptr || blen == 0 || !mcdc_val_is_compressed(bptr, blen)) {
            /* Not compressed – pass through */
            RedisModule_ReplyWithStringBuffer(ctx, bptr, blen);
            continue;
        }

        /* Decode using hash key as context; field name is argv[2 + i] */
        char   *decoded = NULL;
        size_t  dlen    = 0;

        if (mcdc_hash_decode_value(argv[1], bptr, blen,
                                   &decoded, &dlen) != REDISMODULE_OK)
        {
            /* Corrupt compressed blob: delete this field, reply NULL */
            mcdc_hash_del_field(ctx, argv[1], argv[2 + i]);
            RedisModule_ReplyWithNull(ctx);
            continue;
        }

        RedisModule_ReplyWithStringBuffer(ctx, decoded, dlen);
        free(decoded);
    }

    return REDISMODULE_OK;
}
/* ------------------------------------------------------------------------- */
/* mcdc.hset key field value [field value ...]                               */
/*  - Mirrors Redis HSET / HMSET but encodes values with MC/DC               */
/* ------------------------------------------------------------------------- */

static int
MCDC_HSetCommand(RedisModuleCtx *ctx,
                 RedisModuleString **argv,
                 int argc)
{
    RedisModule_AutoMemory(ctx);

    /* HSET key field value [field value ...]
     * argc must be: 1 (cmd) + 1 (key) + 2*N  => argc >= 4 and (argc-2) even
     */
    if (argc < 4 || ((argc - 2) % 2) != 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hset: wrong number of arguments "
                 "(expected: mcdc.hset key field value [field value ...])");
    }

    /* Replica / AOF replay → no compression, just forward. */
    if (!MCDC_ShouldCompress(ctx)) {
        RedisModuleCallReply *reply =
            RedisModule_Call(ctx, "HSET", "v", argv + 1, (size_t)(argc - 1));
        if (!reply) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC hset: underlying HSET failed");
        }
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    /* Compression context: hash key */
    size_t klen = 0;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);
    if (!kptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hset: failed to read key");
    }

    int npairs    = (argc - 2) / 2;
    int inner_argc = argc - 1;        /* everything but module command name */

    /* Build argv for underlying HSET:
     *   HSET key field1 enc_v1 field2 enc_v2 ...
     */
    RedisModuleString **hset_argv =
        RedisModule_PoolAlloc(ctx, sizeof(RedisModuleString *) * inner_argc);

    /* key */
    hset_argv[0] = argv[1];

    for (int i = 0; i < npairs; i++) {
        int field_index = 2 + 2 * i;
        int value_index = field_index + 1;

        RedisModuleString *field = argv[field_index];
        RedisModuleString *value = argv[value_index];

        /* field passes through unchanged */
        hset_argv[1 + 2 * i] = field;

        /* value is compressed */
        size_t vlen = 0;
        const char *vptr = RedisModule_StringPtrLen(value, &vlen);
        if (!vptr) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC hset: failed to read value");
        }

        char *stored = NULL;
        ssize_t   slen   = mcdc_encode_value(kptr, klen, vptr, vlen, &stored);
        if (slen < 0) {
            RedisModule_Log(ctx, "warning",
                "<mcdc> hash compression FAILED key='%.*s' value-len=%zu rc=%zd",
                (int)klen, kptr, vlen, slen);
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC hset: compression failed");
        }

        bool need_dealloc = false;
        if (!stored) {
            /* Too small / too large to compress: store raw bytes. */
            need_dealloc = true;
            slen = (int)vlen;
            stored = RedisModule_Alloc(slen);
            if (!stored) {
                return RedisModule_ReplyWithError(
                    ctx, "ERR MCDC hset: memory allocation failed");
            }
            memcpy(stored, vptr, vlen);
        }

        RedisModuleString *encoded =
            RedisModule_CreateString(ctx, stored, (size_t)slen);

        if (need_dealloc) {
            RedisModule_Free(stored);
        }

        hset_argv[1 + 2 * i + 1] = encoded;
    }

    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "HSET", "!v", hset_argv, (size_t)inner_argc);

    if (!reply) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hset: underlying HSET failed");
    }

    return RedisModule_ReplyWithCallReply(ctx, reply);
}
/* ------------------------------------------------------------------------- */
/* mcdc.hsetex key [options] FIELDS numfields field value [field value ...]  */
/*  - Mirrors Redis HSETEX, but encodes values with MC/DC                    */
/* ------------------------------------------------------------------------- */

static int
MCDC_HSetExCommand(RedisModuleCtx *ctx,
                   RedisModuleString **argv,
                   int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc < 5) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hsetex: wrong number of arguments "
                 "(expected: mcdc.hsetex key [options] FIELDS numfields field value [field value ...])");
    }

    /* Replica / AOF replay → do not compress, just forward */
    if (!MCDC_ShouldCompress(ctx)) {
        RedisModuleCallReply *reply =
            RedisModule_Call(ctx, "HSETEX", "v", argv + 1, (size_t)(argc - 1));
        if (!reply) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC hsetex: underlying HSETEX failed");
        }
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    /* Locate the FIELDS token (case-insensitive). */
    int fields_idx = -1;
    for (int i = 2; i < argc; i++) {
        size_t len = 0;
        const char *s = RedisModule_StringPtrLen(argv[i], &len);
        if (len == 6 &&
            (s[0] == 'F' || s[0] == 'f') &&
            (s[1] == 'I' || s[1] == 'i') &&
            (s[2] == 'E' || s[2] == 'e') &&
            (s[3] == 'L' || s[3] == 'l') &&
            (s[4] == 'D' || s[4] == 'd') &&
            (s[5] == 'S' || s[5] == 's'))
        {
            fields_idx = i;
            break;
        }
    }

    if (fields_idx < 0 || fields_idx + 2 >= argc) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hsetex: malformed FIELDS section");
    }

    /* Parse numfields */
    long long numfields = 0;
    if (RedisModule_StringToLongLong(argv[fields_idx + 1], &numfields) != REDISMODULE_OK ||
        numfields <= 0)
    {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hsetex: invalid numfields");
    }

    int first_field_idx = fields_idx + 2;
    int expected = first_field_idx + 2 * (int)numfields;
    if (expected != argc) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hsetex: wrong number of field/value arguments");
    }

    /* Hash key compression context */
    size_t klen = 0;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);

    /* Build argv for underlying HSETEX; start as a shallow copy of argv[1..] */
    int inner_argc = argc - 1;
    RedisModuleString **hsetex_argv =
        RedisModule_PoolAlloc(ctx, sizeof(RedisModuleString *) * inner_argc);

    for (int i = 1; i < argc; i++) {
        hsetex_argv[i - 1] = argv[i];
    }

    /* Encode each value in-place in hsetex_argv */
    for (int j = 0; j < numfields; j++) {
        int field_idx = first_field_idx + 2 * j;
        int value_idx = field_idx + 1;

        size_t vlen = 0;
        const char *vptr = RedisModule_StringPtrLen(argv[value_idx], &vlen);
        if (!vptr) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC hsetex: failed to read value");
        }

        char *stored = NULL;
        ssize_t   slen   = mcdc_encode_value(kptr, klen, vptr, vlen, &stored);
        if (slen < 0) {
            RedisModule_Log(ctx, "warning",
                "<mcdc> hash compression FAILED key='%.*s' value-len=%zu rc=%zd",
                (int)klen, kptr, vlen, slen);
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC hsetex: compression failed");
        }

        bool need_dealloc = false;
        if (!stored) {
            /* Too small / too large to compress: store raw bytes. */
            need_dealloc = true;
            slen = (int)vlen;
            stored = RedisModule_Alloc(slen);
            if (!stored) {
                return RedisModule_ReplyWithError(
                    ctx, "ERR MCDC hset: memory allocation failed");
            }
            memcpy(stored, vptr, vlen);
        }

        RedisModuleString *encoded =
            RedisModule_CreateString(ctx, stored, (size_t)slen);

        if (need_dealloc) {
            RedisModule_Free(stored);
        }

        hsetex_argv[value_idx - 1] = encoded;
    }

    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "HSETEX", "!v", hsetex_argv, (size_t)inner_argc);

    if (!reply) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hsetex: underlying HSETEX failed");
    }

    return RedisModule_ReplyWithCallReply(ctx, reply);
}

/* -------------------------------------------------------------------------
 * mcdc.hsetnx key field value
 * ------------------------------------------------------------------------- */
static int
MCDC_HSetNXCommand(RedisModuleCtx *ctx,
                   RedisModuleString **argv,
                   int argc)
{
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModule_AutoMemory(ctx);

    /* Snapshot value */
    size_t vlen = 0;
    const char *vptr = RedisModule_StringPtrLen(argv[3], &vlen);

    size_t klen = 0;
    (void)RedisModule_StringPtrLen(argv[1], &klen);

    char   *encoded = NULL;
    ssize_t clen    = mcdc_encode_value(
                          RedisModule_StringPtrLen(argv[1], &klen),
                          klen,
                          vptr,
                          vlen,
                          &encoded);

    RedisModuleString *store_val = NULL;

    if (clen > 0 && encoded) {
        store_val = RedisModule_CreateString(ctx, encoded, (size_t)clen);
        free(encoded);
    } else {
        store_val = argv[3];
    }

    RedisModuleCallReply *r =
        RedisModule_Call(ctx, "HSETNX", "!sss", argv[1], argv[2], store_val);
    if (!r || RedisModule_CallReplyType(r) != REDISMODULE_REPLY_INTEGER) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hsetnx: underlying HSETNX failed");
    }

    long long added = RedisModule_CallReplyInteger(r);
    return RedisModule_ReplyWithLongLong(ctx, added);
}

/* ------------------------------------------------------------------------- */
/* mcdc.chstrlen key  - compressed value length                              */
/* ------------------------------------------------------------------------- */

int MCDC_CHstrlenCommand(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC chstrlen: wrong number of arguments (expected: mcdc.cstrlen key field)");
    }
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "HSTRLEN", "ss", argv[1], argv[2]);

    if (reply == NULL) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC chstrlen: underlying HSTRLEN failed");
    }
    return RedisModule_ReplyWithCallReply(ctx, reply);
}
/* -------------------------------------------------------------------------
 * mcdc.hstrlen
 * ------------------------------------------------------------------------- */

static int
MCDC_HStrlenCommand(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc)
{
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModule_AutoMemory(ctx);

    RedisModuleKey *key =
        RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    if (!key || RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }

    RedisModuleString *field = argv[2];
    RedisModuleString *raw   = NULL;

    if (RedisModule_HashGet(key,
                            REDISMODULE_HASH_NONE,
                            field, &raw,
                            NULL) != REDISMODULE_OK || raw == NULL)
    {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }

    size_t blen = 0;
    const char *bptr = RedisModule_StringPtrLen(raw, &blen);
    if (!bptr || blen == 0) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }

    ssize_t logical = mcdc_hash_logical_strlen(bptr, blen);
    if (logical < 0) {
        /* Treat invalid/compressed-but-bad as 0;
         */
        logical = 0;
    }

    return RedisModule_ReplyWithLongLong(ctx, (long long)logical);
}

/* -------------------------------------------------------------------------
 * mcdc.hvals / mcdc.hgetall
 *  - HVALS: return values only, decoded when compressed.
 *  - HGETALL: return [field, value, field, value, ...], decoding values.
 *    For corrupted compressed values, we return NULL
 * ------------------------------------------------------------------------- */

static int
MCDC_HValsCommand(RedisModuleCtx *ctx,
                  RedisModuleString **argv,
                  int argc)
{
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModule_AutoMemory(ctx);

    /* Use HVALS directly; array of values */
    RedisModuleCallReply *r =
        RedisModule_Call(ctx, "HVALS", "s", argv[1]);
    if (!r || RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ARRAY) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hvals: underlying HVALS failed");
    }

    size_t len = RedisModule_CallReplyLength(r);
    RedisModule_ReplyWithArray(ctx, len);

    for (size_t i = 0; i < len; i++) {
        RedisModuleCallReply *elem =
            RedisModule_CallReplyArrayElement(r, i);

        if (RedisModule_CallReplyType(elem) != REDISMODULE_REPLY_STRING) {
            RedisModule_ReplyWithNull(ctx);
            continue;
        }

        size_t blen = 0;
        const char *bptr = RedisModule_CallReplyStringPtr(elem, &blen);
        if (!bptr || blen == 0) {
            RedisModule_ReplyWithNull(ctx);
            continue;
        }

        if (!mcdc_val_is_compressed(bptr, blen)) {
            RedisModule_ReplyWithStringBuffer(ctx, bptr, blen);
            continue;
        }

        char   *decoded = NULL;
        size_t  dlen    = 0;
        if (mcdc_hash_decode_value(argv[1], bptr, blen,
                                   &decoded, &dlen) != REDISMODULE_OK)
        {
            /* Corrupt compressed blob: reply NULL, we do NOT delete
             * because we don't know the field name here.
             */
            RedisModule_ReplyWithNull(ctx);
            continue;
        }

        RedisModule_ReplyWithStringBuffer(ctx, decoded, dlen);
        free(decoded);
    }

    return REDISMODULE_OK;
}

static int
MCDC_HGetAllCommand(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc)
{
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModule_AutoMemory(ctx);

    /* HGETALL returns [field, value, field, value, ...] */
    RedisModuleCallReply *r =
        RedisModule_Call(ctx, "HGETALL", "s", argv[1]);
    if (!r || RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ARRAY) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hgetall: underlying HGETALL failed");
    }

    size_t len = RedisModule_CallReplyLength(r);
    if (len % 2 != 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hgetall: unexpected array length");
    }

    RedisModule_ReplyWithArray(ctx, len);

    for (size_t i = 0; i < len; i += 2) {
        RedisModuleCallReply *field_rep =
            RedisModule_CallReplyArrayElement(r, i);
        RedisModuleCallReply *val_rep =
            RedisModule_CallReplyArrayElement(r, i + 1);

        /* Field: always forward as-is */
        size_t flen = 0;
        const char *fptr = RedisModule_CallReplyStringPtr(field_rep, &flen);
        RedisModule_ReplyWithStringBuffer(ctx, fptr, flen);

        if (RedisModule_CallReplyType(val_rep) != REDISMODULE_REPLY_STRING) {
            RedisModule_ReplyWithNull(ctx);
            continue;
        }

        size_t blen = 0;
        const char *bptr = RedisModule_CallReplyStringPtr(val_rep, &blen);
        if (!bptr || blen == 0) {
            RedisModule_ReplyWithNull(ctx);
            continue;
        }

        if (!mcdc_val_is_compressed(bptr, blen)) {
            RedisModule_ReplyWithStringBuffer(ctx, bptr, blen);
            continue;
        }

        char   *decoded = NULL;
        size_t  dlen    = 0;
        if (mcdc_hash_decode_value(argv[1], bptr, blen,
                                   &decoded, &dlen) != REDISMODULE_OK)
        {
            /* Corrupt compressed blob: delete field, reply NULL */
            /* Need field name as RedisModuleString to delete on corruption. */
            RedisModuleString *field_rs =
                RedisModule_CreateString(ctx, fptr, flen);
            mcdc_hash_del_field(ctx, argv[1], field_rs);
            RedisModule_ReplyWithNull(ctx);
            continue;
        }

        RedisModule_ReplyWithStringBuffer(ctx, decoded, dlen);
        free(decoded);
    }

    return REDISMODULE_OK;
}

/* ------------------------------------------------------------------------- */
/* mcdc.hrandfield key [count] [WITHVALUES]                                  */
/*  - Pass through fields; decode values when WITHVALUES is used.            */
/* ------------------------------------------------------------------------- */

static int
MCDC_HRandFieldCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hrandfield: wrong number of arguments (expected: mcdc.hrandfield key [count] [WITHVALUES])");
    }

    int withvalues = 0;
    for (int i = 2; i < argc; i++) {
        size_t optlen = 0;
        const char *opt = RedisModule_StringPtrLen(argv[i], &optlen);
        if (optlen == 10 &&
            (opt[0] == 'W' || opt[0] == 'w') &&
            (opt[1] == 'I' || opt[1] == 'i') &&
            (opt[2] == 'T' || opt[2] == 't') &&
            (opt[3] == 'H' || opt[3] == 'h') &&
            (opt[4] == 'V' || opt[4] == 'v') &&
            (opt[5] == 'A' || opt[5] == 'a') &&
            (opt[6] == 'L' || opt[6] == 'l') &&
            (opt[7] == 'U' || opt[7] == 'u') &&
            (opt[8] == 'E' || opt[8] == 'e') &&
            (opt[9] == 'S' || opt[9] == 's'))
        {
            withvalues = 1;
            break;
        }
    }

    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "HRANDFIELD", "v", argv + 1, (size_t)(argc - 1));

    if (!reply) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hrandfield: underlying HRANDFIELD failed");
    }

    int rtype = RedisModule_CallReplyType(reply);

    if (!withvalues) {
        /* No values to decode; just pass through. */
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    if (rtype == REDISMODULE_REPLY_NULL) {
        return RedisModule_ReplyWithNull(ctx);
    }

    if (rtype != REDISMODULE_REPLY_ARRAY) {
        /* WITHVALUES must return array of [field, value, ...] */
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hrandfield: unexpected reply type from HRANDFIELD WITHVALUES");
    }

    size_t len = RedisModule_CallReplyLength(reply);
    if (len % 2 != 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hrandfield: unexpected array length");
    }

    RedisModule_ReplyWithArray(ctx, len);

    for (size_t i = 0; i < len; i += 2) {
        RedisModuleCallReply *field_rep =
            RedisModule_CallReplyArrayElement(reply, i);
        RedisModuleCallReply *val_rep =
            RedisModule_CallReplyArrayElement(reply, i + 1);

        /* Field: forward as-is */
        size_t flen = 0;
        const char *fptr = RedisModule_CallReplyStringPtr(field_rep, &flen);
        RedisModule_ReplyWithStringBuffer(ctx, fptr, flen);

        if (RedisModule_CallReplyType(val_rep) != REDISMODULE_REPLY_STRING) {
            RedisModule_ReplyWithNull(ctx);
            continue;
        }

        size_t blen = 0;
        const char *bptr = RedisModule_CallReplyStringPtr(val_rep, &blen);
        if (!bptr || blen == 0 || !mcdc_val_is_compressed(bptr, blen)) {
            RedisModule_ReplyWithStringBuffer(ctx, bptr, blen);
            continue;
        }

        RedisModuleString *field_rs =
            RedisModule_CreateString(ctx, fptr, flen);

        char   *decoded = NULL;
        size_t  dlen    = 0;
        if (mcdc_hash_decode_value(argv[1], bptr, blen,
                                   &decoded, &dlen) != REDISMODULE_OK)
        {
            /* Corrupt value: delete field and reply NULL. */
            mcdc_hash_del_field(ctx, argv[1], field_rs);
            RedisModule_ReplyWithNull(ctx);
            continue;
        }

        RedisModule_ReplyWithStringBuffer(ctx, decoded, dlen);
        free(decoded);
    }

    return REDISMODULE_OK;
}

/* ------------------------------------------------------------------------- */
/* mcdc.hgetdel key field                                                    */
/*  - Underlying HGETDEL deletes field; we decode and return value.          */
/* ------------------------------------------------------------------------- */

static int
MCDC_HGetDelCommand(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hgetdel: wrong number of arguments (expected: mcdc.hgetdel key field)");
    }

    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "HGETDEL", "!ss", argv[1], argv[2]);

    if (!reply) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hgetdel: underlying HGETDEL failed");
    }

    int rtype = RedisModule_CallReplyType(reply);

    if (rtype == REDISMODULE_REPLY_NULL) {
        return RedisModule_ReplyWithNull(ctx);
    }

    if (rtype != REDISMODULE_REPLY_STRING) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC hgetdel: unexpected reply type from HGETDEL");
    }

    size_t blen = 0;
    const char *bptr = RedisModule_CallReplyStringPtr(reply, &blen);
    if (!bptr || blen == 0) {
        return RedisModule_ReplyWithNull(ctx);
    }

    if (!mcdc_val_is_compressed(bptr, blen)) {
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }

    char   *decoded = NULL;
    size_t  dlen    = 0;
    if (mcdc_hash_decode_value(argv[1], bptr, blen,
                               &decoded, &dlen) != REDISMODULE_OK)
    {
        /* Field is already deleted by HGETDEL; on corrupt return NULL. */
        return RedisModule_ReplyWithNull(ctx);
    }

    RedisModule_ReplyWithStringBuffer(ctx, decoded, dlen);
    free(decoded);
    return REDISMODULE_OK;
}
/* -------------------------------------------------------------------------
 * Registration
 * ------------------------------------------------------------------------- */

int
MCDC_RegisterHashCommands(RedisModuleCtx *ctx)
{
    if (RedisModule_CreateCommand(ctx,
            "mcdc.hget",
            MCDC_HGetCommand,
            "readonly",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
 
    if (RedisModule_CreateCommand(ctx,
                "mcdc.hmget",
                MCDC_HMGetCommand,
                "readonly",
                1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx,
            "mcdc.hset",
            MCDC_HSetCommand,
            "write deny-oom",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if(MCDC_HasHSetEx()){
        if (RedisModule_CreateCommand(ctx,
                                      "mcdc.hsetex",
                                      MCDC_HSetExCommand,
                                      "write deny-oom",
                                      1, 1, 1) == REDISMODULE_ERR)
        {
            return REDISMODULE_ERR;
        }
        
        if (RedisModule_CreateCommand(ctx,
                                      "mcdc.hgetex",
                                      MCDC_HGetExCommand,
                                      "write",   /* changes field TTL */
                                      1, 1, 1) == REDISMODULE_ERR)
        {
            return REDISMODULE_ERR;
        }
    }
    
    if (RedisModule_CreateCommand(ctx,
            "mcdc.hsetnx",
            MCDC_HSetNXCommand,
            "write deny-oom",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx,
            "mcdc.hvals",
            MCDC_HValsCommand,
            "readonly",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx,
            "mcdc.hgetall",
            MCDC_HGetAllCommand,
            "readonly",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx,
            "mcdc.hstrlen",
            MCDC_HStrlenCommand,
            "readonly",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
   
    if (RedisModule_CreateCommand(ctx,
            "mcdc.chstrlen",
            MCDC_CHstrlenCommand,
            "readonly",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx,
            "mcdc.hgetdel",
            MCDC_HGetDelCommand,
            "write",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx,
            "mcdc.hrandfield",
            MCDC_HRandFieldCommand,
            "readonly",
            1, 1, 1) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
