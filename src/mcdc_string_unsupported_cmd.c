#include "mcdc_string_unsupported_cmd.h"
#include "mcdc_compression.h"   /* mcdc_is_compressed, mcdc_decode_value */
#include "redismodule.h"
#include "mcdc_module_utils.h"
#include <string.h>
#include <stdlib.h>

/* -------------------------------------------------------------------------
 * Internal helper: downgrade a single key if it's an MC/DC-compressed value.
 *
 * Steps:
 *   1. GET key
 *   2. If value exists and mcdc_is_compressed(...) is true:
 *        - decode via mcdc_decode_value(...)
 *        - SET key <raw> KEEPTTL   (replicated via "!")
 *   3. Otherwise, do nothing.
 *
 * Returns:
 *   REDISMODULE_OK on success (or no-op),
 *   REDISMODULE_ERR on hard error (GET / decode / SET failure).
 * ------------------------------------------------------------------------- */
static int
MCDC_DowngradeKeyIfCompressed(RedisModuleCtx *ctx, RedisModuleString *key)
{
    RedisModule_AutoMemory(ctx);

    /* Open key for read + potential write */
    RedisModuleKey *k =
        RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
    if (k == NULL) {
        return REDISMODULE_OK;
    }

    int keytype = RedisModule_KeyType(k);
    if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
        /* Key does not exist – nothing to downgrade */
        return REDISMODULE_OK;
    }

    if (keytype != REDISMODULE_KEYTYPE_STRING) {
        /* Unexpected type – be conservative and error out */
        return REDISMODULE_ERR;
    }

    /* Fetch current value via DMA */
    size_t vlen = 0;
    char *vptr = RedisModule_StringDMA(k, &vlen, REDISMODULE_READ);
    if (!vptr) {
        return REDISMODULE_ERR;
    }

    if (vlen <= 6) {
        return REDISMODULE_OK;
    }

    const char *frame = vptr + sizeof(uint16_t);
    size_t frame_len = vlen - sizeof(uint16_t);
    
    const unsigned char zstd_magic[4] = { 0x28, 0xB5, 0x2F, 0xFD };
    if (memcmp(frame, zstd_magic, 4) != 0) {
        return REDISMODULE_OK;
    }
    /* Probe for MC/DC compression (on the Zstd frame, not including dict_id) */
    if (!mcdc_is_compressed(frame, frame_len)) {
        return REDISMODULE_OK;
    }
    /* Decode MC/DC value */
    size_t klen = 0;
    const char *kptr = RedisModule_StringPtrLen(key, &klen);

    char *decoded = NULL;
    ssize_t outlen = mcdc_decode_value(kptr, klen, vptr, vlen, &decoded);

    if (outlen < 0 || !decoded) {
        if (decoded) free(decoded);
        MCDC_DelKey(ctx, key);
        return REDISMODULE_ERR;
    }
    /* Preserve TTL before overwriting */
    mstime_t ttl = RedisModule_GetExpire(k);  /* -1: no expire, -2: no key */
    RedisModule_CloseKey(k);
    k = RedisModule_OpenKey(ctx, key, REDISMODULE_WRITE);
    if (k == NULL) {
        free(decoded);
        return REDISMODULE_ERR;
    }

    /* Create a Redis string from the decoded data */
    RedisModuleString *raw =
        RedisModule_CreateString(ctx, decoded, (size_t)outlen);
    free(decoded);

    /* Overwrite key with raw value */
    if (RedisModule_StringSet(k, raw) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    /* Restore TTL if it existed */
    if (ttl >= 0) {
        RedisModule_SetExpire(k, ttl);
    }

    return REDISMODULE_OK;
}

/* Helper: downgrade a single key and, on error, reply with a generic error. */
static int
MCDC_EnsureKeyDowngradedOrError(RedisModuleCtx *ctx, RedisModuleString *key)
{
    int rc = MCDC_DowngradeKeyIfCompressed(ctx, key);
    if (rc != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC: failed to downgrade compressed value");
    }
    return REDISMODULE_OK;
}

/* Helper: downgrade multiple keys (argv[start_idx..argc-1]) */
static int
MCDC_EnsureKeysDowngradedOrError(RedisModuleCtx *ctx,
                                 RedisModuleString **argv,
                                 int start_idx,
                                 int argc)
{
    for (int i = start_idx; i < argc; ++i) {
        if (MCDC_EnsureKeyDowngradedOrError(ctx, argv[i]) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }
    }
    return REDISMODULE_OK;
}

/* =========================================================================
 * Unsupported String commands – mcdc.<cmd>
 * ========================================================================= */

/* mcdc.append key value */
int
MCDC_AppendCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_ReplyWithError(
            ctx, "ERR wrong number of arguments for 'mcdc.append'");
    }

    if (MCDC_EnsureKeyDowngradedOrError(ctx, argv[1]) != REDISMODULE_OK) {
        return REDISMODULE_OK;
    }

    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "APPEND", "!ss", argv[1], argv[2]);

    if (!reply) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC append: underlying APPEND failed");
    }

    return RedisModule_ReplyWithCallReply(ctx, reply);
}

/* mcdc.getrange key start end */
int
MCDC_GetRangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 4) {
        return RedisModule_ReplyWithError(
            ctx, "ERR wrong number of arguments for 'mcdc.getrange'");
    }

    /* Downgrade if compressed, then delegate (read-only after downgrade) */
    if (MCDC_EnsureKeyDowngradedOrError(ctx, argv[1]) != REDISMODULE_OK) {
        return REDISMODULE_OK;
    }

    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "GETRANGE", "sss", argv[1], argv[2], argv[3]);

    if (!reply) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC getrange: underlying GETRANGE failed");
    }

    return RedisModule_ReplyWithCallReply(ctx, reply);
}

/* mcdc.setrange key offset value */
int
MCDC_SetRangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 4) {
        return RedisModule_ReplyWithError(
            ctx, "ERR wrong number of arguments for 'mcdc.setrange'");
    }

    if (MCDC_EnsureKeyDowngradedOrError(ctx, argv[1]) != REDISMODULE_OK) {
        return REDISMODULE_OK;
    }

    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "SETRANGE", "!sss", argv[1], argv[2], argv[3]);

    if (!reply) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC setrange: underlying SETRANGE failed");
    }

    return RedisModule_ReplyWithCallReply(ctx, reply);
}


/* =========================================================================
 * Registration
 * ========================================================================= */

int
MCDC_RegisterUnsupportedStringCommands(RedisModuleCtx *ctx)
{
 
    if (RedisModule_CreateCommand(ctx, "mcdc.append",
                                  MCDC_AppendCommand,
                                  "write", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mcdc.getrange",
                                  MCDC_GetRangeCommand,
                                  "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mcdc.setrange",
                                  MCDC_SetRangeCommand,
                                  "write", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
