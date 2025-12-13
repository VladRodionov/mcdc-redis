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
 * mcdc_env_redis.c
 *
 * Redis/Valkey environment integration for MC/DC.
 *
 * Key duties:
 *   - Provide Redis-backed implementations of the MC/DC “environment” hooks:
 *       * Dictionary publisher: persists dictionaries + manifests into Redis keys.
 *       * Dictionary ID provider: allocates/releases dict IDs via a bitmap stored in Redis.
 *   - Create and manage a shared ThreadSafeContext (g_ts_ctx) so background threads
 *     (trainer / GC / async loaders) can safely perform Redis calls and logging.
 *   - Initialize node role (master/replica) at module load and expose entry points
 *     for role-related wiring.
 *
 * Storage layout (Redis keys):
 *   - mcdc:dict:<id>     (hash) fields: file_name, data   -> dictionary blob
 *   - mcdc:dict:<id>:mf  (hash) fields: file_name, data   -> manifest blob
 *   - mcdc:dict:ids      (string bitmap)                 -> dict ID allocation map
 *
 * Notes:
 *   - All Redis calls from non-main threads must be wrapped with
 *     RedisModule_ThreadSafeContextLock/Unlock.
 *   - ID allocation uses BITPOS(0) + SETBIT(1); release clears the bit and deletes
 *     the associated dict/manifest keys.
 *   - Namespace filtering: MC/DC only intercepts keys under the "mcdc:dict:" prefix;
 *     these env keys are intentionally in that namespace.
 */
#include "redismodule.h"
#include "mcdc_env.h"
#include "mcdc_log.h"
#include "mcdc_role.h"
#include <stdint.h>
#include <stdio.h>
#include "string.h"

static RedisModuleCtx *g_ts_ctx = NULL; /* shared thread-safe ctx for logging + env */


#include "mcdc_log.h"
#include "redismodule.h"

extern RedisModuleCtx *g_ts_ctx;

void MCDC_LogReply(RedisModuleCtx *ctx,
                   const char *msg,
                   RedisModuleCallReply *reply)
{
    if (!reply) {
        RedisModule_Log(ctx, "warning", "%s: <NULL reply>", msg);
        return;
    }

    int type = RedisModule_CallReplyType(reply);

    switch (type) {
    case REDISMODULE_REPLY_STRING: {
        size_t slen;
        const char *sptr = RedisModule_CallReplyStringPtr(reply, &slen);
        RedisModule_Log(ctx, "notice",
                        "%s: STRING (%.*s)", msg, (int)slen, sptr);
        break;
    }
    case REDISMODULE_REPLY_INTEGER: {
        long long v = RedisModule_CallReplyInteger(reply);
        RedisModule_Log(ctx, "notice",
                        "%s: INTEGER (%lld)", msg, v);
        break;
    }
    case REDISMODULE_REPLY_ERROR: {
        size_t elen;
        const char *eptr = RedisModule_CallReplyStringPtr(reply, &elen);
        RedisModule_Log(ctx, "warning",
                        "%s: ERROR (%.*s)", msg, (int)elen, eptr);
        break;
    }
    case REDISMODULE_REPLY_ARRAY: {
        size_t alen = RedisModule_CallReplyLength(reply);
        RedisModule_Log(ctx, "notice",
                        "%s: ARRAY (len=%zu)", msg, alen);
        break;
    }
    case REDISMODULE_REPLY_NULL:
        RedisModule_Log(ctx, "notice", "%s: NULL reply", msg);
        break;

    default:
        RedisModule_Log(ctx, "notice",
                        "%s: UNKNOWN reply type=%d", msg, type);
        break;
    }
}
/*
 * Publish dictionary & manifest via Redis hashes:
 *
 *   HSET mcdc:dict:<id>:mf file_name <file_name> data <manifest_blob>
 *   HSET mcdc:dict:<id>    file_name <file_name> data <dict_blob>
 */

static int
MCDC_RedisPublishDict(uint16_t dict_id,
                      const char *file_name,
                      const void *dict_buf,     size_t dict_len,
                      const void *manifest_buf, size_t manifest_len,
                      void *user_data)
{
    (void)user_data;

    mcdc_log(MCDC_LOG_INFO,
             "RedisPublishDict: begin publish id=%u file_name='%s'",
             dict_id, file_name);

    if (!g_ts_ctx) {
        mcdc_log(MCDC_LOG_ERROR, "RedisPublishDict: g_ts_ctx is NULL");
        return -1;
    }

    RedisModuleString *mf_key = NULL;
    RedisModuleString *mf_data = NULL;
    RedisModuleString *dict_key = NULL;
    RedisModuleString *field_name = NULL;
    RedisModuleString *field_data = NULL;
    RedisModuleString *val_name = NULL;
    RedisModuleString *val_data = NULL;
    RedisModuleCallReply *r1 = NULL;
    RedisModuleCallReply *r2 = NULL;
    int rc = -1;

    RedisModule_ThreadSafeContextLock(g_ts_ctx);

    char keybuf[64];

    /* Common fields */
    field_name = RedisModule_CreateString(g_ts_ctx, "file_name", 9);
    field_data = RedisModule_CreateString(g_ts_ctx, "data", 4);
    val_name   = RedisModule_CreateString(g_ts_ctx, file_name, strlen(file_name));

    if (!field_name || !field_data || !val_name) {
        mcdc_log(MCDC_LOG_ERROR, "RedisPublishDict: field alloc failed");
        goto cleanup;
    }

    /* ---------------- Manifest hash ---------------- */
    snprintf(keybuf, sizeof(keybuf), "mcdc:dict:%u:mf", dict_id);
    mf_key  = RedisModule_CreateString(g_ts_ctx, keybuf, strlen(keybuf));
    mf_data = RedisModule_CreateString(g_ts_ctx, manifest_buf, manifest_len);

    r1 = RedisModule_Call(g_ts_ctx, "HSET", "!sssss",
                          mf_key,
                          field_name, val_name,
                          field_data, mf_data);

    MCDC_LogReply(g_ts_ctx, "RedisPublishDict: HSET manifest: %s", r1);

    if (!r1) goto cleanup;

    /* ---------------- Dictionary hash ---------------- */
    snprintf(keybuf, sizeof(keybuf), "mcdc:dict:%u", dict_id);
    dict_key = RedisModule_CreateString(g_ts_ctx, keybuf, strlen(keybuf));
    val_data = RedisModule_CreateString(g_ts_ctx, dict_buf, dict_len);

    r2 = RedisModule_Call(g_ts_ctx, "HSET", "!sssss",
                          dict_key,
                          field_name, val_name,
                          field_data, val_data);

    MCDC_LogReply(g_ts_ctx, "RedisPublishDict: HSET dict: %s", r2);

    if (!r2) goto cleanup;

    rc = 0;

cleanup:
    if (r1) RedisModule_FreeCallReply(r1);
    if (r2) RedisModule_FreeCallReply(r2);

    if (mf_key)   RedisModule_FreeString(g_ts_ctx, mf_key);
    if (mf_data)  RedisModule_FreeString(g_ts_ctx, mf_data);
    if (dict_key) RedisModule_FreeString(g_ts_ctx, dict_key);
    if (val_data) RedisModule_FreeString(g_ts_ctx, val_data);
    if (field_name) RedisModule_FreeString(g_ts_ctx, field_name);
    if (field_data) RedisModule_FreeString(g_ts_ctx, field_data);
    if (val_name)   RedisModule_FreeString(g_ts_ctx, val_name);

    mcdc_log(MCDC_LOG_INFO,
             "RedisPublishDict: finished id=%u rc=%d", dict_id, rc);

    RedisModule_ThreadSafeContextUnlock(g_ts_ctx);
    return rc;
}

/* --------------------------------------------------------------------------
 * Redis-backed dictionary ID provider using BITPOS + SETBIT
 *
 * Layout:
 *    key: "mcdc:dict:ids"  (string bitmap)
 *
 *   alloc:
 *     BITPOS mcdc:dict:ids 0          -> first free bit
 *     SETBIT mcdc:dict:ids <bit> 1    -> mark allocated
 *
 *   release:
 *     SETBIT mcdc:dict:ids <bit> 0
 *
 * We limit to 0..65535 for MC/DC.
 * -------------------------------------------------------------------------- */

#define MCDC_DICT_ID_MAX 65535
#define MCDC_DICT_ID_BITMAP_KEY "mcdc:dict:ids"

static int
MCDC_RedisAllocDictId(uint16_t *out_id, void *user_data)
{
    (void)user_data;
    if (!g_ts_ctx || !out_id) return -1;

    RedisModule_ThreadSafeContextLock(g_ts_ctx);

    RedisModuleString *key =
        RedisModule_CreateString(g_ts_ctx,
                                 MCDC_DICT_ID_BITMAP_KEY,
                                 sizeof(MCDC_DICT_ID_BITMAP_KEY) - 1);

    if (!key) {
        RedisModule_ThreadSafeContextUnlock(g_ts_ctx);
        return -1;
    }
    /* BITPOS key 0 */
    RedisModuleCallReply *r =
        RedisModule_Call(g_ts_ctx, "BITPOS", "sl", key, (long long)0);
    if (!r) {
        RedisModule_FreeString(g_ts_ctx, key);
        RedisModule_ThreadSafeContextUnlock(g_ts_ctx);

        return -1;
    }

    if (RedisModule_CallReplyType(r) != REDISMODULE_REPLY_INTEGER) {
        RedisModule_FreeString(g_ts_ctx, key);
        RedisModule_FreeCallReply(r);
        RedisModule_ThreadSafeContextUnlock(g_ts_ctx);

        return -1;
    }

    long long bitpos = RedisModule_CallReplyInteger(r);
    if (bitpos < 0 || bitpos > MCDC_DICT_ID_MAX) {
        /* No free bits in range */
        RedisModule_FreeString(g_ts_ctx, key);
        RedisModule_FreeCallReply(r);
        RedisModule_ThreadSafeContextUnlock(g_ts_ctx);

        return -1;
    }

    /* SETBIT key bitpos 1 */
    RedisModuleCallReply *r2 =
        RedisModule_Call(g_ts_ctx, "SETBIT", "!sll", key, bitpos, (long long)1);
    if (!r2) {
        RedisModule_FreeString(g_ts_ctx, key);
        RedisModule_FreeCallReply(r);
        RedisModule_ThreadSafeContextUnlock(g_ts_ctx);

        return -1;
    }

    RedisModule_FreeCallReply(r);
    RedisModule_FreeCallReply(r2);
    RedisModule_FreeString(g_ts_ctx, key);
    RedisModule_ThreadSafeContextUnlock(g_ts_ctx);

    *out_id = (uint16_t)bitpos;
    return 0;
}

static int
MCDC_RedisReleaseDictId(uint16_t id, void *user_data)
{
    (void)user_data;
    if (!g_ts_ctx) return -1;

    RedisModule_ThreadSafeContextLock(g_ts_ctx);

    /* 1) Clear ID bit in bitmap: SETBIT mcdc:dict:ids id 0 */
    RedisModuleString *bitmap_key =
        RedisModule_CreateString(g_ts_ctx,
                                 MCDC_DICT_ID_BITMAP_KEY,
                                 sizeof(MCDC_DICT_ID_BITMAP_KEY) - 1);
    if (!bitmap_key) {
        RedisModule_ThreadSafeContextUnlock(g_ts_ctx);
        return -1;
    }
    
    RedisModuleCallReply *r =
        RedisModule_Call(g_ts_ctx, "SETBIT", "!sll",
                         bitmap_key, (long long)id, (long long)0);
    if (!r) {
        RedisModule_FreeString(g_ts_ctx, bitmap_key);
        RedisModule_ThreadSafeContextUnlock(g_ts_ctx);

        return -1;
    }

    /* 2) Delete dictionary + manifest keys:
     *    DEL mcdc:dict:<id> mcdc:dict:<id>:mf
     */
    char keybuf1[64];
    char keybuf2[64];

    int n = snprintf(keybuf1, sizeof(keybuf1),
                     "mcdc:dict:%u", (unsigned)id);
    if (n <= 0 || (size_t)n >= sizeof(keybuf1)) {
        RedisModule_FreeString(g_ts_ctx, bitmap_key);
        RedisModule_FreeCallReply(r);
        RedisModule_ThreadSafeContextUnlock(g_ts_ctx);

        return -1;
    }
    RedisModuleString *dict_key =
        RedisModule_CreateString(g_ts_ctx, keybuf1, (size_t)n);
    
    if (!dict_key) {
        RedisModule_FreeString(g_ts_ctx, bitmap_key);
        RedisModule_FreeCallReply(r);
        RedisModule_ThreadSafeContextUnlock(g_ts_ctx);

        return -1;
    }
    
    n = snprintf(keybuf2, sizeof(keybuf2),
                 "mcdc:dict:%u:mf", (unsigned)id);
    if (n <= 0 || (size_t)n >= sizeof(keybuf2)) {
        RedisModule_FreeString(g_ts_ctx, bitmap_key);
        RedisModule_FreeString(g_ts_ctx, dict_key);
        RedisModule_FreeCallReply(r);
        RedisModule_ThreadSafeContextUnlock(g_ts_ctx);
        return -1;
    }
    RedisModuleString *mf_key =
        RedisModule_CreateString(g_ts_ctx, keybuf2, (size_t)n);

    /* DEL dict + manifest; we ignore integer result, only NULL vs non-NULL */
    RedisModuleCallReply *r2 =
        RedisModule_Call(g_ts_ctx, "DEL", "!ss", dict_key, mf_key);
    if (!r2) {
        RedisModule_FreeString(g_ts_ctx, bitmap_key);
        RedisModule_FreeString(g_ts_ctx, dict_key);
        RedisModule_FreeString(g_ts_ctx, mf_key);
        RedisModule_FreeCallReply(r);
        RedisModule_ThreadSafeContextUnlock(g_ts_ctx);
        return -1;
    }
    RedisModule_FreeString(g_ts_ctx, bitmap_key);
    RedisModule_FreeString(g_ts_ctx, dict_key);
    RedisModule_FreeString(g_ts_ctx, mf_key);
    RedisModule_FreeCallReply(r);
    RedisModule_FreeCallReply(r2);
    RedisModule_ThreadSafeContextUnlock(g_ts_ctx);
 
    return 0;
}

static const mcdc_dict_id_provider_t g_redis_id_provider = {
    .alloc   = MCDC_RedisAllocDictId,
    .release = MCDC_RedisReleaseDictId,
};

/* --------------------------------------------------------------------------
 * Replication role change event handler
 *
 * Forwards MASTER/REPLICA transitions to core via mcdc_set_node_role().
 * -------------------------------------------------------------------------- */

void MCDC_EnvRedis_OnRoleChange(RedisModuleCtx *ctx,
                                RedisModuleEvent eid,
                                uint64_t subevent,
                                void *data)
{
    (void)ctx;
    (void)eid;
    (void)subevent;
    (void)data;

    /* redismodule.h does not expose explicit
     * "replication role changed" subevents.
     *
     * For this Redis version we:
     *   - Set the initial role at module load using GetContextFlags()
     *   - Rely on MCDC_ShouldCompress(ctx) per command
     *
     */
}

/* --------------------------------------------------------------------------
 * Initialization entrypoints (called from RedisModule_OnLoad)
 * -------------------------------------------------------------------------- */

int MCDC_EnvRedisInit(RedisModuleCtx *ctx)
{
    /* 1) Create a shared thread-safe context for logging + env callbacks. */
    g_ts_ctx = RedisModule_GetThreadSafeContext(NULL);
    if (!g_ts_ctx) {
        return REDISMODULE_ERR;
    }

    /* 2) Install dict publisher + ID provider into core env. */
    mcdc_set_dict_publisher(MCDC_RedisPublishDict, NULL);
    mcdc_set_dict_id_provider(&g_redis_id_provider, NULL);

    /* 3) Set initial node role based on context flags. */
    uint64_t flags = RedisModule_GetContextFlags(ctx);
    if (flags & REDISMODULE_CTX_FLAGS_MASTER) {
        mcdc_set_node_role(MCDC_NODE_ROLE_MASTER);
    } else {
        mcdc_set_node_role(MCDC_NODE_ROLE_REPLICA);
    }

    /* 4) Subscribe to replication role change events. */
    /*if (RedisModule_SubscribeToServerEvent(
            ctx,
            RedisModuleEvent_ReplRoleChanged,
            MCDC_ReplicationRoleChanged) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }*/

    return REDISMODULE_OK;
}

RedisModuleCtx *MCDC_EnvRedis_GetThreadSafeCtx(void) {
    return g_ts_ctx;
}

