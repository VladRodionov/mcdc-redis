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
 * mcdc_module.c
 *
 * Module entrypoint (RedisModule_OnLoad).
 *
 * Responsibilities:
 * - RedisModule_Init(name, version, APIv1).
 * - Detect Redis version once; set g_mcdc_has_hsetex if server >= 8.0.0
 *   (used by capability checks for HSETEX/HGETEX).
 * - Parse/load module config (MCDC_LoadConfig) and log current working dir.
 * - Initialize core MC/DC state (mcdc_init) and Redis env integration
 *   (MCDC_EnvRedisInit: dict publisher / id provider, etc).
 * - Initialize module logger (MCDC_ModuleInitLogger).
 * - Register all commands:
 *     * admin
 *     * string commands + “unsupported string” wrappers (append/getrange/setrange)
 *     * role debug (mcdc.role)
 *     * async string commands (mcdc.mgetasync / mcdc.msetasync)
 *     * hash commands + async HSET/HMGET
 *     * async dict load commands
 * - Register the command filter that rewrites native Redis commands (GET/SET/H*)
 *   into mcdc.* equivalents (and handles special dict metadata rewrite).
 * - If async commands are enabled, start thread pool using config:
 *     threads = async_thread_pool_size, queue = async_queue_size.
 *
 * Outcome:
 * - Logs “module loaded” + thread-pool info (if enabled) and returns OK.
 */
#include <string.h>
#include <unistd.h>   // getcwd
#include <limits.h>   // PATH_MAX
#include <errno.h>
#define REDISMODULE_MAIN
#include "redismodule.h"
#include "mcdc_module.h"
#include "mcdc_compression.h"
#include "mcdc_admin_cmd.h"
#include "mcdc_string_cmd.h"
#include "mcdc_hash_cmd.h"
#include "mcdc_hash_async.h"
#include "mcdc_string_unsupported_cmd.h"
#include "mcdc_cmd_filter.h"
#include "mcdc_role.h"
#include "mcdc_mget_async.h"
#include "mcdc_mset_async.h"
#include "mcdc_thread_pool.h"
#include "mcdc_module_log.h"
#include "mcdc_env_redis.h"
#include "mcdc_dict_load_async.h"
#include "mcdc_capabilities.h"
#define REDIS_VER(maj, min, pat) (((maj) << 16) | ((min) << 8) | (pat))

int g_mcdc_has_hsetex = 0;

static void MCDC_LogCwd(RedisModuleCtx *ctx) {
    char cwd[PATH_MAX];

    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        RedisModule_Log(ctx, "notice", "MC/DC: current working directory: %s", cwd);
    } else {
        RedisModule_Log(ctx, "warning",
                        "MC/DC: getcwd() failed (errno=%d: %s)",
                        errno, strerror(errno));
    }
}

/* Module init */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, MCDC_MODULE_NAME, MCDC_MODULE_VERSION, REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    
    /* Detect server version once: Redis 8.0.0+ has HSETEX/HGETEX */
    uint64_t ver = RedisModule_GetServerVersion();
    g_mcdc_has_hsetex = (ver >= REDIS_VER(8,0,0));

    if (MCDC_LoadConfig(ctx, argv, argc) != REDISMODULE_OK)
         return REDISMODULE_ERR;
    MCDC_LogCwd(ctx);
    /* Initialize module components */
    //TODO: error handling
    mcdc_init();
    if (MCDC_EnvRedisInit(ctx) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "failed to init env / dict publisher / id provider");
        return REDISMODULE_ERR;
    }
    
    if (MCDC_ModuleInitLogger() != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    
    MCDC_RegisterAdminCommands(ctx);
    MCDC_RegisterStringCommands(ctx);
    MCDC_RegisterUnsupportedStringCommands(ctx);
    MCDC_RegisterRoleDebugCommand(ctx);
    MCDC_RegisterMGetAsyncCommand(ctx);
    MCDC_RegisterMSetAsyncCommand(ctx);
    MCDC_RegisterHashCommands(ctx);
    MCDC_RegisterHSetAsyncCommand(ctx);
    MCDC_RegisterHMGetAsyncCommand(ctx);
    /* Load and parse config directly */

    if (MCDC_RegisterCommandFilter(ctx) == REDISMODULE_ERR) {
        RedisModule_Log(ctx, "warning",
                        "MC/DC: failed to register command filter");
        return REDISMODULE_ERR;
    }
    
    if (MCDC_RegisterDictLoadCommands(ctx) == REDISMODULE_ERR) {
        RedisModule_Log(ctx, "warning",
                        "MC/DC: failed to register dictionary load commands");
        return REDISMODULE_ERR;
    };
    
    RedisModule_Log(ctx, "notice",
                    "MC/DC Redis module loaded with command filters");
    mcdc_cfg_t *cfg = mcdc_config_get();
    if (cfg->async_cmd_enabled){
        int num_threads = cfg->async_thread_pool_size;
        int queue_size = cfg->async_queue_size;
        MCDC_ThreadPoolInit(num_threads, queue_size);
        RedisModule_Log(ctx, "notice",
                        "MC/DC Redis module started thread pool: %d threads, queue_size=%d", num_threads, queue_size);
    }
    
    return REDISMODULE_OK;
}

