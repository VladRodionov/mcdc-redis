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

#include "mcdc_module_log.h"
#include "mcdc_log.h"
#include "mcdc_env_redis.h"

static RedisModuleCtx *g_log_ctx = NULL;

static const char *mcdc_level_to_redis(mcdc_log_level_t lvl) {
    switch (lvl) {
    case MCDC_LOG_DEBUG: return "debug";
    case MCDC_LOG_INFO:  return "notice";
    case MCDC_LOG_WARN:  return "warning";
    case MCDC_LOG_ERROR: return "warning";
    default:             return "notice";
    }
}

static void mcdc_redis_logger(mcdc_log_level_t level,
                              const char *fmt,
                              va_list ap)
{
    if (!g_log_ctx) {
        // Fallback if not initialized yet
        vfprintf(stderr, fmt, ap);
        fprintf(stderr, "\n");
        return;
    }

    char buf[1024];
    vsnprintf(buf, sizeof(buf), fmt, ap);

    RedisModule_Log(g_log_ctx, mcdc_level_to_redis(level),
                    "%s", buf);
}

int MCDC_ModuleInitLogger(void)
{
    g_log_ctx = MCDC_EnvRedis_GetThreadSafeCtx();
    if (!g_log_ctx) {
        return REDISMODULE_ERR;
    }

    mcdc_set_logger(mcdc_redis_logger);
    return REDISMODULE_OK;
}

void MCDC_ModuleShutdownLogger(void)
{
    // Optional: if we want to clean up later:
    if (g_log_ctx) {
        RedisModule_FreeThreadSafeContext(g_log_ctx);
        g_log_ctx = NULL;
    }
    mcdc_set_logger(NULL); // revert to stderr
}
