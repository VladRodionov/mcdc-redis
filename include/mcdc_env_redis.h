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

#ifndef MCDC_ENV_REDIS_H
#define MCDC_ENV_REDIS_H

#include "redismodule.h"
#include "mcdc_env.h"
#include <stdint.h>

/*
 * Initialize Redis-backed MC/DC environment bindings.
 *
 * Responsibilities:
 *   - Create ThreadSafeContext for background-safe dictionary publishing.
 *   - Install Redis-backed implementations for:
 *        + node role provider (master/replica)
 *        + dictionary publisher
 *        + dictionary ID allocator / releaser
 *   - Read initial role at module load (MASTER vs REPLICA)
 *   - Subscribe to replication role change events and push updates into core
 */
int MCDC_EnvRedisInit(RedisModuleCtx *ctx);

/*
 * Shutdown environment (optional).
 * Frees ThreadSafeContext and detaches callbacks.
 */
void MCDC_EnvRedisShutdown(void);

/*
 * Returns pointer to the ThreadSafeContext used for:
 *   - publishing dictionaries
 *   - logging (if integrated)
 */
RedisModuleCtx *MCDC_EnvRedis_GetThreadSafeCtx(void);

#endif /* MCDC_ENV_REDIS_H */
