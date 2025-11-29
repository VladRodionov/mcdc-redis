#ifndef MCDC_ENV_REDIS_H
#define MCDC_ENV_REDIS_H

#include "redismodule.h"
#include "mcdc_env.h"        /* Core environment interfaces */
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
