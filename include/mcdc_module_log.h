// include/mcdc_module_log.h
#pragma once

#include "redismodule.h"

/*
 * Initialize MC/DC logging for the Redis module.
 *
 * - Creates a thread-safe RedisModuleCtx for logging
 * - Installs the Redis-backed logger into the core (mcdc_set_logger)
 *
 * Must be called from RedisModule_OnLoad().
 *
 * Returns:
 *   REDISMODULE_OK on success,
 *   REDISMODULE_ERR on failure.
 */
int MCDC_ModuleInitLogger(void);

/*
 * Optional: shutdown hook for logger.
 *
 * Currently a no-op unless we later decide to:
 *   - free thread-safe context
 *   - reset logger to stderr fallback
 */
void MCDC_ModuleShutdownLogger(void);
