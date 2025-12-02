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

#pragma once
#include "redismodule.h"

/*
 * Initialize MC/DC logging for the Redis module.
 *
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
