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

// include/mcdc_cmd_filter.h
#ifndef MCDC_FILTER_H
#define MCDC_FILTER_H

#include "redismodule.h"
#include <stdbool.h>
#include <stddef.h>


/* -----------------------------------------------------------------------
 * Function declarations implemented in src/mcdc_cmd_filter.c
 * -----------------------------------------------------------------------
 */

#ifdef __cplusplus
extern "C" {
#endif

/* Register the MC/DC command filter */
int MCDC_RegisterCommandFilter(RedisModuleCtx *ctx);

/* Unregister filter (optional; usually not needed) */
int MCDC_UnregisterCommandFilter(RedisModuleCtx *ctx);

#ifdef __cplusplus
}
#endif

#endif /* MCDC_FILTER_H */
