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

#ifndef MCDC_MGET_ASYNC_H
#define MCDC_MGET_ASYNC_H

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Register async mget command: mcdc.mgetasync */
int MCDC_RegisterMGetAsyncCommand(RedisModuleCtx *ctx);

#ifdef __cplusplus
}
#endif

#endif /* MCDC_MGET_ASYNC_H */
