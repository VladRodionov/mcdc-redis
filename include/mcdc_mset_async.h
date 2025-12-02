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

#ifndef MCDC_MSET_ASYNC_H
#define MCDC_MSET_ASYNC_H

#include "redismodule.h"

/*
 * Register mcdc.msetasync command.
 *
 * Usage:
 *   mcdc.msetasync key value [key value ...]
 *
 * Semantics:
 *   - Compress values off-thread using MC/DC dictionaries.
 *   - On the main thread, SET all keys to the (possibly compressed) blobs.
 *   - Replies with "OK" on success or an error if something failed.
 */
int MCDC_RegisterMSetAsyncCommand(RedisModuleCtx *ctx);

#endif /* MCDC_MSET_ASYNC_H */
