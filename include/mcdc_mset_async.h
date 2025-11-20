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
