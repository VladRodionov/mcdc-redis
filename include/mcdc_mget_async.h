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
