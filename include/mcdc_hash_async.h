#pragma once

#include "redismodule.h"

/* Registration helpers */
int MCDC_RegisterHMGetAsyncCommand(RedisModuleCtx *ctx);
int MCDC_RegisterHSetAsyncCommand(RedisModuleCtx *ctx);
