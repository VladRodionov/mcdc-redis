#ifndef MCDC_STRING_CMD_H
#define MCDC_STRING_CMD_H

#include "redismodule.h"

/* helper to register both commands from RedisModule_OnLoad */
int MCDC_RegisterStringCommands(RedisModuleCtx *ctx);

#endif /* MCDC_STRING_CMD_H */
