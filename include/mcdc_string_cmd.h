#ifndef MCDC_STRING_CMD_H
#define MCDC_STRING_CMD_H

#include "redismodule.h"

/* mcdc.set key value */
int MCDC_SetCommand(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc);

/* mcdc.get key */
int MCDC_GetCommand(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc);

/* helper to register both commands from RedisModule_OnLoad */
int MCDC_RegisterStringCommands(RedisModuleCtx *ctx);

#endif /* MCDC_STRING_CMD_H */
