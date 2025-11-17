#ifndef MCDC_STRING_UNSUPPORTED_CMD_H
#define MCDC_STRING_UNSUPPORTED_CMD_H

#include "redismodule.h"

/*
 * Unsupported String + bitmap commands for MC/DC.
 *
 * These commands do:
 *   1. Downgrade key(s) from compressed MC/DC format to raw Redis String
 *      if needed (GET + decode + SET ... KEEPTTL)
 *   2. Delegate to the underlying Redis command
 *   3. Return its reply
 *
 * All commands are exposed as mcdc.<name>, e.g. mcdc.incr, mcdc.setbit, etc.
 */

/* String arithmetic / mutation */
int MCDC_IncrCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_IncrByCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_IncrByFloatCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_DecrCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_DecrByCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_AppendCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_GetRangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_SetRangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/* Bitmap family (all of them) */
int MCDC_SetBitCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_GetBitCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_BitCountCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_BitPosCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_BitOpCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_BitFieldCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_BitFieldRoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/*
 * Register all unsupported string / bitmap commands with Redis.
 *
 * Call from RedisModule_OnLoad():
 *   MCDC_RegisterUnsupportedStringCommands(ctx);
 */
int MCDC_RegisterUnsupportedStringCommands(RedisModuleCtx *ctx);

#endif /* MCDC_STRING_UNSUPPORTED_CMD_H */
