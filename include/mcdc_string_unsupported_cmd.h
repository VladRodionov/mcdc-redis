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

#ifndef MCDC_STRING_UNSUPPORTED_CMD_H
#define MCDC_STRING_UNSUPPORTED_CMD_H

#include "redismodule.h"

/*
 * Unsupported String commands for MC/DC.
 *
 * These commands do:
 *   1. Downgrade key(s) from compressed MC/DC format to raw Redis String
 *      if needed (GET + decode + SET ... KEEPTTL)
 *   2. Delegate to the underlying Redis command
 *   3. Return its reply
 *
 * All commands are exposed as mcdc.<name>, e.g. mcdc.append, mcdc.setrange, etc.
 */

/* String commands */

int MCDC_AppendCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_GetRangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MCDC_SetRangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/*
 * Register all unsupported string commands.
 */
int MCDC_RegisterUnsupportedStringCommands(RedisModuleCtx *ctx);

#endif /* MCDC_STRING_UNSUPPORTED_CMD_H */
