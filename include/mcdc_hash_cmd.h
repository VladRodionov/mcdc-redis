#ifndef MCDC_HASH_CMD_H
#define MCDC_HASH_CMD_H

#include "redismodule.h"

/* Register all MC/DC hash commands:
 *
 *   mcdc.hget
 *   mcdc.hset
 *   mcdc.hsetnx
 *   mcdc.hdel
 *   mcdc.hexists
 *   mcdc.hlen
 *   mcdc.hkeys
 *   mcdc.hvals
 *   mcdc.hgetall
 *   mcdc.hstrlen
 *   mcdc.hrandfield
 *   mcdc.hgetdel
 *
 * Command filter should rewrite:
 *   HGET      -> mcdc.hget
 *   HSET      -> mcdc.hset        (single or multi field)
 *   HSETNX    -> mcdc.hsetnx
 *   HDEL      -> mcdc.hdel
 *   HEXISTS   -> mcdc.hexists
 *   HLEN      -> mcdc.hlen
 *   HKEYS     -> mcdc.hkeys
 *   HVALS     -> mcdc.hvals
 *   HGETALL   -> mcdc.hgetall
 *   HSTRLEN   -> mcdc.hstrlen
 *   HRANDFIELD-> mcdc.hrandfield
 *   HGETDEL   -> mcdc.hgetdel
 *
 * All TTL-related commands are left untouched (operate on metadata only).
 */
int MCDC_RegisterHashCommands(RedisModuleCtx *ctx);

#endif /* MCDC_HASH_CMD_H */
