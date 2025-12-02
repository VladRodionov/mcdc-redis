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

#ifndef MCDC_MODULE_UTILS_H
#define MCDC_MODULE_UTILS_H

#include "redismodule.h"

/*
 * Delete a key from Redis, with proper replication semantics.
 *
 * Parameters:
 *   ctx      - Redis module context
 *   key      - key name as RedisModuleString*
 *   deleted  - optional out-parameter; if non-NULL, will be set to the
 *              integer reply from DEL (0 or 1 in normal cases).
 *
 * Behavior:
 *   - Issues: DEL key
 *   - Uses "!" flag so that the deletion is replicated / written to AOF
 *     when executed on a master handling a real client command.
 *   - On replicas or during AOF/replication replay, the "!" flag is
 *     harmless (no double replication).
 *
 * Returns:
 *   REDISMODULE_OK on success,
 *   REDISMODULE_ERR if the underlying DEL failed or returned
 *   a non-integer reply.
 */
int MCDC_DelKey(RedisModuleCtx *ctx,
                RedisModuleString *key);

void write_u16(char *dst, int v);

int read_u16(const char *src);

ssize_t mcdc_encode_value(const char *key, size_t klen,
                  const char *value, size_t vlen,
                         char **outbuf);

ssize_t mcdc_decode_value(const char *key, size_t klen,
                  const char *input, size_t ilen,
                         char **outbuf);
uint64_t nsec_now(void);


#endif /* MCDC_MODULE_UTILS_H */
