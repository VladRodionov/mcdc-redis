#ifndef MCDC_ROLE_H
#define MCDC_ROLE_H

#include "redismodule.h"


/*
 * Simple helpers to reason about Redis role + command origin.
 * These read RedisModule_GetContextFlags(ctx) each time they are called,
 * so they always reflect the current role (master/replica) and context
 * (client vs replication/AOF).
 */

static inline int MCDC_IsMaster(RedisModuleCtx *ctx) {
    uint64_t flags = RedisModule_GetContextFlags(ctx);
    return (flags & REDISMODULE_CTX_FLAGS_MASTER) != 0;
}

static inline int MCDC_IsReplica(RedisModuleCtx *ctx) {
    uint64_t flags = RedisModule_GetContextFlags(ctx);
    return (flags & REDISMODULE_CTX_FLAGS_SLAVE) != 0;
}

/*
 * True if this particular command invocation came from
 * replication / AOF replay, not from a direct client.
 */
static inline int MCDC_IsReplicatedCommand(RedisModuleCtx *ctx) {
    uint64_t flags = RedisModule_GetContextFlags(ctx);
    return (flags & REDISMODULE_CTX_FLAGS_REPLICATED) != 0;
}

/*
 * Core rule for MC/DC:
 *
 *   Compress only when:
 *     - node is NOT a replica, AND
 *     - this command is NOT from replication/AOF.
 *
 * Everything else (replicas + replay) should store bytes verbatim.
 */
static inline int MCDC_ShouldCompress(RedisModuleCtx *ctx) {
    uint64_t flags = RedisModule_GetContextFlags(ctx);

    if (flags & (REDISMODULE_CTX_FLAGS_SLAVE |
                 REDISMODULE_CTX_FLAGS_REPLICATED)) {
        return 0;   /* do NOT compress */
    }
    return 1;       /* ok to compress */
}

/*
 * For MC/DC, we always want to decompress on reads on both
 * master and replicas, so this is just here for symmetry
 * and readability.
 */
static inline int MCDC_ShouldDecompress(RedisModuleCtx *ctx) {
    (void)ctx;
    return 1;
}

/* Debug command declared here, implemented in mcdc_role.c */
int MCDC_RoleDebugCommand(RedisModuleCtx *ctx,
                          RedisModuleString **argv,
                          int argc);

#endif /* MCDC_ROLE_H */
