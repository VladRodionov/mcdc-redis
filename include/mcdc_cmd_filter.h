// include/mcdc_cmd_filter.h
#ifndef MCDC_FILTER_H
#define MCDC_FILTER_H

#include "redismodule.h"
#include <stdbool.h>
#include <stddef.h>

/* -----------------------------------------------------------------------
 * MC/DC namespace inclusion test
 *
 * Replace this with your real namespace matcher:
 *   bool mcdc_key_in_namespace(const char *key, size_t klen);
 *
 * For now we provide a stub returning true.
 * -----------------------------------------------------------------------
 */

static inline bool MCDC_KeyInMcdcNamespace(const char *key, size_t klen) {
    (void)key;
    (void)klen;

    /* TODO: implement real namespace-based filtering
     * Example:
     *     return mcdc_ns_match(key, klen) != NULL;
     */
    return true;  // enable MC/DC for all keys (initial stub)
}

/* -----------------------------------------------------------------------
 * Function declarations implemented in src/mcdc_cmd_filter.c
 * -----------------------------------------------------------------------
 */

#ifdef __cplusplus
extern "C" {
#endif

/* Register the MC/DC command filter */
int MCDC_RegisterCommandFilter(RedisModuleCtx *ctx);

/* Unregister filter (optional; usually not needed) */
int MCDC_UnregisterCommandFilter(RedisModuleCtx *ctx);

#ifdef __cplusplus
}
#endif

#endif /* MCDC_FILTER_H */
