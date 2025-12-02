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

/* mcdc_env.h
 *
 * MC/DC "environment" abstraction layer:
 * - node role (master / replica)
 * - dictionary publisher callback
 * - dictionary ID provider
 *
 * The core never talks to Redis directly; it only uses these APIs.
 */

#ifndef MCDC_ENV_H
#define MCDC_ENV_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* --------------------------------------------------------------------------
 * Node role API
 * -------------------------------------------------------------------------- */

typedef enum {
    MCDC_NODE_ROLE_UNDEFINED = -1,
    MCDC_NODE_ROLE_MASTER = 0,
    MCDC_NODE_ROLE_REPLICA = 1
} mcdc_node_role_t;

/* Set current node role. Called by Redis module on startup and whenever
 * replication role changes (MASTER <-> REPLICA).
 *
 * Core will use this to enable/disable trainer + GC.
 */
void mcdc_set_node_role(mcdc_node_role_t role);

/* Read current node role (for core internals). */
mcdc_node_role_t mcdc_get_node_role(void);

/* --------------------------------------------------------------------------
 * Dictionary publisher callback
 *
 * Called by trainer / GC when a new dictionary is ready and must be
 * published to the outside world (Redis keys, external service, etc).
 *
 * Implementations MUST be thread-safe; calls may come from background threads.
 * -------------------------------------------------------------------------- */

typedef int (*mcdc_publish_dict_fn)(
    uint16_t dict_id, const char *file_name,
    const void *dict_buf,     size_t dict_len,
    const void *manifest_buf, size_t manifest_len,
    void *user_data);

/* Install a publisher. Passing NULL disables publishing (core will treat
 * that as "no external distribution available").
 */
void mcdc_set_dict_publisher(mcdc_publish_dict_fn fn, void *user_data);

/* Helper for core: publish a dict if callback is installed.
 * Returns 0 on success, non-zero on failure.
 */
int mcdc_env_publish_dict(uint16_t dict_id, const char *file_name,
                          const void *dict_buf,     size_t dict_len,
                          const void *manifest_buf, size_t manifest_len);

/* --------------------------------------------------------------------------
 * Dictionary ID provider
 *
 * Core uses this to allocate/release 16-bit dictionary IDs (0..65535).
 *
 * Implementations may be:
 *  - purely in-memory (single-node)
 *  - Redis-backed via BITPOS + SETBIT on "mcdc:dict:ids"
 *  - external registry over the network
 * -------------------------------------------------------------------------- */

typedef struct mcdc_dict_id_provider {
    /* Allocate the next free ID.
     * Returns 0 on success, non-zero on failure (no ids / error).
     */
    int (*alloc)(uint16_t *out_id, void *user_data);

    /* Release a previously allocated ID (mark it as free again). */
    int (*release)(uint16_t id, void *user_data);
} mcdc_dict_id_provider_t;

/* Install provider. Passing prov = NULL disables allocation from core. */
void mcdc_set_dict_id_provider(const mcdc_dict_id_provider_t *prov,
                               void *user_data);

/* Helper for core: allocate ID via provider. Returns 0 on success. */
int mcdc_env_alloc_dict_id(uint16_t *out_id);

/* Helper for core: release ID via provider. Returns 0 on success. */
int mcdc_env_release_dict_id(uint16_t id);

const char *mcdc_env_get_dict_dir(void);

int mcdc_env_reload_dicts(void);

#ifdef __cplusplus
}
#endif

#endif /* MCDC_ENV_H */
