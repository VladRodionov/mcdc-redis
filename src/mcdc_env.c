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
/*
 * mcdc_env.c
 *
 * Environment / integration glue layer for MC/DC.
 *
 * Key duties:
 *   - Provide a small, stable API surface that the core uses for
 *     deployment-specific hooks (filesystem locations, publishing, ID allocation).
 *   - Expose the configured dictionary directory and trigger dictionary reloads.
 *
 * Node role management:
 *   - Track the node’s role (primary/replica/undefined) via an atomic global.
 *   - Notify the core on role changes (mcdc_core_on_role_change) so it can
 *     start/stop background activities like trainer and GC as appropriate.
 *
 * Dictionary distribution hooks:
 *   - Optional “publisher” callback used to broadcast newly built dictionaries
 *     (dict + manifest) to external systems (registry, control plane, etc.).
 *   - If no publisher is installed, publishing is treated as a no-op success.
 *
 * Dictionary ID allocation hooks:
 *   - Optional ID provider that lets deployments allocate/release cluster-safe
 *     dictionary IDs outside the module (e.g., via an external registry).
 *   - Calls are guarded by an “installed” atomic flag; if not installed, the
 *     alloc/release APIs return an error.
 *
 * Notes:
 *   - This file intentionally contains minimal logic and no Redis key access.
 *   - Hooks are designed to keep the core portable across Redis/Valkey builds
 *     and different deployment environments.
 */
#include "mcdc_env.h"
#include "mcdc_compression.h"
#include <string.h>
#include <stdatomic.h>

/* Optional hook implemented in core to actually start/stop trainer + GC.
 */
void mcdc_core_on_role_change(mcdc_node_role_t role);

const char *mcdc_env_get_dict_dir(void) {
    const mcdc_ctx_t *ctx = mcdc_ctx();
    return ctx->cfg->dict_dir;
}

int mcdc_env_reload_dicts(void) {
    mcdc_reload_status_t *st = mcdc_reload_dictionaries();
    return st->rc;
}


/* --------------------------------------------------------------------------
 * Node role
 * -------------------------------------------------------------------------- */

static _Atomic mcdc_node_role_t g_node_role = MCDC_NODE_ROLE_UNDEFINED;

void mcdc_set_node_role(mcdc_node_role_t role)
{
    mcdc_node_role_t current_role = atomic_load(&g_node_role);
    int role_changed = role != current_role;
    atomic_store(&g_node_role, role);
    /* Let the rest of the core react (start/stop trainer + GC, etc). */
    if (role_changed) {
        mcdc_core_on_role_change(role);
    }
}

mcdc_node_role_t mcdc_get_node_role(void)
{
    return atomic_load(&g_node_role);
}

/* --------------------------------------------------------------------------
 * Dictionary publisher
 * -------------------------------------------------------------------------- */

static mcdc_publish_dict_fn g_publish_fn = NULL;
static void *g_publish_ud = NULL;

void mcdc_set_dict_publisher(mcdc_publish_dict_fn fn, void *user_data)
{
    g_publish_fn = fn;
    g_publish_ud = user_data;
}

int mcdc_env_publish_dict(uint16_t dict_id, const char *file_name,
                          const void *dict_buf,     size_t dict_len,
                          const void *manifest_buf, size_t manifest_len)
{
    mcdc_publish_dict_fn fn = g_publish_fn;
    if (!fn) {
        /* No publisher installed – treat as success for cache-only deployments. */
        return 0;
    }
    return fn(dict_id, file_name, dict_buf, dict_len, manifest_buf, manifest_len, g_publish_ud);
}

/* --------------------------------------------------------------------------
 * Dictionary ID provider
 * -------------------------------------------------------------------------- */

static mcdc_dict_id_provider_t g_id_provider;
static void *g_id_ud = NULL;
static _Atomic int g_id_provider_installed = 0;

void mcdc_set_dict_id_provider(const mcdc_dict_id_provider_t *prov,
                               void *user_data)
{
    if (!prov) {
        memset(&g_id_provider, 0, sizeof(g_id_provider));
        g_id_ud = NULL;
        atomic_store(&g_id_provider_installed, 0);
        return;
    }

    g_id_provider = *prov;
    g_id_ud = user_data;
    atomic_store(&g_id_provider_installed, 1);
}

int mcdc_env_alloc_dict_id(uint16_t *out_id)
{
    if (!out_id) return -1;
    if (!atomic_load(&g_id_provider_installed)) return -1;
    if (!g_id_provider.alloc) return -1;

    return g_id_provider.alloc(out_id, g_id_ud);
}

int mcdc_env_release_dict_id(uint16_t id)
{
    if (!atomic_load(&g_id_provider_installed)) return -1;
    if (!g_id_provider.release) return -1;

    return g_id_provider.release(id, g_id_ud);
}
