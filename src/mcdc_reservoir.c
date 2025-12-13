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
 * mcdc_reservoir.c
 *
 * Reservoir sampler used by MC/DC internal dictionary training.
 *
 * Key duties:
 *   - Maintain a bounded, approximate training corpus under a byte budget
 *     (max_bytes) and optional time window (duration_sec).
 *   - Support “session” lifecycle: init once, start session on demand,
 *     report readiness, snapshot samples for Zstd training, then reset.
 *   - Implement a two-phase sampler:
 *       1) Warmup: append samples until hitting max_bytes or max_items,
 *          then “freeze” k = stored.
 *       2) Reservoir mode: classic replacement (Algorithm R) with fixed k,
 *          updating existing slots without growing the slot array.
 *
 * Concurrency / performance notes:
 *   - Thread-safety is provided via a lightweight try-lock (atomic_flag).
 *     Add operations are non-blocking: on lock contention the sample is dropped.
 *   - Hot path aims to stay O(1) per sample; per-sample memory allocation is
 *     limited to the payload copy that may be stored into a slot.
 *   - Snapshot takes exclusive ownership (spins until lock acquired) and
 *     materializes samples into a flat buffer + sizes[] for Zstd training.
 *
 * Correctness notes:
 *   - Byte budget is enforced during warmup; after freezing, total bytes may
 *     drift due to replacements (accepted trade-off for simplicity).
 *   - start_ts == 0 indicates “no active session”; trainer starts a session
 *     via mcdc_reservoir_check_start_session() and resets via reset_session().
 */
#include "mcdc_reservoir.h"
#include "mcdc_log.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>

/* Conservative “expected min sample size” used only to cap max_items. */
#define MCDC_MIN_SAMPLE_BYTES 100

/* --- Helpers ---------------------------------------------------------------- */

static double
mcdc_now_monotonic_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* xorshift64* RNG: small, fast, non-crypto */
static uint64_t
mcdc_rng_next(_Atomic uint64_t *state)
{
    uint64_t x = atomic_load_explicit(state, memory_order_relaxed);
    if (x == 0) {
        x = 0x9E3779B97F4A7C15ULL;
    }
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    x *= 0x2545F4914F6CDD1DULL;
    atomic_store_explicit(state, x, memory_order_relaxed);
    return x;
}

static int
mcdc_try_lock(atomic_flag *lock)
{
    return !atomic_flag_test_and_set_explicit(lock, memory_order_acquire);
}

static void
mcdc_unlock(atomic_flag *lock)
{
    atomic_flag_clear_explicit(lock, memory_order_release);
}

static void
mcdc_free_item(mcdc_reservoir_item_t *it)
{
    if (it->data) {
        free(it->data);
        it->data = NULL;
    }
    it->len = 0;
}

/* --- Public API ------------------------------------------------------------- */

int
mcdc_reservoir_init(mcdc_reservoir_t *r,
                    size_t max_bytes,
                    double duration_sec,
                    uint64_t seed)
{
    if (!r || max_bytes == 0) {
        return -1;
    }

    memset(r, 0, sizeof(*r));

    r->max_bytes    = max_bytes;
    r->duration_sec = duration_sec;
    r->start_ts     = 0.0;   /* session not started yet */

    /* Upper bound on number of slots; byte budget still enforced separately. */
    size_t max_items = max_bytes / MCDC_MIN_SAMPLE_BYTES;
    if (max_items == 0) {
        max_items = 1;
    }
    r->max_items = max_items;

    r->slots = calloc(max_items, sizeof(mcdc_reservoir_item_t));
    if (!r->slots) {
        return -1;
    }

    if (seed == 0) {
        seed = 0x123456789ABCDEFULL;
    }
    atomic_store(&r->rng_state, seed);
    atomic_flag_clear(&r->lock);

    atomic_store(&r->seen,       0);
    atomic_store(&r->stored,     0);
    atomic_store(&r->bytes_used, 0);
    atomic_store(&r->frozen,     0);

    return 0;
}
void
mcdc_reservoir_check_start_session(mcdc_reservoir_t *r)
{
    if (!r) return;

    /* Already running this session. */
    if (r->start_ts != 0.0) {
        return;
    }

    /* Start a new session: zero per-session counters. */
    if (!mcdc_try_lock(&r->lock)) {
        /* Trainer can just try again next loop iteration. */
        return;
    }

    r->start_ts = mcdc_now_monotonic_sec();

    atomic_store_explicit(&r->seen,       0, memory_order_relaxed);
    atomic_store_explicit(&r->stored,     0, memory_order_relaxed);
    atomic_store_explicit(&r->bytes_used, 0, memory_order_relaxed);
    atomic_store_explicit(&r->frozen,     0, memory_order_relaxed);

    mcdc_unlock(&r->lock);
}

void
mcdc_reservoir_destroy(mcdc_reservoir_t *r)
{
    if (!r) return;

    if (r->slots) {
        for (size_t i = 0; i < r->max_items; i++) {
            mcdc_free_item(&r->slots[i]);
        }
        free(r->slots);
        r->slots = NULL;
    }
}

int
mcdc_reservoir_active(mcdc_reservoir_t *r)
{
    if (!r) return 0;

    /* No time limit: active as soon as a session has started. */
    if (r->duration_sec <= 0.0) {
        return (r->start_ts != 0.0);
    }

    if (r->start_ts == 0.0) {
        /* Session not started yet. */
        return 0;
    }

    double now = mcdc_now_monotonic_sec();
    return (now - r->start_ts) <= r->duration_sec;
}

size_t
mcdc_reservoir_size(mcdc_reservoir_t *r)
{
    if (!r) return 0;
    uint64_t stored = atomic_load_explicit(&r->stored, memory_order_relaxed);
    if (stored > r->max_items) stored = r->max_items;
    return (size_t)stored;
}

/* ---------------------------------------------------------------------------
 * READY / RESET
 * ------------------------------------------------------------------------- */

/* “Sampling complete” if:
 *   - we have at least 1 sample, AND
 *   - either the time window ended OR we’ve frozen the reservoir size.
 */
int
mcdc_reservoir_ready(mcdc_reservoir_t *r)
{
    if (!r) return 0;

    size_t stored = (size_t)atomic_load_explicit(&r->stored,
                                                 memory_order_relaxed);
    if (stored == 0) {
        /* No samples => never ready. */
        return 0;
    }

    /* No time limit case: “ready” is when we froze the reservoir. */
    if (r->duration_sec <= 0.0) {
        return atomic_load_explicit(&r->frozen, memory_order_relaxed) != 0 ||
            stored == r->max_items;
    }

    /* Time-limited case: must wait for the window to end.
     * Being frozen is NOT enough by itself.
     */
    if (r->start_ts == 0.0) {
        /* Session hasn’t started yet. */
        return 0;
    }

    double now = mcdc_now_monotonic_sec();
    double elapsed = now - r->start_ts;

    if (elapsed >= r->duration_sec) {
        return 1;   /* window ended, we have samples */
    }

    return 0;       /* still within window */
}

/* Reset for the next training round.
 * Safe to call from trainer thread. May briefly block while taking the lock.
 */
void
mcdc_reservoir_reset_session(mcdc_reservoir_t *r)
{
    if (!r) return;

    /* Free all stored samples */
    for (size_t i = 0; i < r->max_items; i++) {
        mcdc_free_item(&r->slots[i]);
    }

    atomic_store_explicit(&r->seen,       0, memory_order_relaxed);
    atomic_store_explicit(&r->stored,     0, memory_order_relaxed);
    atomic_store_explicit(&r->bytes_used, 0, memory_order_relaxed);
    atomic_store_explicit(&r->frozen,     0, memory_order_relaxed);

    r->start_ts = 0.0;  /* marks “no active session” */

}

/* ---------------------------------------------------------------------------
 * Reservoir add
 *  - Warmup: fill until byte cap or slot cap hit.
 *  - Freeze: when we first hit either max_bytes or max_items, we freeze k=stored.
 *  - Reservoir mode: classic Algorithm R with k = stored (which never changes).
 * ------------------------------------------------------------------------- */

int
mcdc_reservoir_add(mcdc_reservoir_t *r,
                   const void *buf,
                   size_t len)
{

    if (!r || !buf || len == 0) {
        return 0;
    }

    /* Training window ended? */
    if (!mcdc_reservoir_active(r)) {
        return 0;
    }

    /* Item too big to ever fit => skip */
    if (len > r->max_bytes) {
        return 0;
    }
    
    if (!mcdc_try_lock(&r->lock)) {
        /* Non-blocking: if trainer owns the lock, just drop sample. */
        return 0;
    }
        
    /* Global 1-based index for Algorithm R */
    uint64_t i = atomic_fetch_add_explicit(&r->seen, 1, memory_order_relaxed) + 1;

    /* --- Phase 1: warmup / fill, before we freeze reservoir size --- */
    if (!atomic_load_explicit(&r->frozen, memory_order_relaxed)) {

        int rc = 0;

        /* Re-check window inside lock */
        if (!mcdc_reservoir_active(r)) {
            goto warmup_out;
        }

        size_t stored = (size_t)atomic_load_explicit(&r->stored, memory_order_relaxed);
        size_t bytes  = (size_t)atomic_load_explicit(&r->bytes_used, memory_order_relaxed);

        if (stored < r->max_items && bytes + len <= r->max_bytes) {
            /* We can still grow the reservoir (by slots and bytes). */
            uint8_t *copy = malloc(len);
            if (!copy) {
                rc = -1;
                goto warmup_out;
            }
            memcpy(copy, buf, len);

            r->slots[stored].data = copy;
            r->slots[stored].len  = len;

            atomic_store_explicit(&r->stored, stored + 1, memory_order_relaxed);
            atomic_store_explicit(&r->bytes_used, bytes + len, memory_order_relaxed);
            rc = 1;
            goto warmup_out;
        }

        /* We hit either max_bytes or max_items → freeze k = stored. */
        if (stored > 0) {
            /* From now on, stored is our fixed k (reservoir size). */
            atomic_store_explicit(&r->frozen, 1, memory_order_release);
        }
    warmup_out:
        mcdc_unlock(&r->lock);
        if (rc != 0) {
            return rc; /* success or allocation error */
        }
        /* rc == 0 → fall through to reservoir mode (if frozen==1). */
    }

    /* --- Phase 2: reservoir mode (Algorithm R with fixed k = stored) --- */
    int rc = 0;

    /* If we never managed to store anything (stored==0), nothing to do. */
    size_t k = (size_t)atomic_load_explicit(&r->stored, memory_order_relaxed);
    if (k == 0) {
        goto res_out;
    }

    /* Algorithm R gate: accept with probability k / i. */
    if (i > k) {
        uint64_t rnum = mcdc_rng_next(&r->rng_state);
        if ((rnum % i) >= k) {
            goto res_out;
        }
    }

    if (!mcdc_reservoir_active(r)) {
        goto res_out;
    }

    uint8_t *copy = malloc(len);
    if (!copy) {
        rc = -1;
        goto res_out;
    }
    memcpy(copy, buf, len);

    /* Random replacement in [0, k) */
    uint64_t rnum = mcdc_rng_next(&r->rng_state);
    size_t   idx  = (size_t)(rnum % k);
    mcdc_reservoir_item_t *slot = &r->slots[idx];

    size_t bytes = (size_t)atomic_load_explicit(&r->bytes_used, memory_order_relaxed);
    bytes = bytes - slot->len + len;
    mcdc_free_item(slot);
    slot->data = copy;
    slot->len  = len;
    
    atomic_store_explicit(&r->bytes_used, bytes, memory_order_relaxed);
    rc = 1;

res_out:
    mcdc_unlock(&r->lock);
    return rc;
}

/* Snapshot reservoir into a flat buffer + sizes[].
 * Returns:
 *   1  on success (and sets out_*),
 *   0  if reservoir is empty,
 *  -1  on OOM or internal error.
 *
 * Caller must free(*out_buf) and free(*out_sizes) on success.
 */
int
mcdc_reservoir_snapshot(mcdc_reservoir_t *r,
                        uint8_t **out_buf,
                        size_t **out_sizes,
                        size_t *out_count,
                        size_t *out_total_bytes)
{
    if (!r || !out_buf || !out_sizes || !out_count || !out_total_bytes) {
        return -1;
    }

    *out_buf         = NULL;
    *out_sizes       = NULL;
    *out_count       = 0;
    *out_total_bytes = 0;

    /* Single-writer section for trainer: take the lock */
    while (!mcdc_try_lock(&r->lock)) {
    }

    uint64_t stored_u = atomic_load_explicit(&r->stored, memory_order_relaxed);
    size_t   stored   = (stored_u > r->max_items) ? r->max_items : (size_t)stored_u;

    if (stored == 0) {
        mcdc_unlock(&r->lock);
        return 0; /* nothing to train on */
    }

    size_t total = 0;
    for (size_t i = 0; i < stored; i++) {
        total += r->slots[i].len;
    }

    if (total == 0) {
        mcdc_unlock(&r->lock);
        return 0;
    }

    size_t  *sizes = malloc(sizeof(size_t) * stored);
    uint8_t *buf   = malloc(total);
    if (!sizes || !buf) {
        free(sizes);
        free(buf);
        mcdc_unlock(&r->lock);
        return -1;
    }

    size_t off = 0;
    for (size_t i = 0; i < stored; i++) {
        sizes[i] = r->slots[i].len;
        memcpy(buf + off, r->slots[i].data, r->slots[i].len);
        off += r->slots[i].len;
    }

    mcdc_reservoir_reset_session(r);
    mcdc_unlock(&r->lock);

    *out_buf         = buf;
    *out_sizes       = sizes;
    *out_count       = stored;
    *out_total_bytes = total;
    return 1;
}
