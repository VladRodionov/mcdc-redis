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

#ifndef MCDC_INCOMPRESSIBLE_H
#define MCDC_INCOMPRESSIBLE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <math.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef MCDC_SAMPLE_BYTES
#define MCDC_SAMPLE_BYTES       512u     // ~500 bytes is enough; bounded stack probe
#endif

#ifndef MCDC_ASCII_THRESHOLD
#define MCDC_ASCII_THRESHOLD    0.85     // ≥85% printable ASCII -> compress
#endif

#ifndef MCDC_ENTROPY_NO
#define MCDC_ENTROPY_NO         7.50     // H8 ≥ 7.5 bits/byte -> skip
#endif

#ifndef MCDC_ENTROPY_YES
#define MCDC_ENTROPY_YES        7.00     // H8 ≤ 7.0 bits/byte -> compress
#endif

#ifndef MCDC_PROBE_MIN_GAIN
#define MCDC_PROBE_MIN_GAIN     0.02     // ≥2% savings on sample -> compress
#endif

// Safe conservative upper bound: src + src/128 + 256
enum { MCDC_PROBE_DSTMAX = (int)(MCDC_SAMPLE_BYTES + (MCDC_SAMPLE_BYTES >> 7) + 256) };

bool is_likely_incompressible(const uint8_t *p, size_t n);

#ifdef __cplusplus
}
#endif
#endif
