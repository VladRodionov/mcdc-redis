[**Why MC/DC Exists**](https://github.com/VladRodionov/mcdc/wiki/Why-MC-DC-Exists-(with-personal-notes)) ⚡⚡⚡ [**Documentation**](https://github.com/VladRodionov/mcdc-redis/wiki/Quick-Start-Guide) ⚡⚡⚡ [**Forum**](https://github.com/VladRodionov/mcdc-redis/discussions)

# MC/DC — Memory Cache with Dictionary Compression

MC/DC is a high‑performance **Redis/Valkey module** that provides **transparent dictionary‑based compression** for string and hash values. It delivers dramatic memory savings (up to 4x in real datasets **compared to a client-side compression**) while preserving full compatibility with existing clients.

MC/DC is designed for teams running large caching workloads who want:

- Much **lower RAM costs**
- **Drop‑in compatibility** with Redis/Valkey
- **Fast** compression/decompression using Zstandard + per‑namespace dictionaries
- **Production‑grade safety** (replication-safe, async I/O and dictionary management)

---

## 1. What MC/DC Provides

MC/DC integrates advanced compression into existing caching environments without changing application code.

### Redis/Valkey Module Features

- Transparent compression for:
  - String commands (`GET`, `SET`, `MGET`, `MSET`, etc.)
  - Hash commands (`HGET`, `HSET`, `HMGET`, etc.)
- Async versions for large batch workloads (`mcdc.mgetasync`, `mcdc.hsetasync`, etc.)
- Dictionary‑based Zstandard compression with namespace‑aware training
- Automatic dictionary rotation + dictionary GC (Garbage Collection)
- Replication‑safe dictionary/metadata propagation to replicas
- Hot reload of dictionaries (`mcdc.reload` command)
- Efficient C‑tier implementation

---

## 2. How MC/DC Works

MC/DC introduces **Zstandard dictionary compression** with:

- Per‑namespace dictionaries
- Automatic online training (based on sampled values)
- External dictionary training support
- Dictionary persistence and safe distribution from master to replicas
- Async hot loading on replicas

All compression is transparent to applications. Applications continue using Redis/Valkey normally.
Read more about benefits of a dictionary compression: [Dictionary Compression 101](https://github.com/VladRodionov/mcdc#dictionary-compression-101)

---

## 3. Supported Commands

MC/DC rewrites Redis/Valkey commands internally (this feature is configurable) while preserving behavior.

For the full list of supported/unsupported Redis commands please refer to this [WiKi document](https://github.com/VladRodionov/mcdc-redis/wiki/MC-DC-Phase%E2%80%901-Command-Support-Matrix)

For the full list of MC/DC commands including Admin commands please refer to the [Quick Start Guide](https://github.com/VladRodionov/mcdc-redis/wiki/Quick-Start-Guide)

---

## 4. Performance

Memory usage and performance results for strings are published here:

### [**MC/DC Memory Usage Benchmark (WiKi)**](https://github.com/VladRodionov/mcdc-redis/wiki/Memory-usage-benchmark)

For typical workloads:

- **1.5x - 2.0x** memory reduction is common
- **up to 4x** on highly compressible values
- Decompression throughput is extremely fast (Zstd + optimizations)

>Note: We tested Redis + MC/DC module vs. Redis + client-side compression (what usually users do to save memory).
---

## 5. Architecture Overview

MC/DC has two major components:

### 1. Compression Engine

- Zstandard dictionary compression
- Per‑namespace dictionaries
- Comprehensive per-namepsace stats
- Dedicated GC, trainer and data sampler threads

### 2. Redis/Valkey Module Layer

- Command filter rewrites Redis commands to MC/DC equivalents
- Async blocked‑client engine (no module threads required)
- Replication‑safe dictionary/metadata propagation using HSET wrappers

---

## 6. Directory Layout

```
mcdc-redis/
├── src/                # C sources for the Redis module
├── include/            # Public + internal headers
├── deps/redis/         # Redis module API headers
├── test/
│   ├── smoke/          # C-level smoke tests
│   └── integration/    # Full pytest integration suite
├── build/              # Release build output
├── build-debug/        # Debug build output
├── build-tests/        # Smoke test binaries
├── mcdc.conf           # Module configuration file
└── README.md           # You are here
```

---

## 7. Build, Test, Install, Run

Please refer to this document for instructions: [Build, Test, Install, Run](https://github.com/VladRodionov/mcdc-redis/wiki/MC-DC-–-Build,-Test-&-Installation-Guide)

---

## 8. Redis/Valkey Compatibility

MC/DC is implemented as a Redis module and follows the stable Redis Modules API introduced in Redis 7.0.
It is compiled against redismodule.h from Redis 7.0, which guarantees broad compatibility.

Supported Versions:

- Redis	7.0+	Fully compatible with OSS Redis 7.x. Redis 8.0 requires AGPL but is technically compatible.
- Valkey	7.2.4+	Fully compatible with Valkey’s modules API.

> Contact us if you need support for Redis 6.x

Notes:
- MC/DC avoids Redis 8.0-only APIs, so it is expected to continue working on both branches.
- All functionality relies strictly on public Redis Modules API calls—no private server internals.
- If running mixed Redis/Valkey deployments, ensure module loading paths and configuration formats match.

---

## 9. [Redis / Valkey Cluster Compatibility](https://github.com/VladRodionov/mcdc-redis/wiki/Redis-Valkey-Cluster-Compatibility)

---

## 10. License

MC/DC is offered under a dual-license model:

1. MC/DC Community License (Default)
A source-available, permissive license intended for:
- individual developers
- startups and internal company use
- academic and research use
- commercial products that do not offer Redis/Valkey/Memcached-as-a-Service

Under the Community License you may:
- use MC/DC freely in your own applications and infrastructure
- modify it
- redistribute it inside your software
- deploy it in production
- ship commercial products that embed MC/DC

However, the Community License prohibits offering MC/DC (or any derivative, including forks) as part of a hosted Redis/Valkey/Memcached service, meaning:

You may not use MC/DC to provide Redis-as-a-Service, Valkey-as-a-Service, Memcached-as-a-Service, nor any similar managed/hosted caching service where MC/DC is a primary or enabling component.

(This is similar in spirit to SSPL/Elastic License, but significantly simpler and more lenient.)

2. Commercial License
For cloud providers or anyone wishing to:
- offer Redis/Valkey/Memcached with MC/DC as a hosted/public service
- embed MC/DC inside a broader managed data platform
- avoid the Community License restrictions

A commercial license removes all service restrictions.

Contact: vlad@trycarrots.io for details.

For details, see:

- `LICENSE`
- `LICENSE-COMMUNITY`
- `LICENSE-COMMERCIAL`
- `NOTICE` (Anti‑Cloud Notice)

---

## 11. Support

For commercial support or licensing: **Email:** `vlad@trycarrots.io`

---

## 12. Status

MC/DC is currently in **Developer Preview**. The API is stable, and production-hardening is ongoing.

If you'd like early-adopter support or want to try MC/DC at scale, reach out!

---
© 2025 Carrot Data, Inc. All rights reserved.




