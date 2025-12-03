# MC/DC — Memory Cache with Dictionary Compression

MC/DC is a high‑performance **Redis/Valkey module** that provides **transparent dictionary‑based compression** for string and hash values. It delivers dramatic memory savings (up to 4x in real datasets) while preserving full compatibility with existing clients.

MC/DC is designed for teams running large caching workloads who want:

- Much **lower RAM costs**
- **Drop‑in compatibility** with Redis/Valkey and Memcached
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
- Dictionary persistence
- Async hot loading on replicas

All compression is transparent to applications. Applications continue using Redis/Valkey normally.

---

## 3. Supported Commands

MC/DC rewrites Redis/Valkey commands internally (this feature is configurable) while preserving behavior.

### String Commands

- `GET`, `SET`, `SETEX`, `SETNX`
- `GETEX`, `GETSET`, `GETDEL`
- `MGET`, `MSET`
- `STRLEN`
- Unsupported but wrapped: `APPEND`, `GETRANGE`, `SETRANGE`

### Hash Commands

- `HGET`, `HSET`, `HMGET`, `HMSET`
- `HSETNX`, `HSETEX`, `HGETEX`
- `HVALS`, `HGETALL`, `HSTRLEN`
- `HRANDFIELD`, `HGETDEL`

### Async Commands

- `mcdc.mgetasync`
- `mcdc.msetasync`
- `mcdc.hmgetasync`
- `mcdc.hsetasync`

### Admin Commands

- `mcdc.reload` — hot reload dictionaries/config
- `mcdc.stats` — compression & dictionary stats
- `mcdc.config` - get current MC/DC configuration
- `mcdc.ns` - get current list of supported namespaces
- `mcdc.sampler` - start/stop/ get status of data sampling

### MC/DC Internal Commands

- `mcdc.ld` / `mcdc.lm` — replica dictionary & manifest hydration
- Metadata stored under `mcdc:dict:*`

---

## 4. Performance

Memory usage and preformance results for strings are published here:

### 👉 [**MC/DC Memory Usage Benchmark (WiKi)**](https://github.com/VladRodionov/mcdc-redis/wiki/Memory-usage-benchmark)

For typical workloads:

- **1.5x - 2.0x** memory reduction is common
- **up to 4x** on highly compressible values
- Decompression throughput is extremely fast (Zstd + optimizations)

>Note: We tested Redis + MC/DC module vs. Redis + client-side compression (what usually users do to save memory).
---

## 5. Architecture Overview

MC/DC has three major components:

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

## 7. Build, Test, Install, Run

Please refer to this document for instructions: [Build, Test, Install, Run](https://github.com/VladRodionov/mcdc-redis/wiki/MC-DC-–-Build,-Test-&-Installation-Guide)

---

## 8. License

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

## 9. Support

For commercial support or licensing: **Email:** `vlad@trycarrots.io`

---

## 10. Status

MC/DC is currently in **Developer Preview**. The API is stable, and production-hardening is ongoing.

If you'd like early-adopter support or want to try MC/DC at scale, reach out!


