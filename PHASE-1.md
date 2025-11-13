Phase 1 = Strings only, transparent server-side Zstd dictionary compression with a 2-byte dict-id prefix, selected by key-namespace (prefix). Here’s the command matrix so we know exactly what we do now vs later.

Assumptions / rules (Phase 1)
	•	Values are stored as opaque binary strings: [2-byte dictId][zstd frame] (or raw if disabled).
	•	Namespace = key prefix. Dict selection depends on the destination key’s namespace.
	•	Command Filter intercepts reads/writes and (when needed) decompresses → processes → recompresses.
	•	Cross-namespace moves (e.g., rename/copy to a different prefix) either need recompression or are disallowed in Phase 1 (see below).

⸻

A) Supported as-is (no value inspection needed)

Key metadata / lifetime / basic housekeeping:
	•	EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, TTL, PTTL, EXPIRETIME, PEXPIRETIME
	•	DEL, UNLINK
	•	EXISTS, TOUCH
	•	RANDOMKEY
	•	FLUSHDB, FLUSHALL
	•	TYPE, OBJECT (note: OBJECT ENCODING will reflect regular string encodings)

Transactions / scripting (operate via normal commands, no special handling):
	•	MULTI, EXEC, DISCARD, WATCH, UNWATCH
	•	EVAL, EVALSHA, FUNCTION, FCALL (works because underlying commands are intercepted)

⸻

B) Supported (string reads/writes) — Phase 1 core

We decompress on read, compress on write. All options preserved.

Reads:
	•	GET
	•	MGET
	•	GETEX (respects EX/PX/EXAT/PXAT/PERSIST modifiers)
	•	GETDEL

Writes:
	•	SET (supports NX|XX, EX|PX|EXAT|PXAT, KEEPTTL, GET)
	•	MSET
	•	SETNX
	•	GETSET (legacy; also covered via SET ... GET)

Numeric ops (decode → arithmetic → re-encode):
	•	INCR, INCRBY, INCRBYFLOAT, DECR, DECRBY
	•	Constraint: value must be a valid integer/float per Redis rules; otherwise same errors as Redis.

Notes
	•	Replication/AOF: OK. Replicates the post-compression value (no special work in Phase 1).
	•	Eviction/expiry: OK (value treated as normal string internally).

⸻

C) Supported with constraints (namespace safety)

These commands can cross namespaces by changing the key name. For Phase 1, we keep it simple:
	•	RENAME, RENAMENX
	•	Allowed only if the destination key stays in the same namespace (same prefix rule).
	•	If the destination prefix differs → return error (Phase 1).
Rationale: would require on-the-fly recompress; we’ll add that in a later phase.
	•	COPY
	•	Allowed only within the same namespace (no recompress needed).
	•	Different destination namespace → error (Phase 1).

(We’ll add cross-namespace recompress in Phase 2.)

⸻

D) Deferred (Phase 2/3) — works by full decode/encode but heavier / tricky

We can implement these by “decode → operate → encode”, but they’re less critical and some are multi-key/partial-range/bit-level:
	•	Partial string ops: APPEND, STRLEN, GETRANGE/SUBSTR, SETRANGE
	•	Bit ops: GETBIT, SETBIT, BITCOUNT, BITPOS, BITOP
	•	BITFIELD / BITFIELD_RO
	•	STRALGO LCS

Why deferred: partial/bit operations defeat streaming unless we fully materialize the value, update, then recompress. It’s correct but can be slower for large values; we’ll gate behind a config toggle and add block-wise optimizations later.

⸻

E) Unsupported in Phase 1 (blocked to avoid correctness pits)

These bypass normal value write paths or use serialized forms that we don’t reinterpret yet:
	•	DUMP, RESTORE (client-supplied RDB payload unknown to our module)
	•	MIGRATE (involves RDB serialization)
	•	COPY / RENAME across namespaces (as noted above → error in Phase 1)

We’ll add safe handling later (detect, decompress, re-encode to destination dict, or wrap server calls).

⸻

Edge-case notes
	•	Binary transparency: We must not break keys with raw (uncompressed) values. Plan: store an unambiguous 2-byte dict-id prefix where 0x0000 means “no dict” (raw), and non-zero is a dict selector. GET returns the original payload (decompressed) to clients.
	•	Memory overhead: The 2-byte prefix is part of the stored value; compression should more than compensate for it. (We’ll expose this in MCDC.STATS.)
	•	Errors/semantics: We mirror Redis behavior. If the stored (decompressed) value isn’t a valid integer/float for numeric ops, we return Redis’ standard errors.
	•	Pipelining/transactions: Fine (filter applies per command). WATCH keys behave as usual (we write back a new compressed value if modified).

⸻

TL;DR Matrix

Category	Commands
✅ Supported now	GET, MGET, GETEX, GETDEL, SET (all flags), MSET, SETNX, GETSET, INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT, EXPIRE family, DEL/UNLINK, EXISTS/TOUCH, TYPE/OBJECT, RANDOMKEY, FLUSHDB/FLUSHALL, MULTI/EXEC/EVAL/FCALL
⚠️ Supported with constraint	RENAME/RENAMENX (same-namespace only), COPY (same-namespace only)
⏳ Deferred	APPEND, STRLEN, GETRANGE/SUBSTR, SETRANGE, GETBIT/SETBIT/BITCOUNT/BITPOS/BITOP, BITFIELD, STRALGO LCS
⛔ Not in Phase 1	DUMP/RESTORE, MIGRATE, cross-namespace COPY/RENAME


⸻

If this looks good, I’ll:
	1.	add command filter hooks for the Phase-1 set,
	2.	implement the namespace guard for RENAME/COPY, and
	3.	stub the error paths for the unsupported commands so users get clear messages (with hints to enable in future versions).
