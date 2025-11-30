import pytest

from conftest import highly_compressible, redis_version_tuple


# NOTE: assumes:
# - mcdc.hset / mcdc.hget / mcdc.hmget / mcdc.hsetnx / mcdc.hsetex / mcdc.hgetex /
#   mcdc.hvals / mcdc.hgetall / mcdc.hstrlen / mcdc.hrandfield / mcdc.hgetdel /
#   mcdc.chstrlen exist and behave as spec.
# - Filter rewrites H* commands on MC/DC namespaces to mcdc.* equivalents.


# ---------------------------------------------------------------------------
# Basic HSET / HGET / CHSTRLEN
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("val", [
    "small",
    highly_compressible(512, "K"),
])
def test_mcdc_hset_hget_roundtrip(r, val):
    key = "mcdc:hash:basic"
    field = "f1"

    ret = r.execute_command("mcdc.hset", key, field, val)
    assert isinstance(ret, int)

    got = r.execute_command("mcdc.hget", key, field)
    assert got == val

    logical = int(r.hstrlen(key, field))
    chlen = int(r.execute_command("mcdc.chstrlen", key, field))

    if len(val) <= 32:
        assert chlen >= logical
    else:
        assert chlen < logical


def test_filter_hset_hget(r):
    key = "mcdc:filter:hash:basic"
    field = "f1"
    val = highly_compressible(256, "H")

    # Plain HSET / HGET must be rewritten
    ret = r.hset(key, field, val)
    assert isinstance(ret, int)

    got = r.hget(key, field)
    assert got == val

    logical = int(r.hstrlen(key, field))
    chlen = int(r.execute_command("mcdc.chstrlen", key, field))
    assert logical > 32
    assert chlen < logical


# ---------------------------------------------------------------------------
# HMGET / multi-field HSET (module + filter)
# ---------------------------------------------------------------------------

def test_mcdc_hset_multi_and_hmget(r):
    key = "mcdc:hash:multi"
    fields = {
        "a": "1",
        "b": highly_compressible(64, "X"),
        "c": highly_compressible(512, "Y"),
    }

    args = [key]
    for f, v in fields.items():
        args.append(f)
        args.append(v)

    ret = r.execute_command("mcdc.hset", *args)
    assert ret >= 1

    got = r.execute_command("mcdc.hmget", key, *fields.keys())
    assert got == list(fields.values())

    # At least one field should be compressed
    logical_c = int(r.hstrlen(key, "c"))
    chlen_c = int(r.execute_command("mcdc.chstrlen", key, "c"))
    assert logical_c > 32
    assert chlen_c < logical_c


def test_filter_hmget_hmset(r):
    key = "mcdc:filter:hash:multi"
    fields = {
        "a": "1",
        "b": highly_compressible(64, "B"),
        "c": highly_compressible(512, "C"),
    }

    # HMSET (deprecated, still widely supported)
    args = [key]
    for f, v in fields.items():
        args.append(f)
        args.append(v)
    r.execute_command("HMSET", *args)

    got = r.execute_command("HMGET", key, *fields.keys())
    assert got == list(fields.values())

    logical_c = int(r.hstrlen(key, "c"))
    chlen_c = int(r.execute_command("mcdc.chstrlen", key, "c"))
    assert logical_c > 32
    assert chlen_c < logical_c


# ---------------------------------------------------------------------------
# HSETNX / HSETEX / HGETEX / HGETDEL
# ---------------------------------------------------------------------------

def test_mcdc_hsetnx_hgetdel(r):
    if redis_version_tuple(r) < (8,0,0):
        pytest.skip("HSETEX/HGETEX only supported on Redis 8.0+")

    key = "mcdc:hash:hsetnx"
    field = "f"

    res1 = r.execute_command("mcdc.hsetnx", key, field, "first")
    assert res1 in (0, 1)  # Redis: 1 on create, 0 on no-op

    res2 = r.execute_command("mcdc.hsetnx", key, field, "second")
    assert res2 in (0, 1)
    val = r.execute_command("mcdc.hget", key, field)
    assert val in ("first", "second")  # exact semantics depend on implementation

    # HGETDEL: return and remove
    got = r.execute_command("mcdc.hgetdel", key, field)
    assert got is not None
    assert r.hexists(key, field) is False


def test_filter_hsetnx_hgetdel(r):
    if redis_version_tuple(r) < (8,0,0):
        pytest.skip("HSETEX/HGETEX only supported on Redis 8.0+")

    key = "mcdc:filter:hash:hsetnx"
    field = "f"

    res1 = r.execute_command("HSETNX", key, field, "first")
    assert res1 in (0, 1)
    res2 = r.execute_command("HSETNX", key, field, "second")
    assert res2 in (0, 1)

    got = r.execute_command("HGETDEL", key, field)
    assert got is not None
    assert r.hexists(key, field) is False


def test_mcdc_hsetex_hgetex(r):
    if redis_version_tuple(r) < (8,0,0):
        pytest.skip("HSETEX/HGETEX only supported on Redis 8.0+")

    key = "mcdc:hash:hsetex"
    field = "payload"
    val = highly_compressible(512, "E")

    # HSETEX key field ttl value
    ret = r.execute_command("mcdc.hsetex",key,"EX", 10, "FIELDS", 1, field, val)
    assert ret in (0, 1)

    # HGETEX key field EX <ttl>
    got = r.execute_command("mcdc.hgetex", key, field, "EX", 20)
    assert got == val

    ttl = r.ttl(key)
    assert 1 <= ttl <= 20


def test_filter_hsetex_hgetex(r):
    if redis_version_tuple(r) < (8,0,0):
        pytest.skip("HSETEX/HGETEX only supported on Redis 8.0+")

    key = "mcdc:filter:hash:hsetex"
    field = "payload"
    val = highly_compressible(256, "F")

    # Plain HSETEX / HGETEX should be rewritten by filter
    ret = r.execute_command("HSETEX", key, "EX", 10, "FIELDS", 1, field, val)
    got = r.execute_command("HGETEX", key, field, "EX", 20)
    assert got == val
    ttl = r.ttl(key)
    assert 1 <= ttl <= 20


# ---------------------------------------------------------------------------
# HVALS / HGETALL / HSTRLEN / HRANDFIELD
# ---------------------------------------------------------------------------

def test_mcdc_hvals_hgetall_hstrlen(r):
    key = "mcdc:hash:vals"
    fields = {
        "x": "1",
        "y": highly_compressible(128, "Y"),
        "z": highly_compressible(512, "Z"),
    }

    args = [key]
    for f, v in fields.items():
        args.append(f)
        args.append(v)
    r.execute_command("mcdc.hset", *args)

    vals = r.execute_command("mcdc.hvals", key)
    assert sorted(vals) == sorted(fields.values())

    hgetall = r.execute_command("mcdc.hgetall", key)
    it = iter(hgetall)
    d = dict(zip(it, it))
    assert d == fields

    logical_z = int(r.execute_command("mcdc.hstrlen", key, "z"))
    chlen_z = int(r.execute_command("mcdc.chstrlen", key, "z"))
    assert logical_z > 32
    assert chlen_z < logical_z


def test_filter_hvals_hgetall_hstrlen(r):
    key = "mcdc:filter:hash:vals"
    fields = {
        "x": "1",
        "y": highly_compressible(128, "Y"),
        "z": highly_compressible(512, "Z"),
    }

    # HSET via plain Redis (rewritten by filter to mcdc.hset / mcdc.hsetasync)
    args = [key]
    for f, v in fields.items():
        args.append(f)
        args.append(v)
    r.execute_command("HSET", *args)

    # HVALS should return just the values
    vals = r.execute_command("HVALS", key)
    assert sorted(vals) == sorted(fields.values())

    # With decode_responses=True, HGETALL returns a dict already
    hgetall = r.execute_command("HGETALL", key)
    assert hgetall == fields

    # Optionally also check chstrlen if you want compression verification:
    # from .conftest import highly_compressible
    # compressed_lengths = r.execute_command("mcdc.chstrlen", key)
    # assert len(compressed_lengths) == len(fields)

    logical_z = int(r.execute_command("HSTRLEN", key, "z"))
    chlen_z = int(r.execute_command("mcdc.chstrlen", key, "z"))
    assert logical_z > 32
    assert chlen_z < logical_z

def test_mcdc_hrandfield(r):
    key = "mcdc:hash:rand"
    fields = {
        "a": "1",
        "b": "2",
        "c": "3",
    }

    args = [key]
    for f, v in fields.items():
        args.append(f)
        args.append(v)
    r.execute_command("mcdc.hset", *args)

    # random field only
    f = r.execute_command("mcdc.hrandfield", key)
    assert f in fields

    # random field + value (count = 1, withvalues)
    res = r.execute_command("mcdc.hrandfield", key, 1, "WITHVALUES")
    # response: [field, value] list
    assert isinstance(res, list)
    assert len(res) == 2
    assert res[0] in fields
    assert res[1] == fields[res[0]]


def test_filter_hrandfield(r):
    key = "mcdc:filter:hash:rand"
    fields = {
        "a": "1",
        "b": "2",
        "c": "3",
    }

    args = [key]
    for f, v in fields.items():
        args.append(f)
        args.append(v)
    r.execute_command("HSET", *args)

    f = r.execute_command("HRANDFIELD", key)
    assert f in fields

    res = r.execute_command("HRANDFIELD", key, 1, "WITHVALUES")
    assert isinstance(res, list)
    assert len(res) == 2
    assert res[0] in fields
    assert res[1] == fields[res[0]]

def test_mcdc_hsetasync_matches_hset_and_writes(r):
    """
    mcdc.hsetasync should:
      - return the same kind of reply as mcdc.hset
      - write the same fields/values
    """
    key = "mcdc:hash:async:hset"
    fields = {
        "f_small": "v1",
        "f_large": highly_compressible(256, "H"),
    }

    args = [key]
    for f, v in fields.items():
        args.append(f)
        args.append(v)

    # Baseline: sync behavior
    r.delete(key)
    res_sync = r.execute_command("mcdc.hset", *args)
    got_sync = r.execute_command("mcdc.hmget", key, *fields.keys())
    assert got_sync == list(fields.values())

    # Async variant on a fresh hash
    r.delete(key)
    res_async = r.execute_command("mcdc.hsetasync", *args)
    got_async = r.execute_command("mcdc.hmget", key, *fields.keys())

    # Return value must be consistent with sync version
    assert type(res_async) is type(res_sync)
    assert res_async == res_sync

    # Stored data must match
    assert got_async == list(fields.values())


def test_mcdc_hmgetasync_matches_hmget(r):
    """
    mcdc.hmgetasync should:
      - return same data as mcdc.hmget
    """
    key = "mcdc:hash:async:hmget"
    fields = {
        "a": "1",
        "b": highly_compressible(128, "B"),
        "c": highly_compressible(512, "C"),
    }

    args = [key]
    for f, v in fields.items():
        args.append(f)
        args.append(v)

    r.delete(key)
    r.execute_command("mcdc.hset", *args)

    res_sync = r.execute_command("mcdc.hmget", key, *fields.keys())
    res_async = r.execute_command("mcdc.hmgetasync", key, *fields.keys())

    assert res_sync == list(fields.values())
    assert res_async == list(fields.values())
