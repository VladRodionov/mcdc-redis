# MC/DC - Memory Cache with Dictionary Compression
# Copyright (c) 2025 Carrot Data Inc.
#
# Licensed under the MC/DC Community License.
# Neither this file nor derivative works may be used in any third-party
# Redis/Valkey/Memcached-as-a-Service product without a commercial license.
#
# Full terms: LICENSE-COMMUNITY.txt

import time

import pytest

from conftest import highly_compressible


# NOTE: these tests assume:
# - MC/DC filter treats keys with "mcdc:" prefix as MC/DC-managed
# - mcdc.cstrlen exists and returns compressed stored length (including header)


# ---------------------------------------------------------------------------
# Basic mcdc.set / mcdc.get / mcdc.cstrlen
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("val", [
    "hello",                       # small, not worth compressing
    highly_compressible(512, "X"), # large & compressible
])
def test_mcdc_set_get_roundtrip(r, val):
    key = "mcdc:string:basic"

    assert r.execute_command("mcdc.set", key, val) == "OK"
    got = r.execute_command("mcdc.get", key)
    assert got == val

    logical = int(r.strlen(key))
    cstored = int(r.execute_command("mcdc.cstrlen", key))

    if len(val) <= 32:
        # Too small for compression: stored size >= logical
        assert cstored >= logical
    else:
        # Large & compressible: expect compression
        assert cstored < logical


# ---------------------------------------------------------------------------
# Filter rewrite: SET / GET / STRLEN
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("val", [
    "short",
    highly_compressible(1024, "R"),
])
def test_filter_rewrites_set_get_strlen(r, val):
    key = "mcdc:filter:string:setget"

    # Plain Redis commands, should be rewritten by filter
    assert r.set(key, val)
    got = r.get(key)
    assert got == val

    logical = int(r.strlen(key))
    cstored = int(r.execute_command("mcdc.cstrlen", key))

    if len(val) <= 32:
        assert cstored >= logical
    else:
        assert cstored < logical


# ---------------------------------------------------------------------------
# GETEX / GETSET / GETDEL / SETEX / SETNX / PSETEX
# ---------------------------------------------------------------------------

def test_mcdc_getex_sets_ttl(r):
    key = "mcdc:string:getex"
    val = highly_compressible(512, "G")

    assert r.execute_command("mcdc.set", key, val) == "OK"

    got = r.execute_command("mcdc.getex", key, "EX", 10)
    assert got == val

    ttl = r.ttl(key)
    assert 1 <= ttl <= 10  # some small drift is OK


def test_filter_getex_sets_ttl(r):
    key = "mcdc:filter:string:getex"
    val = highly_compressible(512, "H")

    r.set(key, val)
    got = r.execute_command("GETEX", key, "EX", 10)
    assert got == val

    ttl = r.ttl(key)
    assert 1 <= ttl <= 10


def test_mcdc_getset_and_getdel(r):
    key = "mcdc:string:getsetdel"

    assert r.execute_command("mcdc.set", key, "old") == "OK"
    prev = r.execute_command("mcdc.getset", key, "new")
    assert prev == "old"
    assert r.execute_command("mcdc.get", key) == "new"

    # GETDEL returns current value and removes key
    val = r.execute_command("mcdc.getdel", key)
    assert val == "new"
    assert r.exists(key) == 0


def test_filter_getset_getdel(r):
    key = "mcdc:filter:string:getsetdel"

    r.set(key, "old")
    prev = r.execute_command("GETSET", key, "new")
    assert prev == "old"
    assert r.get(key) == "new"

    val = r.execute_command("GETDEL", key)
    assert val == "new"
    assert r.exists(key) == 0


def test_mcdc_setex_setnx_psetex(r):
    key1 = "mcdc:string:setex"
    key2 = "mcdc:string:setnx"
    key3 = "mcdc:string:psetex"

    # SETEX
    assert r.execute_command("mcdc.setex", key1, 5, "v1") == "OK"
    assert r.execute_command("mcdc.get", key1) == "v1"
    assert 1 <= r.ttl(key1) <= 5

    # SETNX
    res = r.execute_command("mcdc.setnx", key2, "first")
    assert res == 1
    res2 = r.execute_command("mcdc.setnx", key2, "second")
    assert res2 == 0
    assert r.execute_command("mcdc.get", key2) == "first"

    # PSETEX
    assert r.execute_command("mcdc.psetex", key3, 1000, "v3") == "OK"
    assert r.execute_command("mcdc.get", key3) == "v3"
    pttl = r.pttl(key3)
    assert 0 < pttl <= 1000


def test_filter_setex_setnx_psetex(r):
    key1 = "mcdc:filter:string:setex"
    key2 = "mcdc:filter:string:setnx"
    key3 = "mcdc:filter:string:psetex"

    r.execute_command("SETEX", key1, 5, "v1")
    assert r.get(key1) == "v1"
    assert 1 <= r.ttl(key1) <= 5

    assert r.setnx(key2, "first") == 1
    assert r.setnx(key2, "second") == 0
    assert r.get(key2) == "first"

    r.execute_command("PSETEX", key3, 1000, "v3")
    assert r.get(key3) == "v3"
    pttl = r.pttl(key3)
    assert 0 < pttl <= 1000


# ---------------------------------------------------------------------------
# MGET / MSET (module and filter)
# ---------------------------------------------------------------------------

def test_mcdc_mset_mget_compressible_mixed(r):
    keys_vals = {
        "mcdc:string:mset:small1": "foo",
        "mcdc:string:mset:small2": "bar",
        "mcdc:string:mset:large1": highly_compressible(1024, "Z"),
        "mcdc:string:mset:large2": highly_compressible(2048, "Q"),
    }

    args = []
    for k, v in keys_vals.items():
        args.append(k)
        args.append(v)

    # module command
    res = r.execute_command("mcdc.mset", *args)
    assert res in ("OK", "QUEUED")  # depending on async plumbing

    vals = r.execute_command("mcdc.mget", *keys_vals.keys())
    assert vals == list(keys_vals.values())

    # Check at least one is compressed
    logical = r.strlen("mcdc:string:mset:large1")
    cstored = int(r.execute_command("mcdc.cstrlen", "mcdc:string:mset:large1"))
    assert logical > 32
    assert cstored < logical


def test_filter_mset_mget(r):
    keys_vals = {
        "mcdc:filter:string:mset:small": "hello",
        "mcdc:filter:string:mset:large": highly_compressible(2048, "L"),
    }

    # Plain MSET
    args = []
    for k, v in keys_vals.items():
        args.append(k)
        args.append(v)
    r.execute_command("MSET", *args)

    vals = r.execute_command("MGET", *keys_vals.keys())
    assert vals == list(keys_vals.values())

    logical = r.strlen("mcdc:filter:string:mset:large")
    cstored = int(r.execute_command("mcdc.cstrlen", "mcdc:filter:string:mset:large"))
    assert logical > 32
    assert cstored < logical


# ---------------------------------------------------------------------------
# Unsupported string wrappers: APPEND / GETRANGE / SETRANGE
# (They downgrade compressed values to raw, then delegate.)
# ---------------------------------------------------------------------------

def test_mcdc_append_getrange_setrange(r):
    key = "mcdc:string:append"
    base = highly_compressible(256, "A")
    assert r.execute_command("mcdc.set", key, base) == "OK"

    # APPEND (should downgrade if compressed, then append)
    added = r.execute_command("mcdc.append", key, "TAIL")
    assert added == len(base) + len("TAIL")

    val = r.execute_command("mcdc.get", key)
    assert val == base + "TAIL"

    # GETRANGE
    sub = r.execute_command("mcdc.getrange", key, 0, 3)
    assert sub == (base + "TAIL")[0:4]

    # SETRANGE
    reslen = r.execute_command("mcdc.setrange", key, 0, "HEAD")
    assert reslen == len(base) + len("TAIL")
    val2 = r.execute_command("mcdc.get", key)
    assert val2.startswith("HEAD")


def test_filter_append_getrange_setrange(r):
    key = "mcdc:filter:string:append"
    base = highly_compressible(256, "B")
    r.set(key, base)

    added = r.execute_command("APPEND", key, "TAIL")
    assert added == len(base) + len("TAIL")

    sub = r.execute_command("GETRANGE", key, 0, 3)
    assert sub == (base + "TAIL")[0:4]

    reslen = r.execute_command("SETRANGE", key, 0, "HEAD")
    assert reslen == len(base) + len("TAIL")
    val2 = r.get(key)
    assert val2.startswith("HEAD")

def test_mcdc_msetasync_matches_mset_and_writes(r):
    """
    mcdc.msetasync should:
      - return the same kind of reply as mcdc.mset
      - write the same data as mcdc.mset
    """
    keys_vals = {
        "mcdc:string:async:mset:1": "v1",
        "mcdc:string:async:mset:2": highly_compressible(128, "X"),
    }

    args = []
    for k, v in keys_vals.items():
        args.append(k)
        args.append(v)

    # Baseline: sync mset behavior
    r.delete(*keys_vals.keys())
    res_sync = r.execute_command("mcdc.mset", *args)
    vals_sync = r.execute_command("mcdc.mget", *keys_vals.keys())
    assert vals_sync == list(keys_vals.values())

    # Now async variant on fresh keys
    r.delete(*keys_vals.keys())
    res_async = r.execute_command("mcdc.msetasync", *args)
    vals_async = r.execute_command("mcdc.mget", *keys_vals.keys())

    # Return value must be consistent with sync version
    assert type(res_async) is type(res_sync)
    assert res_async == res_sync

    # Data must be identical
    assert vals_async == list(keys_vals.values())


def test_mcdc_mgetasync_matches_mget(r):
    """
    mcdc.mgetasync should:
      - return same data as mcdc.mget
    """
    keys_vals = {
        "mcdc:string:async:mget:1": "foo",
        "mcdc:string:async:mget:2": highly_compressible(256, "Y"),
        "mcdc:string:async:mget:3": "bar",
    }

    # Prepare data via sync path
    args = []
    for k, v in keys_vals.items():
        args.append(k)
        args.append(v)

    r.delete(*keys_vals.keys())
    r.execute_command("mcdc.mset", *args)

    res_sync = r.execute_command("mcdc.mget", *keys_vals.keys())
    res_async = r.execute_command("mcdc.mgetasync", *keys_vals.keys())

    # Both must return the same list of values
    assert res_sync == list(keys_vals.values())
    assert res_async == list(keys_vals.values())
