import os
import time
import socket
import subprocess
import tempfile
import shutil

import pytest
import redis

# --------------------------------------------------------------------------------------
# Paths (always absolute, based on repo root)
# --------------------------------------------------------------------------------------

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT = os.path.abspath(os.path.join(THIS_DIR, "..", ".."))

# Allow overriding via env, but normalize to absolute
_raw_module_path = os.environ.get("MCDC_MODULE_PATH", os.path.join(REPO_ROOT, "build", "mcdc.so"))
MCDC_MODULE_PATH = os.path.abspath(_raw_module_path)

REDIS_SERVER = os.environ.get("REDIS_SERVER", "redis-server")

# Absolute path to mcdc.conf (lives in test/integration/mcdc.conf)
MCDC_CONF_PATH = os.path.join(THIS_DIR, "mcdc.conf")
MCDC_CONF_PATH = os.path.abspath(MCDC_CONF_PATH)


def _get_free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    addr, port = s.getsockname()
    s.close()
    return port


class RedisServer:
    def __init__(self, port, module_path, mcdc_conf_path):
        self.port = port
        self.module_path = module_path
        self.mcdc_conf_path = mcdc_conf_path
        self.proc = None
        self.tmpdir = None

    def start(self):
        self.tmpdir = tempfile.mkdtemp(prefix="mcdc-redis-test-")
        conf_path = os.path.join(self.tmpdir, "redis.conf")

        with open(conf_path, "w") as f:
            f.write(f"""
port {self.port}
bind 127.0.0.1
save ""
appendonly no
dir {self.tmpdir}
logfile ""
""")

        # cfg=<ABSOLUTE PATH TO mcdc.conf>
        mcdc_cfg_arg = f"cfg={self.mcdc_conf_path}"

        cmd = [
            REDIS_SERVER,
            conf_path,
            "--loadmodule", self.module_path, mcdc_cfg_arg,
        ]

        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        client = redis.Redis(host="127.0.0.1", port=self.port, decode_responses=True)
        deadline = time.time() + 10.0
        last_exc = None
        last_output = []

        # Wait for Redis to come up
        while time.time() < deadline:
            try:
                if client.ping():
                    return
            except Exception as e:
                last_exc = e
            # Drain any output for better error messages
            if self.proc and self.proc.poll() is not None:
                last_output.extend(self.proc.stdout.readlines())
                break
            time.sleep(0.05)

        # If we’re here – startup failed
        output = ""
        if self.proc and self.proc.stdout:
            try:
                last_output.extend(self.proc.stdout.readlines())
            except Exception:
                pass
            output = "".join(last_output)

        self.stop()
        raise RuntimeError(
            "Redis did not start:\n"
            f"  last exception: {last_exc}\n"
            f"  command: {' '.join(cmd)}\n"
            f"  output:\n{output}"
        )

    def stop(self):
        if self.proc is not None:
            try:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
            except OSError:
                pass
            self.proc = None
        if self.tmpdir and os.path.isdir(self.tmpdir):
            shutil.rmtree(self.tmpdir, ignore_errors=True)


@pytest.fixture(scope="session")
def redis_server():
    if not os.path.exists(MCDC_MODULE_PATH):
        raise RuntimeError(f"MC/DC module not found at {MCDC_MODULE_PATH}")
    if not os.path.exists(MCDC_CONF_PATH):
        raise RuntimeError(f"MC/DC config not found at {MCDC_CONF_PATH}")

    port = _get_free_port()
    srv = RedisServer(port, MCDC_MODULE_PATH, MCDC_CONF_PATH)
    srv.start()
    try:
        yield {"port": port}
    finally:
        srv.stop()


@pytest.fixture
def r(redis_server):
    """Redis client fixture (flushes DB per test)."""
    port = redis_server["port"]
    client = redis.Redis(host="127.0.0.1", port=port, decode_responses=True)
    client.flushall()
    return client

import pytest

def redis_version_tuple(r):
    info = r.info()
    ver = info["redis_version"]
    parts = ver.split(".")
    # pad to (major, minor, patch)
    parts += ["0"] * (3 - len(parts))
    return tuple(int(p) for p in parts[:3])

def highly_compressible(size=256, ch="A"):
    return ch * size
