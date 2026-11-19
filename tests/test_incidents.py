"""E2E tests for fill ids and the failed-fill incident log.

Reuses the real origin/proxy subprocess fixtures and helpers from
``test_e2e.py``. Run from the repo root:

    python3 -m pytest tests/test_incidents.py -v
"""

from __future__ import annotations

import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

from test_e2e import (
    MIB,
    PKG_SMALL,
    ProxyRunner,
    client,
    free_port,
    origin_reset,
    set_behavior,
    start_server,
    stop,
)

FILL_ID_RE = re.compile(r"\A[0-9a-f]{32}\Z")


def incidents(base: str) -> dict:
    with client() as c:
        return c.get(f"{base}/cache/incidents").json()


def set_fail_status(origin_url: str, status: int | None) -> None:
    with client() as c:
        r = c.post(
            f"{origin_url}/control/behavior",
            json={
                "chunk_delay_ms": 0,
                "truncate_after_bytes": None,
                "chunk_size": 256 * 1024,
                "fail_status": status,
            },
        )
        r.raise_for_status()


def failing_stream_fill_id(base: str, path: str) -> str:
    """Stream a package whose fill dies mid-body; return its X-Fill-ID."""
    with client().stream("GET", f"{base}/packages/{path}") as r:
        fill_id = r.headers["x-fill-id"]
        with pytest.raises(httpx.HTTPError):
            for _ in r.iter_bytes(64 * 1024):
                pass
    return fill_id


# --------------------------------------------------------------------- #
# X-Fill-ID header                                                      #
# --------------------------------------------------------------------- #

def test_fill_id_on_miss_hit_and_new_generation_after_purge(proxy, origin):
    origin_reset(origin.url)
    with client() as c:
        r1 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert r1.status_code == 200
        id1 = r1.headers["x-fill-id"]
        assert FILL_ID_RE.match(id1)
        assert r1.headers["x-cache"] == "MISS"

        r2 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert r2.status_code == 200
        assert r2.headers["x-fill-id"] == id1  # same completed generation
        assert r2.headers["x-cache"] == "HIT"

        c.post(f"{proxy.url}/purge/{PKG_SMALL}")
        r3 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert r3.status_code == 200
        id3 = r3.headers["x-fill-id"]
        assert FILL_ID_RE.match(id3) and id3 != id1  # new fill, new id


def test_fill_id_on_pre_body_error_responses(proxy, origin):
    set_fail_status(origin.url, 503)
    try:
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert r.status_code == 503
        fill_id = r.headers["x-fill-id"]
        assert FILL_ID_RE.match(fill_id)
    finally:
        set_fail_status(origin.url, None)

    # The incident is retrievable by its fill id.
    with client() as c:
        detail = c.get(f"{proxy.url}/cache/incidents/{fill_id}").json()
    assert detail["fill_id"] == fill_id
    assert detail["path"] == PKG_SMALL
    assert detail["reason"] == "origin_status"
    assert detail["origin_status"] == 503
    assert detail["bytes_written"] == 0
    assert detail["error"]

    # A later successful fill is a different generation and does not overwrite.
    with client() as c:
        ok = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert ok.status_code == 200
    assert ok.headers["x-fill-id"] != fill_id
    with client() as c:
        again = c.get(f"{proxy.url}/cache/incidents/{fill_id}")
    assert again.status_code == 200 and again.json()["reason"] == "origin_status"


def test_404_carries_fill_id_but_is_not_an_incident(proxy, origin):
    origin_reset(origin.url)
    with client() as c:
        r1 = c.get(f"{proxy.url}/packages/ghost.bin")
        r2 = c.get(f"{proxy.url}/packages/ghost.bin")
    assert r1.status_code == 404 and r2.status_code == 404
    id1, id2 = r1.headers["x-fill-id"], r2.headers["x-fill-id"]
    assert FILL_ID_RE.match(id1) and FILL_ID_RE.match(id2)
    assert id1 != id2  # each 404 is a fresh fill attempt
    assert incidents(proxy.url)["count"] == 0


# --------------------------------------------------------------------- #
# classification / single-flight accounting                             #
# --------------------------------------------------------------------- #

def test_truncated_transport_one_incident_with_written_bytes(proxy, origin):
    set_behavior(origin.url, delay_ms=20, chunk_size=64 * 1024, truncate=200_000)
    try:
        origin_reset(origin.url)
        fill_id = failing_stream_fill_id(proxy.url, PKG_SMALL)

        log = incidents(proxy.url)
        assert log["count"] == 1
        row = log["incidents"][0]
        assert row["fill_id"] == fill_id
        assert row["path"] == PKG_SMALL
        assert row["reason"] == "transport"
        assert row["origin_status"] == 200  # headers arrived, body died
        assert row["bytes_written"] == 200_000
        assert row["error"]

        # Half package never reaches the cache volume.
        assert list(proxy.cache_dir.glob(f"{PKG_SMALL}*")) == []
    finally:
        set_behavior(origin.url)

    # Retry succeeds; success neither adds nor removes incident rows.
    with client() as c:
        ok = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert ok.status_code == 200 and len(ok.content) == MIB
    log = incidents(proxy.url)
    assert log["count"] == 1 and log["incidents"][0]["reason"] == "transport"


def test_single_flight_failure_shared_fill_id_single_record(proxy, origin):
    set_behavior(origin.url, delay_ms=60, chunk_size=64 * 1024, truncate=120_000)
    try:
        origin_reset(origin.url)
        ids: list[str] = []
        lock = threading.Lock()
        barrier = threading.Barrier(6)

        def worker() -> None:
            barrier.wait()  # all six requests open at the same instant
            try:
                fid = failing_stream_fill_id(proxy.url, PKG_SMALL)
            except Exception:
                return
            with lock:
                ids.append(fid)

        with ThreadPoolExecutor(6) as ex:
            list(ex.map(lambda _: worker(), range(6)))

        assert len(ids) == 6
        assert len(set(ids)) == 1  # one single-flight fill, one shared id
        log = incidents(proxy.url)
        assert log["count"] == 1
        assert log["incidents"][0]["fill_id"] == ids[0]
        assert log["incidents"][0]["bytes_written"] == 120_000
    finally:
        set_behavior(origin.url)


def test_client_disconnect_is_not_an_incident(proxy, origin):
    # Slow origin: client leaves after the first chunk; the fill itself
    # continues to completion and must not be recorded as a failure.
    set_behavior(origin.url, delay_ms=100, chunk_size=256 * 1024)
    try:
        with client().stream(
            "GET", f"{proxy.url}/packages/{PKG_SMALL}"
        ) as r:
            assert r.status_code == 200
            assert FILL_ID_RE.match(r.headers["x-fill-id"])
            assert next(r.iter_bytes(64 * 1024))
        # Exiting the context closes the client connection.

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            with client() as c:
                status = c.get(f"{proxy.url}/cache/status").json()
            if status.get(PKG_SMALL, {}).get("state") == "complete":
                break
            time.sleep(0.2)
        else:
            pytest.fail("fill did not complete after client disconnected")

        assert incidents(proxy.url)["count"] == 0
        with client() as c:
            c.post(f"{proxy.url}/purge/{PKG_SMALL}")
    finally:
        set_behavior(origin.url)


def test_purge_does_not_touch_incident_history(proxy, origin):
    set_fail_status(origin.url, 500)
    try:
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert r.status_code == 500
        fill_id = r.headers["x-fill-id"]
    finally:
        set_fail_status(origin.url, None)

    with client() as c:
        c.post(f"{proxy.url}/purge/{PKG_SMALL}")
    with client() as c:
        detail = c.get(f"{proxy.url}/cache/incidents/{fill_id}")
    assert detail.status_code == 200
    assert detail.json()["path"] == PKG_SMALL


# --------------------------------------------------------------------- #
# retention / local write / endpoint shape                              #
# --------------------------------------------------------------------- #

def test_incident_retention_is_50_and_limit_query_works(proxy, origin):
    set_fail_status(origin.url, 500)
    try:
        first_id = None
        last_id = None
        for i in range(51):
            with client() as c:
                r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            assert r.status_code == 500
            if i == 0:
                first_id = r.headers["x-fill-id"]
            last_id = r.headers["x-fill-id"]

        log = incidents(proxy.url)
        assert log["count"] == 50
        assert log["max_retained"] == 50
        ids = [row["fill_id"] for row in log["incidents"]]
        assert ids[0] == last_id  # newest first
        assert first_id not in ids  # oldest evicted
        with client() as c:
            assert c.get(f"{proxy.url}/cache/incidents/{first_id}").status_code == 404
            assert c.get(f"{proxy.url}/cache/incidents/{last_id}").status_code == 200
            limited = c.get(f"{proxy.url}/cache/incidents?limit=5").json()
        assert limited["count"] == 5
        assert limited["incidents"][0]["fill_id"] == last_id
    finally:
        set_fail_status(origin.url, None)

    # Successful new fill does not enter or alter the incident log.
    with client() as c:
        ok = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert ok.status_code == 200
    assert incidents(proxy.url)["count"] == 50


def test_unknown_and_malformed_fill_id_404(proxy):
    with client() as c:
        assert c.get(f"{proxy.url}/cache/incidents/{'a' * 32}").status_code == 404
        assert c.get(f"{proxy.url}/cache/incidents/not-a-fill-id").status_code == 404


def test_local_write_failure_is_classified(proxy, origin, tmp_path):
    """A read-only cache volume makes the part-file open fail."""
    cache_dir = tmp_path / "readonly-cache"
    cache_dir.mkdir()
    os.chmod(cache_dir, 0o555)
    log = tmp_path / "ro-proxy.log"
    control = tmp_path / "ro-proxy.stop"
    port = free_port()
    try:
        proc = start_server(
            "proxy",
            port,
            {
                "ORIGIN_BASE_URL": f"http://127.0.0.1:{proxy.origin_port}",
                "CACHE_DIR": str(cache_dir),
            },
            log,
            control,
        )
        base = f"http://127.0.0.1:{port}"
        try:
            origin_reset(origin.url)
            with client() as c:
                r = c.get(f"{base}/packages/{PKG_SMALL}")
            assert r.status_code == 502
            fill_id = r.headers["x-fill-id"]
            assert FILL_ID_RE.match(fill_id)

            with client() as c:
                row = c.get(f"{base}/cache/incidents/{fill_id}").json()
            assert row["reason"] == "local_write"
            assert row["bytes_written"] == 0
            assert row["path"] == PKG_SMALL
        finally:
            stop(proc, control)
    finally:
        os.chmod(cache_dir, 0o755)
