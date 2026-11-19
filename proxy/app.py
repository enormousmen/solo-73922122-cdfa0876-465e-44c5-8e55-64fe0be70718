"""Single-process HTTP caching proxy for fixed-origin package downloads.

FastAPI (HTTP surface) + aiohttp (origin client). One uvicorn worker, all
state in this process; completed packages live on disk under CACHE_DIR.

Design notes
------------
* Single-flight: one fill task per path generation; concurrent misses attach
  to the same fill and read the growing ``.part-<fill_id>`` file from offset 0.
* Every origin fill gets a unique ``X-Fill-ID``. The header is returned on
  every download response participating in that fill, including error
  responses produced before the response body starts.
* Failed fills are classified (``origin_status`` / ``transport`` /
  ``local_write``) and recorded in an in-process incident log; the last 50
  failed fills are queryable at ``GET /cache/incidents``. One single-flight
  fill records exactly one incident. Origin 404 and client disconnects are
  not fill failures and are not recorded.
* Slow clients never block the fill or other clients: the fill only writes to
  disk; every reader tails the file at its own pace. Per-client memory is one
  read buffer, independent of package size.
* Purge unregisters the current generation. In-flight fills keep feeding
  their existing readers but are never re-registered; old files are unlinked
  once the last reader detaches. Incident records are immutable and survive
  purges; a later successful fill gets a new fill id and never overwrites an
  old record.
* Origin failure fails every attached download and never leaves a cached
  half-package. Startup deletes leftover ``.part-*`` files and keeps complete
  packages.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

ORIGIN_BASE_URL = os.environ.get("ORIGIN_BASE_URL", "http://127.0.0.1:9000").rstrip("/")
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "./cache")).resolve()
CHUNK = 256 * 1024  # bytes per origin read / client write / disk read
MAX_INCIDENTS = 50  # keep only the most recent failed fills, in-process only

_SEGMENT_RE = re.compile(r"\A[A-Za-z0-9._~!$&'()*+,;=:@-]+\Z")
_FILL_ID_RE = re.compile(r"\A[0-9a-f]{32}\Z")

# Incident reason taxonomy.
REASON_ORIGIN_STATUS = "origin_status"  # origin answered with a non-200 status
REASON_TRANSPORT = "transport"          # connect / timeout / truncated transfer
REASON_LOCAL_WRITE = "local_write"      # proxy could not write its cache volume


def validate_key(raw: str) -> str:
    """Map a URL path to a safe relative cache key; reject traversal."""
    if not raw or raw.endswith("/"):
        raise ValueError("empty package path")
    parts = raw.split("/")
    for seg in parts:
        if seg in ("", ".", ".."):
            raise ValueError(f"illegal path segment: {seg!r}")
        if not _SEGMENT_RE.match(seg):
            raise ValueError(f"illegal characters in segment: {seg!r}")
    return "/".join(parts)


def _unlink_quiet(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


class FillFailed(Exception):
    """Raised inside a client stream when the origin fill failed."""


class _OriginStatusError(Exception):
    """Origin answered with a non-200, non-404 HTTP status."""

    def __init__(self, status: int) -> None:
        super().__init__(f"origin responded with status {status}")
        self.status = status


class _LocalWriteError(Exception):
    """Writing the cache volume (part file, rename, meta) failed."""


class Entry:
    """One generation of a cached object.

    state: filling -> complete          (registered, file renamed into place)
                 \\-> closed             (purged mid-fill; fill finished, file
                                          kept only until readers drain)
                 \\-> failed             (origin error; part file removed)
    """

    def __init__(self, key: str) -> None:
        self.key = key
        self.fill_id = uuid.uuid4().hex
        self.gen = self.fill_id  # backward-compatible alias for part naming/info
        self.state = "filling"
        self.purged = False
        self.readers = 0
        self.written = 0          # bytes flushed to the part file so far
        self.size = -1            # expected total bytes (-1 = not known yet)
        self.error: str | None = None
        self.origin_status: int | None = None
        self.headers_known = False
        self.meta: dict = {}
        self.cond = asyncio.Condition()
        self.fill_task: asyncio.Task | None = None
        self.started_at = time.time()
        self.incident_recorded = False  # one single-flight fill -> at most one row

    @property
    def final_path(self) -> Path:
        return CACHE_DIR / self.key

    @property
    def part_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.part-{self.fill_id}"

    @property
    def meta_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.meta"

    def info(self) -> dict:
        return {
            "state": self.state,
            "purged": self.purged,
            "readers": self.readers,
            "written": self.written,
            "size": self.size,
            "fill_id": self.fill_id,
            "gen": self.fill_id,
            "error": self.error,
        }


class Cache:
    def __init__(self) -> None:
        self.entries: dict[str, Entry] = {}
        self.lock = asyncio.Lock()
        self.session: aiohttp.ClientSession | None = None
        # Bounded in-process incident log (oldest evicted at MAX_INCIDENTS).
        self.incidents: deque[dict] = deque()
        self.incident_by_id: dict[str, dict] = {}

    # ------------------------------------------------------------------ #
    # registry                                                            #
    # ------------------------------------------------------------------ #

    def _entry_from_disk(self, key: str) -> Entry | None:
        """Adopt a complete on-disk package (e.g. after a restart)."""
        final = CACHE_DIR / key
        try:
            st = final.stat()
        except OSError:
            return None
        if not final.is_file():
            return None
        e = Entry(key)
        e.state = "complete"
        e.size = st.st_size
        e.written = st.st_size
        e.headers_known = True
        try:
            e.meta = json.loads(e.meta_path.read_text())
        except (OSError, ValueError):
            e.meta = {}
        e.meta.setdefault("content_type", "application/octet-stream")
        self.entries[key] = e
        return e

    async def attach(self, key: str) -> Entry:
        """Return the current generation for key, starting a fill if needed."""
        async with self.lock:
            e = self.entries.get(key)
            if e is None:
                e = self._entry_from_disk(key)
            if e is None:
                e = Entry(key)
                self.entries[key] = e
                e.fill_task = asyncio.create_task(self._fill(e))
            e.readers += 1
            return e

    async def detach(self, e: Entry) -> None:
        async with self.lock:
            e.readers -= 1
            self._maybe_reap_locked(e)

    async def purge(self, key: str) -> bool:
        """Unregister the current generation; files go away with last reader.

        Incident rows are never touched: old fills keep their old-generation
        records and a later successful fill cannot overwrite them.
        """
        async with self.lock:
            e = self.entries.pop(key, None)
            if e is not None:
                e.purged = True
                self._maybe_reap_locked(e)
                return True
            # Not in memory (e.g. never requested since restart): clean disk.
            existed = False
            final = CACHE_DIR / key
            for p in (final, CACHE_DIR / f"{key}.meta"):
                if p.exists():
                    _unlink_quiet(p)
                    existed = True
            try:
                for p in final.parent.glob(final.name + ".part-*"):
                    _unlink_quiet(p)
                    existed = True
            except OSError:
                pass
            return existed

    def _maybe_reap_locked(self, e: Entry) -> None:
        """Delete files once no readers remain and the fill is over."""
        if e.readers > 0 or e.state == "filling":
            return
        if e.state == "failed":
            _unlink_quiet(e.part_path)  # never keep a half package
        elif e.purged:  # complete or closed
            _unlink_quiet(e.part_path)
            _unlink_quiet(e.final_path)
            _unlink_quiet(e.meta_path)

    # ------------------------------------------------------------------ #
    # incident log                                                        #
    # ------------------------------------------------------------------ #

    def _record_incident_locked(self, e: Entry, reason: str) -> None:
        incident = {
            "fill_id": e.fill_id,
            "path": e.key,
            "bytes_written": e.written,
            "reason": reason,
            "origin_status": e.origin_status,
            "error": e.error,
            "started_at": e.started_at,
            "failed_at": time.time(),
        }
        if len(self.incidents) >= MAX_INCIDENTS:
            oldest = self.incidents.popleft()
            self.incident_by_id.pop(oldest["fill_id"], None)
        self.incidents.append(incident)
        self.incident_by_id[e.fill_id] = incident

    # ------------------------------------------------------------------ #
    # origin fill                                                         #
    # ------------------------------------------------------------------ #

    async def _fill(self, e: Entry) -> None:
        assert self.session is not None
        try:
            await self._do_fill(e)
        except asyncio.CancelledError:
            raise
        except _OriginStatusError as exc:
            e.origin_status = exc.status
            e.error = str(exc)
            await self._fail(e, REASON_ORIGIN_STATUS)
        except _LocalWriteError as exc:
            e.error = f"{type(exc.__cause__).__name__}: {exc.__cause__}" if exc.__cause__ else str(exc)
            await self._fail(e, REASON_LOCAL_WRITE)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            # Connection refused/reset, timeouts, truncated payloads, etc.
            e.error = f"{type(exc).__name__}: {exc}"
            await self._fail(e, REASON_TRANSPORT)
        except Exception as exc:  # noqa: BLE001 - anything else is link-side
            e.error = f"{type(exc).__name__}: {exc}"
            await self._fail(e, REASON_TRANSPORT)

    async def _do_fill(self, e: Entry) -> None:
        url = f"{ORIGIN_BASE_URL}/packages/{e.key}"
        async with self.session.get(url) as resp:
            e.origin_status = resp.status
            if resp.status != 200:
                if resp.status == 404:
                    # 404 is an ordinary miss, not a fill failure: no incident,
                    # no negative cache; the next GET retries the origin.
                    e.error = "origin responded with status 404"
                    await self._fail(e, reason=None)
                    return
                raise _OriginStatusError(resp.status)
            cl = resp.content_length
            e.size = cl if cl is not None else -1
            e.meta = {
                "content_type": resp.headers.get("Content-Type")
                or "application/octet-stream",
                "sha256": resp.headers.get("X-Package-Sha256"),
                "fetched_at": time.time(),
            }
            try:
                e.part_path.parent.mkdir(parents=True, exist_ok=True)
                f = open(e.part_path, "wb")
            except OSError as exc:
                raise _LocalWriteError("cannot open part file") from exc
            try:
                # Part file exists before readers are allowed to open it.
                e.headers_known = True
                async with e.cond:
                    e.cond.notify_all()
                written = 0
                async for chunk in resp.content.iter_chunked(CHUNK):
                    try:
                        await asyncio.to_thread(f.write, chunk)
                        await asyncio.to_thread(f.flush)
                    except OSError as exc:
                        raise _LocalWriteError("write/flush failed") from exc
                    written += len(chunk)
                    e.written = written
                    async with e.cond:
                        e.cond.notify_all()
            finally:
                try:
                    await asyncio.to_thread(f.close)
                except OSError as exc:
                    raise _LocalWriteError("close failed") from exc
            if e.size != -1 and written != e.size:
                # Origin closed the connection before declaring Content-Length.
                raise aiohttp.ClientPayloadError(
                    f"short read from origin: {written}/{e.size} bytes"
                )
        async with self.lock:
            e.size = written if e.size == -1 else e.size
            e.meta["size"] = written
            if e.purged:
                # Purged mid-fill: keep the part file for current readers
                # only; never register this generation.
                e.state = "closed"
            else:
                try:
                    os.replace(e.part_path, e.final_path)  # atomic publish
                    try:
                        e.meta_path.write_text(json.dumps(e.meta))
                    except OSError:
                        pass
                except OSError as exc:
                    raise _LocalWriteError("publish failed") from exc
                e.state = "complete"
            async with e.cond:
                e.cond.notify_all()
            self._maybe_reap_locked(e)

    async def _fail(self, e: Entry, reason: str | None) -> None:
        """Mark the fill failed; record one incident unless reason is None.

        reason=None is reserved for non-incident failures (origin 404).
        """
        async with self.lock:
            e.state = "failed"
            if reason is not None and not e.incident_recorded:
                e.incident_recorded = True
                self._record_incident_locked(e, reason)
            if self.entries.get(e.key) is e:
                del self.entries[e.key]  # next GET retries the origin
            async with e.cond:
                e.cond.notify_all()
            self._maybe_reap_locked(e)

    # ------------------------------------------------------------------ #
    # client streaming                                                    #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _open_current(e: Entry):
        try:
            return open(e.part_path, "rb")
        except FileNotFoundError:
            return open(e.final_path, "rb")

    async def stream(self, e: Entry):
        """Yield the package from byte 0, tailing the fill file if needed."""
        offset = 0
        f = await asyncio.to_thread(self._open_current, e)
        try:
            while True:
                async with e.cond:
                    await e.cond.wait_for(
                        lambda: e.written > offset or e.state != "filling"
                    )
                if e.state == "failed":
                    raise FillFailed(e.error or "origin fill failed")
                if e.state == "filling":
                    limit = CHUNK
                else:  # complete | closed: drain exactly to the final size
                    limit = min(CHUNK, e.size - offset)
                    if limit <= 0:
                        break
                data = await asyncio.to_thread(f.read, limit)
                if not data:
                    if e.state == "filling":
                        continue  # defensive; written>offset implies data
                    break
                offset += len(data)
                yield data
        finally:
            try:
                f.close()
            finally:
                # A client going away (including its own disconnect) only
                # detaches its reader; the fill itself is unaffected and no
                # incident is recorded on the client's behalf.
                await self.detach(e)


cache = Cache()


def cleanup_cache_dir() -> None:
    """On boot: drop partial fills and orphan meta, keep complete packages."""
    for p in CACHE_DIR.rglob("*"):
        if not p.is_file():
            continue
        name = p.name
        if ".part-" in name:
            _unlink_quiet(p)
        elif name.endswith(".meta"):
            final = p.with_name(name[: -len(".meta")])
            if not final.exists():
                _unlink_quiet(p)


@asynccontextmanager
async def lifespan(app: FastAPI):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_cache_dir()
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10, sock_read=300)
    cache.session = aiohttp.ClientSession(timeout=timeout)
    try:
        yield
    finally:
        await cache.session.close()


app = FastAPI(title="pkg-cache-proxy", version="1.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/packages/{path:path}")
async def get_package(path: str):
    """Download a complete package by path (streamed from byte 0).

    Every response tied to a fill carries the fill's ``X-Fill-ID`` header,
    including error responses produced before the body starts. A 400 from
    path validation has no fill and therefore carries no id.
    """
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)

    entry = await cache.attach(key)
    fill_headers = {"X-Fill-ID": entry.fill_id}
    hit = entry.state == "complete"
    try:
        if not entry.headers_known:
            async with entry.cond:
                await entry.cond.wait_for(
                    lambda: entry.headers_known or entry.state == "failed"
                )
    except asyncio.CancelledError:
        await cache.detach(entry)
        raise

    if entry.state == "failed":
        await cache.detach(entry)
        status = (
            entry.origin_status
            if entry.origin_status and entry.origin_status != 200
            else 502
        )
        return PlainTextResponse(
            f"origin fetch failed: {entry.error}",
            status_code=status,
            headers=fill_headers,
        )

    headers = {"X-Cache": "HIT" if hit else "MISS", **fill_headers}
    if entry.size >= 0:
        headers["Content-Length"] = str(entry.size)
    sha256 = entry.meta.get("sha256")
    if sha256:
        headers["X-Package-Sha256"] = sha256
    return StreamingResponse(
        cache.stream(entry),
        media_type=entry.meta.get("content_type", "application/octet-stream"),
        headers=headers,
    )


@app.post("/purge/{path:path}")
async def purge_package(path: str):
    """Purge one path. The next download re-fills from the origin; downloads
    already in progress keep reading their old generation to completion.

    Incident records are not affected: old rows keep their old fill ids and a
    new successful fill cannot overwrite them.
    """
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    was_cached = await cache.purge(key)
    return {"path": key, "purged": True, "was_cached": was_cached}


@app.get("/cache/status")
async def cache_status():
    async with cache.lock:
        return {k: e.info() for k, e in cache.entries.items()}


@app.get("/cache/incidents")
async def list_incidents(limit: int | None = None):
    """List the most recent failed-fill incidents (newest first, max 50).

    Each row gives the package path, bytes already written to the cache
    volume before failure, and the classified reason (``origin_status``,
    ``transport`` or ``local_write``). Records live in this process only and
    are gone after a restart.
    """
    async with cache.lock:
        items = [dict(i) for i in reversed(cache.incidents)]
    if limit is not None:
        items = items[: max(0, min(limit, MAX_INCIDENTS))]
    return {
        "count": len(items),
        "max_retained": MAX_INCIDENTS,
        "incidents": [dict(i) for i in items],
    }


@app.get("/cache/incidents/{fill_id}")
async def get_incident(fill_id: str):
    """Look up one failed fill by its X-Fill-ID."""
    if not _FILL_ID_RE.match(fill_id):
        return PlainTextResponse("unknown fill id", status_code=404)
    async with cache.lock:
        incident = cache.incident_by_id.get(fill_id)
        payload = dict(incident) if incident is not None else None
    if payload is None:
        return PlainTextResponse("unknown fill id", status_code=404)
    return JSONResponse(payload)
