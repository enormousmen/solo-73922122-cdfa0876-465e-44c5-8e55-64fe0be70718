"""Shared pytest fixtures for the e2e suites.

The real origin and proxy are launched as subprocesses via the helpers in
``test_e2e`` (which also keeps its own module-local copies of these fixtures
for its tests). Both suites therefore exercise the real uvicorn processes.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from test_e2e import ProxyRunner, free_port, start_server, stop


@pytest.fixture(scope="session")
def origin(tmp_path_factory):
    pkg_dir = tmp_path_factory.mktemp("pkgs")
    log_dir = tmp_path_factory.mktemp("logs")
    log = log_dir / "origin.log"
    control = log_dir / "origin.stop"
    port = free_port()
    proc = start_server("origin", port, {"PKG_DIR": str(pkg_dir)}, log, control)
    yield SimpleNamespace(url=f"http://127.0.0.1:{port}", port=port, dir=pkg_dir, log=log)
    stop(proc, control)


@pytest.fixture()
def proxy(origin, tmp_path):
    runner = ProxyRunner(origin.port, tmp_path / "cache", tmp_path / "proxy.log",
                         tmp_path / "proxy.stop")
    runner.start()
    yield runner
    runner.stop()
