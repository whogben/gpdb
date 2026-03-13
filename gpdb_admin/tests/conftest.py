from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from gpdb.admin import entry
from gpdb.admin.config import ConfigStore


@pytest.fixture(scope="session")
def admin_test_env(tmp_path_factory):
    """Shared admin ServerManager + TestClient for HTTP/MCP integration tests.

    FastMCP's StreamableHTTPSessionManager is one-shot per process, so we must
    avoid starting the HTTP app (and its MCP HTTP server) multiple times.
    This fixture creates a single manager + TestClient whose lifespan runs once
    for the whole test session.
    """
    tmp = tmp_path_factory.mktemp("admin-data")
    manager = _create_test_manager(tmp)
    with TestClient(manager.app) as client:
        yield SimpleNamespace(manager=manager, client=client)


def _create_test_manager(tmp_path: Path):
    """Create a manager backed by a temporary config and captive data dir."""
    config_path = tmp_path / "admin.toml"
    data_dir = tmp_path / "admin data"
    config_path.write_text(
        (
            "[server]\n"
            'host = "127.0.0.1"\n'
            "port = 8747\n"
            "[runtime]\n"
            f'data_dir = "{data_dir.as_posix()}"\n'
            "[auth]\n"
            'session_secret = "test-session-secret"\n'
        ),
        encoding="utf-8",
    )
    config_store = ConfigStore.from_sources(cli_path=config_path)
    resolved_config = config_store.load()
    return entry.create_manager(
        resolved_config=resolved_config, config_store=config_store
    )
