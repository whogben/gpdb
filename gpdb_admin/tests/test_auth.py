import pytest
from fastapi.testclient import TestClient

from gpdb.admin import entry
from gpdb.admin.config import ConfigStore
from gpdb.admin.store import AdminStore


@pytest.fixture(scope="session")
def admin_test_env(tmp_path_factory):
    """Shared admin ServerManager + TestClient for HTTP/MCP integration tests.

    FastMCP's StreamableHTTPSessionManager is one-shot per process, so we must
    avoid starting the HTTP app (and its MCP HTTP server) multiple times.
    This fixture creates a single manager + TestClient whose lifespan runs once
    for the whole test session.
    """
    from types import SimpleNamespace

    tmp = tmp_path_factory.mktemp("admin-data")
    manager = _create_test_manager(tmp)
    with TestClient(manager.app) as client:
        yield SimpleNamespace(manager=manager, client=client)


def test_first_run_setup_and_login_flow(admin_test_env):
    """Test owner bootstrap and subsequent login-gated home page access."""
    client = admin_test_env.client

    response = client.get("/")
    assert response.status_code == 200
    assert "Create the initial owner user." in response.text

    response = client.post(
        "/setup",
        data={
            "username": "owner",
            "display_name": "Primary Owner",
            "password": "secret-pass",
            "confirm_password": "secret-pass",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/login")

    response = client.get("/", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"].startswith("/login")

    response = client.get("/login")
    assert response.status_code == 200
    assert "Log in to GPDB admin." in response.text

    _login(client, username="owner", password="secret-pass")

    response = client.get("/")
    assert response.status_code == 200
    assert "All graphs across all managed instances." in response.text
    assert "Primary Owner" in response.text
    assert "Default instance" in response.text
    assert "Default graph" in response.text


def test_startup_requires_session_secret(tmp_path):
    """Test that startup fails clearly when the session secret is missing."""
    config_path = tmp_path / "admin.toml"
    data_dir = tmp_path / "admin data"
    config_path.write_text(
        (
            "[server]\n"
            'host = "127.0.0.1"\n'
            "port = 8747\n"
            "[runtime]\n"
            f'data_dir = "{data_dir.as_posix()}"\n'
        ),
        encoding="utf-8",
    )

    config_store = ConfigStore.from_sources(cli_path=config_path)
    resolved_config = config_store.load()
    manager = entry.create_manager(
        resolved_config=resolved_config,
        config_store=config_store,
    )

    with pytest.raises(RuntimeError, match="auth.session_secret"):
        with TestClient(manager.app):
            pass


# Helper functions


def _bootstrap_owner(client: TestClient) -> None:
    response = client.post(
        "/setup",
        data={
            "username": "owner",
            "display_name": "Primary Owner",
            "password": "secret-pass",
            "confirm_password": "secret-pass",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/login")


def _login(
    client: TestClient,
    *,
    username: str = "owner",
    password: str = "secret-pass",
) -> None:
    response = client.post(
        "/login",
        data={"username": username, "password": password},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/"
    assert "gpdb_admin_session" in response.cookies


def _create_test_manager(tmp_path):
    """Create a test ServerManager with a temporary data directory."""
    from pathlib import Path

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
            'session_secret = "test-secret-for-testing-only"\n'
        ),
        encoding="utf-8",
    )

    config_store = ConfigStore.from_sources(cli_path=config_path)
    resolved_config = config_store.load()
    return entry.create_manager(
        resolved_config=resolved_config,
        config_store=config_store,
    )
