from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from gpdb.admin import entry
from gpdb.admin.config import AdminConfig, ConfigPathSource, ConfigStore, extract_config_arg, resolve_config_location


def test_cli_status_command():
    """Test that the status command is accessible via CLI."""
    admin_service = entry.ToolService("admin", [entry.status])
    cli = entry.CLIServer("gpdb")
    cli.mount(admin_service)

    manager = entry.ServerManager(name="gpdb-admin")
    manager.add_server(cli)

    result = manager.cli(["gpdb", "status"], standalone_mode=False)
    assert result == "OK"


def test_fastapi_status_command():
    """Test that the status command is accessible via FastAPI REST API."""
    admin_service = entry.ToolService("admin", [entry.status])
    rest_api = entry.OpenAPIServer(path_prefix="/api", title="GPDB Admin API")
    rest_api.mount(admin_service)

    manager = entry.ServerManager(name="gpdb-admin")
    manager.add_server(rest_api)

    client = TestClient(manager.app)
    response = client.post("/api/status")
    assert response.status_code == 200
    assert response.json() == "OK"


def test_mcp_status_command():
    """Test that the status command is accessible via MCP server."""
    admin_service = entry.ToolService("admin", [entry.status])
    mcp_server = entry.SSEMCPServer("gpdb")
    mcp_server.mount(admin_service)

    manager = entry.ServerManager(name="gpdb-admin")
    manager.add_server(mcp_server)

    assert "gpdb" in manager.mcp_servers
    assert manager.mcp_servers["gpdb"] is not None


def test_health_endpoint():
    """Test that the health endpoint returns registered MCP servers."""
    admin_service = entry.ToolService("admin", [entry.status])
    mcp_server = entry.SSEMCPServer("gpdb")
    mcp_server.mount(admin_service)

    manager = entry.ServerManager(name="gpdb-admin")
    manager.add_server(mcp_server)

    client = TestClient(manager.app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"mcp_servers": ["gpdb"]}


def test_first_run_setup_and_login_flow(tmp_path):
    """Test owner bootstrap and subsequent login-gated home page access."""
    manager = _create_test_manager(tmp_path)

    with TestClient(manager.app) as client:
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
        assert response.headers["location"] == "/login"

        response = client.get("/", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/login"

        response = client.get("/login")
        assert response.status_code == 200
        assert "Log in to GPDB admin." in response.text

        response = client.post(
            "/login",
            data={"username": "owner", "password": "secret-pass"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert response.headers["location"] == "/"
        assert "gpdb_admin_session" in response.cookies

        response = client.get("/")
        assert response.status_code == 200
        assert "Welcome to the GPDB admin web app." in response.text
        assert "Primary Owner" in response.text


def test_startup_requires_session_secret(tmp_path):
    """Test that startup fails clearly when the session secret is missing."""
    config_path = tmp_path / "admin.toml"
    data_dir = tmp_path / "admin data"
    config_path.write_text(
        (
            "[server]\n"
            "host = \"127.0.0.1\"\n"
            "port = 8747\n"
            "[runtime]\n"
            f"data_dir = \"{data_dir.as_posix()}\"\n"
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


def test_extract_config_arg_strips_global_option():
    """Test that the global config option is removed before CLI dispatch."""
    config_path, remaining = extract_config_arg(["start", "--config", "./admin.toml", "--port", "9000"])

    assert config_path == Path("./admin.toml")
    assert remaining == ["start", "--port", "9000"]


def test_resolve_config_location_prefers_environment(monkeypatch):
    """Test that config resolution uses the env var when no CLI path is passed."""
    monkeypatch.setenv("GPDB_CONFIG", "/tmp/gpdb-admin.toml")

    location = resolve_config_location()

    assert location.path == Path("/tmp/gpdb-admin.toml")
    assert location.source == ConfigPathSource.ENV


def test_config_store_round_trip(tmp_path):
    """Test that file-backed config can be saved and loaded."""
    path = tmp_path / "admin.toml"
    store = ConfigStore.from_sources(cli_path=path)

    store.save(
        AdminConfig.model_validate(
            {
                "server": {"host": "0.0.0.0", "port": 9010},
                "runtime": {"data_dir": str(tmp_path / "data")},
                "auth": {"session_secret": "test-secret"},
            }
        )
    )
    resolved = store.load()

    assert resolved.location.path == path
    assert resolved.location.exists is True
    assert resolved.server.host == "0.0.0.0"
    assert resolved.server.port == 9010
    assert resolved.runtime.data_dir == str(tmp_path / "data")
    assert resolved.auth.session_secret == "test-secret"


def test_bootstrap_runtime_uses_config_file(tmp_path):
    """Test that bootstrap loads config before creating the runtime."""
    path = tmp_path / "admin.toml"
    path.write_text(
        (
            "[server]\n"
            "host = \"0.0.0.0\"\n"
            "port = 9011\n"
            "[runtime]\n"
            f"data_dir = \"{(tmp_path / 'runtime-data').as_posix()}\"\n"
        ),
        encoding="utf-8",
    )

    manager, resolved_config, remaining_args = entry.bootstrap_runtime(
        ["--config", str(path), "status"]
    )

    assert remaining_args == ["status"]
    assert resolved_config.location.path == path
    assert resolved_config.server.host == "0.0.0.0"
    assert resolved_config.server.port == 9011
    assert resolved_config.runtime.data_dir == str(tmp_path / "runtime-data")
    assert resolved_config.auth.session_secret is not None
    assert manager.app.state.config.server.port == 9011


def _create_test_manager(tmp_path: Path):
    """Create a manager backed by a temporary config and captive data dir."""
    config_path = tmp_path / "admin.toml"
    data_dir = tmp_path / "admin data"
    config_path.write_text(
        (
            "[server]\n"
            "host = \"127.0.0.1\"\n"
            "port = 8747\n"
            "[runtime]\n"
            f"data_dir = \"{data_dir.as_posix()}\"\n"
            "[auth]\n"
            "session_secret = \"test-session-secret\"\n"
        ),
        encoding="utf-8",
    )
    config_store = ConfigStore.from_sources(cli_path=config_path)
    resolved_config = config_store.load()
    return entry.create_manager(resolved_config=resolved_config, config_store=config_store)
