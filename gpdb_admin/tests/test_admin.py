import asyncio
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import make_url

from gpdb.admin import entry
from gpdb.admin.config import AdminConfig, ConfigPathSource, ConfigStore, extract_config_arg, resolve_config_location
from gpdb.admin.store import AdminStore


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

        _login(client, username="owner", password="secret-pass")

        response = client.get("/")
        assert response.status_code == 200
        assert "All graphs across all managed instances." in response.text
        assert "Primary Owner" in response.text
        assert "Default instance" in response.text
        assert "Default graph" in response.text


def test_instance_and_graph_crud_flow(tmp_path):
    """Test owner-managed instance and graph CRUD from the web UI."""
    manager = _create_test_manager(tmp_path)

    with TestClient(manager.app) as client:
        _bootstrap_owner(client)
        _login(client)

        response = client.get("/graphs/new")
        assert response.status_code == 200
        default_instance_id = _extract_instance_option_value(response.text, "Default instance")

        response = client.post(
            "/graphs",
            data={
                "instance_id": default_instance_id,
                "table_prefix": "scratch",
                "display_name": "Scratch graph",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303

        response = client.get("/")
        assert "Scratch graph" in response.text
        assert "scratch" in response.text

        response = client.post(
            "/instances",
            data=_captive_instance_form(
                manager,
                slug="mirror",
                display_name="Mirror instance",
                description="Shared test connection",
            ),
            follow_redirects=False,
        )
        assert response.status_code == 303

        raw_password = _captive_instance_form(
            manager,
            slug="mirror",
            display_name="Mirror instance",
            description="Shared test connection",
        )["password"]
        stored_password = _read_stored_instance_password(
            manager,
            slug="mirror",
        )
        assert stored_password != raw_password
        assert str(stored_password).startswith("fernet:")

        response = client.get("/")
        assert "Mirror instance" in response.text

        instance_edit_page = _extract_instance_action(response.text, "Mirror instance", "edit")
        response = client.get(instance_edit_page)
        assert response.status_code == 200
        assert 'name="password" type="password"' in response.text
        assert 'value="' not in response.text.split('name="password" type="password"', 1)[1].split(">", 1)[0]

        response = client.get("/graphs/new")
        mirror_instance_id = _extract_instance_option_value(response.text, "Mirror instance")

        response = client.post(
            "/graphs",
            data={
                "instance_id": mirror_instance_id,
                "table_prefix": "mirror_scratch",
                "display_name": "Mirror scratch",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303

        response = client.get("/")
        assert "Mirror scratch" in response.text

        graph_edit_path = _extract_graph_action(response.text, "Mirror scratch", "edit")
        response = client.post(
            graph_edit_path,
            data={"display_name": "Mirror scratch renamed"},
            follow_redirects=False,
        )
        assert response.status_code == 303

        response = client.get("/")
        assert "Mirror scratch renamed" in response.text

        instance_edit_path = _extract_instance_action(response.text, "Mirror instance", "edit")
        response = client.post(
            instance_edit_path,
            data={
                **_captive_instance_form(
                    manager,
                    slug="mirror",
                    display_name="Mirror instance renamed",
                    description="Shared test connection updated",
                ),
                "display_name": "Mirror instance renamed",
                "description": "Shared test connection updated",
                "is_active": "true",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303

        response = client.get("/")
        assert "Mirror instance renamed" in response.text

        graph_delete_path = _extract_graph_action(
            response.text,
            "Mirror scratch renamed",
            "delete",
        )
        response = client.post(graph_delete_path, follow_redirects=False)
        assert response.status_code == 303

        response = client.get("/")
        assert "Mirror scratch renamed" not in response.text

        instance_delete_path = _extract_instance_action(
            response.text,
            "Mirror instance renamed",
            "delete",
        )
        response = client.post(instance_delete_path, follow_redirects=False)
        assert response.status_code == 303

        response = client.get("/")
        assert "Mirror instance renamed" not in response.text


def test_api_key_lifecycle_for_web_rest_and_mcp(tmp_path):
    """Test API key create, reveal, use, last-used update, and revoke flow."""
    manager = _create_test_manager(tmp_path)

    with TestClient(manager.app) as client:
        _bootstrap_owner(client)
        _login(client)

        response = client.post("/api/status")
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == "Bearer"

        response = client.post(
            "/apikeys",
            data={"label": "Automation key"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        api_key_detail_path = response.headers["location"]
        assert api_key_detail_path.startswith("/apikeys/")

        response = client.get(api_key_detail_path)
        assert response.status_code == 200
        assert "Automation key" in response.text
        api_key_value = _extract_revealed_api_key(response.text)
        assert api_key_value.startswith("gpdb_")

        response = client.get("/apikeys")
        assert response.status_code == 200
        assert "Automation key" in response.text

        stored_key = _read_stored_api_key(manager, label="Automation key")
        assert str(stored_key["key_value"]).startswith("fernet:")
        assert stored_key["key_value"] != api_key_value
        assert stored_key["secret_hash"] != api_key_value
        assert stored_key["last_used_at"] is None
        assert stored_key["revoked_at"] is None

        response = client.post(
            "/api/status",
            headers={"Authorization": "Bearer gpdb_invalid_deadbeef"},
        )
        assert response.status_code == 401

        response = client.post(
            "/api/status",
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        assert response.json() == "OK"

        stored_key = _read_stored_api_key(manager, label="Automation key")
        assert stored_key["last_used_at"] is not None

        assert manager.mcp_servers["gpdb"].auth is not None
        verified_token = _verify_api_key_with_mcp_verifier(manager, api_key_value)
        assert verified_token is not None
        assert verified_token.claims["username"] == "owner"
        assert verified_token.claims["api_key_label"] == "Automation key"

        api_key_id = api_key_detail_path.split("?", 1)[0].rsplit("/", 1)[-1]
        response = client.post(
            f"/apikeys/{api_key_id}/revoke",
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert response.headers["location"].startswith("/apikeys")

        response = client.get(api_key_detail_path)
        assert response.status_code == 200
        assert api_key_value in response.text
        assert "revoked" in response.text

        response = client.post(
            "/api/status",
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 401
        assert _verify_api_key_with_mcp_verifier(manager, api_key_value) is None


def test_rest_api_public_docs_and_health_endpoints(tmp_path):
    """Test that documented public REST endpoints stay accessible without API keys."""
    manager = _create_test_manager(tmp_path)

    with TestClient(manager.app) as client:
        for path in sorted(entry.REST_API_PUBLIC_PATHS):
            response = client.get(f"/api{path}")
            assert response.status_code == 200

        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"mcp_servers": ["gpdb"]}

        response = client.post("/api/status")
        assert response.status_code == 401


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
    assert response.headers["location"] == "/login"


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


def _captive_instance_form(
    manager,
    *,
    slug: str,
    display_name: str,
    description: str,
    password: str = "top-secret",
) -> dict[str, str]:
    uri = manager.app.state.services.captive_server.get_uri()
    parsed = make_url(uri)
    host = str(parsed.query.get("host") or parsed.host or "127.0.0.1")
    port = str(parsed.query.get("port") or parsed.port or "")
    return {
        "slug": slug,
        "display_name": display_name,
        "description": description,
        "host": host,
        "port": port,
        "database": str(parsed.database or "postgres"),
        "username": str(parsed.username or "postgres"),
        "password": password,
    }


def _extract_instance_option_value(html: str, label: str) -> str:
    match = re.search(
        rf'<option[^>]*value="([^"]+)"[^>]*>\s*{re.escape(label)}\s*\([^)]*\)\s*</option>',
        html,
        re.S,
    )
    assert match is not None
    return match.group(1)


def _extract_graph_action(html: str, graph_name: str, action: str) -> str:
    if action == "edit":
        pattern = rf"<h3>{re.escape(graph_name)}</h3>.*?href=\"([^\"]*?/graphs/[^\"]+/edit)\""
    else:
        pattern = rf"<h3>{re.escape(graph_name)}</h3>.*?action=\"([^\"]*?/graphs/[^\"]+/delete)\""
    match = re.search(pattern, html, re.S)
    assert match is not None
    return match.group(1)


def _extract_instance_action(html: str, instance_name: str, action: str) -> str:
    if action == "edit":
        pattern = (
            rf"<h3>{re.escape(instance_name)}</h3>.*?resource-meta.*?"
            rf"href=\"([^\"]*?/instances/[^\"]+/edit)\""
        )
    else:
        pattern = (
            rf"<h3>{re.escape(instance_name)}</h3>.*?resource-meta.*?"
            rf"action=\"([^\"]*?/instances/[^\"]+/delete)\""
        )
    match = re.search(pattern, html, re.S)
    assert match is not None
    return match.group(1)


def _extract_revealed_api_key(html: str) -> str:
    match = re.search(r'<input readonly value="([^"]+)"', html)
    assert match is not None
    return match.group(1)


def _read_stored_api_key(manager, *, label: str) -> dict[str, object]:
    services = manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None

    async def _load() -> dict[str, object]:
        store = AdminStore(
            services.captive_server.get_uri(),
            instance_secret=services.resolved_config.auth.session_secret,
        )
        try:
            owner = await store.get_user_by_username("owner")
            assert owner is not None
            api_keys = await store.list_api_keys_for_user(owner.id)
            api_key = next(item for item in api_keys if item.label == label)
            node = await store.db.get_node(api_key.id)
            assert node is not None
            return dict(node.data)
        finally:
            await store.close()

    return asyncio.run(_load())


def _verify_api_key_with_mcp_verifier(manager, api_key_value: str):
    services = manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None

    async def _verify():
        store = AdminStore(
            services.captive_server.get_uri(),
            instance_secret=services.resolved_config.auth.session_secret,
        )
        try:
            verifier = entry._AdminAPIKeyTokenVerifier(SimpleNamespace(admin_store=store))
            return await verifier.verify_token(api_key_value)
        finally:
            await store.close()

    return asyncio.run(_verify())


def _read_stored_instance_password(manager, *, slug: str) -> object:
    services = manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None

    async def _load() -> object:
        store = AdminStore(
            services.captive_server.get_uri(),
            instance_secret=services.resolved_config.auth.session_secret,
        )
        try:
            instance = await store.get_instance_by_slug(slug)
            assert instance is not None
            node = await store.db.get_node(instance.id)
            assert node is not None
            return node.data["password"]
        finally:
            await store.close()

    return asyncio.run(_load())
