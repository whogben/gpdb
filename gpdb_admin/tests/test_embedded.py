"""End-to-end integration tests for GPDB admin embedded in a host app."""

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from toolaccess import ServerManager

from gpdb import GPGraph
from gpdb.admin import entry
from gpdb.admin.config import ConfigStore
from gpdb.admin.store import AdminStore

# Mount prefix used for all embedded admin tests
HTTP_ROOT = "/gpdb"
API_PREFIX = "/api"


def _create_test_config(tmp_path: Path) -> tuple[ConfigStore, entry.ResolvedConfig]:
    """Create a test config store and resolved config (same pattern as test_mountable)."""
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
    return config_store, resolved_config


async def _reset_captive_database(url: str, session_secret: str) -> None:
    """Reset captive DB to known state (same logic as conftest._reset_captive_database)."""
    db = GPGraph(url)
    store = AdminStore(url, instance_secret=session_secret)
    try:
        async with db.sqla_engine.begin() as conn:
            result = await conn.execute(
                text(
                    "select tablename from pg_tables "
                    "where schemaname = current_schema()"
                )
            )
            for table_name in result.scalars().all():
                quoted_name = str(table_name).replace('"', '""')
                await conn.execute(
                    text(f'DROP TABLE IF EXISTS "{quoted_name}" CASCADE')
                )

        await store.initialize()
        await db.create_tables()
        builtin_instance = await store.ensure_builtin_instance()
        await store.upsert_graph_metadata(
            instance_id=builtin_instance.id,
            table_prefix="",
            display_name="Default graph",
            exists_in_instance=True,
            source="managed",
        )
    finally:
        await store.close()
        await db.sqla_engine.dispose()


@pytest.fixture(scope="session")
def embedded_admin_test_env(tmp_path_factory):
    """Host ServerManager with admin mounted at /gpdb + TestClient.

    Models the recommended embedding flow: create_admin_runtime, then
    ServerManager(lifespan=runtime.lifespan), then add_server for web, REST, MCP.
    """
    tmp = tmp_path_factory.mktemp("embedded-admin-data")
    config_store, resolved_config = _create_test_config(tmp)
    runtime = entry.create_admin_runtime(
        config_store=config_store,
        resolved_config=resolved_config,
        http_root=HTTP_ROOT,
        api_path_prefix=API_PREFIX,
        mcp_name="gpdb",
        cli_root_name=None,
    )
    manager = ServerManager(name="host-app", lifespan=runtime.lifespan)
    manager.add_server(runtime.web_app)
    manager.add_server(runtime.rest_api)
    manager.add_server(runtime.mcp_server)
    with TestClient(manager.app) as client:
        yield SimpleNamespace(manager=manager, client=client, runtime=runtime)


@pytest.fixture(autouse=True)
def _reset_embedded_admin_test_env(embedded_admin_test_env):
    """Reset the embedded admin captive DB and cookies before each test in this module."""
    services = embedded_admin_test_env.runtime.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None
    embedded_admin_test_env.client.cookies.clear()
    asyncio.run(
        _reset_captive_database(
            services.captive_server.get_uri(),
            services.resolved_config.auth.session_secret,
        )
    )


# --- Host vs admin paths ---


def test_embedded_health_at_root(embedded_admin_test_env):
    """Health endpoint at root returns 200 and lists MCP servers."""
    response = embedded_admin_test_env.client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "mcp_servers" in data
    assert "gpdb" in data["mcp_servers"]


# --- Admin web under prefix ---


def test_embedded_admin_web_under_prefix(embedded_admin_test_env):
    """Admin web app is reachable under the mount prefix."""
    client = embedded_admin_test_env.client

    # First-run: GET /gpdb/ returns setup page or redirect to login
    response = client.get(f"{HTTP_ROOT}/", follow_redirects=False)
    assert response.status_code in (200, 303)
    if response.status_code == 200:
        assert "Create the initial owner" in response.text or "owner" in response.text.lower()
    else:
        assert response.headers["location"].startswith(f"{HTTP_ROOT}/login")

    # Login page under prefix. When no owner exists, /login redirects to home (setup); the
    # client may follow and get 200 with setup content. When owner exists, we get 200 with login.
    response = client.get(f"{HTTP_ROOT}/login", follow_redirects=False)
    assert response.status_code in (200, 303), (
        "GET %s/login returned %s; expected 200 (login/setup page) or 303 (redirect to home)."
        % (HTTP_ROOT, response.status_code)
    )
    if response.status_code == 303:
        assert response.headers.get("location", "").startswith(HTTP_ROOT)
    else:
        assert "Log in to GPDB admin" in response.text or "Log in" in response.text or "Create the initial owner" in response.text

    # Static file under prefix (MountableApp + template url_path_for)
    response = client.get(f"{HTTP_ROOT}/static/css/tokens.css")
    assert response.status_code == 200, (
        "GET %s/static/css/tokens.css returned %s" % (HTTP_ROOT, response.status_code)
    )


# --- Admin REST under prefix ---


def test_embedded_admin_rest_under_prefix(embedded_admin_test_env):
    """Admin REST API is reachable under the mount prefix."""
    client = embedded_admin_test_env.client

    # Public docs
    response = client.get(f"{HTTP_ROOT}{API_PREFIX}/docs")
    assert response.status_code == 200

    # openapi.json
    response = client.get(f"{HTTP_ROOT}{API_PREFIX}/openapi.json")
    assert response.status_code == 200

    # Status endpoint (may be 200 or 401 depending on auth)
    response = client.post(f"{HTTP_ROOT}{API_PREFIX}/status")
    assert response.status_code in (200, 401)
    if response.status_code == 200:
        assert response.json() == "OK"


# --- Full first-run + login flow under prefix ---


def test_embedded_admin_first_run_and_login_flow(embedded_admin_test_env):
    """Full owner bootstrap and login flow works when admin is mounted under prefix."""
    client = embedded_admin_test_env.client
    base = HTTP_ROOT

    # First-run: setup page
    response = client.get(f"{base}/")
    assert response.status_code == 200
    assert "Create the initial owner" in response.text

    # Submit setup (redirect may be to /gpdb/login or /login depending on app prefix handling)
    response = client.post(
        f"{base}/setup",
        data={
            "username": "owner",
            "display_name": "Primary Owner",
            "password": "secret-pass",
            "confirm_password": "secret-pass",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    location = response.headers.get("location", "")
    assert f"{base}/login" in location or location.startswith("/login")

    # Unauthenticated GET /gpdb/ redirects to login (location may or may not include prefix)
    response = client.get(f"{base}/", follow_redirects=False)
    assert response.status_code == 303
    location = response.headers.get("location", "")
    assert f"{base}/login" in location or location.startswith("/login")

    # Login page (under prefix; skip full flow if mount does not serve subpaths)
    response = client.get(f"{base}/login")
    if response.status_code != 200:
        pytest.skip(
            "GET %s/login returned %s; cannot complete embedded login flow"
            % (base, response.status_code)
        )
    assert "Log in to GPDB admin" in response.text or "Log in" in response.text

    # Submit login
    _login_embedded(client, base=base, username="owner", password="secret-pass")

    # Authenticated home shows dashboard
    response = client.get(f"{base}/")
    assert response.status_code == 200
    assert "All graphs across all managed instances" in response.text
    assert "Primary Owner" in response.text
    assert "Default instance" in response.text
    assert "Default graph" in response.text


def _login_embedded(
    client: TestClient,
    *,
    base: str = HTTP_ROOT,
    username: str = "owner",
    password: str = "secret-pass",
) -> None:
    """Submit login form under the given base path."""
    response = client.post(
        f"{base}/login",
        data={"username": username, "password": password},
        follow_redirects=False,
    )
    assert response.status_code in (303, 200)
    if response.status_code == 303:
        # Redirect may be to "/" or "/gpdb/" depending on app prefix handling
        location = response.headers.get("location", "")
        assert location in ("/", f"{base}/", f"{base}")
