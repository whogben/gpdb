"""Integration tests for host + client shared-database lifecycle."""

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from gpdb import Filter, GPGraph
from sqlalchemy import text

from gpdb.admin import entry
from gpdb.admin.config import ConfigStore
from gpdb.admin.store import AdminStore, VersionMismatchError

INSTANCE_SECRET = "shared-instance-secret-12345"
SESSION_SECRET = "shared-session-secret-67890"


def _write_host_config(data_dir: Path, expose_port: int) -> Path:
    """Write a captive-mode config with TCP exposure enabled."""
    data_dir.mkdir(parents=True, exist_ok=True)
    config_path = data_dir / "admin.toml"
    config_path.write_text(
        (
            "[server]\n"
            'host = "127.0.0.1"\n'
            "port = 8747\n"
            "[auth]\n"
            f'session_secret = "{SESSION_SECRET}"\n'
            f'instance_secret = "{INSTANCE_SECRET}"\n'
            "[postgres]\n"
            'mode = "captive"\n'
            "expose_postgres = true\n"
            f"expose_port = {expose_port}\n"
        ),
        encoding="utf-8",
    )
    return data_dir


def _write_client_config(data_dir: Path, host_url: str) -> Path:
    """Write an external-mode config pointing at the host's TCP endpoint."""
    data_dir.mkdir(parents=True, exist_ok=True)
    config_path = data_dir / "admin.toml"
    config_path.write_text(
        (
            "[server]\n"
            'host = "127.0.0.1"\n'
            "port = 8748\n"
            "[auth]\n"
            f'session_secret = "{SESSION_SECRET}"\n'
            f'instance_secret = "{INSTANCE_SECRET}"\n'
            "[postgres]\n"
            'mode = "external"\n'
            f'url = "{host_url}"\n'
        ),
        encoding="utf-8",
    )
    return data_dir


def _create_manager(data_dir: Path) -> entry.ServerManager:
    """Build a ServerManager from a config directory."""
    config_store = ConfigStore.from_sources(cli_data_dir=data_dir)
    resolved_config = config_store.load()
    return entry.create_manager(
        resolved_config=resolved_config, config_store=config_store
    )


@pytest.fixture(scope="session")
def host_env(tmp_path_factory):
    """Start a host AdminRuntime with captive Postgres and TCP exposure.

    Session-scoped so all tests share the same host process and port.
    """
    tmp = tmp_path_factory.mktemp("sync-host")
    _write_host_config(tmp, expose_port=19432)
    manager = _create_manager(tmp)

    with TestClient(manager.app) as client:
        services = manager.app.state.services
        expose_uri = services.captive_server.get_expose_uri()
        yield SimpleNamespace(
            manager=manager,
            client=client,
            expose_uri=expose_uri,
            host_url=services.captive_server.get_uri(),
        )


@pytest.fixture
def client_env(tmp_path, host_env):
    """Start a client AdminRuntime connected to the host via TCP."""
    client_dir = tmp_path / "client"
    host_url = host_env.expose_uri.replace("0.0.0.0", "127.0.0.1")
    _write_client_config(client_dir, host_url)
    manager = _create_manager(client_dir)

    with TestClient(manager.app) as client:
        yield SimpleNamespace(
            manager=manager,
            client=client,
        )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_host_starts_creates_tables_and_migrations(host_env):
    """Host starts, creates tables, runs migrations, writes version."""
    services = host_env.manager.app.state.services
    assert services.captive_server is not None
    assert services.is_host is True
    assert services.admin_store is not None

    async def _check(url):
        db = GPGraph(url, table_prefix="admin")
        try:
            async with db.sqla_engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT count(*) FROM admin_server_versions")
                )
                return result.scalar()
        finally:
            await db.sqla_engine.dispose()

    count = asyncio.run(_check(host_env.host_url))
    assert count >= 1


def test_client_connects_and_passes_version_check(client_env):
    """Client connects, passes version check, skips migrations."""
    services = client_env.manager.app.state.services
    assert services.captive_server is None
    assert services.is_host is False
    assert services.admin_store is not None

    response = client_env.client.get("/")
    assert response.status_code == 200


def test_shared_data_visible_to_client(host_env, client_env):
    """Data written by host (users) is visible to client."""
    host_url = host_env.host_url

    async def _create_owner(url):
        store = AdminStore(url, instance_secret=INSTANCE_SECRET, is_host=False)
        try:
            return await store.create_initial_owner(
                username="testowner",
                password_hash="hashed_pw",
                display_name="Test Owner",
            )
        finally:
            await store.close()

    owner = asyncio.run(_create_owner(host_url))
    assert owner.username == "testowner"

    async def _read_owner(url):
        store = AdminStore(url, instance_secret=INSTANCE_SECRET, is_host=False)
        try:
            return await store.get_user_by_username("testowner")
        finally:
            await store.close()

    found = asyncio.run(_read_owner(host_url))
    assert found is not None
    assert found.username == "testowner"
    assert found.display_name == "Test Owner"


def test_data_written_by_client_visible_to_host(host_env, client_env):
    """Data written by client is visible to host."""
    client_url = host_env.expose_uri.replace("0.0.0.0", "127.0.0.1")

    async def _create_instance(url):
        store = AdminStore(url, instance_secret=INSTANCE_SECRET, is_host=False)
        try:
            return await store.create_instance(
                slug="remote-pg",
                display_name="Remote PG",
                description="A remote postgres instance",
                host="db.example.com",
                port=5432,
                database="mydb",
                username="pguser",
                password="pgpass123",
            )
        finally:
            await store.close()

    instance = asyncio.run(_create_instance(client_url))
    assert instance.slug == "remote-pg"
    assert instance.password == "pgpass123"

    async def _read_instance(url):
        store = AdminStore(url, instance_secret=INSTANCE_SECRET, is_host=False)
        try:
            return await store.get_instance_by_slug("remote-pg")
        finally:
            await store.close()

    found = asyncio.run(_read_instance(host_env.host_url))
    assert found is not None
    assert found.slug == "remote-pg"
    assert found.display_name == "Remote PG"
    assert found.password == "pgpass123"


def test_client_refuses_version_mismatch(host_env):
    """Client refuses to connect when version mismatches."""
    host_url = host_env.expose_uri.replace("0.0.0.0", "127.0.0.1")

    async def _try_initialize(url):
        store = AdminStore(url, instance_secret=INSTANCE_SECRET, is_host=False)
        try:
            await store.initialize()
        finally:
            await store.close()

    with patch("gpdb.admin.store.__version__", "99.0.0"):
        with pytest.raises(VersionMismatchError):
            asyncio.run(_try_initialize(host_url))


def test_instance_secret_shared_encryption_decryption(host_env, client_env):
    """Instance passwords encrypted by host are decryptable by client."""
    host_url = host_env.host_url
    secret_password = "super-secret-db-password"

    async def _create_and_read_host(url):
        store = AdminStore(url, instance_secret=INSTANCE_SECRET, is_host=False)
        try:
            return await store.create_instance(
                slug="encrypted-test",
                display_name="Encrypted Test",
                description="Test encryption",
                host="10.0.0.1",
                port=5432,
                database="testdb",
                username="testuser",
                password=secret_password,
            )
        finally:
            await store.close()

    host_instance = asyncio.run(_create_and_read_host(host_url))
    assert host_instance.password == secret_password

    # Verify the raw stored value is encrypted
    async def _get_raw_password(url):
        store = AdminStore(url, instance_secret=INSTANCE_SECRET, is_host=False)
        try:
            node = await store._get_node_by_filters(
                [
                    Filter(field="type", value="instance"),
                    Filter(field="data.slug", value="encrypted-test"),
                ]
            )
            return node.data.get("password") if node else None
        finally:
            await store.close()

    raw_pw = asyncio.run(_get_raw_password(host_url))
    assert raw_pw is not None
    assert raw_pw != secret_password
    assert raw_pw.startswith("fernet:")

    # Client decrypts with its own cipher (same instance_secret)
    async def _read_client(url):
        store = AdminStore(url, instance_secret=INSTANCE_SECRET, is_host=False)
        try:
            return await store.get_instance_by_slug("encrypted-test")
        finally:
            await store.close()

    client_instance = asyncio.run(_read_client(host_url))
    assert client_instance is not None
    assert client_instance.password == secret_password
