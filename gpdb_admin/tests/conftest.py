import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from gpdb import GPGraph
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
    tmp = tmp_path_factory.mktemp("admin-data")
    manager = _create_test_manager(tmp)
    with TestClient(manager.app) as client:
        yield SimpleNamespace(manager=manager, client=client)


@pytest.fixture(autouse=True)
def _reset_admin_test_env(admin_test_env):
    """Reset the shared admin runtime to a fresh state before each test."""
    services = admin_test_env.manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None

    admin_test_env.client.cookies.clear()
    asyncio.run(
        _reset_captive_database(
            services.captive_server.get_uri(),
            services.resolved_config.auth.session_secret,
        )
    )


async def _reset_captive_database(url: str, session_secret: str) -> None:
    # Clear model cache BEFORE creating GPGraph instance
    from gpdb.models.factories import _model_cache
    from gpdb.models.base import _Base
    _model_cache.clear()
    _Base.metadata.clear()
    
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
        # Create schemas for admin node types (instance, graph, user, api_key)
        # These are used as type identifiers, not for validation
        from gpdb import SchemaUpsert
        await store.db.set_schemas([
            SchemaUpsert(name="instance", json_schema={"type": "object"}, kind="node"),
            SchemaUpsert(name="graph", json_schema={"type": "object"}, kind="node"),
            SchemaUpsert(name="user", json_schema={"type": "object"}, kind="node"),
            SchemaUpsert(name="api_key", json_schema={"type": "object"}, kind="node"),
        ])
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


def _create_test_manager(tmp_path: Path):
    """Create a manager backed by a temporary config and captive data dir."""
    data_dir = tmp_path / "admin data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "admin.toml").write_text(
        (
            "[server]\n"
            'host = "127.0.0.1"\n'
            "port = 8747\n"
            "[auth]\n"
            'session_secret = "test-session-secret"\n'
        ),
        encoding="utf-8",
    )
    config_store = ConfigStore.from_sources(cli_data_dir=data_dir)
    resolved_config = config_store.load()
    return entry.create_manager(
        resolved_config=resolved_config, config_store=config_store
    )
