"""Tests for version compatibility gating."""

from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy import text

from gpdb import GPGraph
from gpdb.admin.migrations import (
    check_version_compatibility,
    register_server_version,
)
from gpdb.admin.store import AdminStore, VersionMismatchError


@pytest_asyncio.fixture
async def fresh_admin_store(pg_server):
    """Fresh database with admin tables and migrations applied."""
    url = pg_server.get_uri()
    from gpdb.models.base import _Base
    from gpdb.models.factories import _model_cache

    db = GPGraph(url)
    try:
        async with db.sqla_engine.begin() as conn:
            result = await conn.execute(
                text(
                    "select tablename from pg_tables "
                    "where schemaname = current_schema()"
                )
            )
            for table_name in result.scalars().all():
                quoted = str(table_name).replace('"', '""')
                await conn.execute(text(f'DROP TABLE IF EXISTS "{quoted}" CASCADE'))
        _model_cache.clear()
        _Base.metadata.clear()

        store = AdminStore(url, instance_secret="version-gate-test-secret", is_host=True)
        await store.initialize()
        yield store
        await store.close()
    finally:
        await db.sqla_engine.dispose()


@pytest.mark.asyncio
async def test_version_match_passes(fresh_admin_store: AdminStore):
    """Version check passes when all registered servers have the same version."""
    engine = fresh_admin_store.db.sqla_engine

    await register_server_version(engine, "server-1", "0.6")
    await register_server_version(engine, "server-2", "0.6")

    await check_version_compatibility(engine, "0.6")


@pytest.mark.asyncio
async def test_major_version_mismatch_fails(fresh_admin_store: AdminStore):
    """Version check fails when a server has a different major version."""
    engine = fresh_admin_store.db.sqla_engine

    await register_server_version(engine, "server-1", "0.6")
    await register_server_version(engine, "server-2", "1.0")

    with pytest.raises(VersionMismatchError) as exc_info:
        await check_version_compatibility(engine, "0.6")

    assert "server-2" in str(exc_info.value)
    assert "1.0" in str(exc_info.value)
    assert "0.6" in str(exc_info.value)


@pytest.mark.asyncio
async def test_minor_version_mismatch_fails(fresh_admin_store: AdminStore):
    """Version check fails when a server has a different minor version."""
    engine = fresh_admin_store.db.sqla_engine

    await register_server_version(engine, "server-1", "0.6")
    await register_server_version(engine, "server-2", "0.7")

    with pytest.raises(VersionMismatchError) as exc_info:
        await check_version_compatibility(engine, "0.6")

    assert "server-2" in str(exc_info.value)
    assert "0.7" in str(exc_info.value)
    assert "0.6" in str(exc_info.value)


@pytest.mark.asyncio
async def test_empty_version_table_allows_check(fresh_admin_store: AdminStore):
    """Version check passes with a warning when the version table is empty."""
    engine = fresh_admin_store.db.sqla_engine

    await check_version_compatibility(engine, "0.6")


@pytest.mark.asyncio
async def test_host_registers_version_on_initialize(pg_server):
    """Host mode registers its version when initializing."""
    url = pg_server.get_uri()
    from gpdb.models.base import _Base
    from gpdb.models.factories import _model_cache

    db = GPGraph(url)
    try:
        async with db.sqla_engine.begin() as conn:
            result = await conn.execute(
                text(
                    "select tablename from pg_tables "
                    "where schemaname = current_schema()"
                )
            )
            for table_name in result.scalars().all():
                quoted = str(table_name).replace('"', '""')
                await conn.execute(text(f'DROP TABLE IF EXISTS "{quoted}" CASCADE'))
        _model_cache.clear()
        _Base.metadata.clear()

        store = AdminStore(url, instance_secret="host-test-secret", is_host=True)
        await store.initialize()

        async with db.sqla_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT server_id, major_minor FROM admin_server_versions")
            )
            rows = result.fetchall()

        assert len(rows) == 1
        server_id, major_minor = rows[0]
        assert major_minor == "0.6"

        await store.close()
    finally:
        await db.sqla_engine.dispose()


@pytest.mark.asyncio
async def test_client_skips_migration_execution(pg_server):
    """Client mode skips admin migration execution even when DB is behind."""
    url = pg_server.get_uri()
    from gpdb.models.base import _Base
    from gpdb.models.factories import _model_cache

    db = GPGraph(url)
    try:
        async with db.sqla_engine.begin() as conn:
            result = await conn.execute(
                text(
                    "select tablename from pg_tables "
                    "where schemaname = current_schema()"
                )
            )
            for table_name in result.scalars().all():
                quoted = str(table_name).replace('"', '""')
                await conn.execute(text(f'DROP TABLE IF EXISTS "{quoted}" CASCADE'))
        _model_cache.clear()
        _Base.metadata.clear()

        host_store = AdminStore(url, instance_secret="host-test-secret", is_host=True)
        await host_store.initialize()
        await host_store.close()

        client_store = AdminStore(url, instance_secret="client-test-secret", is_host=False)
        await client_store.initialize()
        await client_store.close()

        async with db.sqla_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM gpdb_migrations WHERE scope = 'admin'")
            )
            count = result.scalar()

        assert count == 2
    finally:
        await db.sqla_engine.dispose()
