"""Tests for gpdb-admin database migrations."""

from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy import text

from gpdb import GPGraph, SchemaRef
from gpdb.admin.migrations import _migrate_1000_admin_baseline
from gpdb.admin.store import AdminStore


@pytest_asyncio.fixture
async def empty_admin_store(pg_server):
    """Fresh database with admin tables and migrations applied; store left open."""
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

        store = AdminStore(url, instance_secret="admin-migration-test-secret")
        await store.initialize()
        yield store
        await store.close()
    finally:
        await db.sqla_engine.dispose()


@pytest.mark.asyncio
async def test_admin_baseline_skips_set_schemas_when_present(empty_admin_store: AdminStore):
    """Re-running baseline must not bump schema versions (set_schemas is not a no-op)."""
    refs = [
        SchemaRef(name=n, kind="node")
        for n in ("instance", "graph", "user", "api_key")
    ]
    schemas = await empty_admin_store.db.get_schemas(refs)
    before = [s.version for s in schemas]

    await _migrate_1000_admin_baseline(empty_admin_store)
    await _migrate_1000_admin_baseline(empty_admin_store)

    schemas_after = await empty_admin_store.db.get_schemas(refs)
    assert [s.version for s in schemas_after] == before
