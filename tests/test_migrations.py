import asyncio

import pytest
import pytest_asyncio

from sqlalchemy import text

from gpdb import GPGraph
from gpdb.migrations import CORE_MIGRATIONS, discover_prefixed_tables, run_migrations


@pytest_asyncio.fixture
async def db(pg_server):
    """Fresh GPGraph instance with tables created. Drops everything after each test."""
    url = pg_server.get_uri()
    db = GPGraph(url)
    await db.create_tables()
    yield db
    # Drop migration table first, then graph tables
    async with db.sqla_engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS gpdb_migrations CASCADE"))
    await db.drop_tables()
    await db.sqla_engine.dispose()


# --- Helpers ---


async def _migration_version(conn, scope: str) -> int:
    row = (await conn.execute(
        text("SELECT COALESCE(MAX(version), 0) FROM gpdb_migrations WHERE scope = :s"),
        {"s": scope},
    )).one()
    return row[0]


async def _migration_rows(conn, scope: str) -> list:
    result = await conn.execute(
        text("SELECT scope, version, applied FROM gpdb_migrations WHERE scope = :s ORDER BY version"),
        {"s": scope},
    )
    return result.all()


# --- Tests ---


@pytest.mark.asyncio
async def test_migration_table_creation(db: GPGraph):
    """gpdb_migrations table is created with the correct schema."""
    # run_migrations creates the table as its first step
    await run_migrations(db.sqla_engine, scope="test", migrations=[])

    async with db.sqla_engine.connect() as conn:
        # Verify the table exists
        result = await conn.execute(text(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_name = 'gpdb_migrations' ORDER BY ordinal_position"
        ))
        columns = {row[0]: (row[1], row[2]) for row in result}

    assert "scope" in columns
    assert "version" in columns
    assert "applied" in columns

    # scope is TEXT, NOT NULL
    assert columns["scope"][0] == "text"
    assert columns["scope"][1] == "NO"

    # version is INTEGER, NOT NULL
    assert columns["version"][0] == "integer"
    assert columns["version"][1] == "NO"

    # applied is TIMESTAMPTZ, nullable (has DEFAULT)
    assert columns["applied"][0] == "timestamp with time zone"
    assert columns["applied"][1] == "YES"

    # Verify primary key constraint
    async with db.sqla_engine.connect() as conn:
        result = await conn.execute(text(
            "SELECT constraint_name FROM information_schema.table_constraints "
            "WHERE table_name = 'gpdb_migrations' AND constraint_type = 'PRIMARY KEY'"
        ))
        pk = result.fetchall()
    assert len(pk) == 1


@pytest.mark.asyncio
async def test_baseline_migration_runs_once(db: GPGraph):
    """Migration 1 (baseline) runs and records in the table, then skips on re-run."""
    # First run — baseline should apply
    version = await run_migrations(db.sqla_engine, scope="core", migrations=CORE_MIGRATIONS)
    assert version == 1

    async with db.sqla_engine.connect() as conn:
        rows = await _migration_rows(conn, "core")
    assert len(rows) == 1
    assert rows[0].version == 1
    assert rows[0].scope == "core"

    # Second run — nothing new should apply
    version2 = await run_migrations(db.sqla_engine, scope="core", migrations=CORE_MIGRATIONS)
    assert version2 == 1

    async with db.sqla_engine.connect() as conn:
        rows2 = await _migration_rows(conn, "core")
    # Still exactly one row — no duplicate
    assert len(rows2) == 1


@pytest.mark.asyncio
async def test_version_tracking(db: GPGraph):
    """After running migrations, the correct version is returned."""
    # No migrations yet — version should be 0
    # (run_migrations creates the table first, then checks)
    version = await run_migrations(db.sqla_engine, scope="test", migrations=[])
    assert version == 0

    # Run with baseline
    version = await run_migrations(db.sqla_engine, scope="test", migrations=CORE_MIGRATIONS)
    assert version == 1

    # Add a second migration and run
    async def _migration_2(conn):
        pass

    migrations = list(CORE_MIGRATIONS) + [(2, "test migration", _migration_2)]
    version = await run_migrations(db.sqla_engine, scope="test", migrations=migrations)
    assert version == 2

    # Verify stored version
    async with db.sqla_engine.connect() as conn:
        stored = await _migration_version(conn, "test")
    assert stored == 2


@pytest.mark.asyncio
async def test_discover_prefixed_tables(db: GPGraph):
    """discover_prefixed_tables finds prefixed tables and parses out prefixes."""
    # Default tables (no prefix) are created by the fixture
    # Create additional prefixed tables
    async with db.sqla_engine.begin() as conn:
        await conn.execute(text("CREATE TABLE IF NOT EXISTS mygraph_nodes (id SERIAL PRIMARY KEY)"))
        await conn.execute(text("CREATE TABLE IF NOT EXISTS mygraph_edges (id SERIAL PRIMARY KEY)"))
        await conn.execute(text("CREATE TABLE IF NOT EXISTS mygraph_schemas (id SERIAL PRIMARY KEY)"))
        await conn.execute(text("CREATE TABLE IF NOT EXISTS other_nodes (id SERIAL PRIMARY KEY)"))
        await conn.execute(text("CREATE TABLE IF NOT EXISTS other_edges (id SERIAL PRIMARY KEY)"))

    async with db.sqla_engine.connect() as conn:
        prefixes = await discover_prefixed_tables(conn)

    # Should find "mygraph" (has all three suffixes) and "other" (has nodes + edges).
    # Bare tables (nodes, edges, schemas) are NOT matched because the LIKE pattern
    # requires at least one character before the underscore suffix.
    assert "mygraph" in prefixes
    assert "other" in prefixes
    assert "" not in prefixes


@pytest.mark.asyncio
async def test_migration_idempotency(db: GPGraph):
    """Running migrations multiple times is safe and doesn't cause errors."""
    for _ in range(5):
        version = await run_migrations(
            db.sqla_engine, scope="core", migrations=CORE_MIGRATIONS
        )
        assert version == 1

    # Only one row should exist
    async with db.sqla_engine.connect() as conn:
        rows = await _migration_rows(conn, "core")
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_concurrent_run_migrations_single_row(db: GPGraph):
    """Parallel runners for the same scope must not hit PRIMARY KEY errors."""
    scope = "concurrent_migrations"

    async def _run() -> int:
        return await run_migrations(db.sqla_engine, scope=scope, migrations=CORE_MIGRATIONS)

    versions = await asyncio.gather(_run(), _run(), _run())
    assert versions == [1, 1, 1]

    async with db.sqla_engine.connect() as conn:
        rows = await _migration_rows(conn, scope)
    assert len(rows) == 1
    assert rows[0].version == 1
