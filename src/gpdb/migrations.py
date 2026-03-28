"""
Database migration system for gpdb.

Tracks schema versions via a ``gpdb_migrations`` table and runs ordered
migration functions to bring a database up to the current schema.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, List, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

logger = logging.getLogger(__name__)

# Per-(scope, version) keys for pg_advisory_xact_lock(hashtext(...)) so concurrent
# startup cannot double-apply the same migration or race on INSERT.
_MIGRATION_LOCK_KEY_PREFIX = "gpdb.migrations:"

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_CREATE_MIGRATIONS_TABLE = text("""
CREATE TABLE IF NOT EXISTS gpdb_migrations (
    scope    TEXT        NOT NULL,
    version  INTEGER     NOT NULL,
    applied  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (scope, version)
)
""")

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

MigrationFn = Callable[[AsyncConnection], Any]
Migration = Tuple[int, str, MigrationFn]

_ADVISORY_LOCK_MIGRATION = text(
    "SELECT pg_advisory_xact_lock(hashtext(CAST(:lock_key AS text)))"
)
_INSERT_MIGRATION_ROW = text(
    "INSERT INTO gpdb_migrations (scope, version) VALUES (:s, :v) "
    "ON CONFLICT (scope, version) DO NOTHING"
)
_MAX_VERSION_FOR_SCOPE = text(
    "SELECT COALESCE(MAX(version), 0) FROM gpdb_migrations WHERE scope = :s"
)


async def run_migrations(
    engine: AsyncEngine,
    scope: str,
    migrations: Sequence[Migration],
) -> int:
    """Run pending *migrations* for *scope* and return the current max version.

    Concurrent callers with the same *scope* serialize per migration version via
    ``pg_advisory_xact_lock``: each migration runs only if the row is not yet
    recorded, avoiding duplicate work and PRIMARY KEY violations on
    ``gpdb_migrations``.
    """
    async with engine.begin() as conn:
        await conn.execute(_CREATE_MIGRATIONS_TABLE)

    async with engine.connect() as conn:
        row = (await conn.execute(_MAX_VERSION_FOR_SCOPE, {"s": scope})).one()
        initial_current = row[0]

    pending = [(v, desc, fn) for v, desc, fn in migrations if v > initial_current]
    pending.sort(key=lambda m: m[0])

    for version, desc, fn in pending:
        logger.info("migration %s/%d: %s", scope, version, desc)
        lock_key = f"{_MIGRATION_LOCK_KEY_PREFIX}{scope}:{version}"
        async with engine.begin() as conn:
            await conn.execute(_ADVISORY_LOCK_MIGRATION, {"lock_key": lock_key})
            row = (await conn.execute(_MAX_VERSION_FOR_SCOPE, {"s": scope})).one()
            if row[0] >= version:
                continue
            await fn(conn)
            await conn.execute(
                _INSERT_MIGRATION_ROW,
                {"s": scope, "v": version},
            )

    async with engine.connect() as conn:
        row = (await conn.execute(_MAX_VERSION_FOR_SCOPE, {"s": scope})).one()
        return int(row[0])

# ---------------------------------------------------------------------------
# Table discovery
# ---------------------------------------------------------------------------

_TABLE_SUFFIXES = ("_nodes", "_edges", "_schemas")


async def discover_prefixed_tables(conn: AsyncConnection) -> List[str]:
    """Return prefixes for all ``*_nodes``, ``*_edges``, ``*_schemas`` tables.

    Limitation: the ``LIKE '%\\_nodes'`` pattern requires at least one character
    before the underscore, so bare ``nodes`` / ``edges`` / ``schemas`` (default
    ``table_prefix=""``) are not returned. Core migrations that must touch those
    tables should match them explicitly (e.g. ``tablename = 'nodes'``) in addition
    to calling this helper.
    """
    conditions = " OR ".join(
        f"tablename LIKE '%\\_{s}'" for s in ("nodes", "edges", "schemas")
    )
    result = await conn.execute(text(
        f"SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND ({conditions})"
    ))
    prefixes: set[str] = set()
    for (tablename,) in result:
        for suffix in _TABLE_SUFFIXES:
            if tablename.endswith(suffix):
                prefixes.add(tablename[: -len(suffix)])
                break
    return sorted(prefixes)

# ---------------------------------------------------------------------------
# Core migrations
# ---------------------------------------------------------------------------


async def _baseline(conn: AsyncConnection) -> None:
    """No-op baseline — records that the database is now under migration control."""


CORE_MIGRATIONS: List[Migration] = [
    (1, "baseline", _baseline),
]
