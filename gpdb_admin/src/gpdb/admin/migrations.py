"""Admin-specific database migrations for gpdb-admin."""

from __future__ import annotations

import logging
from typing import Any, Callable, List, Tuple

from gpdb import SchemaUpsert
from gpdb.admin.store.exceptions import VersionMismatchError
from gpdb.migrations import run_migrations
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

logger = logging.getLogger(__name__)

# Admin graph node schemas used as type identifiers (not for validation).
_BASELINE_ADMIN_NODE_SCHEMAS: tuple[SchemaUpsert, ...] = (
    SchemaUpsert(name="instance", json_schema={"type": "object"}, kind="node"),
    SchemaUpsert(name="graph", json_schema={"type": "object"}, kind="node"),
    SchemaUpsert(name="user", json_schema={"type": "object"}, kind="node"),
    SchemaUpsert(name="api_key", json_schema={"type": "object"}, kind="node"),
    SchemaUpsert(name="postgres_credential", json_schema={"type": "object"}, kind="node"),
)
_BASELINE_ADMIN_NODE_NAMES = frozenset(s.name for s in _BASELINE_ADMIN_NODE_SCHEMAS)

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

# Admin migration functions receive the AdminStore instance so they can use
# admin_store.db (GPGraph) for high-level operations like set_schemas.
AdminMigrationFn = Callable[..., Any]
AdminMigration = Tuple[int, str, AdminMigrationFn]

# ---------------------------------------------------------------------------
# Admin migrations
# ---------------------------------------------------------------------------


async def _migrate_1000_admin_baseline(admin_store: Any) -> None:
    """Ensure admin schemas (instance, graph, user, api_key) exist.

    Only registers missing node schemas. ``set_schemas`` treats identical JSON as a
    patch bump; skipping existing rows keeps re-runs (e.g. after a failed migration
    bookkeeping insert) from advancing versions.
    """
    db = admin_store.db
    existing_node_names = {ref.name for ref in await db.list_schemas(kind="node")}
    missing = _BASELINE_ADMIN_NODE_NAMES - existing_node_names
    if not missing:
        return
    to_apply = [s for s in _BASELINE_ADMIN_NODE_SCHEMAS if s.name in missing]
    await db.set_schemas(to_apply)


async def _migrate_1001_server_versions(admin_store: Any) -> None:
    """Create the admin_server_versions table for version compatibility tracking."""
    create_table_sql = text("""
        CREATE TABLE IF NOT EXISTS admin_server_versions (
            server_id TEXT PRIMARY KEY,
            major_minor TEXT NOT NULL,
            connected_at TIMESTAMPTZ DEFAULT now()
        )
    """)
    async with admin_store.db.sqla_engine.begin() as conn:
        await conn.execute(create_table_sql)


ADMIN_MIGRATIONS: List[AdminMigration] = [
    (1000, "admin baseline schemas", _migrate_1000_admin_baseline),
    (1001, "server versions table", _migrate_1001_server_versions),
]

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


async def run_admin_migrations(admin_store: Any) -> int:
    """Run pending admin migrations against *admin_store* and return the new version."""
    engine = admin_store.db.sqla_engine

    # Adapt admin callables (admin_store) to the core runner's (conn) -> None.
    # *conn* is unused: admin work uses admin_store.db sessions, which commit in
    # separate transactions from the runner's migration-row INSERT. Baseline
    # migrations only call set_schemas for missing names so re-runs stay safe if
    # that INSERT ever fails after schema writes.
    wrapped: List[Tuple[int, str, Callable[[AsyncConnection], Any]]] = [
        (v, desc, lambda conn, _fn=fn: _fn(admin_store))
        for v, desc, fn in ADMIN_MIGRATIONS
    ]

    return await run_migrations(engine, scope="admin", migrations=wrapped)


# ---------------------------------------------------------------------------
# Version compatibility
# ---------------------------------------------------------------------------


async def check_version_compatibility(engine: AsyncEngine, expected_version: str) -> None:
    """Check that all registered server versions match the expected version.

    Args:
        engine: SQLAlchemy async engine
        expected_version: The major.minor version string to check against (e.g., "0.6")

    Raises:
        VersionMismatchError: If any registered server has a different version
    """
    select_versions = text("SELECT server_id, major_minor FROM admin_server_versions")
    try:
        async with engine.connect() as conn:
            result = await conn.execute(select_versions)
            rows = result.fetchall()
    except ProgrammingError:
        logger.warning("admin_server_versions table does not exist - host may not have run migrations yet")
        return

    if not rows:
        logger.warning("admin_server_versions table is empty - host may not have written yet")
        return

    for server_id, major_minor in rows:
        if major_minor != expected_version:
            raise VersionMismatchError(
                f"Version mismatch: server {server_id} has version {major_minor}, "
                f"but expected {expected_version}"
            )


async def register_server_version(engine: AsyncEngine, server_id: str, version: str) -> None:
    """Register or update a server's version in the admin_server_versions table.

    Args:
        engine: SQLAlchemy async engine
        server_id: Unique identifier for this server instance
        version: The major.minor version string (e.g., "0.6")
    """
    upsert_sql = text("""
        INSERT INTO admin_server_versions (server_id, major_minor, connected_at)
        VALUES (:server_id, :major_minor, now())
        ON CONFLICT (server_id) DO UPDATE SET
            major_minor = EXCLUDED.major_minor,
            connected_at = EXCLUDED.connected_at
    """)
    async with engine.begin() as conn:
        await conn.execute(upsert_sql, {"server_id": server_id, "major_minor": version})
