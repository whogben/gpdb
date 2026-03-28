"""Admin-specific database migrations for gpdb-admin."""

from __future__ import annotations

import logging
from typing import Any, Callable, List, Tuple

from gpdb import SchemaUpsert
from gpdb.migrations import run_migrations
from sqlalchemy.ext.asyncio import AsyncConnection

logger = logging.getLogger(__name__)

# Admin graph node schemas used as type identifiers (not for validation).
_BASELINE_ADMIN_NODE_SCHEMAS: tuple[SchemaUpsert, ...] = (
    SchemaUpsert(name="instance", json_schema={"type": "object"}, kind="node"),
    SchemaUpsert(name="graph", json_schema={"type": "object"}, kind="node"),
    SchemaUpsert(name="user", json_schema={"type": "object"}, kind="node"),
    SchemaUpsert(name="api_key", json_schema={"type": "object"}, kind="node"),
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


ADMIN_MIGRATIONS: List[AdminMigration] = [
    (1000, "admin baseline schemas", _migrate_1000_admin_baseline),
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
