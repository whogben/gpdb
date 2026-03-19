"""
Store / retrieve / search data that is somewhat graph shaped.

Generic utility - designed to live in this one file
(and in test_graph_db.py) and be portable for other projects.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, Dict, List, Union

from sqlalchemy import event, inspect, MetaData, Table
from sqlalchemy.schema import DropTable

# Import from new modules
from gpdb.models import (
    SchemaKind,
    SchemaNotFoundError,
    SchemaValidationError,
    SchemaKindMismatchError,
    SchemaBreakingChangeError,
    SchemaInUseError,
    _Base,
    _GPRecord,
    _GPNodeBase,
    _GPEdgeBase,
    _GPNode,
    _GPEdge,
    _GPSchema,
    create_node_model,
    create_edge_model,
    create_schema_model,
    NodeUpsert,
    NodeRead,
    NodeReadWithPayload,
    EdgeUpsert,
    EdgeRead,
    SchemaUpsert,
    generate_id,
)
from gpdb.odm import NodeModel, EdgeModel
from gpdb.search import (
    Op,
    Logic,
    Filter,
    FilterGroup,
    Sort,
    SearchQuery,
    Page,
)
from gpdb.graph_schemas import SchemaMixin
from gpdb.graph_nodes import NodeMixin
from gpdb.graph_edges import EdgeMixin


class GPGraph(SchemaMixin, NodeMixin, EdgeMixin):
    """
    Generic database utility for storing/retrieving/searching graph records.
    Uses SQLAlchemy for ORM and asyncpg for PostgreSQL client.

    Args:
        url: PostgreSQL connection URL
        table_prefix: Optional prefix for table names (e.g., "scratch" -> "scratch_nodes", "scratch_edges")
    """

    def __init__(self, url: str, table_prefix: str = ""):
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

        if "+asyncpg" not in url.split("://")[0]:
            url = url.replace("://", "+asyncpg://")
        self.sqla_engine = create_async_engine(url)
        self.sqla_sessionmaker = async_sessionmaker(
            self.sqla_engine, expire_on_commit=False
        )
        self._session_ctx = ContextVar(f"session_{id(self)}", default=None)
        self._validators: Dict[str, Any] = {}
        self._schema_kinds: Dict[str, SchemaKind] = {}

        # Create dynamic models if prefix specified, else use defaults
        if table_prefix:
            node_table = f"{table_prefix}_nodes"
            edge_table = f"{table_prefix}_edges"
            schema_table = f"{table_prefix}_schemas"
            self._Node = create_node_model(node_table)
            self._Edge = create_edge_model(edge_table, node_table)
            self._Schema = create_schema_model(schema_table)
        else:
            self._Node = _GPNode
            self._Edge = _GPEdge
            self._Schema = _GPSchema

        # Expose ORM models for external access
        self.SchemaTable = self._Schema
        self.NodeTable = self._Node
        self.EdgeTable = self._Edge

    @asynccontextmanager
    async def transaction(self):
        """
        Wrap multiple GPGraph operations in a single atomic transaction.

        Usage:
            async with db.transaction():
                await db.set_nodes([NodeUpsert(...)])
                await db.set_edges([EdgeUpsert(...)])
                # Both committed together, or both rolled back on error
        """
        async with self.sqla_sessionmaker() as session:
            async with session.begin():
                token = self._session_ctx.set(session)
                try:
                    yield  # Yield nothing — callers use self.set_nodes() etc.
                finally:
                    self._session_ctx.reset(token)

    @asynccontextmanager
    async def _get_session(self):
        session = self._session_ctx.get()
        if session:
            yield session
        else:
            async with self.sqla_sessionmaker() as new_session:
                async with new_session.begin():
                    yield new_session

    async def create_tables(self):
        """
        Create tables for this GPGraph instance's models.
        Idempotent: does nothing if tables already exist.
        """
        async with self.sqla_engine.begin() as conn:
            # Only create this instance's specific tables
            await conn.run_sync(
                lambda sync_conn: self._Schema.__table__.create(
                    sync_conn, checkfirst=True
                )
            )
            await conn.run_sync(
                lambda sync_conn: self._Node.__table__.create(
                    sync_conn, checkfirst=True
                )
            )
            await conn.run_sync(
                lambda sync_conn: self._Edge.__table__.create(
                    sync_conn, checkfirst=True
                )
            )

    async def drop_tables(self):
        """
        Drop tables for this GPGraph instance's models.
        """
        async with self.sqla_engine.begin() as conn:
            # Drop in reverse order of create_tables (edges, nodes, schema) due to FKs
            await conn.run_sync(
                lambda sync_conn: self._Edge.__table__.drop(sync_conn, checkfirst=True)
            )
            await conn.run_sync(
                lambda sync_conn: self._Node.__table__.drop(sync_conn, checkfirst=True)
            )
            await conn.run_sync(
                lambda sync_conn: self._Schema.__table__.drop(
                    sync_conn, checkfirst=True
                )
            )

    async def drop_tables_for_prefix(self, table_prefix: str):
        """
        Drop tables for a specific table prefix.
        Used for cleaning up materialized view tables when views are deleted.
        Must be called within a db.transaction() context.

        Args:
            table_prefix: The prefix to drop tables for (e.g., "view_abc123")
        """
        import re

        # Validate prefix format
        if not re.match(r"^[a-zA-Z0-9_-]+$", table_prefix):
            raise ValueError(f"Invalid table_prefix: {table_prefix}")

        node_table = f"{table_prefix}_nodes"
        edge_table = f"{table_prefix}_edges"
        schema_table = f"{table_prefix}_schemas"

        # Get current session from transaction context
        session = self._session_ctx.get()
        if session is None:
            raise RuntimeError(
                "drop_tables_for_prefix must be called within db.transaction()"
            )

        # Drop edges first (due to FKs), then nodes, then schema table
        metadata = MetaData()
        edge_table_obj = Table(edge_table, metadata)
        node_table_obj = Table(node_table, metadata)
        schema_table_obj = Table(schema_table, metadata)

        await session.execute(DropTable(edge_table_obj, if_exists=True))
        await session.execute(DropTable(node_table_obj, if_exists=True))
        await session.execute(DropTable(schema_table_obj, if_exists=True))


# Update payload hash and size when it is inserted or updated.
@event.listens_for(_GPNodeBase, "before_insert", propagate=True)
@event.listens_for(_GPNodeBase, "before_update", propagate=True)
def _update_payload_metadata(mapper, connection, target):
    import hashlib

    # Check if payload has been modified
    hist = inspect(target).attrs.payload.history
    if hist.has_changes():
        if target.payload is not None:
            target.payload_size = len(target.payload)
            target.payload_hash = hashlib.sha256(target.payload).hexdigest()
            if not target.payload_mime:
                target.payload_mime = "application/octet-stream"
        else:
            # Payload was cleared
            target.payload_size = 0
            target.payload_hash = None
            target.payload_mime = None
            target.payload_filename = None


# -----------------------------------------------------------------------------
# Public Exports
# -----------------------------------------------------------------------------

__all__ = [
    # Exceptions
    "SchemaKind",
    "SchemaNotFoundError",
    "SchemaValidationError",
    "SchemaKindMismatchError",
    "SchemaBreakingChangeError",
    "SchemaInUseError",
    # Pydantic models
    "Op",
    "Logic",
    "Filter",
    "FilterGroup",
    "Sort",
    "SearchQuery",
    "Page",
    "NodeUpsert",
    "NodeRead",
    "NodeReadWithPayload",
    "EdgeUpsert",
    "EdgeRead",
    # ODM Base Classes
    "NodeModel",
    "EdgeModel",
    # Utilities
    "generate_id",
    # Main class
    "GPGraph",
]
