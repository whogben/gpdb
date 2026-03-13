"""
Store / retrieve / search data that is somewhat graph shaped.

Generic utility - designed to live in this one file
(and in test_graph_db.py) and be portable for other projects.
"""

from __future__ import annotations

import copy
import hashlib
import re
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generic, List, Literal, Optional, TypeVar, Union, cast
from uuid import uuid4

import jsonschema
import jsonschema.exceptions
from pydantic import BaseModel, ConfigDict, Field, field_validator

from sqlalchemy import (
    DateTime,
    ForeignKey,
    MetaData,
    Table,
    func,
    Index,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
    and_,
    or_,
    delete,
    event,
    inspect,
    select,
)
from sqlalchemy.schema import DropTable
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    declared_attr,
    undefer,
)
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker


# -----------------------------------------------------------------------------
# Schema & Models
# -----------------------------------------------------------------------------


class SchemaNotFoundError(Exception):
    """Raised when a schema is not found."""

    pass


class SchemaValidationError(Exception):
    """Raised when data validation against a schema fails."""

    pass


class SchemaKindMismatchError(SchemaValidationError):
    """Raised when a schema is attached to the wrong graph record kind."""

    pass


class SchemaBreakingChangeError(Exception):
    """Raised when a schema update contains breaking changes."""

    pass


class SchemaInUseError(Exception):
    """Raised when attempting to delete a schema that is still referenced by nodes or edges."""

    pass


SchemaKind = Literal["node", "edge"]
_SCHEMA_KIND_FIELD = "x-gpdb-kind"
_SCHEMA_KIND_VALUES = {"node", "edge"}


def _normalize_schema_kind(kind: str) -> SchemaKind:
    clean_kind = kind.strip().lower()
    if clean_kind not in _SCHEMA_KIND_VALUES:
        raise ValueError("Schema kind must be either 'node' or 'edge'.")
    return cast(SchemaKind, clean_kind)


def _extract_schema_kind(
    json_schema: Dict[str, Any], *, required: bool = True
) -> SchemaKind | None:
    raw_kind = json_schema.get(_SCHEMA_KIND_FIELD)
    if raw_kind is None:
        if required:
            raise ValueError(
                f"Schema JSON must include '{_SCHEMA_KIND_FIELD}' with value 'node' or 'edge'."
            )
        return None
    if not isinstance(raw_kind, str):
        raise ValueError(
            f"Schema field '{_SCHEMA_KIND_FIELD}' must be a string with value 'node' or 'edge'."
        )
    return _normalize_schema_kind(raw_kind)


class Op(str, Enum):
    EQ = "eq"
    GT = "gt"
    LT = "lt"
    CONTAINS = "contains"
    IN = "in"


class Logic(str, Enum):
    AND = "and"
    OR = "or"


class Filter(BaseModel):
    field: str
    op: Op = Op.EQ
    value: Any

    def to_dsl(self) -> str:
        """Convert Filter to DSL string."""
        val_str = _value_to_dsl(self.value)
        return f"{self.field} {self.op.value} {val_str}"


class FilterGroup(BaseModel):
    logic: Logic = Logic.AND
    filters: List[Union[Filter, "FilterGroup"]]

    def to_dsl(self) -> str:
        """Convert FilterGroup to DSL string."""
        parts = []
        for f in self.filters:
            if isinstance(f, Filter):
                parts.append(f.to_dsl())
            else:
                parts.append(f.to_dsl())
        inner = f" {self.logic.value} ".join(parts)
        if len(parts) > 1:
            return f"({inner})"
        return inner

    @classmethod
    def from_dsl(cls, text: str) -> Union[Filter, "FilterGroup"]:
        """Parse DSL string into Filter or FilterGroup."""
        tokens = _tokenize(text)
        result, _ = _parse_expr(tokens, 0)
        return result


# Allow recursive nesting
FilterGroup.model_rebuild()


class Sort(BaseModel):
    field: str
    desc: bool = True


class SearchQuery(BaseModel):
    filter: Optional[Union[FilterGroup, Filter, str]] = None
    sort: List[Sort] = Field(default_factory=list)
    limit: int = 50
    offset: int = 0
    select: Optional[List[str]] = None

    @field_validator("filter", mode="before")
    @classmethod
    def parse_filter(cls, v: Any) -> Any:
        if isinstance(v, str):
            if not v.strip():
                return None
            return FilterGroup.from_dsl(v)
        return v


T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    items: List[T]
    total: int
    limit: int
    offset: int


# -----------------------------------------------------------------------------
# Pydantic Data Transfer Objects for Public API
# -----------------------------------------------------------------------------


class NodeUpsert(BaseModel):
    """Input model for creating/updating nodes."""

    id: Optional[str] = None
    type: str
    name: Optional[str] = None
    owner_id: Optional[str] = None
    parent_id: Optional[str] = None
    schema_name: Optional[str] = None
    data: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    payload: Optional[bytes] = None
    payload_mime: Optional[str] = None
    payload_filename: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class NodeRead(BaseModel):
    """Output model for nodes without payload."""

    id: str
    type: str
    name: Optional[str] = None
    owner_id: Optional[str] = None
    parent_id: Optional[str] = None
    schema_name: Optional[str] = None
    data: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    version: int
    payload_size: int = 0
    payload_hash: Optional[str] = None
    payload_mime: Optional[str] = None
    payload_filename: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class NodeReadWithPayload(NodeRead):
    """Output model for nodes with payload."""

    payload: Optional[bytes] = None


class EdgeUpsert(BaseModel):
    """Input model for creating/updating edges."""

    id: Optional[str] = None
    type: str
    source_id: str
    target_id: str
    schema_name: Optional[str] = None
    data: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class EdgeRead(BaseModel):
    """Output model for edges."""

    id: str
    type: str
    source_id: str
    target_id: str
    schema_name: Optional[str] = None
    data: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    version: int

    model_config = ConfigDict(from_attributes=True)


# -----------------------------------------------------------------------------
# Domain Model Base Classes (ODM)
# -----------------------------------------------------------------------------


class NodeModel(BaseModel):
    """
    Base class for domain models backed by the graph DB.

    Subclass this to define specific node types with strong typing.
    Any fields not defined in the standard schema will be automatically
    packed into the 'data' dictionary.
    """

    # System fields are prefixed with 'node_' to avoid collisions
    # with user-defined fields in subclasses.
    node_id: Optional[str] = None
    node_type: str = "node"
    node_name: Optional[str] = None
    node_owner_id: Optional[str] = None
    node_parent_id: Optional[str] = None
    node_tags: List[str] = Field(default_factory=list)

    # Read-only metadata
    node_created_at: Optional[datetime] = None
    node_updated_at: Optional[datetime] = None
    node_version: Optional[int] = None
    node_payload_size: int = 0
    node_payload_hash: Optional[str] = None
    node_payload_mime: Optional[str] = None
    node_payload_filename: Optional[str] = None

    # Payload content (optional)
    node_payload: Optional[bytes] = None

    model_config = ConfigDict(extra="allow")

    def to_upsert(self) -> NodeUpsert:
        """Convert this domain model to a DB-ready Upsert DTO."""
        # Map system fields to DTO fields
        upsert_data = {
            "id": self.node_id,
            "type": self.node_type,
            "name": self.node_name,
            "owner_id": self.node_owner_id,
            "parent_id": self.node_parent_id,
            "tags": self.node_tags,
            "payload": self.node_payload,
            "payload_mime": self.node_payload_mime,
            "payload_filename": self.node_payload_filename,
        }

        # Identify all system keys to exclude from data
        system_keys = {
            "node_id",
            "node_type",
            "node_name",
            "node_owner_id",
            "node_parent_id",
            "node_tags",
            "node_created_at",
            "node_updated_at",
            "node_version",
            "node_payload_size",
            "node_payload_hash",
            "node_payload_mime",
            "node_payload_filename",
            "node_payload",
        }

        # Everything else goes into data
        data_payload = self.model_dump(mode="json", exclude=system_keys)

        return NodeUpsert(**upsert_data, data=data_payload)

    @classmethod
    def from_read(cls, node: NodeRead) -> "NodeModel":
        """Create a domain model instance from a DB Read DTO."""
        # 1. Map DTO fields to system fields
        system_data = {
            "node_id": node.id,
            "node_type": node.type,
            "node_name": node.name,
            "node_owner_id": node.owner_id,
            "node_parent_id": node.parent_id,
            "node_tags": node.tags,
            "node_created_at": node.created_at,
            "node_updated_at": node.updated_at,
            "node_version": node.version,
            "node_payload_size": node.payload_size,
            "node_payload_hash": node.payload_hash,
            "node_payload_mime": node.payload_mime,
            "node_payload_filename": node.payload_filename,
        }

        # 2. Add payload if present (from NodeReadWithPayload)
        if hasattr(node, "payload"):
            system_data["node_payload"] = node.payload

        # 3. Combine with unpacked data
        # 'data' dictionary contents become top-level fields
        combined_data = {**system_data, **node.data}

        return cls(**combined_data)


class EdgeModel(BaseModel):
    """
    Base class for edge domain models.
    """

    # System fields are prefixed with 'edge_'
    edge_id: Optional[str] = None
    edge_type: str = "edge"
    edge_source_id: str
    edge_target_id: str
    edge_tags: List[str] = Field(default_factory=list)

    # Read-only metadata
    edge_created_at: Optional[datetime] = None
    edge_updated_at: Optional[datetime] = None
    edge_version: Optional[int] = None

    model_config = ConfigDict(extra="allow")

    def to_upsert(self) -> EdgeUpsert:
        """Convert this domain model to a DB-ready Upsert DTO."""
        upsert_data = {
            "id": self.edge_id,
            "type": self.edge_type,
            "source_id": self.edge_source_id,
            "target_id": self.edge_target_id,
            "tags": self.edge_tags,
        }

        system_keys = {
            "edge_id",
            "edge_type",
            "edge_source_id",
            "edge_target_id",
            "edge_tags",
            "edge_created_at",
            "edge_updated_at",
            "edge_version",
        }

        data_payload = self.model_dump(mode="json", exclude=system_keys)

        return EdgeUpsert(**upsert_data, data=data_payload)

    @classmethod
    def from_read(cls, edge: EdgeRead) -> "EdgeModel":
        """Create a domain model instance from a DB Read DTO."""
        system_data = {
            "edge_id": edge.id,
            "edge_type": edge.type,
            "edge_source_id": edge.source_id,
            "edge_target_id": edge.target_id,
            "edge_tags": edge.tags,
            "edge_created_at": edge.created_at,
            "edge_updated_at": edge.updated_at,
            "edge_version": edge.version,
        }

        combined_data = {**system_data, **edge.data}

        return cls(**combined_data)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def generate_id() -> str:
    """Default ID generator (UUID4 string)."""
    return str(uuid4())


class _Base(DeclarativeBase):
    pass


class _GPRecord(_Base):
    """
    Common schema for both Nodes and Edges.
    """

    __abstract__ = True

    # -- Identifying Fields --
    id: Mapped[str] = mapped_column(String, primary_key=True, default=generate_id)

    # -- User-defined Content --
    type: Mapped[str] = mapped_column(String, index=True, nullable=False)
    schema_name: Mapped[Optional[str]] = mapped_column(
        String, index=True, nullable=True
    )
    data: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict)
    tags: Mapped[List[str]] = mapped_column(JSONB, default=list)

    # -- Auto-managed Metadata --
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    # Validate and increment version numbers on update
    __mapper_args__ = {"version_id_col": version}

    @declared_attr.directive
    def __table_args__(cls):
        """GIN indexes for JSONB columns on each concrete subclass."""
        return (
            Index(f"ix_{cls.__tablename__}_data_gin", "data", postgresql_using="gin"),
            Index(f"ix_{cls.__tablename__}_tags_gin", "tags", postgresql_using="gin"),
        )


class _GPNodeBase(_GPRecord):
    """
    Abstract base for Node tables.
    """

    __abstract__ = True

    # -- Identifying Fields --
    owner_id: Mapped[Optional[str]] = mapped_column(String, index=True)
    name: Mapped[Optional[str]] = mapped_column(String, index=True)

    # -- Payload --
    payload: Mapped[Optional[bytes]] = mapped_column(LargeBinary, deferred=True)
    payload_size: Mapped[int] = mapped_column(Integer, default=0)
    payload_hash: Mapped[Optional[str]] = mapped_column(String)
    payload_mime: Mapped[Optional[str]] = mapped_column(String)
    payload_filename: Mapped[Optional[str]] = mapped_column(String)

    @declared_attr
    def parent_id(cls):
        """Self-referential FK to same table."""
        return mapped_column(
            ForeignKey(f"{cls.__tablename__}.id", ondelete="RESTRICT"), index=True
        )

    @declared_attr.directive
    def __table_args__(cls):
        """GIN indexes plus unique constraint on (parent_id, name)."""
        uq_name = f"uq_{cls.__tablename__}_parent_name"
        return (
            Index(f"ix_{cls.__tablename__}_data_gin", "data", postgresql_using="gin"),
            Index(f"ix_{cls.__tablename__}_tags_gin", "tags", postgresql_using="gin"),
            UniqueConstraint("parent_id", "name", name=uq_name),
        )


class _GPEdgeBase(_GPRecord):
    """
    Abstract base for Edge tables.
    """

    __abstract__ = True

    # Node table name for FK references - set by subclasses
    _node_table: str = "nodes"

    @declared_attr
    def source_id(cls):
        return mapped_column(
            ForeignKey(f"{cls._node_table}.id", ondelete="RESTRICT"), index=True
        )

    @declared_attr
    def target_id(cls):
        return mapped_column(
            ForeignKey(f"{cls._node_table}.id", ondelete="RESTRICT"), index=True
        )


# Concrete default tables
class _GPNode(_GPNodeBase):
    __tablename__ = "nodes"


class _GPEdge(_GPEdgeBase):
    __tablename__ = "edges"


class _GPSchema(_Base):
    """Schema registry table for storing JSON schemas."""

    __tablename__ = "schemas"

    name: Mapped[str] = mapped_column(String, primary_key=True)
    version: Mapped[str] = mapped_column(String, default="1.0.0")
    json_schema: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


# -----------------------------------------------------------------------------
# Dynamic Table Factories
# -----------------------------------------------------------------------------

# Cache for dynamic models to avoid redefining tables for the same prefix
_model_cache: Dict[str, type] = {}


def create_node_model(table_name: str) -> type:
    """
    Create a node ORM class for a specific table name.
    Used for side tables with custom prefixes.
    Caches models to avoid redefining tables for the same prefix.
    """
    cache_key = f"node:{table_name}"
    if cache_key in _model_cache:
        return _model_cache[cache_key]

    # Use unique class name to avoid SQLAlchemy registry collisions
    import uuid

    class_name = f"_DynamicNode_{table_name}_{uuid.uuid4().hex[:8]}"
    DynamicNode = type(class_name, (_GPNodeBase,), {"__tablename__": table_name})

    _model_cache[cache_key] = DynamicNode
    return DynamicNode


def create_edge_model(table_name: str, node_table_name: str) -> type:
    """
    Create an edge ORM class for a specific table name.
    node_table_name specifies which table the FK references.
    Caches models to avoid redefining tables for the same prefix.
    """
    cache_key = f"edge:{table_name}"
    if cache_key in _model_cache:
        return _model_cache[cache_key]

    # Use unique class name to avoid SQLAlchemy registry collisions
    import uuid

    class_name = f"_DynamicEdge_{table_name}_{uuid.uuid4().hex[:8]}"
    DynamicEdge = type(
        class_name,
        (_GPEdgeBase,),
        {"__tablename__": table_name, "_node_table": node_table_name},
    )

    _model_cache[cache_key] = DynamicEdge
    return DynamicEdge


def create_schema_model(table_name: str) -> type:
    """
    Create a schema ORM class for a specific table name.
    Used for side tables with custom prefixes.
    Caches models to avoid redefining tables for the same prefix.
    """
    cache_key = f"schema:{table_name}"
    if cache_key in _model_cache:
        return _model_cache[cache_key]

    # Use unique class name to avoid SQLAlchemy registry collisions
    import uuid

    class_name = f"_DynamicSchema_{table_name}_{uuid.uuid4().hex[:8]}"
    DynamicSchema = type(
        class_name,
        (_Base,),
        {
            "__tablename__": table_name,
            "name": mapped_column(String, primary_key=True),
            "version": mapped_column(String, default="1.0.0"),
            "json_schema": mapped_column(JSONB, default=dict),
            "created_at": mapped_column(
                DateTime(timezone=True), server_default=func.now()
            ),
        },
    )

    _model_cache[cache_key] = DynamicSchema
    return DynamicSchema


# -----------------------------------------------------------------------------
# Conversion Helpers (Internal)
# -----------------------------------------------------------------------------


def _node_upsert_to_orm(
    dto: NodeUpsert, existing: Any = None, model: type = _GPNode
) -> Any:
    """Convert NodeUpsert DTO to ORM instance."""
    if existing:
        # Update existing
        if dto.type is not None:
            existing.type = dto.type
        if dto.name is not None:
            existing.name = dto.name
        if dto.owner_id is not None:
            existing.owner_id = dto.owner_id
        if dto.parent_id is not None:
            existing.parent_id = dto.parent_id
        if dto.schema_name is not None:
            existing.schema_name = dto.schema_name
        if dto.data is not None:
            existing.data = dto.data
        if dto.tags is not None:
            existing.tags = dto.tags
        if dto.payload is not None:
            existing.payload = dto.payload
        if dto.payload_mime is not None:
            existing.payload_mime = dto.payload_mime
        if dto.payload_filename is not None:
            existing.payload_filename = dto.payload_filename
        return existing
    # Create new - only pass id if provided, otherwise let SQLAlchemy use its default
    kwargs: dict = {
        "type": dto.type,
        "name": dto.name,
        "owner_id": dto.owner_id,
        "parent_id": dto.parent_id,
        "schema_name": dto.schema_name,
        "data": dto.data,
        "tags": dto.tags,
        "payload": dto.payload,
        "payload_mime": dto.payload_mime,
        "payload_filename": dto.payload_filename,
    }
    if dto.id is not None:
        kwargs["id"] = dto.id
    return model(**kwargs)


def _node_orm_to_read(orm: Any) -> NodeRead:
    """Convert ORM instance to NodeRead DTO."""
    return NodeRead.model_validate(orm)


def _node_orm_to_read_with_payload(orm: Any) -> NodeReadWithPayload:
    """Convert ORM instance to NodeReadWithPayload DTO."""
    return NodeReadWithPayload.model_validate(orm)


def _edge_upsert_to_orm(
    dto: EdgeUpsert, existing: Any = None, model: type = _GPEdge
) -> Any:
    """Convert EdgeUpsert DTO to ORM instance."""
    if existing:
        if dto.type is not None:
            existing.type = dto.type
        if dto.source_id is not None:
            existing.source_id = dto.source_id
        if dto.target_id is not None:
            existing.target_id = dto.target_id
        if dto.schema_name is not None:
            existing.schema_name = dto.schema_name
        if dto.data is not None:
            existing.data = dto.data
        if dto.tags is not None:
            existing.tags = dto.tags
        return existing
    # Create new - only pass id if provided, otherwise let SQLAlchemy use its default
    kwargs: dict = {
        "type": dto.type,
        "source_id": dto.source_id,
        "target_id": dto.target_id,
        "schema_name": dto.schema_name,
        "data": dto.data,
        "tags": dto.tags,
    }
    if dto.id is not None:
        kwargs["id"] = dto.id
    return model(**kwargs)


def _edge_orm_to_read(orm: Any) -> EdgeRead:
    """Convert ORM instance to EdgeRead DTO."""
    return EdgeRead.model_validate(orm)


class GPGraph:
    """
    Generic database utility for storing/retrieving/searching graph records.
    Uses SQLAlchemy for ORM and asyncpg for PostgreSQL client.

    Args:
        url: PostgreSQL connection URL
        table_prefix: Optional prefix for table names (e.g., "scratch" -> "scratch_nodes", "scratch_edges")
    """

    def __init__(self, url: str, table_prefix: str = ""):
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

    @property
    def async_session(self):
        """Alias for sqla_sessionmaker for backward compatibility."""
        return self.sqla_sessionmaker

    @asynccontextmanager
    async def transaction(self):
        """
        Wrap multiple GPGraph operations in a single atomic transaction.

        Usage:
            async with db.transaction():
                await db.set_node(NodeUpsert(...))
                await db.set_edge(EdgeUpsert(...))
                # Both committed together, or both rolled back on error
        """
        async with self.sqla_sessionmaker() as session:
            async with session.begin():
                token = self._session_ctx.set(session)
                try:
                    yield  # Yield nothing — callers use self.set_node() etc.
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

    def _bump_semver(self, old_version: str, change_type: str) -> str:
        """
        Bump a semantic version string.

        Args:
            old_version: Current version string (e.g., "1.2.3")
            change_type: "major", "minor", or "patch"

        Returns:
            New version string
        """
        parts = old_version.split(".")
        major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])

        if change_type == "major":
            major += 1
            minor = 0
            patch = 0
        elif change_type == "minor":
            minor += 1
            patch = 0
        elif change_type == "patch":
            patch += 1

        return f"{major}.{minor}.{patch}"

    def _detect_semver_change(
        self, old_schema: Dict[str, Any], new_schema: Dict[str, Any]
    ) -> str:
        """
        Detect the type of SemVer change between two schemas.

        Returns:
            "major" if breaking changes detected
            "minor" if backward compatible changes (e.g., new optional field)
            "patch" if only non-consequential changes (descriptions, titles, examples)
        """
        old_props = old_schema.get("properties", {})
        new_props = new_schema.get("properties", {})
        old_required = set(old_schema.get("required", []))
        new_required = set(new_schema.get("required", []))

        # Check for breaking changes (major)
        removed_fields = set(old_props.keys()) - set(new_props.keys())
        if removed_fields:
            return "major"

        for field in old_props:
            if field in new_props:
                old_type = old_props[field].get("type")
                new_type = new_props[field].get("type")
                if old_type != new_type:
                    return "major"

        newly_required = new_required - old_required
        if newly_required:
            return "major"

        # Check for backward compatible changes (minor)
        added_fields = set(new_props.keys()) - set(old_props.keys())
        if added_fields:
            # If any added field is required, it's a major change (e.g. was implicitly required before)
            for field in added_fields:
                if field in new_required:
                    return "major"

            # If all added fields are optional, it's a minor change
            return "minor"

        # Otherwise, it's a patch change (descriptions, titles, examples)
        return "patch"

    def _inline_refs(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Inline all $ref references in a JSON Schema to make it standalone.

        Resolves references from the $defs section (or $definitions for older schemas).

        Args:
            schema: JSON Schema dictionary that may contain $ref

        Returns:
            JSON Schema with all $ref references inlined
        """
        # Extract definitions from $defs or $definitions
        defs = schema.get("$defs", schema.get("definitions", {}))

        def inline(obj: Any) -> Any:
            if isinstance(obj, dict):
                if "$ref" in obj:
                    ref = obj["$ref"]
                    # Handle #/$defs/name or #/definitions/name format
                    if ref.startswith("#/$defs/"):
                        def_name = ref[len("#/$defs/") :]
                    elif ref.startswith("#/definitions/"):
                        def_name = ref[len("#/definitions/") :]
                    else:
                        # Simple name reference
                        def_name = ref

                    # Get the definition and recursively inline it
                    if def_name in defs:
                        return inline(copy.deepcopy(defs[def_name]))
                    return {}
                return {k: inline(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [inline(item) for item in obj]
            return obj

        # Inline all references
        result = inline(copy.deepcopy(schema))

        # Remove $defs/$definitions from the result since everything is inlined
        if "$defs" in result:
            del result["$defs"]
        if "definitions" in result:
            del result["definitions"]

        return result

    def _schema_kind_from_record(self, schema: Any) -> SchemaKind:
        try:
            return _extract_schema_kind(schema.json_schema)
        except ValueError as exc:
            raise SchemaValidationError(
                f"Schema '{schema.name}' is missing valid kind metadata. "
                "Re-register it as a node or edge schema."
            ) from exc

    def _prepare_schema_registration(
        self,
        schema: Union[Dict[str, Any], type[BaseModel]],
        *,
        kind: str | None,
        existing: Any | None = None,
    ) -> tuple[Dict[str, Any], SchemaKind]:
        """Normalize a schema payload and resolve the schema kind."""
        if isinstance(schema, type) and issubclass(schema, BaseModel):
            json_schema = schema.model_json_schema()
        else:
            json_schema = copy.deepcopy(schema)

        json_schema = self._inline_refs(json_schema)

        embedded_kind = _extract_schema_kind(json_schema, required=False)
        resolved_kind = embedded_kind
        if kind is not None:
            requested_kind = _normalize_schema_kind(kind)
            if embedded_kind is not None and embedded_kind != requested_kind:
                raise ValueError(
                    "Schema kind does not match the JSON schema metadata field "
                    f"'{_SCHEMA_KIND_FIELD}'."
                )
            resolved_kind = requested_kind

        if resolved_kind is None and existing is not None:
            resolved_kind = self._schema_kind_from_record(existing)
        if resolved_kind is None:
            resolved_kind = "node"

        json_schema[_SCHEMA_KIND_FIELD] = resolved_kind
        return json_schema, resolved_kind

    async def register_schema(
        self,
        name: str,
        schema: Union[Dict[str, Any], type[BaseModel]],
        kind: str = "node",
    ):
        """
        Register a JSON schema in the schema registry.

        The system automatically detects the type of change and bumps the version:
        - Major: Breaking changes (removed fields, type changes, newly required fields)
        - Minor: Backward compatible changes (new optional fields)
        - Patch: Non-consequential changes (descriptions, titles, examples)

        Args:
            name: Unique name for the schema
            schema: JSON schema dictionary or Pydantic model class
            kind: Schema compatibility target, either "node" or "edge"

        Raises:
            SchemaBreakingChangeError: If breaking changes are detected

        Returns:
            The schema ORM object with updated version
        """
        async with self._get_session() as session:
            # Check if schema already exists
            existing = await session.get(self._Schema, name)
            json_schema, resolved_kind = self._prepare_schema_registration(
                schema,
                kind=kind,
                existing=existing,
            )
            if existing:
                existing_kind = self._schema_kind_from_record(existing)
                if resolved_kind != existing_kind:
                    raise SchemaBreakingChangeError(
                        f"Schema '{name}' cannot change kind from "
                        f"'{existing_kind}' to '{resolved_kind}'."
                    )
                # Detect type of change
                change_type = self._detect_semver_change(
                    existing.json_schema, json_schema
                )

                # Fail on breaking changes
                if change_type == "major":
                    self._check_breaking_changes(
                        existing.json_schema, json_schema, name
                    )

                # Bump version
                new_version = self._bump_semver(existing.version, change_type)

                # Update existing schema
                existing.json_schema = json_schema
                existing.version = new_version
                self._validators.pop(name, None)  # invalidate cache for updated schema
                self._schema_kinds.pop(name, None)
                await session.flush()
                await session.refresh(existing)
                return existing

            # Create new schema with version 1.0.0
            new_schema = self._Schema(
                name=name, json_schema=json_schema, version="1.0.0"
            )
            session.add(new_schema)
            self._schema_kinds.pop(name, None)
            await session.flush()
            await session.refresh(new_schema)
            return new_schema

    async def get_schema(self, name: str) -> Optional[Any]:
        """
        Retrieve a registered schema by name.

        Args:
            name: Schema name to retrieve

        Returns:
            Schema ORM object if found, None otherwise
        """
        async with self._get_session() as session:
            return await session.get(self._Schema, name)

    async def delete_schema(self, name: str) -> None:
        """
        Delete a schema from the registry.

        Args:
            name: Schema name to delete

        Raises:
            SchemaInUseError: If any nodes or edges reference this schema
        """
        async with self._get_session() as session:
            # Check if any nodes use this schema
            node_stmt = select(self._Node).where(self._Node.schema_name == name)
            node_result = await session.execute(node_stmt)
            if node_result.scalars().first() is not None:
                raise SchemaInUseError(
                    f"Cannot delete schema '{name}': it is referenced by one or more nodes"
                )

            # Check if any edges use this schema
            edge_stmt = select(self._Edge).where(self._Edge.schema_name == name)
            edge_result = await session.execute(edge_stmt)
            if edge_result.scalars().first() is not None:
                raise SchemaInUseError(
                    f"Cannot delete schema '{name}': it is referenced by one or more edges"
                )

            # Delete the schema record
            schema = await session.get(self._Schema, name)
            if schema is not None:
                await session.delete(schema)
                self._validators.pop(name, None)
                self._schema_kinds.pop(name, None)

    async def list_schemas(self, kind: str | None = None) -> List[str]:
        """
        List all registered schema names.

        Args:
            kind: Optional compatibility filter ("node" or "edge")

        Returns:
            List of schema names
        """
        resolved_kind = _normalize_schema_kind(kind) if kind is not None else None
        async with self._get_session() as session:
            stmt = select(self._Schema)
            result = await session.execute(stmt)
            names: List[str] = []
            for schema in result.scalars().all():
                schema_kind = self._schema_kind_from_record(schema)
                if resolved_kind is None or schema_kind == resolved_kind:
                    names.append(str(schema.name))
            return names

    def _check_breaking_changes(
        self, old_schema: Dict[str, Any], new_schema: Dict[str, Any], name: str
    ):
        """
        Check if new schema contains breaking changes compared to old schema.

        Breaking changes:
        - Adding a required field
        - Removing a field
        - Changing a field's type

        Args:
            old_schema: Existing JSON schema
            new_schema: New JSON schema to validate
            name: Schema name (for error messages)

        Raises:
            SchemaBreakingChangeError: If breaking changes detected
        """
        old_props = old_schema.get("properties", {})
        new_props = new_schema.get("properties", {})
        old_required = set(old_schema.get("required", []))
        new_required = set(new_schema.get("required", []))

        # Check for removed fields
        removed_fields = set(old_props.keys()) - set(new_props.keys())
        if removed_fields:
            raise SchemaBreakingChangeError(
                f"Schema '{name}' has breaking changes: removed fields {removed_fields}"
            )

        # Check for type changes
        for field in old_props:
            if field in new_props:
                old_type = old_props[field].get("type")
                new_type = new_props[field].get("type")
                if old_type != new_type:
                    raise SchemaBreakingChangeError(
                        f"Schema '{name}' has breaking changes: field '{field}' type changed from {old_type} to {new_type}"
                    )

        # Check for newly required fields
        newly_required = new_required - old_required
        if newly_required:
            raise SchemaBreakingChangeError(
                f"Schema '{name}' has breaking changes: newly required fields {newly_required}"
            )

    async def migrate_schema(
        self,
        name: str,
        migration_func: callable,
        new_schema: Union[Dict[str, Any], type[BaseModel]],
        kind: str | None = None,
    ):
        """
        Migrate all nodes/edges using a schema to a new schema version.

        This method atomically:
        1. Migrates all data using the provided migration function
        2. Registers the new schema (with auto SemVer bump)
        3. All in a single transaction for 100% integrity

        Args:
            name: Schema name to migrate
            migration_func: Function that transforms old data to new data: (old_data) -> new_data
            new_schema: New JSON schema or Pydantic model class
            kind: Optional schema kind override. Must match the existing kind.
        """
        async with self.sqla_sessionmaker() as session:
            async with session.begin():
                existing = await session.get(self._Schema, name)
                json_schema, resolved_kind = self._prepare_schema_registration(
                    new_schema,
                    kind=kind,
                    existing=existing,
                )
                if existing is not None:
                    existing_kind = self._schema_kind_from_record(existing)
                    if resolved_kind != existing_kind:
                        raise SchemaBreakingChangeError(
                            f"Schema '{name}' cannot change kind from "
                            f"'{existing_kind}' to '{resolved_kind}'."
                        )

                # Create validator for new schema directly (not from cache since schema not yet registered)
                validator = jsonschema.Draft7Validator(json_schema)

                # Get all nodes with this schema
                stmt = select(self._Node).where(self._Node.schema_name == name)
                result = await session.execute(stmt)
                nodes = result.scalars().all()

                # Migrate each node's data and validate
                for node in nodes:
                    new_data = migration_func(node.data)
                    try:
                        validator.validate(new_data)
                    except jsonschema.exceptions.ValidationError as e:
                        raise SchemaValidationError(
                            f"Migration produced invalid data for node {node.id}: {e.message}"
                        )
                    node.data = new_data

                # Get all edges with this schema
                stmt = select(self._Edge).where(self._Edge.schema_name == name)
                result = await session.execute(stmt)
                edges = result.scalars().all()

                # Migrate each edge's data and validate
                for edge in edges:
                    new_data = migration_func(edge.data)
                    try:
                        validator.validate(new_data)
                    except jsonschema.exceptions.ValidationError as e:
                        raise SchemaValidationError(
                            f"Migration produced invalid data for edge {edge.id}: {e.message}"
                        )
                    edge.data = new_data

                # Update schema with new version (bump major for breaking changes)
                if existing:
                    # Detect change type and bump version
                    change_type = self._detect_semver_change(
                        existing.json_schema, json_schema
                    )
                    new_version = self._bump_semver(existing.version, change_type)
                    existing.json_schema = json_schema
                    existing.version = new_version
                else:
                    new_schema_record = self._Schema(
                        name=name, json_schema=json_schema, version="1.0.0"
                    )
                    session.add(new_schema_record)
                self._validators.pop(name, None)  # invalidate cache for updated schema
                self._schema_kinds.pop(name, None)

    async def _get_registered_schema_kind(self, schema_name: str) -> SchemaKind:
        """Return the registered kind for one schema."""
        if schema_name in self._schema_kinds:
            return self._schema_kinds[schema_name]

        schema = await self.get_schema(schema_name)
        if schema is None:
            raise SchemaNotFoundError(f"Schema '{schema_name}' not found")

        kind = self._schema_kind_from_record(schema)
        self._schema_kinds[schema_name] = kind
        return kind

    async def _get_validator(self, schema_name: str) -> Any:
        """
        Get a cached jsonschema validator for the given schema name.

        Args:
            schema_name: Name of the schema to get validator for

        Returns:
            Compiled jsonschema validator

        Raises:
            SchemaNotFoundError: If schema is not found
        """
        if schema_name in self._validators:
            return self._validators[schema_name]

        schema = await self.get_schema(schema_name)
        if schema is None:
            raise SchemaNotFoundError(f"Schema '{schema_name}' not found")

        validator = jsonschema.Draft7Validator(schema.json_schema)
        self._validators[schema_name] = validator
        return validator

    async def _validate_data(
        self,
        schema_name: str,
        data: Dict[str, Any],
        *,
        expected_kind: SchemaKind,
    ):
        """
        Validate data against a registered schema.

        Args:
            schema_name: Name of the schema to validate against
            data: Data to validate
            expected_kind: Graph record kind the schema must be compatible with

        Raises:
            SchemaNotFoundError: If schema is not found
            SchemaValidationError: If validation fails
        """
        actual_kind = await self._get_registered_schema_kind(schema_name)
        if actual_kind != expected_kind:
            raise SchemaKindMismatchError(
                f"Schema '{schema_name}' is a {actual_kind} schema and cannot be "
                f"attached to a {expected_kind}."
            )
        validator = await self._get_validator(schema_name)
        errors = list(validator.iter_errors(data))
        if errors:
            error_details = [e.message for e in errors]
            raise SchemaValidationError(
                f"Validation failed for schema '{schema_name}': {error_details}"
            )

    async def set_node(self, node: NodeUpsert) -> NodeRead:
        """
        Upsert a Node.
        Creates if new, updates if existing (matched by id).
        Returns NodeRead (without payload).

        Note: If node.payload is provided, it will be stored.
        For updating only payload, use set_node_payload().
        """
        async with self._get_session() as session:
            # Check if node exists (for update)
            existing = None
            if node.id:
                existing = await session.get(self._Node, node.id)

            # Preserve existing schema_name if updating and not provided
            schema_to_validate = node.schema_name
            if existing and node.schema_name is None and existing.schema_name:
                schema_to_validate = existing.schema_name
                node.schema_name = schema_to_validate

            # Validate data against schema if schema_name is provided
            if schema_to_validate:
                await self._validate_data(
                    schema_to_validate,
                    node.data,
                    expected_kind="node",
                )

            # Convert DTO to ORM
            orm = _node_upsert_to_orm(node, existing, self._Node)

            # Add or update
            if existing is None:
                session.add(orm)

            await session.flush()
            await session.refresh(orm)
            return _node_orm_to_read(orm)

    async def get_node(self, id: str) -> NodeRead | None:
        """
        Get a Node without payload.
        Returns NodeRead if found, None if not found.
        """
        async with self._get_session() as session:
            orm = await session.get(self._Node, id)
            if orm is None:
                return None
            return _node_orm_to_read(orm)

    async def get_node_with_payload(self, id: str) -> NodeReadWithPayload | None:
        """
        Get a Node with payload included.
        Returns NodeReadWithPayload if found, None if not found.
        """
        async with self._get_session() as session:
            orm = await session.get(
                self._Node, id, options=[undefer(self._Node.payload)]
            )
            if orm is None:
                return None
            return _node_orm_to_read_with_payload(orm)

    async def get_node_payload(self, id: str) -> bytes | None:
        """
        Get only the payload bytes for a Node.
        Returns bytes if node exists and has payload, None otherwise.
        """
        async with self._get_session() as session:
            stmt = select(self._Node.payload).where(self._Node.id == id)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def set_node_payload(
        self,
        id: str,
        payload: bytes,
        mime: str | None = None,
        filename: str | None = None,
    ) -> NodeRead:
        """
        Set payload for an existing Node.
        Auto-calculates size and hash.
        Returns updated NodeRead.
        """
        async with self._get_session() as session:
            orm = await session.get(self._Node, id)
            if orm is None:
                raise ValueError(f"Node not found: {id}")
            orm.payload = payload
            if mime is not None:  # Only update mime when explicitly provided
                orm.payload_mime = mime
            if filename is not None:  # Only update filename when explicitly provided
                orm.payload_filename = filename
            await session.flush()
            await session.refresh(orm)
            return _node_orm_to_read(orm)

    async def clear_node_payload(self, id: str) -> NodeRead:
        """
        Remove payload bytes and payload metadata from an existing Node.
        Returns updated NodeRead.
        """
        async with self._get_session() as session:
            orm = await session.get(self._Node, id)
            if orm is None:
                raise ValueError(f"Node not found: {id}")
            orm.payload = None
            await session.flush()
            await session.refresh(orm)
            return _node_orm_to_read(orm)

    async def get_node_child(self, parent_id: str, name: str) -> NodeRead | None:
        """
        Get a child node by name under a specific parent.
        Returns NodeRead if found, None if not found.
        """
        async with self._get_session() as session:
            stmt = select(self._Node).where(
                self._Node.parent_id == parent_id, self._Node.name == name
            )
            result = await session.execute(stmt)
            orm = result.scalar_one_or_none()
            if orm is None:
                return None
            return _node_orm_to_read(orm)

    async def delete_node(self, id: str):
        """
        Hard delete a Node.
        """
        async with self._get_session() as session:
            await session.execute(delete(self._Node).where(self._Node.id == id))

    async def search_nodes(self, query: SearchQuery) -> Page[NodeRead]:
        """
        Search for Nodes. Returns NodeRead without payload.

        For nodes with payload, use get_node_with_payload() on individual results.
        For column projection, use search_nodes_projection().
        """
        if query.select:
            raise ValueError(
                "query.select is not supported in search_nodes(). "
                "Use search_nodes_projection() instead."
            )
        page = await self._search(self._Node, query)
        return Page(
            items=[_node_orm_to_read(orm) for orm in page.items],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
        )

    async def search_nodes_projection(
        self,
        query: SearchQuery,
    ) -> Page[Dict[str, Any]]:
        """
        Search for Nodes with field projection.
        query.select determines returned fields.
        Returns paginated dict results.
        """
        if not query.select:
            raise ValueError("query.select is required for projection search")
        return await self._search(self._Node, query)

    async def set_edge(self, edge: EdgeUpsert) -> EdgeRead:
        """
        Upsert an Edge.
        Creates if new, updates if existing (matched by id).
        """
        # Preserve existing schema_name if updating and not provided
        schema_to_validate = edge.schema_name
        if edge.id and edge.schema_name is None:
            async with self._get_session() as session:
                existing = await session.get(self._Edge, edge.id)
                if existing and existing.schema_name:
                    schema_to_validate = existing.schema_name
                    # Update DTO to ensure schema persistence
                    edge.schema_name = schema_to_validate

        # Validate data against schema if schema_name is provided
        if schema_to_validate:
            await self._validate_data(
                schema_to_validate,
                edge.data,
                expected_kind="edge",
            )

        async with self._get_session() as session:
            # Check if edge exists (for update)
            existing = None
            if edge.id:
                existing = await session.get(self._Edge, edge.id)

            # Convert DTO to ORM
            orm = _edge_upsert_to_orm(edge, existing, self._Edge)

            # Merge and flush
            merged = await session.merge(orm)
            await session.flush()
            await session.refresh(merged)
            return _edge_orm_to_read(merged)

    async def get_edge(self, id: str) -> EdgeRead | None:
        """
        Get an Edge.
        Returns EdgeRead if found, None if not found.
        """
        async with self._get_session() as session:
            orm = await session.get(self._Edge, id)
            if orm is None:
                return None
            return _edge_orm_to_read(orm)

    async def delete_edge(self, id: str):
        """
        Hard delete an Edge.
        """
        async with self._get_session() as session:
            await session.execute(delete(self._Edge).where(self._Edge.id == id))

    async def search_edges(self, query: SearchQuery) -> Page[EdgeRead]:
        """
        Search for Edges. Returns paginated EdgeRead results.

        For column projection, use search_edges_projection().
        """
        if query.select:
            raise ValueError(
                "query.select is not supported in search_edges(). "
                "Use search_edges_projection() instead."
            )
        page = await self._search(self._Edge, query)
        return Page(
            items=[_edge_orm_to_read(orm) for orm in page.items],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
        )

    # -------------------------------------------------------------------------
    # Search Engine
    # -------------------------------------------------------------------------

    def _build_condition(
        self,
        model: Any,
        item: Union[Filter, FilterGroup],
    ):
        """
        Recursively build SQLAlchemy conditions from Filter/FilterGroup.
        """
        if isinstance(item, Filter):
            # 1. Check for JSON path (dot notation)
            if "." in item.field:
                base, *path = item.field.split(".")
                if hasattr(model, base):
                    col = getattr(model, base)
                    # We assume it's a JSONB column (data, tags)

                    # For equality, use JSON containment (efficient with GIN index)
                    if item.op == Op.EQ:
                        val = item.value
                        for p in reversed(path):
                            val = {p: val}
                        return col.contains(val)

                    # For other ops, we need to extract the value.
                    # This is tricky without knowing the type.
                    # We'll implement basic extraction as text.
                    expr = col
                    for p in path[:-1]:
                        expr = expr[p]
                    # Final element as text
                    expr = expr[path[-1]].astext

                    # Attempt to cast based on value type
                    if isinstance(item.value, int):
                        expr = expr.cast(Integer)
                    elif isinstance(item.value, float):
                        # Import Float/Numeric if needed, or just cast to Float
                        from sqlalchemy import Float

                        expr = expr.cast(Float)
                    elif isinstance(item.value, bool):
                        from sqlalchemy import Boolean

                        expr = expr.cast(Boolean)
                else:
                    # Field looks like path but base doesn't exist
                    return False
            else:
                # Standard column
                if not hasattr(model, item.field):
                    return False
                expr = getattr(model, item.field)

            # 2. Apply Operator
            if item.op == Op.EQ:
                return expr == item.value
            if item.op == Op.GT:
                return expr > item.value
            if item.op == Op.LT:
                return expr < item.value
            if item.op == Op.CONTAINS:
                return expr.ilike(f"%{item.value}%")
            if item.op == Op.IN and isinstance(item.value, (list, tuple)):
                if hasattr(expr.type, "as_generic") and hasattr(
                    expr.type, "python_type"
                ):
                    # Check if it's JSONB (e.g. tags list)
                    if expr.type.python_type in (dict, list):
                        # For JSONB arrays, "IN" implies overlap (has_any / ?|)
                        # e.g. "tags IN (a, b)" matches if tags contains "a" OR "b".
                        from sqlalchemy.dialects.postgresql import array

                        return expr.has_any(array(item.value))

                return expr.in_(item.value)

            return expr == item.value

        elif isinstance(item, FilterGroup):
            conditions = [self._build_condition(model, f) for f in item.filters]

            # Filter out explicit False (invalid fields)
            valid_conditions = [c for c in conditions if c is not False]

            if not valid_conditions:
                # If group is empty or all invalid
                return True if item.logic == Logic.AND else False

            if item.logic == Logic.OR:
                return or_(*valid_conditions)
            return and_(*valid_conditions)

    async def _search(
        self,
        model: Any,
        query: SearchQuery,
        extra_options: List[Any] = None,
    ) -> Page[Any]:
        """
        Internal: Generic search for Nodes or Edges.
        """
        if query.select:
            cols = []
            for field in query.select:
                if "." in field:
                    base, *path = field.split(".")
                    if hasattr(model, base):
                        col = getattr(model, base)
                        # Descend into JSON path
                        expr = col
                        for p in path:
                            expr = expr[p]
                        cols.append(expr.label(field))
                elif hasattr(model, field):
                    cols.append(getattr(model, field).label(field))

            if cols:
                stmt = select(*cols)
            else:
                # Fallback to full model if select list yielded no valid columns
                stmt = select(model)
        else:
            stmt = select(model)

        # 1. Apply Filters
        if query.filter:
            cond = self._build_condition(model, query.filter)

            # Check if condition is valid (not True/False literals unless supported by DB)
            if cond is not True and cond is not False:
                stmt = stmt.where(cond)
            elif cond is False:
                # If condition resolved to False, return empty result immediately
                return Page(items=[], total=0, limit=query.limit, offset=query.offset)

        # 2. Apply Sorts
        for s in query.sort:
            col = getattr(model, s.field, None)
            if col is not None:
                stmt = stmt.order_by(col.desc() if s.desc else col.asc())

        # Default sort if none provided
        if not query.sort:
            stmt = stmt.order_by(model.created_at.desc())

        # 3. Apply Options (e.g. undefer)
        # Options usually apply to Model entities. If we are projecting specific columns,
        # some options might not be relevant, but we'll apply them if we are selecting the model (fallback)
        if extra_options and not query.select:
            stmt = stmt.options(*extra_options)

        # 4. Execute (Count + Fetch)
        async with self._get_session() as session:
            # Total count
            # We use a subquery count to support complex where clauses safely
            subq = stmt.subquery()
            count_stmt = select(func.count()).select_from(subq)

            total = (await session.execute(count_stmt)).scalar_one()

            # Paged items
            paged_stmt = stmt.limit(query.limit).offset(query.offset)

            result = await session.execute(paged_stmt)

            if query.select:
                # Return list of dicts
                items = [r._asdict() for r in result.all()]
            else:
                # Return list of models
                items = result.scalars().all()

            return Page(
                items=items,
                total=total,
                limit=query.limit,
                offset=query.offset,
            )


# Update payload hash and size when it is inserted or updated.
@event.listens_for(_GPNodeBase, "before_insert", propagate=True)
@event.listens_for(_GPNodeBase, "before_update", propagate=True)
def _update_payload_metadata(mapper, connection, target):

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
# DSL Parser/Serializer
# -----------------------------------------------------------------------------

_OP_MAP = {
    # Symbols & Aliases
    ":": Op.EQ,
    "=": Op.EQ,
    ">": Op.GT,
    "after": Op.GT,
    "<": Op.LT,
    "before": Op.LT,
    "~": Op.CONTAINS,
}

# Automatically register all Op values as themselves (e.g. "eq", "gt", "contains")
for op in Op:
    _OP_MAP[op.value] = op


def _value_to_dsl(value: Any) -> str:
    """Convert a Python value to DSL string representation."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        if re.search(r'[\s()"\']', value):
            escaped = value.replace('"', '\\"')
            return f'"{escaped}"'
        return value
    if isinstance(value, (list, tuple)):
        items = [_value_to_dsl(v) for v in value]
        return f"({', '.join(items)})"
    return str(value)


def _tokenize(text: str) -> List[Any]:
    """Split DSL text into tokens."""
    tokens = []
    i = 0
    while i < len(text):
        if text[i].isspace():
            i += 1
            continue

        char = text[i]

        # 1. Structural chars and simple operators
        if char in "()=<>:,":
            tokens.append(char)
            i += 1
            continue

        # 2. Strings
        if char in "\"'":
            quote = char
            i += 1
            val = ""
            while i < len(text) and text[i] != quote:
                if text[i] == "\\" and i + 1 < len(text):
                    i += 1
                val += text[i]
                i += 1
            i += 1
            tokens.append(("STRING", val))
            continue

        # 3. Unquoted text (identifiers, keywords, numbers)
        j = i
        while j < len(text) and not text[j].isspace() and text[j] not in "()=<>:,":
            j += 1

        word = text[i:j]
        tokens.append(word)
        i = j

    return tokens


def _parse_value(token: Any) -> Any:
    """Convert a token to its Python value."""
    if isinstance(token, tuple) and token[0] == "STRING":
        return token[1]
    if token == "null":
        return None
    if token == "true":
        return True
    if token == "false":
        return False
    # Try int
    try:
        return int(token)
    except ValueError:
        pass
    # Try float
    try:
        return float(token)
    except ValueError:
        pass
    return token


def _parse_list(tokens: List[Any], pos: int) -> tuple[List[Any], int]:
    """Parse a list of values: (v1, v2, ...)"""
    if pos >= len(tokens) or tokens[pos] != "(":
        raise ValueError("Expected list starting with '('")
    pos += 1
    values = []
    while pos < len(tokens) and tokens[pos] != ")":
        # Handle comma if present
        if values and tokens[pos] == ",":
            pos += 1

        if pos >= len(tokens) or tokens[pos] == ")":
            break

        val = _parse_value(tokens[pos])
        values.append(val)
        pos += 1

    if pos >= len(tokens) or tokens[pos] != ")":
        raise ValueError("Missing closing parenthesis for list")
    pos += 1
    return values, pos


def _parse_filter(tokens: List[Any], pos: int) -> tuple[Filter, int]:
    """Parse a single filter: field op value"""
    if pos >= len(tokens):
        raise ValueError("Unexpected end of input, expected field name")

    field = tokens[pos]
    if isinstance(field, tuple):
        raise ValueError(f"Expected field name, got string: {field[1]}")
    # Check if this is actually the start of a parenthesized group handled by _parse_primary
    # But _parse_filter is called ONLY when _parse_primary sees a non-paren.

    pos += 1
    if pos >= len(tokens):
        # Implicit boolean true? e.g. "active" -> active=true
        return Filter(field=field, op=Op.EQ, value=True), pos

    op_token = tokens[pos]

    # Check for operator
    op = None
    if isinstance(op_token, str):
        if op_token.lower() in _OP_MAP:
            op = _OP_MAP[op_token.lower()]
        elif op_token in _OP_MAP:  # Case sensitive fallback
            op = _OP_MAP[op_token]

    if op:
        pos += 1
        if pos >= len(tokens):
            raise ValueError(f"Unexpected end of input after operator '{op_token}'")

        # Special handling for IN operator which expects a list
        if op == Op.IN:
            if tokens[pos] == "(":
                value, pos = _parse_list(tokens, pos)
            else:
                # Single value IN? Treat as single item list
                value = _parse_value(tokens[pos])
                value = [value]
                pos += 1
        else:
            value = _parse_value(tokens[pos])
            pos += 1
        return Filter(field=field, op=op, value=value), pos

    # No operator found.
    # We will assume "field" alone means "field=True" (boolean flag)
    return Filter(field=field, op=Op.EQ, value=True), pos


def _parse_primary(
    tokens: List[Any], pos: int
) -> tuple[Union[Filter, FilterGroup], int]:
    """Parse a primary expression: filter or (expr)"""
    if pos >= len(tokens):
        raise ValueError("Unexpected end of input")

    if tokens[pos] == "(":
        pos += 1
        expr, pos = _parse_expr(tokens, pos)
        if pos >= len(tokens) or tokens[pos] != ")":
            raise ValueError("Missing closing parenthesis")
        pos += 1
        return expr, pos

    return _parse_filter(tokens, pos)


def _parse_and_expr(
    tokens: List[Any], pos: int
) -> tuple[Union[Filter, FilterGroup], int]:
    """Parse AND expression (left-associative, implicit AND)."""
    left, pos = _parse_primary(tokens, pos)
    filters = [left]

    while pos < len(tokens) and tokens[pos] != ")":
        token = tokens[pos]

        # Check for explicit OR (terminates AND group)
        if isinstance(token, str) and token.lower() == "or":
            break

        # Check for explicit AND
        if isinstance(token, str) and token.lower() == "and":
            pos += 1
            if pos >= len(tokens):
                raise ValueError("Unexpected end of input after AND")

        # Parse next primary (implicit AND)
        right, pos = _parse_primary(tokens, pos)
        filters.append(right)

    if len(filters) == 1:
        return filters[0], pos
    return FilterGroup(logic=Logic.AND, filters=filters), pos


def _parse_expr(tokens: List[Any], pos: int) -> tuple[Union[Filter, FilterGroup], int]:
    """Parse OR expression (left-associative)."""
    left, pos = _parse_and_expr(tokens, pos)
    filters = [left]

    while pos < len(tokens):
        token = tokens[pos]
        if isinstance(token, str) and token.lower() == "or":
            pos += 1
            right, pos = _parse_and_expr(tokens, pos)
            filters.append(right)
        else:
            break

    if len(filters) == 1:
        return filters[0], pos
    return FilterGroup(logic=Logic.OR, filters=filters), pos


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
    # Main class
    "GPGraph",
]
