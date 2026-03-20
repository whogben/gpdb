"""
SQLAlchemy ORM record classes for nodes and edges.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy import DateTime, ForeignKey, Index, Integer, LargeBinary, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, declared_attr, mapped_column

from gpdb.models.base import _Base


class _GPRecord(_Base):
    """
    Common schema for both Nodes and Edges.
    """

    __abstract__ = True

    # -- Identifying Fields --
    id: Mapped[str] = mapped_column(String, primary_key=True)

    # -- User-defined Content --
    type: Mapped[str] = mapped_column(String, index=True, nullable=False)
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
    owner_id: Mapped[str | None] = mapped_column(String, index=True)
    name: Mapped[str | None] = mapped_column(String, index=True)

    # -- Payload --
    payload: Mapped[bytes | None] = mapped_column(LargeBinary, deferred=True)
    payload_size: Mapped[int] = mapped_column(Integer, default=0)
    payload_hash: Mapped[str | None] = mapped_column(String)
    payload_mime: Mapped[str | None] = mapped_column(String)
    payload_filename: Mapped[str | None] = mapped_column(String)

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
    kind: Mapped[str] = mapped_column(String, primary_key=True)
    version: Mapped[str] = mapped_column(String, default="1.0.0")
    json_schema: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict)
    extends: Mapped[List[str]] = mapped_column(JSONB, default=list)
    effective_json_schema: Mapped[Dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    alias: Mapped[str | None] = mapped_column(String, nullable=True)
    svg_icon: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
