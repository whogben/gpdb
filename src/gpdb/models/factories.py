"""
Dynamic table factory functions for creating ORM models with custom prefixes.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy import DateTime, ForeignKey, Index, Integer, LargeBinary, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, declared_attr, mapped_column

from gpdb.models.records import _GPNodeBase, _GPEdgeBase, _Base


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
            "kind": mapped_column(String, index=True),
            "created_at": mapped_column(
                DateTime(timezone=True), server_default=func.now()
            ),
        },
    )

    _model_cache[cache_key] = DynamicSchema
    return DynamicSchema
