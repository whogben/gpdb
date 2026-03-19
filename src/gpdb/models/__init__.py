"""
Models package - re-exports for backward compatibility.
"""

from __future__ import annotations

from gpdb.models.base import (
    SchemaKind,
    SchemaNotFoundError,
    SchemaValidationError,
    SchemaKindMismatchError,
    SchemaBreakingChangeError,
    SchemaInUseError,
    _Base,
    _normalize_schema_kind,
    _SCHEMA_KIND_VALUES,
    _ID_ALPHABET,
    _ID_MAX_COLLISION_ATTEMPTS,
    _PG_UNIQUE_VIOLATION,
    _is_primary_key_violation,
    ID_GENERATOR,
    generate_id,
)
from gpdb.models.records import (
    _GPRecord,
    _GPNodeBase,
    _GPEdgeBase,
    _GPNode,
    _GPEdge,
    _GPSchema,
)
from gpdb.models.factories import (
    create_node_model,
    create_edge_model,
    create_schema_model,
)
from gpdb.models.dto import (
    NodeUpsert,
    NodeRead,
    NodeReadWithPayload,
    EdgeUpsert,
    EdgeRead,
    SchemaUpsert,
    SchemaRef,
)

__all__ = [
    # Exceptions and types
    "SchemaKind",
    "SchemaNotFoundError",
    "SchemaValidationError",
    "SchemaKindMismatchError",
    "SchemaBreakingChangeError",
    "SchemaInUseError",
    # Base classes
    "_Base",
    "_GPRecord",
    "_GPNodeBase",
    "_GPEdgeBase",
    # Concrete models
    "_GPNode",
    "_GPEdge",
    "_GPSchema",
    # Factories
    "create_node_model",
    "create_edge_model",
    "create_schema_model",
    # DTOs
    "NodeUpsert",
    "NodeRead",
    "NodeReadWithPayload",
    "EdgeUpsert",
    "EdgeRead",
    "SchemaUpsert",
    "SchemaRef",
    # Helpers
    "_normalize_schema_kind",
    "_SCHEMA_KIND_VALUES",
    # ID generation
    "_ID_ALPHABET",
    "_ID_MAX_COLLISION_ATTEMPTS",
    "_PG_UNIQUE_VIOLATION",
    "_is_primary_key_violation",
    "ID_GENERATOR",
    "generate_id",
]   
