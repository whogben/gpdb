"""
Conversion helpers between DTOs and ORM objects.
"""

from __future__ import annotations

from typing import Any

from gpdb.models.dto import NodeUpsert, NodeRead, EdgeUpsert, EdgeRead
from gpdb.models.records import _GPNode, _GPEdge

__all__ = [
    "_node_upsert_to_orm",
    "_node_orm_to_read",
    "_node_orm_to_read_with_payload",
    "_edge_upsert_to_orm",
    "_edge_orm_to_read",
]


def _node_upsert_to_orm(
    dto: NodeUpsert, existing: Any = None, model: type = _GPNode
) -> Any:
    """Convert NodeUpsert DTO to ORM instance."""
    if existing:
        # `NodeUpsert.data` / `NodeUpsert.tags` have default values, so
        # `dto.data` is never None even when the caller omitted them.
        # Use `model_fields_set` to detect what the caller explicitly provided.
        fields_set = dto.model_fields_set
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
        if "data" in fields_set:
            existing.data = dto.data
        if "tags" in fields_set:
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


def _node_orm_to_read_with_payload(orm: Any) -> NodeRead:
    """Convert ORM instance to NodeReadWithPayload DTO."""
    from gpdb.models.dto import NodeReadWithPayload

    base = NodeRead.model_validate(orm)
    return NodeReadWithPayload(
        **base.model_dump(),
        payload=getattr(orm, "payload", None),
    )


def _edge_upsert_to_orm(
    dto: EdgeUpsert, existing: Any = None, model: type = _GPEdge
) -> Any:
    """Convert EdgeUpsert DTO to ORM instance."""
    if existing:
        # `EdgeUpsert.data` / `EdgeUpsert.tags` have default values, so
        # `dto.data` is never None even when the caller omitted them.
        # Use `model_fields_set` to detect what the caller explicitly provided.
        fields_set = dto.model_fields_set
        if dto.type is not None:
            existing.type = dto.type
        if dto.source_id is not None:
            existing.source_id = dto.source_id
        if dto.target_id is not None:
            existing.target_id = dto.target_id
        if dto.schema_name is not None:
            existing.schema_name = dto.schema_name
        if "data" in fields_set:
            existing.data = dto.data
        if "tags" in fields_set:
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
