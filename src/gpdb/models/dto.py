"""
Pydantic Data Transfer Objects for the public API.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Union

from pydantic import BaseModel, ConfigDict, Field, model_serializer


class NodeUpsert(BaseModel):
    """Input model for creating/updating nodes."""

    id: str | None = None
    type: str = "__default__"
    name: str | None = None
    owner_id: str | None = None
    parent_id: str | None = None
    data: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    payload: bytes | None = None
    payload_mime: str | None = None
    payload_filename: str | None = None

    model_config = ConfigDict(from_attributes=True)


class NodeRead(BaseModel):
    """Output model for nodes without payload."""

    id: str
    type: str | None = None
    name: str | None = None
    owner_id: str | None = None
    parent_id: str | None = None
    data: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    version: int
    deleted_at: datetime | None = None
    payload_size: int = 0
    payload_hash: str | None = None
    payload_mime: str | None = None
    payload_filename: str | None = None

    model_config = ConfigDict(from_attributes=True)

    @model_serializer(mode="wrap")
    def _omit_deleted_at_when_null(self, handler):
        data = handler(self)
        if isinstance(data, dict) and data.get("deleted_at") is None:
            data.pop("deleted_at", None)
        return data


class NodeReadWithPayload(NodeRead):
    """Output model for nodes with payload."""

    id: str
    payload: bytes | None = None


class EdgeUpsert(BaseModel):
    """Input model for creating/updating edges."""

    id: str | None = None
    type: str = "__default__"
    source_id: str
    target_id: str
    data: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class EdgeRead(BaseModel):
    """Output model for edges."""

    id: str
    type: str | None = None
    source_id: str | None = None
    target_id: str | None = None
    data: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    version: int
    deleted_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)

    @model_serializer(mode="wrap")
    def _omit_deleted_at_when_null(self, handler):
        data = handler(self)
        if isinstance(data, dict) and data.get("deleted_at") is None:
            data.pop("deleted_at", None)
        return data


class SchemaUpsert(BaseModel):
    """Input model for creating/updating schemas.

    For ``extends``: on update, ``None`` leaves the stored list unchanged; ``[]``
    clears parents. On create, omitted ``extends`` means no parents.
    """

    name: str
    json_schema: Union[Dict[str, Any], type[BaseModel]]
    kind: str
    extends: list[str] | None = None
    alias: str | None = None
    svg_icon: str | None = None

    model_config = ConfigDict(from_attributes=True)


class SchemaRef(BaseModel):
    """Reference to a schema by name and kind."""

    name: str
    kind: str

    model_config = ConfigDict(from_attributes=True)
