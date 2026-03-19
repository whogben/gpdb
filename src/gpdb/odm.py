"""
Domain Model (ODM) base classes for nodes and edges.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict, Field

from gpdb.models.dto import NodeUpsert, NodeRead, EdgeUpsert, EdgeRead


class NodeModel(BaseModel):
    """
    Base class for domain models backed by the graph DB.

    Subclass this to define specific node types with strong typing.
    Any fields not defined in the standard schema will be automatically
    packed into the 'data' dictionary.
    """

    # System fields are prefixed with 'node_' to avoid collisions
    # with user-defined fields in subclasses.
    node_id: str | None = None
    node_type: str = "node"
    node_name: str | None = None
    node_owner_id: str | None = None
    node_parent_id: str | None = None
    node_tags: List[str] = Field(default_factory=list)

    # Read-only metadata
    node_created_at: datetime | None = None
    node_updated_at: datetime | None = None
    node_version: int | None = None
    node_payload_size: int = 0
    node_payload_hash: str | None = None
    node_payload_mime: str | None = None
    node_payload_filename: str | None = None

    # Payload content (optional)
    node_payload: bytes | None = None

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
    edge_id: str | None = None
    edge_type: str = "edge"
    edge_source_id: str
    edge_target_id: str
    edge_tags: List[str] = Field(default_factory=list)

    # Read-only metadata
    edge_created_at: datetime | None = None
    edge_updated_at: datetime | None = None
    edge_version: int | None = None

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
