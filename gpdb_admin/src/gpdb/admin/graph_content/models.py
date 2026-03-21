"""Pydantic models for graph-content operations."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class GraphContentSummary(BaseModel):
    """Live counts captured from one managed graph."""

    schema_count: int | None = None
    node_count: int | None = None
    edge_count: int | None = None


class GraphOverview(BaseModel):
    """Overview data returned by the first graph-content vertical slice."""

    graph: dict[str, object]
    instance: dict[str, object]
    summary: GraphContentSummary = Field(default_factory=GraphContentSummary)
    content_status: str = "ready"
    content_error: str | None = None


class GraphSchemaUsage(BaseModel):
    """Schema usage summary for admin list/detail views."""

    node_count: int = 0
    edge_count: int = 0
    sample_node_ids: list[str] = Field(default_factory=list)
    sample_edge_ids: list[str] = Field(default_factory=list)


class GraphSchemaRecord(BaseModel):
    """Stable schema payload returned by admin graph-content APIs."""

    name: str
    kind: str
    version: str
    json_schema: dict[str, Any] | None = None
    alias: str | None = None
    svg_icon: str | None = None
    extends: list[str] = Field(default_factory=list)
    effective_json_schema: dict[str, Any] | None = None


class GraphSchemaList(BaseModel):
    """List response for graph schemas."""

    items: list[GraphSchemaRecord] = Field(default_factory=list)
    total: int = 0


class GraphSchemaDetail(BaseModel):
    """Detail response for one graph schema."""

    schema_record: GraphSchemaRecord = Field(serialization_alias="schema")
    usage: GraphSchemaUsage = Field(default_factory=GraphSchemaUsage)

    @property
    def schema(self) -> GraphSchemaRecord:
        """Backwards-compatible accessor for the schema payload."""
        return self.schema_record


class GraphSchemaUpdateParam(BaseModel):
    """Parameters for updating a graph schema. Omitted fields are left unchanged."""

    name: str = Field(..., description="Schema name (identity field).")
    json_schema: dict[str, Any] | None = Field(
        None, description="JSON Schema object."
    )
    kind: str | None = Field(None, description="Schema kind: node or edge.")
    alias: str | None = Field(None, description="Display alias for the schema.")
    svg_icon: str | None = Field(None, description="SVG icon for the schema.")
    extends: list[str] | None = Field(
        None, description="List of parent schema names this schema extends. None = unchanged, [] = clear."
    )


class GraphSchemaCreateParam(BaseModel):
    """Parameters for creating a graph schema."""

    name: str = Field(..., description="Schema name.")
    json_schema: dict[str, Any] = Field(
        ..., description="JSON Schema object."
    )
    kind: str = Field(
        default="node", description="Schema kind: node or edge."
    )
    alias: str | None = Field(None, description="Display alias for the schema.")
    svg_icon: str | None = Field(None, description="SVG icon for the schema.")
    extends: list[str] | None = Field(
        None, description="List of parent schema names this schema extends."
    )


class GraphNodeCreateParam(BaseModel):
    """Parameters for creating nodes in a bulk request."""

    node_id: str | None = Field(
        None,
        description="Optional node id for upsert. If omitted, the GPDB layer will generate ids.",
    )
    type: str = Field(..., description="Node type.")
    name: str | None = Field(None, description="Node name.")
    owner_id: str | None = Field(None, description="Owner ID.")
    parent_id: str | None = Field(None, description="Parent node ID.")
    tags: list[str] = Field(default_factory=list, description="Node tags.")
    data: dict[str, Any] = Field(
        ..., description="Node data as a JSON object."
    )
    payload_base64: str | None = Field(
        None,
        description="Base64-encoded payload data. Omit to create/update without payload.",
    )
    payload_mime: str | None = Field(None, description="MIME type of the payload.")
    payload_filename: str | None = Field(None, description="Filename for the payload.")


class GraphNodeUpdateParam(BaseModel):
    """Parameters for updating nodes in a bulk request.

    Omitted fields are left unchanged.
    """

    node_id: str = Field(..., description="Node ID (identity field).")
    type: str | None = Field(None, description="Node type.")
    data: dict[str, Any] | None = Field(
        None, description="Node data as a JSON object."
    )
    name: str | None = Field(None, description="Node name.")
    owner_id: str | None = Field(None, description="Owner ID.")
    parent_id: str | None = Field(None, description="Parent node ID.")
    tags: list[str] | None = Field(None, description="Node tags.")
    payload_base64: str | None = Field(
        None,
        description="Base64-encoded payload data. Omit to leave payload bytes unchanged.",
    )
    payload_mime: str | None = Field(None, description="MIME type of the payload.")
    payload_filename: str | None = Field(None, description="Filename for the payload.")
    clear_payload: bool = Field(
        default=False, description="Whether to clear the payload bytes."
    )


class GraphEdgeCreateParam(BaseModel):
    """Parameters for creating edges in a bulk request."""

    edge_id: str | None = Field(
        None,
        description="Optional edge id for upsert. If omitted, the GPDB layer will generate ids.",
    )
    type: str = Field(..., description="Edge type.")
    source_id: str = Field(..., description="Source node ID.")
    target_id: str = Field(..., description="Target node ID.")
    tags: list[str] = Field(default_factory=list, description="Edge tags.")
    data: dict[str, Any] = Field(..., description="Edge data as a JSON object.")


class GraphEdgeUpdateParam(BaseModel):
    """Parameters for updating edges in a bulk request.

    Omitted fields are left unchanged.
    """

    edge_id: str = Field(..., description="Edge ID (identity field).")
    type: str | None = Field(None, description="Edge type.")
    source_id: str | None = Field(None, description="Source node ID.")
    target_id: str | None = Field(None, description="Target node ID.")
    tags: list[str] | None = Field(None, description="Edge tags.")
    data: dict[str, Any] | None = Field(None, description="Edge data as a JSON object.")


class GraphNodePayloadSetParam(BaseModel):
    """Parameters for setting node payloads in a bulk request."""

    node_id: str = Field(..., description="Node ID (identity field).")
    payload: bytes = Field(..., description="Payload bytes to store on the node.")
    mime: str | None = Field(
        None, description="Optional payload MIME type."
    )
    payload_filename: str | None = Field(
        None, description="Optional payload filename."
    )


class GraphNodeFilters(BaseModel):
    """Current node list filters echoed back to callers."""

    type: str | None = None
    parent_id: str | None = None
    filter_dsl: str | None = None
    sort: str = "created_at_desc"


class GraphNodeRecord(BaseModel):
    """Stable node payload returned by admin graph-content APIs."""

    id: str
    type: str
    name: str | None = None
    owner_id: str | None = None
    parent_id: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    created_at: str
    updated_at: str
    version: int
    payload_size: int = 0
    payload_hash: str | None = None
    payload_mime: str | None = None
    payload_filename: str | None = None
    has_payload: bool = False


class GraphNodeList(BaseModel):
    """List response for graph nodes."""

    items: list[GraphNodeRecord] = Field(default_factory=list)
    total: int = 0
    limit: int = 50
    offset: int = 0
    filters: GraphNodeFilters = Field(default_factory=GraphNodeFilters)


class GraphNodeDeleteBlockers(BaseModel):
    """Delete preflight summary for one graph node."""

    child_count: int = 0
    incident_edge_count: int = 0
    sample_child_ids: list[str] = Field(default_factory=list)
    sample_edge_ids: list[str] = Field(default_factory=list)
    can_delete: bool = True


class GraphNodeDetail(BaseModel):
    """Detail response for one graph node."""

    node_record: GraphNodeRecord = Field(serialization_alias="node")
    delete_blockers: GraphNodeDeleteBlockers | None = None

    @property
    def node(self) -> GraphNodeRecord:
        """Backwards-compatible accessor for the node payload."""
        return self.node_record


class GraphNodePayload(BaseModel):
    """Stable payload response for one graph node."""

    node_record: GraphNodeRecord = Field(serialization_alias="node")
    payload_base64: str
    encoding: str = "base64"
    filename: str

    @property
    def node(self) -> GraphNodeRecord:
        """Backwards-compatible accessor for the node payload."""
        return self.node_record


class GraphEdgeFilters(BaseModel):
    """Current edge list filters echoed back to callers."""

    type: str | None = None
    source_id: str | None = None
    target_id: str | None = None
    filter_dsl: str | None = None
    sort: str = "created_at_desc"


class GraphEdgeRecord(BaseModel):
    """Stable edge payload returned by admin graph-content APIs."""

    id: str
    type: str
    source_id: str
    target_id: str
    data: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    created_at: str
    updated_at: str
    version: int


class GraphEdgeList(BaseModel):
    """List response for graph edges."""

    items: list[GraphEdgeRecord] = Field(default_factory=list)
    total: int = 0
    limit: int = 50
    offset: int = 0
    filters: GraphEdgeFilters = Field(default_factory=GraphEdgeFilters)


class GraphEdgeDetail(BaseModel):
    """Detail response for one graph edge."""

    edge_record: GraphEdgeRecord = Field(serialization_alias="edge")

    @property
    def edge(self) -> GraphEdgeRecord:
        """Backwards-compatible accessor for the edge payload."""
        return self.edge_record


class GraphSchemaDeleteResult(BaseModel):
    """Result of deleting a graph schema - only the name is returned."""

    name: str


class GraphNodeDeleteResult(BaseModel):
    """Result of deleting a graph node - only the ID is returned."""

    id: str


class GraphEdgeDeleteResult(BaseModel):
    """Result of deleting a graph edge - only the ID is returned."""

    id: str


class InstanceRecord(BaseModel):
    """Stable instance payload returned by admin instance APIs."""

    id: str
    slug: str
    display_name: str
    description: str
    mode: str
    is_builtin: bool
    is_default: bool
    is_active: bool
    connection_kind: str
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    status: str
    status_message: str | None = None
    last_checked_at: str | None = None


class InstanceList(BaseModel):
    """List response for instances."""

    items: list[InstanceRecord] = Field(default_factory=list)
    total: int = 0


class InstanceDetail(BaseModel):
    """Detail response for one instance."""

    instance: InstanceRecord


class GraphRecord(BaseModel):
    """Stable graph payload returned by admin graph APIs."""

    id: str
    instance_id: str
    instance_slug: str
    instance_display_name: str
    display_name: str
    table_prefix: str
    status: str
    status_message: str | None = None
    last_checked_at: str | None = None
    exists_in_instance: bool
    source: str
    is_default: bool


class GraphList(BaseModel):
    """List response for graphs."""

    items: list[GraphRecord] = Field(default_factory=list)
    total: int = 0


class GraphDetail(BaseModel):
    """Detail response for one graph."""

    graph: GraphRecord


class GraphViewerData(BaseModel):
    """Combined nodes and edges for the graph viewer (Cytoscape-oriented)."""

    elements: list[dict[str, object]] = Field(default_factory=list)
    node_count: int = 0
    edge_count: int = 0
    schemas: dict[str, dict[str, str | None]] = Field(
        default_factory=dict,
        description="Display metadata keyed as 'node:<name>' or 'edge:<name>'.",
    )
    error: str | None = None
