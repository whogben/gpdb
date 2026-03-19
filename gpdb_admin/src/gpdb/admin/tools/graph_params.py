from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from gpdb.admin.graph_content import (
    GraphEdgeCreateParam,
    GraphEdgeUpdateParam,
    GraphNodeCreateParam,
    GraphNodeUpdateParam,
    GraphSchemaCreateParam,
    GraphSchemaUpdateParam,
)
from gpdb.query_docs import (
    EDGE_LIST_SORT_DESCRIPTION,
    FILTER_DSL_DESCRIPTION,
    NODE_LIST_SORT_DESCRIPTION,
)

SchemaKindLiteral = Literal["node", "edge"]


class GraphSchemaCreateParams(BaseModel):
    """Parameters for creating one graph schema."""

    graph_id: str = Field(..., description="Graph ID.")
    name: str = Field(..., description="Schema name.")
    json_schema: dict[str, object] = Field(..., description="JSON Schema object.")
    kind: SchemaKindLiteral = Field(..., description="Schema kind: node or edge.")


class GraphSchemasCreateParams(BaseModel):
    """Parameters for creating multiple graph schemas."""

    graph_id: str = Field(..., description="Graph ID.")
    schemas: list[GraphSchemaCreateParam] = Field(
        ..., description="List of schema create parameters."
    )


class GraphIdParams(BaseModel):
    """Base parameters for operations that require a graph ID."""

    graph_id: str = Field(..., description="Graph ID.")


class SchemaIdentifierParams(BaseModel):
    """Base parameters for operations that require a graph ID and schema name + kind."""

    graph_id: str = Field(..., description="Graph ID.")
    name: str = Field(..., description="Schema name.")
    kind: SchemaKindLiteral = Field(..., description="Schema kind: node or edge.")


class GraphOverviewParams(GraphIdParams):
    """Parameters for getting a graph overview."""


class GraphSchemaListParams(GraphIdParams):
    """Parameters for listing graph schemas."""

    kind: SchemaKindLiteral = Field(
        ..., description="Schema kind: list only node or only edge schemas."
    )


class GraphSchemaGetParams(SchemaIdentifierParams):
    """Parameters for getting a graph schema."""


class GraphSchemasGetParams(BaseModel):
    """Parameters for getting multiple graph schemas."""

    graph_id: str = Field(..., description="Graph ID.")
    names: list[str] = Field(..., description="List of schema names.")
    kind: SchemaKindLiteral = Field(..., description="Schema kind: node or edge.")


class GraphSchemaDeleteParams(SchemaIdentifierParams):
    """Parameters for deleting a graph schema."""


class GraphSchemasDeleteParams(BaseModel):
    """Parameters for deleting multiple graph schemas."""

    graph_id: str = Field(..., description="Graph ID.")
    names: list[str] = Field(..., description="List of schema names to delete.")
    kind: SchemaKindLiteral = Field(..., description="Schema kind: node or edge.")


class InstanceIdParams(BaseModel):
    """Base parameters for operations that require an instance ID."""

    instance_id: str = Field(..., description="Instance ID.")


class InstanceListParams(BaseModel):
    """Parameters for listing instances."""


class InstanceGetParams(InstanceIdParams):
    """Parameters for getting an instance."""


class InstanceDeleteParams(InstanceIdParams):
    """Parameters for deleting an instance."""


class InstanceCreateParams(BaseModel):
    """Parameters for creating an instance."""

    slug: str = Field(..., description="Instance slug (unique identifier).")
    display_name: str = Field(..., description="Display name for the instance.")
    description: str = Field(..., description="Description of the instance.")
    host: str = Field(..., description="Database host address.")
    port: int | None = Field(None, description="Database port number.")
    database: str = Field(..., description="Database name.")
    username: str = Field(..., description="Database username.")
    password: str | None = Field(None, description="Database password.")


class InstanceUpdateParams(BaseModel):
    """Parameters for updating an instance. Omitted fields are left unchanged."""

    instance_id: str = Field(..., description="Instance ID.")
    display_name: str | None = Field(None, description="Display name for the instance.")
    description: str | None = Field(None, description="Description of the instance.")
    is_active: bool | None = Field(None, description="Whether the instance is active.")
    host: str | None = Field(None, description="Database host address.")
    port: int | None = Field(None, description="Database port number.")
    database: str | None = Field(None, description="Database name.")
    username: str | None = Field(None, description="Database username.")
    password: str | None = Field(None, description="Database password.")


class NodeIdParams(BaseModel):
    """Base parameters for operations that require a graph ID and node ID."""

    graph_id: str = Field(..., description="Graph ID.")
    node_id: str = Field(..., description="Node ID.")


class EdgeIdParams(BaseModel):
    """Base parameters for operations that require a graph ID and edge ID."""

    graph_id: str = Field(..., description="Graph ID.")
    edge_id: str = Field(..., description="Edge ID.")


class GraphNodesGetParams(BaseModel):
    """Parameters for getting multiple graph nodes."""

    graph_id: str = Field(..., description="Graph ID.")
    node_ids: list[str] = Field(..., description="List of node IDs.")


class GraphNodesCreateParams(BaseModel):
    """Parameters for creating multiple graph nodes."""

    graph_id: str = Field(..., description="Graph ID.")
    nodes: list[GraphNodeCreateParam] = Field(
        ..., description="List of node create parameters."
    )


class GraphNodesUpdateParams(BaseModel):
    """Parameters for updating multiple graph nodes."""

    graph_id: str = Field(..., description="Graph ID.")
    nodes: list[GraphNodeUpdateParam] = Field(
        ..., description="List of node update parameters."
    )


class GraphNodesDeleteParams(BaseModel):
    """Parameters for deleting multiple graph nodes."""

    graph_id: str = Field(..., description="Graph ID.")
    node_ids: list[str] = Field(..., description="List of node ids to delete.")


class NodeDeleteParams(NodeIdParams):
    """Parameters for deleting a graph node."""


class GraphNodePayloadsGetParams(BaseModel):
    """Parameters for getting multiple graph node payloads."""

    graph_id: str = Field(..., description="Graph ID.")
    node_ids: list[str] = Field(..., description="List of node IDs.")


class GraphNodePayloadSetItemParams(BaseModel):
    """Parameters for setting one node payload within a bulk request."""

    node_id: str = Field(..., description="Node ID.")
    payload_base64: str = Field(..., description="Base64-encoded payload data.")
    payload_mime: str = Field(default="", description="MIME type of the payload.")
    payload_filename: str = Field(default="", description="Filename for the payload.")


class GraphNodePayloadsSetParams(BaseModel):
    """Parameters for setting multiple graph node payloads."""

    graph_id: str = Field(..., description="Graph ID.")
    payloads: list[GraphNodePayloadSetItemParams] = Field(
        ..., description="List of node payload set operations."
    )


class EdgeGetParams(EdgeIdParams):
    """Parameters for getting a graph edge."""


class EdgeDeleteParams(EdgeIdParams):
    """Parameters for deleting a graph edge."""


class EdgeListParams(BaseModel):
    """Parameters for listing graph edges."""

    graph_id: str = Field(..., description="Graph ID.")
    type: str = Field(default="", description="Filter by edge type.")
    source_id: str = Field(default="", description="Filter by source node ID.")
    target_id: str = Field(default="", description="Filter by target node ID.")
    filter: str = Field(
        default="",
        description=(
            "gpdb filter DSL string. If non-empty, this overrides the structured "
            "filters (`type`, `source_id`, `target_id`). When empty, "
            "those structured filters are used instead.\n\n"
            f"{FILTER_DSL_DESCRIPTION}"
        ),
    )
    limit: int = Field(default=50, description="Maximum number of results to return.")
    offset: int = Field(default=0, description="Number of results to skip.")
    sort: str = Field(
        default="created_at_desc",
        description=EDGE_LIST_SORT_DESCRIPTION,
    )


class EdgeCreateParams(BaseModel):
    """Parameters for creating a graph edge."""

    graph_id: str = Field(..., description="Graph ID.")
    type: str = Field(..., description="Edge type.")
    source_id: str = Field(..., description="Source node ID.")
    target_id: str = Field(..., description="Target node ID.")
    data: dict[str, object] = Field(..., description="Edge data as JSON object.")
    tags: list[str] = Field(default_factory=list, description="Edge tags.")


class EdgeUpdateParams(BaseModel):
    """Parameters for updating a graph edge. Omitted fields are left unchanged."""

    graph_id: str = Field(..., description="Graph ID.")
    edge_id: str = Field(..., description="Edge ID.")
    type: str | None = Field(None, description="Edge type.")
    source_id: str | None = Field(None, description="Source node ID.")
    target_id: str | None = Field(None, description="Target node ID.")
    data: dict[str, object] | None = Field(
        None, description="Edge data as JSON object."
    )
    tags: list[str] | None = Field(None, description="Edge tags.")


class GraphEdgesGetParams(BaseModel):
    """Parameters for getting multiple graph edges."""

    graph_id: str = Field(..., description="Graph ID.")
    edge_ids: list[str] = Field(..., description="List of edge IDs.")


class GraphEdgesCreateParams(BaseModel):
    """Parameters for creating multiple graph edges."""

    graph_id: str = Field(..., description="Graph ID.")
    edges: list[GraphEdgeCreateParam] = Field(
        ..., description="List of edge create parameters."
    )


class GraphEdgesUpdateParams(BaseModel):
    """Parameters for updating multiple graph edges."""

    graph_id: str = Field(..., description="Graph ID.")
    updates: list[GraphEdgeUpdateParam] = Field(
        ..., description="List of edge update parameters."
    )


class GraphEdgesDeleteParams(BaseModel):
    """Parameters for deleting multiple graph edges."""

    graph_id: str = Field(..., description="Graph ID.")
    edge_ids: list[str] = Field(..., description="List of edge ids to delete.")


class NodeListParams(BaseModel):
    """Parameters for listing graph nodes."""

    graph_id: str = Field(..., description="Graph ID.")
    type: str = Field(default="", description="Filter by node type.")
    parent_id: str = Field(default="", description="Filter by parent node ID.")
    filter: str = Field(
        default="",
        description=(
            "gpdb filter DSL string. If non-empty, this overrides the structured "
            "filters (`type`, `parent_id`). When empty, those "
            "structured filters are used instead.\n\n"
            f"{FILTER_DSL_DESCRIPTION}"
        ),
    )
    limit: int = Field(default=50, description="Maximum number of results to return.")
    offset: int = Field(default=0, description="Number of results to skip.")
    sort: str = Field(
        default="created_at_desc",
        description=NODE_LIST_SORT_DESCRIPTION,
    )


class NodeCreateParams(BaseModel):
    """Parameters for creating a graph node."""

    graph_id: str = Field(..., description="Graph ID.")
    type: str = Field(..., description="Node type.")
    data: dict[str, object] = Field(..., description="Node data as JSON object.")
    name: str = Field(default="", description="Node name.")
    owner_id: str = Field(default="", description="Owner ID.")
    parent_id: str = Field(default="", description="Parent node ID.")
    tags: list[str] = Field(default_factory=list, description="Node tags.")
    payload_base64: str | None = Field(None, description="Base64-encoded payload data.")
    payload_mime: str = Field(default="", description="MIME type of the payload.")
    payload_filename: str = Field(default="", description="Filename for the payload.")


class NodeUpdateParams(BaseModel):
    """Parameters for updating a graph node. Omitted fields are left unchanged."""

    graph_id: str = Field(..., description="Graph ID.")
    node_id: str = Field(..., description="Node ID.")
    type: str | None = Field(None, description="Node type.")
    data: dict[str, object] | None = Field(
        None, description="Node data as JSON object."
    )
    name: str | None = Field(None, description="Node name.")
    owner_id: str | None = Field(None, description="Owner ID.")
    parent_id: str | None = Field(None, description="Parent node ID.")
    tags: list[str] | None = Field(None, description="Node tags.")
    payload_base64: str | None = Field(None, description="Base64-encoded payload data.")
    payload_mime: str | None = Field(None, description="MIME type of the payload.")
    payload_filename: str | None = Field(None, description="Filename for the payload.")
    clear_payload: bool = Field(
        default=False, description="Whether to clear the payload."
    )


class GraphListParams(BaseModel):
    """Parameters for listing graphs."""

    instance_id: str | None = Field(
        default=None, description="Instance ID to filter graphs by."
    )


class GraphSchemaUpdateParams(BaseModel):
    """Parameters for updating multiple graph schemas. Omitted fields are left unchanged."""

    graph_id: str = Field(..., description="Graph ID.")
    schemas: list[GraphSchemaUpdateParam] = Field(
        ..., description="List of schema update parameters."
    )


class GraphCreateParams(BaseModel):
    """Parameters for creating one graph."""

    instance_id: str = Field(..., description="Instance ID.")
    table_prefix: str = Field(..., description="Table prefix for the graph.")
    display_name: str | None = Field(
        default=None, description="Display name for the graph."
    )


class GraphUpdateParams(BaseModel):
    """Parameters for updating one graph. Omitted fields are left unchanged."""

    graph_id: str = Field(..., description="Graph ID.")
    display_name: str | None = Field(None, description="Display name for the graph.")
