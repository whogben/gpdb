from __future__ import annotations

import base64
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field
from toolaccess import (
    ToolService,
    inject_context,
)

from gpdb.admin.context import _call_graph_content_from_context
from gpdb.admin.graph_content import (
    GraphDetail,
    GraphEdgeDetail,
    GraphEdgeList,
    GraphList,
    GraphNodeDetail,
    GraphNodeList,
    GraphNodePayload,
    GraphOverview,
    GraphSchemaDetail,
    GraphSchemaList,
    InstanceDetail,
    InstanceList,
)
from gpdb.admin.tools.base import (
    CLI_ALIAS_JSON_RENDERER,
    GRAPH_TOOL_ACCESS,
    _graph_surface_specs,
)


class GraphSchemaCreateParams(BaseModel):
    """Parameters for creating one graph schema."""

    graph_id: str = Field(..., description="Graph ID.")
    name: str = Field(..., description="Schema name.")
    json_schema: dict[str, object] = Field(..., description="JSON Schema object.")
    kind: str = Field(default="node", description="Schema kind: node or edge.")


class GraphIdParams(BaseModel):
    """Base parameters for operations that require a graph ID."""

    graph_id: str = Field(..., description="Graph ID.")


class SchemaIdentifierParams(BaseModel):
    """Base parameters for operations that require a graph ID and schema name."""

    graph_id: str = Field(..., description="Graph ID.")
    name: str = Field(..., description="Schema name.")


class GraphOverviewParams(GraphIdParams):
    """Parameters for getting a graph overview."""


class GraphSchemaListParams(GraphIdParams):
    """Parameters for listing graph schemas."""

    kind: str = Field(default="", description="Filter by schema kind (node or edge).")


class GraphSchemaGetParams(SchemaIdentifierParams):
    """Parameters for getting a graph schema."""


class GraphSchemaDeleteParams(SchemaIdentifierParams):
    """Parameters for deleting a graph schema."""


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


class NodeGetParams(NodeIdParams):
    """Parameters for getting a graph node."""


class NodeDeleteParams(NodeIdParams):
    """Parameters for deleting a graph node."""


class NodePayloadGetParams(NodeIdParams):
    """Parameters for getting a graph node payload."""


class NodePayloadSetParams(NodeIdParams):
    """Parameters for setting a graph node payload."""

    payload_base64: str = Field(..., description="Base64-encoded payload data.")
    payload_mime: str = Field(default="", description="MIME type of the payload.")
    payload_filename: str = Field(default="", description="Filename for the payload.")


class EdgeGetParams(EdgeIdParams):
    """Parameters for getting a graph edge."""


class EdgeDeleteParams(EdgeIdParams):
    """Parameters for deleting a graph edge."""


class EdgeListParams(BaseModel):
    """Parameters for listing graph edges."""

    graph_id: str = Field(..., description="Graph ID.")
    type: str = Field(default="", description="Filter by edge type.")
    schema_name: str = Field(default="", description="Filter by schema name.")
    source_id: str = Field(default="", description="Filter by source node ID.")
    target_id: str = Field(default="", description="Filter by target node ID.")
    filter: str = Field(default="", description="Filter DSL string.")
    limit: int = Field(default=50, description="Maximum number of results to return.")
    offset: int = Field(default=0, description="Number of results to skip.")
    sort: str = Field(default="created_at_desc", description="Sort order.")


class EdgeCreateParams(BaseModel):
    """Parameters for creating a graph edge."""

    graph_id: str = Field(..., description="Graph ID.")
    type: str = Field(..., description="Edge type.")
    source_id: str = Field(..., description="Source node ID.")
    target_id: str = Field(..., description="Target node ID.")
    data: dict[str, object] = Field(..., description="Edge data as JSON object.")
    schema_name: str = Field(default="", description="Schema name.")
    tags: list[str] = Field(default_factory=list, description="Edge tags.")


class EdgeUpdateParams(BaseModel):
    """Parameters for updating a graph edge. Omitted fields are left unchanged."""

    graph_id: str = Field(..., description="Graph ID.")
    edge_id: str = Field(..., description="Edge ID.")
    type: str | None = Field(None, description="Edge type.")
    source_id: str | None = Field(None, description="Source node ID.")
    target_id: str | None = Field(None, description="Target node ID.")
    data: dict[str, object] | None = Field(None, description="Edge data as JSON object.")
    schema_name: str | None = Field(None, description="Schema name.")
    tags: list[str] | None = Field(None, description="Edge tags.")


class NodeListParams(BaseModel):
    """Parameters for listing graph nodes."""

    graph_id: str = Field(..., description="Graph ID.")
    type: str = Field(default="", description="Filter by node type.")
    schema_name: str = Field(default="", description="Filter by schema name.")
    parent_id: str = Field(default="", description="Filter by parent node ID.")
    filter: str = Field(default="", description="Filter DSL string.")
    limit: int = Field(default=50, description="Maximum number of results to return.")
    offset: int = Field(default=0, description="Number of results to skip.")
    sort: str = Field(default="created_at_desc", description="Sort order.")


class NodeCreateParams(BaseModel):
    """Parameters for creating a graph node."""

    graph_id: str = Field(..., description="Graph ID.")
    type: str = Field(..., description="Node type.")
    data: dict[str, object] = Field(..., description="Node data as JSON object.")
    name: str = Field(default="", description="Node name.")
    schema_name: str = Field(default="", description="Schema name.")
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
    data: dict[str, object] | None = Field(None, description="Node data as JSON object.")
    name: str | None = Field(None, description="Node name.")
    schema_name: str | None = Field(None, description="Schema name.")
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
    """Parameters for updating one graph schema. Omitted fields are left unchanged."""

    graph_id: str = Field(..., description="Graph ID.")
    name: str = Field(..., description="Schema name.")
    json_schema: dict[str, object] | None = Field(None, description="JSON Schema object.")
    kind: str | None = Field(None, description="Schema kind: node or edge.")


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


if TYPE_CHECKING:
    from gpdb.admin.runtime import AdminServices

from toolaccess import InvocationContext


def _build_graph_content_service(services: AdminServices) -> ToolService:
    """Build graph-content tools once and expose them on all surfaces."""
    service = ToolService("admin-graph")

    @service.tool(
        name="graph_overview",
        surfaces=_graph_surface_specs(),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_overview(
        params: GraphOverviewParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphOverview:
        """Return one managed graph overview for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_overview",
            ctx,
            graph_id=params.graph_id,
        )

    @service.tool(
        name="graph_schema_list",
        surfaces=_graph_surface_specs(),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schema_list(
        params: GraphSchemaListParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaList:
        """List graph schemas for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "list_graph_schemas",
            ctx,
            graph_id=params.graph_id,
            kind=params.kind,
        )

    @service.tool(
        name="graph_schema_get",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schema_get(
        params: GraphSchemaGetParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaDetail:
        """Return one graph schema for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_schema",
            ctx,
            graph_id=params.graph_id,
            name=params.name,
        )

    @service.tool(
        name="graph_schema_create",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schema_create(
        params: GraphSchemaCreateParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaDetail:
        """Create one graph schema for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph_schema",
            ctx,
            graph_id=params.graph_id,
            name=params.name,
            json_schema=params.json_schema,
            kind=params.kind,
        )

    @service.tool(
        name="graph_schema_update",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schema_update(
        params: GraphSchemaUpdateParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaDetail:
        """Update one graph schema for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph_schema",
            ctx,
            graph_id=params.graph_id,
            name=params.name,
            json_schema=params.json_schema,
            kind=params.kind,
        )

    @service.tool(
        name="graph_schema_delete",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schema_delete(
        params: GraphSchemaDeleteParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaDetail:
        """Delete one graph schema for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph_schema",
            ctx,
            graph_id=params.graph_id,
            name=params.name,
        )

    @service.tool(
        name="instance_list",
        surfaces=_graph_surface_specs(),
        access=GRAPH_TOOL_ACCESS,
    )
    async def instance_list(
        params: InstanceListParams,
        ctx: InvocationContext = inject_context(),
    ) -> InstanceList:
        """List instances for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "list_instances",
            ctx,
        )

    @service.tool(
        name="instance_get",
        surfaces=_graph_surface_specs(),
        access=GRAPH_TOOL_ACCESS,
    )
    async def instance_get(
        params: InstanceGetParams,
        ctx: InvocationContext = inject_context(),
    ) -> InstanceDetail:
        """Return one instance for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_instance",
            ctx,
            instance_id=params.instance_id,
        )

    @service.tool(
        name="instance_create",
        surfaces=_graph_surface_specs(http_method="POST"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def instance_create(
        params: InstanceCreateParams,
        ctx: InvocationContext = inject_context(),
    ) -> InstanceDetail:
        """Create one external instance for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_instance",
            ctx,
            slug=params.slug,
            display_name=params.display_name,
            description=params.description,
            host=params.host,
            port=params.port,
            database=params.database,
            username=params.username,
            password=params.password,
        )

    @service.tool(
        name="instance_update",
        surfaces=_graph_surface_specs(http_method="POST"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def instance_update(
        params: InstanceUpdateParams,
        ctx: InvocationContext = inject_context(),
    ) -> InstanceDetail:
        """Update one instance for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_instance",
            ctx,
            instance_id=params.instance_id,
            display_name=params.display_name,
            description=params.description,
            is_active=params.is_active,
            host=params.host,
            port=params.port,
            database=params.database,
            username=params.username,
            password=params.password,
        )

    @service.tool(
        name="instance_delete",
        surfaces=_graph_surface_specs(),
        access=GRAPH_TOOL_ACCESS,
    )
    async def instance_delete(
        params: InstanceDeleteParams,
        ctx: InvocationContext = inject_context(),
    ) -> None:
        """Delete one instance for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_instance",
            ctx,
            instance_id=params.instance_id,
        )

    @service.tool(
        name="graph_list",
        surfaces=_graph_surface_specs(),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_list(
        params: GraphListParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphList:
        """List graphs for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "list_graphs",
            ctx,
            instance_id=params.instance_id,
        )

    @service.tool(
        name="graph_get",
        surfaces=_graph_surface_specs(),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_get(
        params: GraphIdParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphDetail:
        """Return one graph for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph",
            ctx,
            graph_id=params.graph_id,
        )

    @service.tool(
        name="graph_create",
        surfaces=_graph_surface_specs(http_method="POST"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_create(
        params: GraphCreateParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphDetail:
        """Create one graph for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph",
            ctx,
            instance_id=params.instance_id,
            table_prefix=params.table_prefix,
            display_name=params.display_name,
        )

    @service.tool(
        name="graph_update",
        surfaces=_graph_surface_specs(http_method="POST"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_update(
        params: GraphUpdateParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphDetail:
        """Update one graph for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph",
            ctx,
            graph_id=params.graph_id,
            display_name=params.display_name,
        )

    @service.tool(
        name="graph_delete",
        surfaces=_graph_surface_specs(),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_delete(
        params: GraphIdParams,
        ctx: InvocationContext = inject_context(),
    ) -> None:
        """Delete one graph for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph",
            ctx,
            graph_id=params.graph_id,
        )

    @service.tool(
        name="graph_node_list",
        surfaces=_graph_surface_specs(),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_list(
        params: NodeListParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeList:
        """List graph nodes for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "list_graph_nodes",
            ctx,
            graph_id=params.graph_id,
            type=params.type,
            schema_name=params.schema_name,
            parent_id=params.parent_id,
            filter_dsl=params.filter.strip() or None,
            limit=params.limit,
            offset=params.offset,
            sort=params.sort,
        )

    @service.tool(
        name="graph_node_get",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_get(
        params: NodeGetParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Return one graph node for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_node",
            ctx,
            graph_id=params.graph_id,
            node_id=params.node_id,
        )

    @service.tool(
        name="graph_node_create",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_create(
        params: NodeCreateParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Create one graph node for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph_node",
            ctx,
            graph_id=params.graph_id,
            type=params.type,
            name=params.name,
            schema_name=params.schema_name,
            owner_id=params.owner_id,
            parent_id=params.parent_id,
            tags=params.tags,
            data=params.data,
            payload=(
                base64.b64decode(params.payload_base64)
                if params.payload_base64
                else None
            ),
            payload_mime=params.payload_mime,
            payload_filename=params.payload_filename,
        )

    @service.tool(
        name="graph_node_update",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_update(
        params: NodeUpdateParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Update one graph node for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph_node",
            ctx,
            graph_id=params.graph_id,
            node_id=params.node_id,
            type=params.type,
            name=params.name,
            schema_name=params.schema_name,
            owner_id=params.owner_id,
            parent_id=params.parent_id,
            tags=params.tags,
            data=params.data,
            payload=(
                base64.b64decode(params.payload_base64)
                if params.payload_base64
                else None
            ),
            payload_mime=params.payload_mime,
            payload_filename=params.payload_filename,
            clear_payload=params.clear_payload,
        )

    @service.tool(
        name="graph_node_delete",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_delete(
        params: NodeDeleteParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Delete one graph node for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph_node",
            ctx,
            graph_id=params.graph_id,
            node_id=params.node_id,
        )

    @service.tool(
        name="graph_node_payload_get",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_payload_get(
        params: NodePayloadGetParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodePayload:
        """Return one graph node payload for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_node_payload",
            ctx,
            graph_id=params.graph_id,
            node_id=params.node_id,
        )

    @service.tool(
        name="graph_node_payload_set",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_payload_set(
        params: NodePayloadSetParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Set one graph node payload for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "set_graph_node_payload",
            ctx,
            graph_id=params.graph_id,
            node_id=params.node_id,
            payload=base64.b64decode(params.payload_base64),
            mime=params.payload_mime,
            payload_filename=params.payload_filename,
        )

    @service.tool(
        name="graph_edge_list",
        surfaces=_graph_surface_specs(),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edge_list(
        params: EdgeListParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeList:
        """List graph edges for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "list_graph_edges",
            ctx,
            graph_id=params.graph_id,
            type=params.type,
            schema_name=params.schema_name,
            source_id=params.source_id,
            target_id=params.target_id,
            filter_dsl=params.filter.strip() or None,
            limit=params.limit,
            offset=params.offset,
            sort=params.sort,
        )

    @service.tool(
        name="graph_edge_get",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edge_get(
        params: EdgeGetParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeDetail:
        """Return one graph edge for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_edge",
            ctx,
            graph_id=params.graph_id,
            edge_id=params.edge_id,
        )

    @service.tool(
        name="graph_edge_create",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edge_create(
        params: EdgeCreateParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeDetail:
        """Create one graph edge for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph_edge",
            ctx,
            graph_id=params.graph_id,
            type=params.type,
            source_id=params.source_id,
            target_id=params.target_id,
            schema_name=params.schema_name,
            tags=params.tags,
            data=params.data,
        )

    @service.tool(
        name="graph_edge_update",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edge_update(
        params: EdgeUpdateParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeDetail:
        """Update one graph edge for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph_edge",
            ctx,
            graph_id=params.graph_id,
            edge_id=params.edge_id,
            type=params.type,
            source_id=params.source_id,
            target_id=params.target_id,
            schema_name=params.schema_name,
            tags=params.tags,
            data=params.data,
        )

    @service.tool(
        name="graph_edge_delete",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edge_delete(
        params: EdgeDeleteParams,
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeDetail:
        """Delete one graph edge for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph_edge",
            ctx,
            graph_id=params.graph_id,
            edge_id=params.edge_id,
        )

    return service
