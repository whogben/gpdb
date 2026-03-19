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
    GraphEdgeCreateParam,
    GraphList,
    GraphNodeCreateParam,
    GraphNodeDetail,
    GraphNodeList,
    GraphNodePayload,
    GraphNodePayloadSetParam,
    GraphOverview,
    GraphNodeUpdateParam,
    GraphEdgeUpdateParam,
    GraphSchemaDetail,
    GraphSchemaList,
    GraphSchemaCreateParam,
    GraphSchemaUpdateParam,
    InstanceDetail,
    InstanceList,
)
from gpdb.admin.tools.base import (
    CLI_ALIAS_JSON_RENDERER,
    GRAPH_TOOL_ACCESS,
    _graph_surface_specs,
)
from gpdb.query_docs import (
    EDGE_LIST_SORT_DESCRIPTION,
    FILTER_DSL_DESCRIPTION,
    NODE_LIST_SORT_DESCRIPTION,
)


class GraphSchemaCreateParams(BaseModel):
    """Parameters for creating one graph schema."""

    graph_id: str = Field(..., description="Graph ID.")
    name: str = Field(..., description="Schema name.")
    json_schema: dict[str, object] = Field(..., description="JSON Schema object.")
    kind: str = Field(default="node", description="Schema kind: node or edge.")


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


class GraphSchemasGetParams(BaseModel):
    """Parameters for getting multiple graph schemas."""

    graph_id: str = Field(..., description="Graph ID.")
    names: list[str] = Field(..., description="List of schema names.")


class GraphSchemaDeleteParams(SchemaIdentifierParams):
    """Parameters for deleting a graph schema."""

class GraphSchemasDeleteParams(BaseModel):
    """Parameters for deleting multiple graph schemas."""

    graph_id: str = Field(..., description="Graph ID.")
    names: list[str] = Field(..., description="List of schema names to delete.")


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
    schema_name: str = Field(default="", description="Filter by schema name.")
    source_id: str = Field(default="", description="Filter by source node ID.")
    target_id: str = Field(default="", description="Filter by target node ID.")
    filter: str = Field(
        default="",
        description=(
            "gpdb filter DSL string. If non-empty, this overrides the structured "
            "filters (`type`, `schema_name`, `source_id`, `target_id`). When empty, "
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
    schema_name: str = Field(default="", description="Filter by schema name.")
    parent_id: str = Field(default="", description="Filter by parent node ID.")
    filter: str = Field(
        default="",
        description=(
            "gpdb filter DSL string. If non-empty, this overrides the structured "
            "filters (`type`, `schema_name`, `parent_id`). When empty, those "
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
    """Parameters for updating multiple graph schemas. Omitted fields are left unchanged."""

    graph_id: str = Field(..., description="Graph ID.")
    schemas: list[GraphSchemaUpdateParam] = Field(..., description="List of schema update parameters.")


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
        name="graph_schemas_get",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schemas_get(
        params: GraphSchemasGetParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphSchemaDetail]:
        """Return multiple graph schemas for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_schemas",
            ctx,
            graph_id=params.graph_id,
            names=params.names,
        )

    @service.tool(
        name="graph_schemas_create",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schemas_create(
        params: GraphSchemasCreateParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphSchemaDetail]:
        """Create multiple graph schemas for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph_schemas",
            ctx,
            graph_id=params.graph_id,
            schemas=params.schemas,
        )

    @service.tool(
        name="graph_schemas_update",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schemas_update(
        params: GraphSchemaUpdateParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphSchemaDetail]:
        """Update multiple graph schemas for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph_schemas",
            ctx,
            graph_id=params.graph_id,
            schemas=params.schemas,
        )

    @service.tool(
        name="graph_schemas_delete",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schemas_delete(
        params: GraphSchemasDeleteParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphSchemaDetail]:
        """Delete multiple graph schemas for the authenticated caller."""
        deleted = await _call_graph_content_from_context(
            services,
            "delete_graph_schemas",
            ctx,
            graph_id=params.graph_id,
            names=params.names,
        )
        return deleted

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
        name="graph_nodes_get",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_nodes_get(
        params: GraphNodesGetParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphNodeDetail]:
        """Return multiple graph nodes for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_nodes",
            ctx,
            graph_id=params.graph_id,
            node_ids=params.node_ids,
        )

    @service.tool(
        name="graph_nodes_create",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_nodes_create(
        params: GraphNodesCreateParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphNodeDetail]:
        """Create multiple graph nodes for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph_nodes",
            ctx,
            graph_id=params.graph_id,
            nodes=params.nodes,
        )

    @service.tool(
        name="graph_nodes_update",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_nodes_update(
        params: GraphNodesUpdateParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphNodeDetail]:
        """Update multiple graph nodes for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph_nodes",
            ctx,
            graph_id=params.graph_id,
            updates=params.nodes,
        )

    @service.tool(
        name="graph_nodes_delete",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_nodes_delete(
        params: GraphNodesDeleteParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphNodeDetail]:
        """Delete multiple graph nodes for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph_nodes",
            ctx,
            graph_id=params.graph_id,
            node_ids=params.node_ids,
        )

    @service.tool(
        name="graph_node_payloads_get",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_payloads_get(
        params: GraphNodePayloadsGetParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphNodePayload]:
        """Return multiple graph node payloads for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_node_payloads",
            ctx,
            graph_id=params.graph_id,
            node_ids=params.node_ids,
        )

    @service.tool(
        name="graph_node_payloads_set",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_payloads_set(
        params: GraphNodePayloadsSetParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphNodeDetail]:
        """Set multiple graph node payloads for the authenticated caller."""
        payloads = [
            GraphNodePayloadSetParam(
                node_id=item.node_id,
                payload=base64.b64decode(item.payload_base64),
                mime=item.payload_mime,
                payload_filename=item.payload_filename,
            )
            for item in params.payloads
        ]
        return await _call_graph_content_from_context(
            services,
            "set_graph_node_payloads",
            ctx,
            graph_id=params.graph_id,
            payloads=payloads,
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
        name="graph_edges_get",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edges_get(
        params: GraphEdgesGetParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphEdgeDetail]:
        """Return multiple graph edges for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_edges",
            ctx,
            graph_id=params.graph_id,
            edge_ids=params.edge_ids,
        )

    @service.tool(
        name="graph_edges_create",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edges_create(
        params: GraphEdgesCreateParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphEdgeDetail]:
        """Create multiple graph edges for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph_edges",
            ctx,
            graph_id=params.graph_id,
            edges=params.edges,
        )

    @service.tool(
        name="graph_edges_update",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edges_update(
        params: GraphEdgesUpdateParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphEdgeDetail]:
        """Update multiple graph edges for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph_edges",
            ctx,
            graph_id=params.graph_id,
            updates=params.updates,
        )

    @service.tool(
        name="graph_edges_delete",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edges_delete(
        params: GraphEdgesDeleteParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[GraphEdgeDetail]:
        """Delete multiple graph edges for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph_edges",
            ctx,
            graph_id=params.graph_id,
            edge_ids=params.edge_ids,
        )

    return service
