from __future__ import annotations

import base64
from typing import TYPE_CHECKING

from toolaccess import (
    ToolService,
    inject_context,
)

from gpdb.admin.context import _call_graph_content_from_context
from gpdb.admin.graph_content import (
    GraphDetail,
    GraphEdgeDeleteResult,
    GraphEdgeDetail,
    GraphEdgeList,
    GraphList,
    GraphNodeDeleteResult,
    GraphNodeDetail,
    GraphNodeList,
    GraphNodePayload,
    GraphNodePayloadSetParam,
    GraphOverview,
    GraphSchemaDeleteResult,
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
from gpdb.admin.tools.graph_params import (
    EdgeCreateParams,
    EdgeDeleteParams,
    EdgeGetParams,
    EdgeListParams,
    EdgeUpdateParams,
    GraphCreateParams,
    GraphEdgesCreateParams,
    GraphEdgesDeleteParams,
    GraphEdgesGetParams,
    GraphEdgesUpdateParams,
    GraphIdParams,
    GraphListParams,
    GraphNodePayloadsGetParams,
    GraphNodePayloadsSetParams,
    GraphNodePayloadSetItemParams,
    GraphNodesCreateParams,
    GraphNodesDeleteParams,
    GraphNodesGetParams,
    GraphNodesUpdateParams,
    GraphOverviewParams,
    GraphSchemaDeleteParams,
    GraphSchemaGetParams,
    GraphSchemaListParams,
    GraphSchemaUpdateParams,
    GraphSchemasCreateParams,
    GraphSchemasDeleteParams,
    GraphSchemasGetParams,
    GraphUpdateParams,
    InstanceCreateParams,
    InstanceDeleteParams,
    InstanceGetParams,
    InstanceListParams,
    InstanceUpdateParams,
    NodeListParams,
    NodeDeleteParams,
)

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
            kind=params.kind,
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
    ) -> list[GraphSchemaDeleteResult]:
        """Delete multiple graph schemas for the authenticated caller."""
        deleted = await _call_graph_content_from_context(
            services,
            "delete_graph_schemas",
            ctx,
            graph_id=params.graph_id,
            names=params.names,
            kind=params.kind,
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
            include_delete_preflight=params.include_delete_preflight,
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
    ) -> list[GraphNodeDeleteResult]:
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
    ) -> list[GraphEdgeDeleteResult]:
        """Delete multiple graph edges for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph_edges",
            ctx,
            graph_id=params.graph_id,
            edge_ids=params.edge_ids,
        )

    return service
