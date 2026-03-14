from __future__ import annotations

from typing import TYPE_CHECKING

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
    JSON_OBJECT_CODEC,
    OPTIONAL_PAYLOAD_BASE64_CODEC,
    PAYLOAD_BASE64_CODEC,
    TAGS_CODEC,
    _graph_surface_specs,
)

if TYPE_CHECKING:
    from gpdb.admin.runtime import AdminServices

from toolaccess import InvocationContext


def _build_graph_content_service(services: AdminServices) -> ToolService:
    """Build graph-content tools once and expose them on all surfaces."""
    service = ToolService("admin-graph")

    @service.tool(
        name="graph_overview",
        surfaces=_graph_surface_specs(http_method="GET"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_overview(
        graph_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphOverview:
        """Return one managed graph overview for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_overview",
            ctx,
            graph_id=graph_id,
        )

    @service.tool(
        name="graph_schema_list",
        surfaces=_graph_surface_specs(http_method="GET"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schema_list(
        graph_id: str,
        kind: str = "",
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaList:
        """List graph schemas for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "list_graph_schemas",
            ctx,
            graph_id=graph_id,
            kind=kind,
        )

    @service.tool(
        name="graph_schema_get",
        surfaces=_graph_surface_specs(
            http_method="GET",
            cli_renderer=CLI_ALIAS_JSON_RENDERER,
        ),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schema_get(
        graph_id: str,
        name: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaDetail:
        """Return one graph schema for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_schema",
            ctx,
            graph_id=graph_id,
            name=name,
        )

    @service.tool(
        name="graph_schema_create",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={"json_schema": JSON_OBJECT_CODEC},
    )
    async def graph_schema_create(
        graph_id: str,
        name: str,
        json_schema: dict[str, object],
        kind: str = "node",
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaDetail:
        """Create one graph schema for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph_schema",
            ctx,
            graph_id=graph_id,
            name=name,
            json_schema=json_schema,
            kind=kind,
        )

    @service.tool(
        name="graph_schema_update",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={"json_schema": JSON_OBJECT_CODEC},
    )
    async def graph_schema_update(
        graph_id: str,
        name: str,
        json_schema: dict[str, object],
        kind: str = "node",
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaDetail:
        """Update one graph schema for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph_schema",
            ctx,
            graph_id=graph_id,
            name=name,
            json_schema=json_schema,
            kind=kind,
        )

    @service.tool(
        name="graph_schema_delete",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schema_delete(
        graph_id: str,
        name: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaDetail:
        """Delete one graph schema for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph_schema",
            ctx,
            graph_id=graph_id,
            name=name,
        )

    @service.tool(
        name="instance_list",
        surfaces=_graph_surface_specs(http_method="GET"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def instance_list(
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
        surfaces=_graph_surface_specs(http_method="GET"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def instance_get(
        instance_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> InstanceDetail:
        """Return one instance for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_instance",
            ctx,
            instance_id=instance_id,
        )

    @service.tool(
        name="instance_create",
        surfaces=_graph_surface_specs(http_method="POST"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def instance_create(
        slug: str,
        display_name: str,
        description: str,
        host: str,
        port: int | None,
        database: str,
        username: str,
        password: str | None,
        ctx: InvocationContext = inject_context(),
    ) -> InstanceDetail:
        """Create one external instance for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_instance",
            ctx,
            slug=slug,
            display_name=display_name,
            description=description,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
        )

    @service.tool(
        name="instance_update",
        surfaces=_graph_surface_specs(http_method="PUT"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def instance_update(
        instance_id: str,
        display_name: str,
        description: str,
        is_active: bool,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        username: str | None = None,
        password: str | None = None,
        ctx: InvocationContext = inject_context(),
    ) -> InstanceDetail:
        """Update one instance for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_instance",
            ctx,
            instance_id=instance_id,
            display_name=display_name,
            description=description,
            is_active=is_active,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
        )

    @service.tool(
        name="instance_delete",
        surfaces=_graph_surface_specs(http_method="DELETE"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def instance_delete(
        instance_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> None:
        """Delete one instance for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_instance",
            ctx,
            instance_id=instance_id,
        )

    @service.tool(
        name="graph_list",
        surfaces=_graph_surface_specs(http_method="GET"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_list(
        instance_id: str | None = None,
        ctx: InvocationContext = inject_context(),
    ) -> GraphList:
        """List graphs for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "list_graphs",
            ctx,
            instance_id=instance_id,
        )

    @service.tool(
        name="graph_get",
        surfaces=_graph_surface_specs(http_method="GET"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_get(
        graph_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphDetail:
        """Return one graph for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph",
            ctx,
            graph_id=graph_id,
        )

    @service.tool(
        name="graph_create",
        surfaces=_graph_surface_specs(http_method="POST"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_create(
        instance_id: str,
        table_prefix: str,
        display_name: str | None = None,
        ctx: InvocationContext = inject_context(),
    ) -> GraphDetail:
        """Create one graph for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph",
            ctx,
            instance_id=instance_id,
            table_prefix=table_prefix,
            display_name=display_name,
        )

    @service.tool(
        name="graph_update",
        surfaces=_graph_surface_specs(http_method="PUT"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_update(
        graph_id: str,
        display_name: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphDetail:
        """Update one graph for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph",
            ctx,
            graph_id=graph_id,
            display_name=display_name,
        )

    @service.tool(
        name="graph_delete",
        surfaces=_graph_surface_specs(http_method="DELETE"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_delete(
        graph_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> None:
        """Delete one graph for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph",
            ctx,
            graph_id=graph_id,
        )

    @service.tool(
        name="graph_node_list",
        surfaces=_graph_surface_specs(http_method="GET"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_list(
        graph_id: str,
        type: str = "",
        schema_name: str = "",
        parent_id: str = "",
        filter: str = "",
        limit: int = 50,
        offset: int = 0,
        sort: str = "created_at_desc",
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeList:
        """List graph nodes for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "list_graph_nodes",
            ctx,
            graph_id=graph_id,
            type=type,
            schema_name=schema_name,
            parent_id=parent_id,
            filter_dsl=filter.strip() or None,
            limit=limit,
            offset=offset,
            sort=sort,
        )

    @service.tool(
        name="graph_node_get",
        surfaces=_graph_surface_specs(
            http_method="GET",
            cli_renderer=CLI_ALIAS_JSON_RENDERER,
        ),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_get(
        graph_id: str,
        node_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Return one graph node for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_node",
            ctx,
            graph_id=graph_id,
            node_id=node_id,
        )

    @service.tool(
        name="graph_node_create",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={
            "data": JSON_OBJECT_CODEC,
            "tags": TAGS_CODEC,
            "payload_base64": OPTIONAL_PAYLOAD_BASE64_CODEC,
        },
    )
    async def graph_node_create(
        graph_id: str,
        type: str,
        data: dict[str, object],
        name: str = "",
        schema_name: str = "",
        owner_id: str = "",
        parent_id: str = "",
        tags: str = "",
        payload_base64: str | None = None,
        payload_mime: str = "",
        payload_filename: str = "",
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Create one graph node for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph_node",
            ctx,
            graph_id=graph_id,
            type=type,
            name=name,
            schema_name=schema_name,
            owner_id=owner_id,
            parent_id=parent_id,
            tags=tags,
            data=data,
            payload=payload_base64,
            payload_mime=payload_mime,
            payload_filename=payload_filename,
        )

    @service.tool(
        name="graph_node_update",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={
            "data": JSON_OBJECT_CODEC,
            "tags": TAGS_CODEC,
            "payload_base64": OPTIONAL_PAYLOAD_BASE64_CODEC,
        },
    )
    async def graph_node_update(
        graph_id: str,
        node_id: str,
        type: str,
        data: dict[str, object],
        name: str = "",
        schema_name: str = "",
        owner_id: str = "",
        parent_id: str = "",
        tags: str = "",
        payload_base64: str | None = None,
        payload_mime: str = "",
        payload_filename: str = "",
        clear_payload: bool = False,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Update one graph node for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph_node",
            ctx,
            graph_id=graph_id,
            node_id=node_id,
            type=type,
            name=name,
            schema_name=schema_name,
            owner_id=owner_id,
            parent_id=parent_id,
            tags=tags,
            data=data,
            payload=payload_base64,
            payload_mime=payload_mime,
            payload_filename=payload_filename,
            clear_payload=clear_payload,
        )

    @service.tool(
        name="graph_node_delete",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_delete(
        graph_id: str,
        node_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Delete one graph node for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph_node",
            ctx,
            graph_id=graph_id,
            node_id=node_id,
        )

    @service.tool(
        name="graph_node_payload_get",
        surfaces=_graph_surface_specs(
            http_method="GET",
            cli_renderer=CLI_ALIAS_JSON_RENDERER,
        ),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_payload_get(
        graph_id: str,
        node_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodePayload:
        """Return one graph node payload for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_node_payload",
            ctx,
            graph_id=graph_id,
            node_id=node_id,
        )

    @service.tool(
        name="graph_node_payload_set",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={"payload_base64": PAYLOAD_BASE64_CODEC},
    )
    async def graph_node_payload_set(
        graph_id: str,
        node_id: str,
        payload_base64: str,
        mime: str = "",
        payload_filename: str = "",
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Set one graph node payload for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "set_graph_node_payload",
            ctx,
            graph_id=graph_id,
            node_id=node_id,
            payload=payload_base64,
            mime=mime,
            payload_filename=payload_filename,
        )

    @service.tool(
        name="graph_edge_list",
        surfaces=_graph_surface_specs(http_method="GET"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edge_list(
        graph_id: str,
        type: str = "",
        schema_name: str = "",
        source_id: str = "",
        target_id: str = "",
        filter: str = "",
        limit: int = 50,
        offset: int = 0,
        sort: str = "created_at_desc",
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeList:
        """List graph edges for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "list_graph_edges",
            ctx,
            graph_id=graph_id,
            type=type,
            schema_name=schema_name,
            source_id=source_id,
            target_id=target_id,
            filter_dsl=filter.strip() or None,
            limit=limit,
            offset=offset,
            sort=sort,
        )

    @service.tool(
        name="graph_edge_get",
        surfaces=_graph_surface_specs(
            http_method="GET",
            cli_renderer=CLI_ALIAS_JSON_RENDERER,
        ),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edge_get(
        graph_id: str,
        edge_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeDetail:
        """Return one graph edge for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_edge",
            ctx,
            graph_id=graph_id,
            edge_id=edge_id,
        )

    @service.tool(
        name="graph_edge_create",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={"data": JSON_OBJECT_CODEC, "tags": TAGS_CODEC},
    )
    async def graph_edge_create(
        graph_id: str,
        type: str,
        source_id: str,
        target_id: str,
        data: dict[str, object],
        schema_name: str = "",
        tags: str = "",
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeDetail:
        """Create one graph edge for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph_edge",
            ctx,
            graph_id=graph_id,
            type=type,
            source_id=source_id,
            target_id=target_id,
            schema_name=schema_name,
            tags=tags,
            data=data,
        )

    @service.tool(
        name="graph_edge_update",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={"data": JSON_OBJECT_CODEC, "tags": TAGS_CODEC},
    )
    async def graph_edge_update(
        graph_id: str,
        edge_id: str,
        type: str,
        source_id: str,
        target_id: str,
        data: dict[str, object],
        schema_name: str = "",
        tags: str = "",
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeDetail:
        """Update one graph edge for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph_edge",
            ctx,
            graph_id=graph_id,
            edge_id=edge_id,
            type=type,
            source_id=source_id,
            target_id=target_id,
            schema_name=schema_name,
            tags=tags,
            data=data,
        )

    @service.tool(
        name="graph_edge_delete",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edge_delete(
        graph_id: str,
        edge_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeDetail:
        """Delete one graph edge for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph_edge",
            ctx,
            graph_id=graph_id,
            edge_id=edge_id,
        )

    return service
