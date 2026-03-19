"""Edge operations for graph-content service."""

from __future__ import annotations

from gpdb import (
    EdgeUpsert,
    Filter,
    FilterGroup,
    GPGraph,
    SchemaNotFoundError,
    SchemaValidationError,
    SearchQuery,
)
from gpdb.admin.store import AdminUser
from sqlalchemy.exc import IntegrityError

from gpdb.admin.graph_content.exceptions import (
    GraphContentConflictError,
    GraphContentNotFoundError,
    GraphContentValidationError,
)
from gpdb.admin.graph_content.models import (
    GraphEdgeCreateParam,
    GraphEdgeDeleteResult,
    GraphEdgeDetail,
    GraphEdgeFilters,
    GraphEdgeList,
    GraphEdgeRecord,
    GraphEdgeUpdateParam,
    GraphViewerData,
)
from gpdb.admin.graph_content._helpers import (
    build_edge_filter,
    normalize_optional_text,
    normalize_tag_list,
    open_graph,
    parse_edge_sort,
    require_admin_store,
    serialize_edge_record,
    validate_edge_id,
    validate_edge_type,
    validate_json_object,
    validate_page_limit,
    validate_page_offset,
    validate_related_node_id,
)


async def list_graph_edges(
    self,
    *,
    graph_id: str,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
    type: str | None = None,
    schema_name: str | None = None,
    source_id: str | None = None,
    target_id: str | None = None,
    filter_dsl: str | None = None,
    limit: int = 50,
    offset: int = 0,
    sort: str = "created_at_desc",
) -> GraphEdgeList:
    """Return paginated edge records for one managed graph."""
    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="view",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        clean_filter_dsl = normalize_optional_text(filter_dsl)
        if clean_filter_dsl:
            try:
                parsed_filter = FilterGroup.from_dsl(clean_filter_dsl)
            except ValueError as exc:
                raise GraphContentValidationError(
                    f"Invalid filter (DSL): {exc}"
                ) from exc
            filter_value: Filter | FilterGroup | None = parsed_filter
        else:
            filter_value = build_edge_filter(
                type=type,
                schema_name=schema_name,
                source_id=source_id,
                target_id=target_id,
            )
        query = SearchQuery(
            filter=filter_value,
            sort=[parse_edge_sort(sort)],
            limit=validate_page_limit(limit),
            offset=validate_page_offset(offset),
        )
        page = await db.search_edges(query)
        return GraphEdgeList(
            items=[serialize_edge_record(item) for item in page.items],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
            filters=GraphEdgeFilters(
                type=normalize_optional_text(type),
                schema_name=normalize_optional_text(schema_name),
                source_id=normalize_optional_text(source_id),
                target_id=normalize_optional_text(target_id),
                filter_dsl=clean_filter_dsl,
                sort=sort,
            ),
        )
    finally:
        await db.sqla_engine.dispose()


async def get_graph_viewer_data(
    self,
    *,
    graph_id: str,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
    node_type: str | None = None,
    node_schema_name: str | None = None,
    node_parent_id: str | None = None,
    node_filter_dsl: str | None = None,
    node_limit: int = 200,
    edge_type: str | None = None,
    edge_schema_name: str | None = None,
    edge_source_id: str | None = None,
    edge_target_id: str | None = None,
    edge_filter_dsl: str | None = None,
    edge_limit: int = 200,
) -> GraphViewerData:
    """Return combined filtered nodes and edges for the graph viewer (Cytoscape elements)."""
    from gpdb.admin.graph_content.nodes import list_graph_nodes
    
    try:
        node_list = await list_graph_nodes(
            self,
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            type=node_type,
            schema_name=node_schema_name,
            parent_id=node_parent_id,
            filter_dsl=node_filter_dsl,
            limit=min(node_limit, 500),
            offset=0,
            sort="created_at_desc",
        )
        edge_list = await list_graph_edges(
            self,
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            type=edge_type,
            schema_name=edge_schema_name,
            source_id=edge_source_id,
            target_id=edge_target_id,
            filter_dsl=edge_filter_dsl,
            limit=min(edge_limit, 500),
            offset=0,
            sort="created_at_desc",
        )
    except GraphContentValidationError as exc:
        return GraphViewerData(
            elements=[],
            node_count=0,
            edge_count=0,
            error=str(exc),
        )

    elements: list[dict[str, object]] = []

    for node in node_list.items:
        elements.append(
            {
                "group": "nodes",
                "data": {
                    "id": node.id,
                    "label": node.name or node.id,
                    "type": node.type,
                },
            }
        )
    for edge in edge_list.items:
        elements.append(
            {
                "group": "edges",
                "data": {
                    "id": edge.id,
                    "source": edge.source_id,
                    "target": edge.target_id,
                    "label": edge.type,
                },
            }
        )

    return GraphViewerData(
        elements=elements,
        node_count=len(node_list.items),
        edge_count=len(edge_list.items),
    )


async def get_graph_edges(
    self,
    *,
    graph_id: str,
    edge_ids: list[str],
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphEdgeDetail]:
    """Return multiple graph edges plus metadata."""
    if not edge_ids:
        raise GraphContentValidationError("At least one edge id is required.")

    if len(edge_ids) != len(set(edge_ids)):
        duplicates = [eid for eid in edge_ids if edge_ids.count(eid) > 1]
        raise GraphContentValidationError(
            f"Duplicate edge ids provided: {set(duplicates)}"
        )

    clean_edge_ids = [validate_edge_id(eid) for eid in edge_ids]

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="view",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        try:
            edges = await db.get_edges(clean_edge_ids)
        except ValueError as e:
            if "edge ids not found" in str(e).lower():
                raise GraphContentNotFoundError(
                    "One or more edges were not found."
                ) from e
            raise

        return [
            GraphEdgeDetail(edge_record=serialize_edge_record(edge))
            for edge in edges
        ]
    finally:
        await db.sqla_engine.dispose()


async def create_graph_edges(
    self,
    *,
    graph_id: str,
    edges: list[GraphEdgeCreateParam],
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphEdgeDetail]:
    """Create multiple edges in a managed graph (atomic bulk)."""
    if not edges:
        raise GraphContentValidationError("At least one edge is required.")

    edge_ids = [e.edge_id for e in edges if e.edge_id is not None]
    if edge_ids and len(edge_ids) != len(set(edge_ids)):
        duplicates = [eid for eid in edge_ids if edge_ids.count(eid) > 1]
        raise GraphContentValidationError(
            f"Duplicate edge ids provided: {set(duplicates)}"
        )

    edge_upserts: list[EdgeUpsert] = []
    for edge_param in edges:
        clean_edge_id = (
            validate_edge_id(edge_param.edge_id)
            if edge_param.edge_id is not None
            else None
        )
        clean_type = validate_edge_type(edge_param.type)
        clean_source_id = validate_related_node_id(
            edge_param.source_id, field_name="Source"
        )
        clean_target_id = validate_related_node_id(
            edge_param.target_id, field_name="Target"
        )
        clean_schema_name = normalize_optional_text(edge_param.schema_name)
        normalized_tags = normalize_tag_list(edge_param.tags)
        validate_json_object(edge_param.data, object_name="Edge data")

        edge_upserts.append(
            EdgeUpsert(
                id=clean_edge_id,
                type=clean_type,
                source_id=clean_source_id,
                target_id=clean_target_id,
                schema_name=clean_schema_name,
                data=edge_param.data,
                tags=normalized_tags,
            )
        )

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="manage_edges",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        try:
            created_edges = await db.set_edges(edge_upserts)
        except IntegrityError as exc:
            raise GraphContentValidationError(
                "Source and target nodes must exist before creating an edge."
            ) from exc
        except (SchemaNotFoundError, SchemaValidationError, ValueError) as exc:
            raise GraphContentValidationError(str(exc)) from exc

        return [
            GraphEdgeDetail(edge_record=serialize_edge_record(edge))
            for edge in created_edges
        ]
    finally:
        await db.sqla_engine.dispose()


async def update_graph_edges(
    self,
    *,
    graph_id: str,
    updates: list[GraphEdgeUpdateParam],
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphEdgeDetail]:
    """Update multiple edges in a managed graph (atomic bulk)."""
    if not updates:
        raise GraphContentValidationError("At least one edge is required.")

    edge_ids = [u.edge_id for u in updates]
    if len(edge_ids) != len(set(edge_ids)):
        duplicates = [eid for eid in edge_ids if edge_ids.count(eid) > 1]
        raise GraphContentValidationError(
            f"Duplicate edge ids provided: {set(duplicates)}"
        )

    clean_edge_ids = [validate_edge_id(u.edge_id) for u in updates]

    # Validate fields up-front so failures happen before any writes.
    for update_param in updates:
        if update_param.type is not None:
            validate_edge_type(update_param.type)
        if update_param.source_id is not None:
            validate_related_node_id(
                update_param.source_id, field_name="Source"
            )
        if update_param.target_id is not None:
            validate_related_node_id(
                update_param.target_id, field_name="Target"
            )
        if update_param.data is not None:
            validate_json_object(
                update_param.data, object_name="Edge data"
            )

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="manage_edges",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        try:
            existing_edges = await db.get_edges(clean_edge_ids)
        except ValueError as e:
            if "edge ids not found" in str(e).lower():
                raise GraphContentNotFoundError(
                    "One or more edges were not found."
                ) from e
            raise

        existing_by_id = {edge.id: edge for edge in existing_edges}

        edge_upserts: list[EdgeUpsert] = []
        for update_param, clean_edge_id in zip(updates, clean_edge_ids):
            existing = existing_by_id[clean_edge_id]

            type_ = (
                validate_edge_type(update_param.type)
                if update_param.type is not None
                else existing.type
            )
            source_id_ = (
                validate_related_node_id(
                    update_param.source_id, field_name="Source"
                )
                if update_param.source_id is not None
                else existing.source_id
            )
            target_id_ = (
                validate_related_node_id(
                    update_param.target_id, field_name="Target"
                )
                if update_param.target_id is not None
                else existing.target_id
            )
            schema_name_ = (
                normalize_optional_text(update_param.schema_name)
                if update_param.schema_name is not None
                else existing.schema_name
            )
            data_ = (
                update_param.data
                if update_param.data is not None
                else existing.data
            )
            tags_ = (
                normalize_tag_list(update_param.tags)
                if update_param.tags is not None
                else (existing.tags or [])
            )

            edge_upserts.append(
                EdgeUpsert(
                    id=clean_edge_id,
                    type=type_,
                    source_id=source_id_,
                    target_id=target_id_,
                    schema_name=schema_name_,
                    data=data_,
                    tags=tags_,
                )
            )

        try:
            updated_edges = await db.set_edges(edge_upserts)
        except IntegrityError as exc:
            raise GraphContentValidationError(
                "Source and target nodes must exist before updating an edge."
            ) from exc
        except (SchemaNotFoundError, SchemaValidationError, ValueError) as exc:
            raise GraphContentValidationError(str(exc)) from exc

        return [
            GraphEdgeDetail(edge_record=serialize_edge_record(edge))
            for edge in updated_edges
        ]
    finally:
        await db.sqla_engine.dispose()


async def delete_graph_edges(
    self,
    *,
    graph_id: str,
    edge_ids: list[str],
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphEdgeDeleteResult]:
    """Delete multiple edges from a managed graph (atomic bulk)."""
    if not edge_ids:
        raise GraphContentValidationError("At least one edge id is required.")

    if len(edge_ids) != len(set(edge_ids)):
        duplicates = [eid for eid in edge_ids if edge_ids.count(eid) > 1]
        raise GraphContentValidationError(
            f"Duplicate edge ids provided: {set(duplicates)}"
        )

    clean_edge_ids = [validate_edge_id(eid) for eid in edge_ids]

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="manage_edges",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        try:
            edges = await db.get_edges(clean_edge_ids)
        except ValueError as e:
            if "edge ids not found" in str(e).lower():
                raise GraphContentNotFoundError(
                    "One or more edges were not found."
                ) from e
            raise

        try:
            await db.delete_edges(clean_edge_ids)
        except ValueError as e:
            if "edge ids not found" in str(e).lower():
                raise GraphContentNotFoundError(
                    "One or more edges were not found."
                ) from e
            raise

        return [GraphEdgeDeleteResult(id=edge_id) for edge_id in clean_edge_ids]
    finally:
        await db.sqla_engine.dispose()
