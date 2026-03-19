"""Node operations for graph-content service."""

from __future__ import annotations

import base64

from gpdb import (
    Filter,
    FilterGroup,
    GPGraph,
    NodeUpsert,
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
    GraphNodeCreateParam,
    GraphNodeDeleteBlockers,
    GraphNodeDeleteResult,
    GraphNodeDetail,
    GraphNodeFilters,
    GraphNodeList,
    GraphNodePayload,
    GraphNodePayloadSetParam,
    GraphNodeRecord,
    GraphNodeUpdateParam,
)
from gpdb.admin.graph_content._helpers import (
    build_node_filter,
    build_node_payload_filename,
    format_node_delete_blocker_message,
    inspect_node_delete_blockers,
    normalize_optional_text,
    normalize_tag_list,
    open_graph,
    parse_node_sort,
    require_admin_store,
    serialize_node_record,
    validate_json_object,
    validate_node_id,
    validate_node_type,
    validate_page_limit,
    validate_page_offset,
)


async def list_graph_nodes(
    self,
    *,
    graph_id: str,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
    type: str | None = None,
    parent_id: str | None = None,
    filter_dsl: str | None = None,
    limit: int = 50,
    offset: int = 0,
    sort: str = "created_at_desc",
) -> GraphNodeList:
    """Return paginated node records for one managed graph."""
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
            filter_value = build_node_filter(
                type=type,
                parent_id=parent_id,
            )
        query = SearchQuery(
            filter=filter_value,
            sort=[parse_node_sort(sort)],
            limit=validate_page_limit(limit),
            offset=validate_page_offset(offset),
        )
        page = await db.search_nodes(query)
        return GraphNodeList(
            items=[serialize_node_record(item) for item in page.items],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
            filters=GraphNodeFilters(
                type=normalize_optional_text(type),
                parent_id=normalize_optional_text(parent_id),
                filter_dsl=clean_filter_dsl,
                sort=sort,
            ),
        )
    finally:
        await db.sqla_engine.dispose()


async def get_graph_nodes(
    self,
    *,
    graph_id: str,
    node_ids: list[str],
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphNodeDetail]:
    """Return multiple graph nodes plus metadata."""
    if not node_ids:
        raise GraphContentValidationError(
            "At least one node id is required."
        )

    # Reject duplicate ids
    if len(node_ids) != len(set(node_ids)):
        duplicates = [nid for nid in node_ids if node_ids.count(nid) > 1]
        raise GraphContentValidationError(
            f"Duplicate node ids provided: {set(duplicates)}"
        )

    clean_node_ids = [validate_node_id(nid) for nid in node_ids]

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
            nodes = await db.get_nodes(clean_node_ids)
        except ValueError as e:
            if "Node ids not found" in str(e):
                raise GraphContentNotFoundError(
                    "One or more nodes were not found."
                ) from e
            raise

        results = []
        for node in nodes:
            results.append(
                GraphNodeDetail(
                    node_record=serialize_node_record(node),
                    delete_blockers=await inspect_node_delete_blockers(
                        db, node.id
                    ),
                )
            )
        return results
    finally:
        await db.sqla_engine.dispose()


async def create_graph_nodes(
    self,
    *,
    graph_id: str,
    nodes: list[GraphNodeCreateParam],
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphNodeDetail]:
    """Create multiple nodes in a managed graph.

    This operation is atomic: either all nodes are created, or none are.
    """
    if not nodes:
        raise GraphContentValidationError("At least one node is required.")

    node_ids = [n.node_id for n in nodes if n.node_id is not None]
    if len(node_ids) != len(set(node_ids)):
        duplicates = [nid for nid in node_ids if node_ids.count(nid) > 1]
        raise GraphContentValidationError(
            f"Duplicate node ids provided: {set(duplicates)}"
        )

    node_upserts: list[NodeUpsert] = []
    for node_param in nodes:
        clean_type = validate_node_type(node_param.type)
        clean_name = normalize_optional_text(node_param.name)
        clean_owner_id = normalize_optional_text(node_param.owner_id)
        clean_parent_id = normalize_optional_text(node_param.parent_id)
        normalized_tags = normalize_tag_list(node_param.tags)
        clean_payload_mime = normalize_optional_text(node_param.payload_mime)
        clean_payload_filename = normalize_optional_text(
            node_param.payload_filename
        )
        validate_json_object(node_param.data, object_name="Node data")

        clean_node_id = (
            validate_node_id(node_param.node_id)
            if node_param.node_id is not None
            else None
        )
        payload = (
            base64.b64decode(node_param.payload_base64)
            if node_param.payload_base64 is not None
            else None
        )

        node_upserts.append(
            NodeUpsert(
                id=clean_node_id,
                type=clean_type,
                name=clean_name,
                owner_id=clean_owner_id,
                parent_id=clean_parent_id,
                data=node_param.data,
                tags=normalized_tags,
                payload=payload,
                payload_mime=clean_payload_mime,
                payload_filename=clean_payload_filename,
            )
        )

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="manage_nodes",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        try:
            created_nodes = await db.set_nodes(node_upserts)
        except (SchemaNotFoundError, SchemaValidationError, ValueError) as exc:
            raise GraphContentValidationError(str(exc)) from exc
        return [
            GraphNodeDetail(
                node_record=serialize_node_record(node),
                delete_blockers=GraphNodeDeleteBlockers(),
            )
            for node in created_nodes
        ]
    finally:
        await db.sqla_engine.dispose()


async def update_graph_nodes(
    self,
    *,
    graph_id: str,
    updates: list[GraphNodeUpdateParam],
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphNodeDetail]:
    """Update multiple nodes in a managed graph.

    This operation is atomic: either all nodes are updated, or none are.
    """
    if not updates:
        raise GraphContentValidationError("At least one node is required.")

    node_ids = [u.node_id for u in updates if u.node_id is not None]
    if len(node_ids) != len(set(node_ids)):
        duplicates = [nid for nid in node_ids if node_ids.count(nid) > 1]
        raise GraphContentValidationError(
            f"Duplicate node ids provided: {set(duplicates)}"
        )

    clean_node_ids = [validate_node_id(u.node_id) for u in updates if u.node_id is not None]

    # Validate fields up-front so failures happen before any writes.
    for update_param in updates:
        if update_param.node_id is None:
            continue
        payload = (
            base64.b64decode(update_param.payload_base64)
            if update_param.payload_base64 is not None
            else None
        )
        if payload is not None and update_param.clear_payload:
            raise GraphContentValidationError(
                "Provide either payload bytes or clear_payload, not both."
            )
        if update_param.type is not None:
            validate_node_type(update_param.type)
        if update_param.data is not None:
            validate_json_object(
                update_param.data, object_name="Node data"
            )

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="manage_nodes",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        try:
            existing_nodes = await db.get_nodes(clean_node_ids)
        except ValueError as e:
            if "Node ids not found" in str(e):
                raise GraphContentNotFoundError(
                    "One or more nodes were not found."
                ) from e
            raise

        existing_by_id = {node.id: node for node in existing_nodes}

        node_upserts: list[NodeUpsert] = []
        clear_payload_ids: list[str] = []
        for update_param, clean_node_id in zip(updates, clean_node_ids):
            existing = existing_by_id[clean_node_id]

            type_ = (
                validate_node_type(update_param.type)
                if update_param.type is not None
                else existing.type
            )
            name_ = (
                normalize_optional_text(update_param.name)
                if update_param.name is not None
                else existing.name
            )
            owner_id_ = (
                normalize_optional_text(update_param.owner_id)
                if update_param.owner_id is not None
                else existing.owner_id
            )
            parent_id_ = (
                normalize_optional_text(update_param.parent_id)
                if update_param.parent_id is not None
                else existing.parent_id
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

            payload = (
                base64.b64decode(update_param.payload_base64)
                if update_param.payload_base64 is not None
                else None
            )
            if payload is not None:
                payload_ = payload
                payload_mime_ = (
                    normalize_optional_text(update_param.payload_mime)
                    if update_param.payload_mime is not None
                    else (existing.payload_mime or None)
                )
                payload_filename_ = (
                    normalize_optional_text(update_param.payload_filename)
                    if update_param.payload_filename is not None
                    else (existing.payload_filename or None)
                )
            else:
                # None means do not change payload bytes; core assigns payload only when dto.payload is not None.
                payload_ = None
                payload_mime_ = (
                    normalize_optional_text(update_param.payload_mime)
                    if update_param.payload_mime is not None
                    else existing.payload_mime
                )
                payload_filename_ = (
                    normalize_optional_text(update_param.payload_filename)
                    if update_param.payload_filename is not None
                    else existing.payload_filename
                )

            node_upserts.append(
                NodeUpsert(
                    id=clean_node_id,
                    type=type_,
                    name=name_,
                    owner_id=owner_id_,
                    parent_id=parent_id_,
                    data=data_,
                    tags=tags_,
                    payload=payload_,
                    payload_mime=payload_mime_,
                    payload_filename=payload_filename_,
                )
            )
            if update_param.clear_payload:
                clear_payload_ids.append(clean_node_id)

        try:
            node_by_id: dict[str, object] = {}
            async with db.transaction():
                node_list = await db.set_nodes(node_upserts)
                node_by_id = {node.id: node for node in node_list}
                for clean_node_id in clear_payload_ids:
                    node_by_id[clean_node_id] = (
                        await db.clear_node_payload(clean_node_id)
                    )
        except (SchemaNotFoundError, SchemaValidationError, ValueError) as exc:
            raise GraphContentValidationError(str(exc)) from exc

        return [
            GraphNodeDetail(
                node_record=serialize_node_record(node_by_id[clean_node_id]),
                delete_blockers=await inspect_node_delete_blockers(
                    db, clean_node_id
                ),
            )
            for clean_node_id in clean_node_ids
        ]
    finally:
        await db.sqla_engine.dispose()


async def delete_graph_nodes(
    self,
    *,
    graph_id: str,
    node_ids: list[str],
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphNodeDeleteResult]:
    """Delete multiple graph nodes when they have no child/edge blockers."""
    if not node_ids:
        raise GraphContentValidationError("At least one node id is required.")

    if len(node_ids) != len(set(node_ids)):
        duplicates = [nid for nid in node_ids if node_ids.count(nid) > 1]
        raise GraphContentValidationError(
            f"Duplicate node ids provided: {set(duplicates)}"
        )

    clean_node_ids = [validate_node_id(nid) for nid in node_ids]

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="manage_nodes",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        try:
            nodes = await db.get_nodes(clean_node_ids)
        except ValueError as e:
            if "Node ids not found" in str(e):
                raise GraphContentNotFoundError(
                    "One or more nodes were not found."
                ) from e
            raise

        blockers_by_id: dict[str, GraphNodeDeleteBlockers] = {}
        in_use_messages: list[str] = []

        for clean_node_id in clean_node_ids:
            blockers = await inspect_node_delete_blockers(
                db, clean_node_id
            )
            blockers_by_id[clean_node_id] = blockers
            if blockers.child_count or blockers.incident_edge_count:
                in_use_messages.append(
                    format_node_delete_blocker_message(
                        clean_node_id, blockers
                    )
                )

        if in_use_messages:
            raise GraphContentConflictError("; ".join(in_use_messages))
        try:
            await db.delete_nodes(clean_node_ids)
        except IntegrityError as exc:
            # Concurrency: recompute blocker info for a helpful message.
            recomputed_messages: list[str] = []
            for clean_node_id in clean_node_ids:
                blockers = await inspect_node_delete_blockers(
                    db, clean_node_id
                )
                if blockers.child_count or blockers.incident_edge_count:
                    recomputed_messages.append(
                        format_node_delete_blocker_message(
                            clean_node_id, blockers
                        )
                    )
            if recomputed_messages:
                raise GraphContentConflictError(
                    "; ".join(recomputed_messages)
                ) from exc
            raise

        return [GraphNodeDeleteResult(id=node_id) for node_id in clean_node_ids]
    finally:
        await db.sqla_engine.dispose()


async def set_graph_node_payloads(
    self,
    *,
    graph_id: str,
    payloads: list[GraphNodePayloadSetParam],
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphNodeDetail]:
    """Upload or replace node payloads in an atomic bulk request."""
    if not payloads:
        raise GraphContentValidationError("At least one payload is required.")

    node_ids = [p.node_id for p in payloads]
    if len(node_ids) != len(set(node_ids)):
        duplicates = [nid for nid in node_ids if node_ids.count(nid) > 1]
        raise GraphContentValidationError(
            f"Duplicate node ids provided: {set(duplicates)}"
        )

    clean_node_ids = [validate_node_id(nid) for nid in node_ids]

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="manage_nodes",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        # Preflight: ensure all nodes exist before writing anything.
        try:
            await db.get_nodes(clean_node_ids)
        except ValueError as exc:
            raise GraphContentNotFoundError(
                "One or more nodes were not found."
            ) from exc

        updated_nodes: list[object] = []
        async with db.transaction():
            for payload_param, clean_node_id in zip(
                payloads, clean_node_ids
            ):
                clean_mime = normalize_optional_text(payload_param.mime)
                clean_payload_filename = normalize_optional_text(
                    payload_param.payload_filename
                )
                try:
                    node = await db.set_node_payload(
                        clean_node_id,
                        payload_param.payload,
                        mime=clean_mime,
                        filename=clean_payload_filename,
                    )
                except ValueError as exc:
                    raise GraphContentNotFoundError(
                        f"Node '{clean_node_id}' was not found."
                    ) from exc
                updated_nodes.append(node)

        results: list[GraphNodeDetail] = []
        for node, clean_node_id in zip(updated_nodes, clean_node_ids):
            results.append(
                GraphNodeDetail(
                    node_record=serialize_node_record(node),
                    delete_blockers=await inspect_node_delete_blockers(
                        db, clean_node_id
                    ),
                )
            )
        return results
    finally:
        await db.sqla_engine.dispose()


async def get_graph_node_payloads(
    self,
    *,
    graph_id: str,
    node_ids: list[str],
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphNodePayload]:
    """Return multiple node payloads encoded for JSON-based admin surfaces."""
    if not node_ids:
        raise GraphContentValidationError(
            "At least one node id is required."
        )

    if len(node_ids) != len(set(node_ids)):
        duplicates = [nid for nid in node_ids if node_ids.count(nid) > 1]
        raise GraphContentValidationError(
            f"Duplicate node ids provided: {set(duplicates)}"
        )

    clean_node_ids = [validate_node_id(nid) for nid in node_ids]

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
            nodes = await db.get_node_payloads(clean_node_ids)
        except ValueError as exc:
            raise GraphContentNotFoundError(
                "One or more nodes were not found."
            ) from exc

        node_by_id = {node.id: node for node in nodes}
        results: list[GraphNodePayload] = []
        for clean_node_id in clean_node_ids:
            node = node_by_id[clean_node_id]
            if node.payload is None:
                raise GraphContentConflictError(
                    f"Node '{clean_node_id}' does not have a payload."
                )
            node_record = serialize_node_record(node)
            results.append(
                GraphNodePayload(
                    node_record=node_record,
                    payload_base64=base64.b64encode(node.payload).decode(
                        "ascii"
                    ),
                    filename=build_node_payload_filename(
                        node_record
                    ),
                )
            )
        return results
    finally:
        await db.sqla_engine.dispose()
