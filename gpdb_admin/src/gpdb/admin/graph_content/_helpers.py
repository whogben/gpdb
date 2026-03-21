"""Helper functions for graph-content operations."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from gpdb import Filter, FilterGroup, GPGraph, Logic, SearchQuery, Sort
from gpdb.admin.instances import build_postgres_url
from gpdb.admin.store import AdminStore, AdminUser, ManagedGraph, ManagedInstance

from gpdb.admin.graph_content.exceptions import (
    GraphContentNotFoundError,
    GraphContentNotReadyError,
    GraphContentPermissionError,
    GraphContentValidationError,
)
from gpdb.admin.graph_content.models import (
    GraphEdgeRecord,
    GraphNodeDeleteBlockers,
    GraphNodeRecord,
    GraphSchemaUsage,
    InstanceRecord,
)
from gpdb.svg_sanitizer import normalize_svg_icon_for_display


def serialize_graph(graph: ManagedGraph) -> dict[str, object]:
    """Project managed graph metadata into a stable admin response shape."""
    return {
        "id": graph.id,
        "instance_id": graph.instance_id,
        "instance_slug": graph.instance_slug,
        "instance_display_name": graph.instance_display_name,
        "display_name": graph.display_name,
        "table_prefix": graph.table_prefix,
        "table_prefix_label": graph.table_prefix or "(default)",
        "status": graph.status,
        "status_message": graph.status_message,
        "last_checked_at": graph.last_checked_at,
        "exists_in_instance": graph.exists_in_instance,
        "source": graph.source,
        "is_default": graph.is_default,
    }


def serialize_instance(instance: ManagedInstance) -> dict[str, object]:
    """Project managed instance metadata into a stable admin response shape."""
    return {
        "id": instance.id,
        "slug": instance.slug,
        "display_name": instance.display_name,
        "description": instance.description,
        "mode": instance.mode,
        "is_builtin": instance.is_builtin,
        "is_default": instance.is_default,
        "is_active": instance.is_active,
        "connection_kind": instance.connection_kind,
        "host": instance.host,
        "port": instance.port,
        "database": instance.database,
        "username": instance.username,
        "status": instance.status,
        "status_message": instance.status_message,
        "last_checked_at": instance.last_checked_at,
    }


def serialize_instance_record(instance: ManagedInstance) -> InstanceRecord:
    """Project one managed instance record into a stable admin response."""
    return InstanceRecord(
        id=instance.id,
        slug=instance.slug,
        display_name=instance.display_name,
        description=instance.description,
        mode=instance.mode,
        is_builtin=instance.is_builtin,
        is_default=instance.is_default,
        is_active=instance.is_active,
        connection_kind=instance.connection_kind,
        host=instance.host,
        port=instance.port,
        database=instance.database,
        username=instance.username,
        status=instance.status,
        status_message=instance.status_message,
        last_checked_at=instance.last_checked_at,
    )


def serialize_graph_record(graph: ManagedGraph) -> dict[str, object]:
    """Project one managed graph record into a stable admin response."""
    return {
        "id": graph.id,
        "instance_id": graph.instance_id,
        "instance_slug": graph.instance_slug,
        "instance_display_name": graph.instance_display_name,
        "display_name": graph.display_name,
        "table_prefix": graph.table_prefix,
        "status": graph.status,
        "status_message": graph.status_message,
        "last_checked_at": graph.last_checked_at,
        "exists_in_instance": graph.exists_in_instance,
        "source": graph.source,
        "is_default": graph.is_default,
    }


def serialize_schema_record(
    schema: Any,
    *,
    include_json_schema: bool,
) -> dict[str, object]:
    """Project one core schema record into a stable admin response.

    ``effective_json_schema`` is omitted from this dict when unset so callers
    constructing ``GraphSchemaRecord`` get a missing key (model default ``None``)
    rather than pushing ``None`` through explicitly; JSON tools may still emit
    ``null`` for that field on the wire.
    """
    result = {
        "name": str(schema.name),
        "kind": schema_kind_from_record(schema),
        "version": str(schema.version),
        "json_schema": schema.json_schema if include_json_schema else None,
        "alias": getattr(schema, "alias", None),
        "svg_icon": normalize_svg_icon_for_display(getattr(schema, "svg_icon", None)),
        "extends": list(getattr(schema, "extends", []) or []),
    }
    
    # Include effective_json_schema only when non-null
    effective_json_schema = getattr(schema, "effective_json_schema", None)
    if effective_json_schema is not None:
        result["effective_json_schema"] = effective_json_schema
    
    return result


def serialize_node_record(node: Any) -> GraphNodeRecord:
    """Project one core node record into a stable admin response."""
    return GraphNodeRecord(
        id=str(node.id),
        type=str(node.type),
        name=node.name,
        owner_id=node.owner_id,
        parent_id=node.parent_id,
        data=dict(node.data or {}),
        tags=list(node.tags or []),
        created_at=node.created_at.isoformat(),
        updated_at=node.updated_at.isoformat(),
        version=int(node.version),
        payload_size=int(node.payload_size or 0),
        payload_hash=node.payload_hash,
        payload_mime=node.payload_mime,
        payload_filename=node.payload_filename,
        has_payload=bool(node.payload_size or node.payload_hash),
    )


def serialize_edge_record(edge: Any) -> GraphEdgeRecord:
    """Project one core edge record into a stable admin response."""
    return GraphEdgeRecord(
        id=str(edge.id),
        type=str(edge.type),
        source_id=str(edge.source_id),
        target_id=str(edge.target_id),
        data=dict(edge.data or {}),
        tags=list(edge.tags or []),
        created_at=edge.created_at.isoformat(),
        updated_at=edge.updated_at.isoformat(),
        version=int(edge.version),
    )


def validate_schema_name(name: str) -> str:
    """Validate and clean a schema name."""
    clean_name = name.strip()
    if not clean_name:
        raise GraphContentValidationError("Schema name is required.")
    return clean_name


def validate_json_schema(json_schema: dict[str, Any]) -> None:
    """Validate that a JSON schema is a dict."""
    if not isinstance(json_schema, dict):
        raise GraphContentValidationError("Schema JSON must be a JSON object.")


def validate_schema_kind(kind: str) -> str:
    """Validate and normalize a schema kind."""
    clean_kind = kind.strip().lower()
    if clean_kind not in {"node", "edge"}:
        raise GraphContentValidationError(
            "Schema kind must be either 'node' or 'edge'."
        )
    return clean_kind


def normalize_optional_schema_kind(kind: str | None) -> str | None:
    """Normalize an optional schema kind."""
    if kind is None:
        return None
    clean_kind = kind.strip()
    if not clean_kind:
        return None
    return validate_schema_kind(clean_kind)


def validate_json_object(
    value: dict[str, Any],
    *,
    object_name: str,
) -> None:
    """Validate that a value is a JSON object."""
    if not isinstance(value, dict):
        raise GraphContentValidationError(f"{object_name} must be a JSON object.")


def validate_node_type(value: str) -> str:
    """Validate and clean a node type."""
    clean_value = value.strip()
    if not clean_value:
        raise GraphContentValidationError("Node type is required.")
    return clean_value


def validate_node_id(value: str) -> str:
    """Validate and clean a node ID."""
    clean_value = value.strip()
    if not clean_value:
        raise GraphContentValidationError("Node id is required.")
    return clean_value


def validate_edge_id(value: str) -> str:
    """Validate and clean an edge ID."""
    clean_value = value.strip()
    if not clean_value:
        raise GraphContentValidationError("Edge id is required.")
    return clean_value


def validate_edge_type(value: str) -> str:
    """Validate and clean an edge type."""
    clean_value = value.strip()
    if not clean_value:
        raise GraphContentValidationError("Edge type is required.")
    return clean_value


def validate_related_node_id(value: str, *, field_name: str) -> str:
    """Validate and clean a related node ID."""
    clean_value = value.strip()
    if not clean_value:
        raise GraphContentValidationError(f"{field_name} node id is required.")
    return clean_value


def normalize_optional_text(value: str | None) -> str | None:
    """Normalize optional text, returning None if empty."""
    if value is None:
        return None
    clean_value = value.strip()
    return clean_value or None


def schema_kind_from_record(schema: Any) -> str:
    """Extract schema kind from a schema record."""
    kind = schema.kind
    if kind not in {"node", "edge"}:
        raise GraphContentValidationError(
            f"Schema '{schema.name}' is missing valid kind metadata."
        )
    return str(kind)


def normalize_tag_list(tags: list[str] | None) -> list[str]:
    """Normalize a list of tags."""
    if not tags:
        return []
    normalized: list[str] = []
    for item in tags:
        clean_item = str(item).strip()
        if clean_item:
            normalized.append(clean_item)
    return normalized


def validate_page_limit(limit: int) -> int:
    """Validate a page limit."""
    if limit < 1:
        raise GraphContentValidationError("Limit must be at least 1.")
    if limit > 200:
        raise GraphContentValidationError("Limit cannot be greater than 200.")
    return limit


def validate_page_offset(offset: int) -> int:
    """Validate a page offset."""
    if offset < 0:
        raise GraphContentValidationError("Offset cannot be negative.")
    return offset


def parse_node_sort(sort: str) -> Sort:
    """Parse a node sort string into a Sort object."""
    allowed = {
        "created_at_desc": Sort(field="created_at", desc=True),
        "created_at_asc": Sort(field="created_at", desc=False),
        "updated_at_desc": Sort(field="updated_at", desc=True),
        "updated_at_asc": Sort(field="updated_at", desc=False),
        "name_asc": Sort(field="name", desc=False),
        "name_desc": Sort(field="name", desc=True),
    }
    if sort not in allowed:
        raise GraphContentValidationError(
            "Sort must be one of: created_at_desc, created_at_asc, "
            "updated_at_desc, updated_at_asc, name_asc, name_desc."
        )
    return allowed[sort]


def parse_edge_sort(sort: str) -> Sort:
    """Parse an edge sort string into a Sort object."""
    allowed = {
        "created_at_desc": Sort(field="created_at", desc=True),
        "created_at_asc": Sort(field="created_at", desc=False),
        "updated_at_desc": Sort(field="updated_at", desc=True),
        "updated_at_asc": Sort(field="updated_at", desc=False),
        "type_asc": Sort(field="type", desc=False),
        "type_desc": Sort(field="type", desc=True),
    }
    if sort not in allowed:
        raise GraphContentValidationError(
            "Sort must be one of: created_at_desc, created_at_asc, "
            "updated_at_desc, updated_at_asc, type_asc, type_desc."
        )
    return allowed[sort]


def build_node_filter(
    *,
    type: str | None,
    parent_id: str | None,
) -> FilterGroup | Filter | None:
    """Build a node filter from optional parameters."""
    filters: list[Filter] = []
    clean_type = normalize_optional_text(type)
    clean_parent_id = normalize_optional_text(parent_id)
    if clean_type:
        filters.append(Filter(field="type", value=clean_type))
    if clean_parent_id:
        filters.append(Filter(field="parent_id", value=clean_parent_id))
    if not filters:
        return None
    if len(filters) == 1:
        return filters[0]
    return FilterGroup(logic=Logic.AND, filters=filters)


def build_edge_filter(
    *,
    type: str | None,
    source_id: str | None,
    target_id: str | None,
) -> FilterGroup | Filter | None:
    """Build an edge filter from optional parameters."""
    filters: list[Filter] = []
    clean_type = normalize_optional_text(type)
    clean_source_id = normalize_optional_text(source_id)
    clean_target_id = normalize_optional_text(target_id)
    if clean_type:
        filters.append(Filter(field="type", value=clean_type))
    if clean_source_id:
        filters.append(Filter(field="source_id", value=clean_source_id))
    if clean_target_id:
        filters.append(Filter(field="target_id", value=clean_target_id))
    if not filters:
        return None
    if len(filters) == 1:
        return filters[0]
    return FilterGroup(logic=Logic.AND, filters=filters)


def format_schema_delete_blocker_message(
    schema_name: str,
    usage: GraphSchemaUsage,
) -> str:
    """Format a schema delete blocker message."""
    blockers: list[str] = []
    if usage.node_count:
        blockers.append(
            f"{usage.node_count} node{'s' if usage.node_count != 1 else ''}"
        )
    if usage.edge_count:
        blockers.append(
            f"{usage.edge_count} edge{'s' if usage.edge_count != 1 else ''}"
        )
    if not blockers:
        return f"Schema '{schema_name}' cannot be deleted."
    joined = " and ".join(blockers)
    return f"Schema '{schema_name}' cannot be deleted because it is still referenced by {joined}."


def format_node_delete_blocker_message(
    node_id: str,
    blockers: GraphNodeDeleteBlockers,
) -> str:
    """Format a node delete blocker message."""
    parts: list[str] = []
    if blockers.child_count:
        parts.append(
            f"{blockers.child_count} child node{'s' if blockers.child_count != 1 else ''}"
        )
    if blockers.incident_edge_count:
        parts.append(
            f"{blockers.incident_edge_count} incident edge{'s' if blockers.incident_edge_count != 1 else ''}"
        )
    if not parts:
        return f"Node '{node_id}' cannot be deleted."
    return (
        f"Node '{node_id}' cannot be deleted because it still has "
        f"{' and '.join(parts)}."
    )


def build_node_payload_filename(node: GraphNodeRecord) -> str:
    """Build a safe filename for a node payload."""
    base_name = (node.payload_filename or node.name or node.id).strip() or node.id
    safe_name = base_name.replace("/", "_")
    return safe_name


async def inspect_schema_usage(
    db: GPGraph,
    schema_name: str,
    *,
    sample_limit: int = 3,
) -> GraphSchemaUsage:
    """Inspect schema usage in a graph."""
    node_page = await db.search_nodes(
        SearchQuery(
            filter=Filter(field="type", value=schema_name),
            limit=max(1, sample_limit),
        )
    )
    edge_page = await db.search_edges(
        SearchQuery(
            filter=Filter(field="type", value=schema_name),
            limit=max(1, sample_limit),
        )
    )
    usage = GraphSchemaUsage(
        node_count=node_page.total,
        edge_count=edge_page.total,
    )
    if sample_limit > 0:
        usage.sample_node_ids = [item.id for item in node_page.items[:sample_limit]]
        usage.sample_edge_ids = [item.id for item in edge_page.items[:sample_limit]]
    return usage


async def inspect_node_delete_blockers(
    db: GPGraph,
    node_id: str,
    *,
    sample_limit: int = 3,
) -> GraphNodeDeleteBlockers:
    """Inspect blockers for node deletion."""
    child_page = await db.search_nodes(
        SearchQuery(
            filter=Filter(field="parent_id", value=node_id),
            limit=max(1, sample_limit),
        )
    )
    edge_page = await db.search_edges(
        SearchQuery(
            filter=FilterGroup(
                logic=Logic.OR,
                filters=[
                    Filter(field="source_id", value=node_id),
                    Filter(field="target_id", value=node_id),
                ],
            ),
            limit=max(1, sample_limit),
        )
    )
    blockers = GraphNodeDeleteBlockers(
        child_count=child_page.total,
        incident_edge_count=edge_page.total,
    )
    if sample_limit > 0:
        blockers.sample_child_ids = [
            item.id for item in child_page.items[:sample_limit]
        ]
        blockers.sample_edge_ids = [item.id for item in edge_page.items[:sample_limit]]
    blockers.can_delete = not (blockers.child_count or blockers.incident_edge_count)
    return blockers


def authorize_graph_access(
    *,
    instance_id: str,
    table_prefix: str,
    permission_kind: str,
    current_user: AdminUser | None,
    allow_local_system: bool,
) -> None:
    """Authorize graph access for a user."""
    # Keep authorization graph-scoped so future `(instance, prefix, permission)`
    # grants can replace the current owner-only check without rewriting callers.
    _ = (instance_id, table_prefix, permission_kind)
    if allow_local_system:
        return
    if current_user is None or not current_user.is_owner:
        raise GraphContentPermissionError(
            "Only the server owner can access graph content."
        )


async def require_graph(
    graph_id: str,
    admin_store: AdminStore,
) -> ManagedGraph:
    """Require a graph to exist."""
    graph = await admin_store.get_graph_by_id(graph_id)
    if graph is None:
        raise GraphContentNotFoundError("Managed graph was not found.")
    return graph


async def require_instance(
    instance_id: str,
    admin_store: AdminStore,
) -> ManagedInstance:
    """Require an instance to exist."""
    instance = await admin_store.get_instance_by_id(instance_id)
    if instance is None:
        raise GraphContentNotFoundError("Managed instance was not found.")
    return instance


def require_admin_store(admin_store: AdminStore | None) -> AdminStore:
    """Require an admin store to be available."""
    if admin_store is None:
        raise GraphContentNotReadyError("Graph content service is not ready yet.")
    return admin_store


async def open_graph(
    *,
    graph_id: str,
    current_user: AdminUser | None,
    allow_local_system: bool,
    permission_kind: str,
    admin_store: AdminStore,
    captive_url_factory: Callable[[], str] | None,
) -> tuple[ManagedGraph, ManagedInstance, GPGraph]:
    """Open a graph for operations."""
    graph = await require_graph(graph_id, admin_store)
    authorize_graph_access(
        instance_id=graph.instance_id,
        table_prefix=graph.table_prefix,
        permission_kind=permission_kind,
        current_user=current_user,
        allow_local_system=allow_local_system,
    )
    instance = await require_instance(graph.instance_id, admin_store)
    return (
        graph,
        instance,
        GPGraph(
            resolve_instance_url(instance, captive_url_factory),
            table_prefix=graph.table_prefix,
        ),
    )


def resolve_instance_url(
    instance: ManagedInstance,
    captive_url_factory: Callable[[], str] | None,
) -> str:
    """Resolve the connection URL for an instance."""
    if instance.mode == "captive":
        if captive_url_factory is None:
            raise GraphContentNotReadyError("Captive graph access is not ready yet.")
        return captive_url_factory()
    return build_postgres_url(instance)
