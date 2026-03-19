"""Graph operations for the admin store."""

from __future__ import annotations

from datetime import UTC, datetime

from gpdb import Filter, FilterGroup, Logic, NodeRead, NodeUpsert, SearchQuery
from gpdb.admin.store.models import ManagedGraph, ManagedInstance
from gpdb.admin.store.exceptions import GraphAlreadyExistsError
from gpdb.admin.store.instances import get_instance_by_id, list_instances

GRAPH_NODE_TYPE = "graph"
DEFAULT_GRAPH_NODE_NAME = "__default__"


async def list_graphs(store) -> list[ManagedGraph]:
    """Return all managed graph records across all instances."""
    instances = {item.id: item for item in await list_instances(store)}
    nodes = await store._search_nodes(
        filters=[Filter(field="type", value=GRAPH_NODE_TYPE)],
        limit=2000,
    )
    graphs: list[ManagedGraph] = []
    for node in nodes:
        instance = instances.get(node.parent_id or "")
        if instance is None:
            continue
        graphs.append(_managed_graph_from_node(node, instance))
    return sorted(
        graphs,
        key=lambda item: (
            item.display_name.lower(),
            item.instance_display_name.lower(),
            item.table_prefix.lower(),
        ),
    )


async def list_graphs_for_instance(store, instance_id: str) -> list[ManagedGraph]:
    """Return all managed graph records for one instance."""
    instance = await get_instance_by_id(store, instance_id)
    if instance is None:
        return []
    nodes = await store._search_nodes(
        filters=[
            Filter(field="type", value=GRAPH_NODE_TYPE),
            Filter(field="parent_id", value=instance_id),
        ],
        limit=1000,
    )
    graphs = [_managed_graph_from_node(node, instance) for node in nodes]
    return sorted(
        graphs,
        key=lambda item: (
            item.display_name.lower(),
            item.table_prefix.lower(),
        ),
    )


async def get_graph_by_id(store, graph_id: str) -> ManagedGraph | None:
    """Return one managed graph by node id."""
    try:
        nodes = await store.db.get_nodes([graph_id])
        node = nodes[0]
        if node.type != GRAPH_NODE_TYPE or not node.parent_id:
            return None
        instance = await get_instance_by_id(store, node.parent_id)
        if instance is None:
            return None
        return _managed_graph_from_node(node, instance)
    except ValueError:
        return None


async def get_graph_by_scope(
    store,
    instance_id: str,
    table_prefix: str,
) -> ManagedGraph | None:
    """Return one managed graph by its `(instance, table_prefix)` scope."""
    instance = await get_instance_by_id(store, instance_id)
    if instance is None:
        return None
    node = await store._get_node_by_filters(
        [
            Filter(field="type", value=GRAPH_NODE_TYPE),
            Filter(field="parent_id", value=instance_id),
            Filter(field="data.table_prefix", value=table_prefix),
        ]
    )
    if node is None:
        return None
    return _managed_graph_from_node(node, instance)


async def update_graph(
    store,
    *,
    graph_id: str,
    display_name: str | None = None,
) -> ManagedGraph | None:
    """Update one managed graph's display name. Omitted fields are left unchanged."""
    try:
        nodes = await store.db.get_nodes([graph_id])
        node = nodes[0]
        if node.type != GRAPH_NODE_TYPE or not node.parent_id:
            return None
        instance = await get_instance_by_id(store, node.parent_id)
        if instance is None:
            return None

        updated_data = dict(node.data)
        if display_name is not None:
            updated_data["display_name"] = display_name
        updated_list = await store.db.set_nodes(
            [
                NodeUpsert(
                    id=node.id,
                    type=node.type,
                    name=node.name,
                    parent_id=node.parent_id,
                    data=updated_data,
                )
            ]
        )
        updated = updated_list[0]
        return _managed_graph_from_node(updated, instance)
    except ValueError:
        return None


async def delete_graph(store, graph_id: str) -> None:
    """Delete one managed graph metadata node."""
    await store.db.delete_nodes([graph_id])


async def upsert_graph_metadata(
    store,
    *,
    instance_id: str,
    table_prefix: str,
    display_name: str | None = None,
    status: str | None = None,
    status_message: str | None = None,
    exists_in_instance: bool | None = None,
    source: str | None = None,
) -> ManagedGraph:
    """Create or update graph metadata for one `(instance, table_prefix)` scope."""
    instance = await get_instance_by_id(store, instance_id)
    if instance is None:
        raise ValueError("Managed instance was not found")

    existing_node = await store._get_node_by_filters(
        [
            Filter(field="type", value=GRAPH_NODE_TYPE),
            Filter(field="parent_id", value=instance_id),
            Filter(field="data.table_prefix", value=table_prefix),
        ]
    )
    graph_name = _graph_node_name(table_prefix)
    if existing_node is None:
        sibling = await store._get_node_by_filters(
            [
                Filter(field="type", value=GRAPH_NODE_TYPE),
                Filter(field="parent_id", value=instance_id),
                Filter(field="name", value=graph_name),
            ]
        )
        if sibling is not None:
            raise GraphAlreadyExistsError(
                f"Graph '{table_prefix or 'default'}' already exists"
            )

    current_data = dict(existing_node.data) if existing_node else {}
    node_list = await store.db.set_nodes(
        [
            NodeUpsert(
                id=existing_node.id if existing_node else None,
                type=GRAPH_NODE_TYPE,
                name=graph_name,
                parent_id=instance_id,
                data={
                    "table_prefix": table_prefix,
                    "display_name": display_name
                    or current_data.get("display_name")
                    or _default_graph_display_name(table_prefix, instance.display_name),
                    "status": status or current_data.get("status", "checking"),
                    "status_message": (
                        status_message
                        if status_message is not None
                        else current_data.get("status_message")
                    ),
                    "last_checked_at": _timestamp_now(),
                    "exists_in_instance": (
                        exists_in_instance
                        if exists_in_instance is not None
                        else current_data.get("exists_in_instance", False)
                    ),
                    "source": source or current_data.get("source", "discovered"),
                },
            )
        ]
    )
    node = node_list[0]
    return _managed_graph_from_node(node, instance)


async def sync_graph_snapshot(
    store,
    instance_id: str,
    *,
    discovered_prefixes: set[str] | None,
    instance_status: str,
    instance_status_message: str | None,
) -> None:
    """Sync graph metadata against the latest discovery snapshot."""
    instance = await get_instance_by_id(store, instance_id)
    if instance is None:
        return

    existing_graphs = {
        graph.table_prefix: graph
        for graph in await list_graphs_for_instance(store, instance_id)
    }
    if discovered_prefixes is not None:
        for table_prefix in discovered_prefixes:
            if table_prefix in existing_graphs:
                continue
            graph = await upsert_graph_metadata(
                store,
                instance_id=instance_id,
                table_prefix=table_prefix,
                exists_in_instance=True,
                source="discovered",
            )
            existing_graphs[table_prefix] = graph

    for table_prefix, graph in existing_graphs.items():
        exists_in_instance = (
            graph.exists_in_instance
            if discovered_prefixes is None
            else table_prefix in discovered_prefixes
        )
        if instance_status == "online":
            status = "ready" if exists_in_instance else "missing_tables"
        else:
            status = instance_status

        await upsert_graph_metadata(
            store,
            instance_id=instance_id,
            table_prefix=table_prefix,
            display_name=graph.display_name,
            status=status,
            status_message=instance_status_message,
            exists_in_instance=exists_in_instance,
            source=graph.source,
        )


def _managed_graph_from_node(node: NodeRead, instance: ManagedInstance) -> ManagedGraph:
    """Project a GPDB node into a managed graph view."""
    table_prefix = str(node.data.get("table_prefix") or "")
    return ManagedGraph(
        id=node.id,
        instance_id=instance.id,
        instance_slug=instance.slug,
        instance_display_name=instance.display_name,
        display_name=str(
            node.data.get("display_name")
            or _default_graph_display_name(table_prefix, instance.display_name)
        ),
        table_prefix=table_prefix,
        status=str(node.data.get("status") or "checking"),
        status_message=_optional_string(node.data.get("status_message")),
        last_checked_at=_optional_string(node.data.get("last_checked_at")),
        exists_in_instance=bool(node.data.get("exists_in_instance", False)),
        source=str(node.data.get("source") or "discovered"),
        is_default=table_prefix == "",
    )


def _default_graph_display_name(table_prefix: str, instance_display_name: str) -> str:
    """Return a default display name for one graph scope."""
    if not table_prefix:
        return f"{instance_display_name} default graph"
    return table_prefix


def _graph_node_name(table_prefix: str) -> str:
    """Return the node name used for one graph metadata record."""
    if not table_prefix:
        return DEFAULT_GRAPH_NODE_NAME
    return table_prefix


def _optional_string(value: object) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _timestamp_now() -> str:
    """Return the current UTC timestamp for admin metadata writes."""
    return datetime.now(tz=UTC).isoformat()
