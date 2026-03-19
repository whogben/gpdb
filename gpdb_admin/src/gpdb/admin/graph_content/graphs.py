"""Graph operations for graph-content service."""

from __future__ import annotations

from gpdb.admin.store import AdminStore, AdminUser, GraphAlreadyExistsError

from gpdb.admin.graph_content.exceptions import (
    GraphContentConflictError,
    GraphContentNotFoundError,
    GraphContentNotReadyError,
)
from gpdb.admin.graph_content.models import GraphDetail, GraphList
from gpdb.admin.graph_content._helpers import (
    serialize_graph,
    serialize_graph_record,
    require_admin_store,
)


async def list_graphs(
    self,
    *,
    instance_id: str | None = None,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> GraphList:
    """Return all managed graphs for the authenticated caller."""
    _ = (current_user, allow_local_system)
    admin_store = require_admin_store(self._admin_store)
    if instance_id is not None:
        graphs = await admin_store.list_graphs_for_instance(instance_id)
    else:
        graphs = await admin_store.list_graphs()
    items = [serialize_graph_record(graph) for graph in graphs]
    return GraphList(items=items, total=len(items))


async def get_graph(
    self,
    *,
    graph_id: str,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> GraphDetail:
    """Return one managed graph for the authenticated caller."""
    _ = (current_user, allow_local_system)
    admin_store = require_admin_store(self._admin_store)
    graph = await admin_store.get_graph_by_id(graph_id)
    if graph is None:
        raise GraphContentNotFoundError(f"Graph '{graph_id}' was not found.")
    return GraphDetail(graph=serialize_graph_record(graph))


async def create_graph(
    self,
    *,
    instance_id: str,
    table_prefix: str,
    display_name: str | None = None,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> GraphDetail:
    """Create one managed graph for the authenticated caller."""
    _ = (current_user, allow_local_system)
    if self._instance_monitor is None:
        raise GraphContentNotReadyError("Instance monitor is not available.")
    try:
        graph = await self._instance_monitor.create_graph(
            instance_id=instance_id,
            table_prefix=table_prefix,
            display_name=display_name,
        )
    except GraphAlreadyExistsError as exc:
        raise GraphContentConflictError(str(exc)) from exc
    return GraphDetail(graph=serialize_graph_record(graph))


async def update_graph(
    self,
    *,
    graph_id: str,
    display_name: str | None = None,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> GraphDetail:
    """Update one managed graph's display name. Omitted fields are left unchanged."""
    _ = (current_user, allow_local_system)
    admin_store = require_admin_store(self._admin_store)
    existing = await admin_store.get_graph_by_id(graph_id)
    if existing is None:
        raise GraphContentNotFoundError(f"Graph '{graph_id}' was not found.")
    display_name_ = (
        display_name if display_name is not None else existing.display_name
    )
    graph = await admin_store.update_graph(
        graph_id=graph_id,
        display_name=display_name_,
    )
    if graph is None:
        raise GraphContentNotFoundError(f"Graph '{graph_id}' was not found.")
    return GraphDetail(graph=serialize_graph_record(graph))


async def delete_graph(
    self,
    *,
    graph_id: str,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> None:
    """Delete one managed graph for the authenticated caller."""
    _ = (current_user, allow_local_system)
    admin_store = require_admin_store(self._admin_store)
    graph = await admin_store.get_graph_by_id(graph_id)
    if graph is None:
        raise GraphContentNotFoundError(f"Graph '{graph_id}' was not found.")
    if self._instance_monitor is None:
        raise GraphContentNotReadyError("Instance monitor is not available.")
    await self._instance_monitor.delete_graph(graph_id)
