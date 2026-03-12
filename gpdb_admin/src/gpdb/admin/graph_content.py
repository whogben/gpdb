"""Shared graph-content service for admin graph operations."""

from __future__ import annotations

from collections.abc import Callable

from pydantic import BaseModel, Field

from gpdb import GPGraph, SearchQuery
from gpdb.admin.instances import build_postgres_url
from gpdb.admin.store import AdminStore, AdminUser, ManagedGraph, ManagedInstance


class GraphContentError(RuntimeError):
    """Base class for graph-content errors surfaced by admin code."""


class GraphContentNotReadyError(GraphContentError):
    """Raised when graph-content services are requested before startup completes."""


class GraphContentPermissionError(GraphContentError):
    """Raised when the current actor cannot access graph content."""


class GraphContentNotFoundError(GraphContentError):
    """Raised when a managed graph or instance cannot be resolved."""


class GraphContentSummary(BaseModel):
    """Live counts captured from one managed graph."""

    schema_count: int | None = None
    node_count: int | None = None
    edge_count: int | None = None


class GraphOverview(BaseModel):
    """Overview data returned by the first graph-content vertical slice."""

    graph: dict[str, object]
    instance: dict[str, object]
    summary: GraphContentSummary = Field(default_factory=GraphContentSummary)
    content_status: str = "ready"
    content_error: str | None = None


class GraphContentService:
    """Resolve managed graphs and provide graph-scoped admin operations."""

    def __init__(
        self,
        *,
        admin_store: AdminStore | None,
        captive_url_factory: Callable[[], str] | None,
    ) -> None:
        self._admin_store = admin_store
        self._captive_url_factory = captive_url_factory

    async def get_graph_overview(
        self,
        *,
        graph_id: str,
        current_user: AdminUser | None,
        allow_local_system: bool = False,
    ) -> GraphOverview:
        """Return managed graph metadata plus live schema/node/edge counts."""
        graph = await self._require_graph(graph_id)
        self._authorize_graph_access(
            instance_id=graph.instance_id,
            table_prefix=graph.table_prefix,
            permission_kind="view",
            current_user=current_user,
            allow_local_system=allow_local_system,
        )
        instance = await self._require_instance(graph.instance_id)

        overview = GraphOverview(
            graph=self.serialize_graph(graph),
            instance=self.serialize_instance(instance),
        )

        db = GPGraph(self._resolve_instance_url(instance), table_prefix=graph.table_prefix)
        try:
            schema_names = await db.list_schemas()
            node_page = await db.search_nodes(SearchQuery(limit=1))
            edge_page = await db.search_edges(SearchQuery(limit=1))
            overview.summary = GraphContentSummary(
                schema_count=len(schema_names),
                node_count=node_page.total,
                edge_count=edge_page.total,
            )
        except Exception as exc:
            overview.content_status = "unavailable"
            overview.content_error = f"Could not load live graph content counts: {exc}"
        finally:
            await db.sqla_engine.dispose()

        return overview

    def serialize_graph(self, graph: ManagedGraph) -> dict[str, object]:
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

    def serialize_instance(self, instance: ManagedInstance) -> dict[str, object]:
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

    def _authorize_graph_access(
        self,
        *,
        instance_id: str,
        table_prefix: str,
        permission_kind: str,
        current_user: AdminUser | None,
        allow_local_system: bool,
    ) -> None:
        # Keep authorization graph-scoped so future `(instance, prefix, permission)`
        # grants can replace the current owner-only check without rewriting callers.
        _ = (instance_id, table_prefix, permission_kind)
        if allow_local_system:
            return
        if current_user is None or not current_user.is_owner:
            raise GraphContentPermissionError(
                "Only the server owner can access graph content."
            )

    async def _require_graph(self, graph_id: str) -> ManagedGraph:
        admin_store = self._require_admin_store()
        graph = await admin_store.get_graph_by_id(graph_id)
        if graph is None:
            raise GraphContentNotFoundError("Managed graph was not found.")
        return graph

    async def _require_instance(self, instance_id: str) -> ManagedInstance:
        admin_store = self._require_admin_store()
        instance = await admin_store.get_instance_by_id(instance_id)
        if instance is None:
            raise GraphContentNotFoundError("Managed instance was not found.")
        return instance

    def _require_admin_store(self) -> AdminStore:
        if self._admin_store is None:
            raise GraphContentNotReadyError("Graph content service is not ready yet.")
        return self._admin_store

    def _resolve_instance_url(self, instance: ManagedInstance) -> str:
        if instance.mode == "captive":
            if self._captive_url_factory is None:
                raise GraphContentNotReadyError(
                    "Captive graph access is not ready yet."
                )
            return self._captive_url_factory()
        return build_postgres_url(instance)
