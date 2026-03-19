"""Shared graph-content service for admin graph operations."""

from __future__ import annotations

from collections.abc import Callable

from gpdb.admin.store import AdminStore
from gpdb.admin.instances import ManagedInstanceMonitor

# Re-export exceptions
from gpdb.admin.graph_content.exceptions import (
    GraphContentConflictError,
    GraphContentError,
    GraphContentNotFoundError,
    GraphContentNotReadyError,
    GraphContentPermissionError,
    GraphContentValidationError,
)

# Re-export models
from gpdb.admin.graph_content.models import (
    GraphContentSummary,
    GraphEdgeCreateParam,
    GraphEdgeDeleteResult,
    GraphEdgeDetail,
    GraphEdgeFilters,
    GraphEdgeList,
    GraphEdgeRecord,
    GraphEdgeUpdateParam,
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
    GraphOverview,
    GraphRecord,
    GraphSchemaCreateParam,
    GraphSchemaDeleteResult,
    GraphSchemaDetail,
    GraphSchemaList,
    GraphSchemaRecord,
    GraphSchemaUpdateParam,
    GraphSchemaUsage,
    GraphViewerData,
    InstanceDetail,
    InstanceList,
    InstanceRecord,
    GraphDetail,
    GraphList,
)

# Import operation modules
from gpdb.admin.graph_content import edges, graphs, instances, nodes, schemas


class GraphContentService:
    """Resolve managed graphs and provide graph-scoped admin operations."""

    def __init__(
        self,
        *,
        admin_store: AdminStore | None,
        captive_url_factory: Callable[[], str] | None,
        instance_monitor: ManagedInstanceMonitor | None = None,
    ) -> None:
        self._admin_store = admin_store
        self._captive_url_factory = captive_url_factory
        self._instance_monitor = instance_monitor

    # Import methods from operation modules
    # Instance operations
    list_instances = instances.list_instances
    get_instance = instances.get_instance
    create_instance = instances.create_instance
    update_instance = instances.update_instance
    delete_instance = instances.delete_instance

    # Graph operations
    list_graphs = graphs.list_graphs
    get_graph = graphs.get_graph
    create_graph = graphs.create_graph
    update_graph = graphs.update_graph
    delete_graph = graphs.delete_graph

    # Schema operations
    get_graph_overview = schemas.get_graph_overview
    list_graph_schemas = schemas.list_graph_schemas
    get_graph_schemas = schemas.get_graph_schemas
    create_graph_schemas = schemas.create_graph_schemas
    update_graph_schemas = schemas.update_graph_schemas
    delete_graph_schemas = schemas.delete_graph_schemas

    # Node operations
    list_graph_nodes = nodes.list_graph_nodes
    get_graph_nodes = nodes.get_graph_nodes
    create_graph_nodes = nodes.create_graph_nodes
    update_graph_nodes = nodes.update_graph_nodes
    delete_graph_nodes = nodes.delete_graph_nodes
    set_graph_node_payloads = nodes.set_graph_node_payloads
    get_graph_node_payloads = nodes.get_graph_node_payloads

    # Edge operations
    list_graph_edges = edges.list_graph_edges
    get_graph_edges = edges.get_graph_edges
    create_graph_edges = edges.create_graph_edges
    update_graph_edges = edges.update_graph_edges
    delete_graph_edges = edges.delete_graph_edges
    get_graph_viewer_data = edges.get_graph_viewer_data


__all__ = [
    # Exceptions
    "GraphContentError",
    "GraphContentNotReadyError",
    "GraphContentPermissionError",
    "GraphContentNotFoundError",
    "GraphContentConflictError",
    "GraphContentValidationError",
    # Models
    "GraphContentSummary",
    "GraphOverview",
    "GraphSchemaUsage",
    "GraphSchemaRecord",
    "GraphSchemaList",
    "GraphSchemaDetail",
    "GraphSchemaUpdateParam",
    "GraphSchemaCreateParam",
    "GraphNodeCreateParam",
    "GraphNodeUpdateParam",
    "GraphEdgeCreateParam",
    "GraphEdgeUpdateParam",
    "GraphNodePayloadSetParam",
    "GraphNodeFilters",
    "GraphNodeRecord",
    "GraphNodeList",
    "GraphNodeDetail",
    "GraphNodeDeleteBlockers",
    "GraphNodePayload",
    "GraphEdgeFilters",
    "GraphEdgeRecord",
    "GraphEdgeList",
    "GraphEdgeDetail",
    "GraphSchemaDeleteResult",
    "GraphNodeDeleteResult",
    "GraphEdgeDeleteResult",
    "InstanceRecord",
    "InstanceList",
    "InstanceDetail",
    "GraphRecord",
    "GraphList",
    "GraphDetail",
    "GraphViewerData",
    # Service
    "GraphContentService",
]
