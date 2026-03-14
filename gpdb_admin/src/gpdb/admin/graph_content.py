"""Shared graph-content service for admin graph operations."""

from __future__ import annotations

import base64
import logging
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError

from gpdb import (
    EdgeUpsert,
    Filter,
    FilterGroup,
    GPGraph,
    Logic,
    NodeUpsert,
    SchemaBreakingChangeError,
    SchemaInUseError,
    SchemaNotFoundError,
    SchemaValidationError,
    SearchQuery,
    Sort,
)
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


class GraphContentConflictError(GraphContentError):
    """Raised when a requested create operation conflicts with existing content."""


class GraphContentValidationError(GraphContentError):
    """Raised when graph-content input is invalid for admin use."""


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


class GraphSchemaUsage(BaseModel):
    """Schema usage summary for admin list/detail views."""

    node_count: int = 0
    edge_count: int = 0
    sample_node_ids: list[str] = Field(default_factory=list)
    sample_edge_ids: list[str] = Field(default_factory=list)


class GraphSchemaRecord(BaseModel):
    """Stable schema payload returned by admin graph-content APIs."""

    name: str
    kind: str
    version: str
    json_schema: dict[str, Any] | None = None
    usage: GraphSchemaUsage = Field(default_factory=GraphSchemaUsage)


class GraphSchemaList(BaseModel):
    """List response for graph schemas."""

    graph: dict[str, object]
    instance: dict[str, object]
    items: list[GraphSchemaRecord] = Field(default_factory=list)
    total: int = 0


class GraphSchemaDetail(BaseModel):
    """Detail response for one graph schema."""

    graph: dict[str, object]
    instance: dict[str, object]
    schema_record: GraphSchemaRecord = Field(serialization_alias="schema")

    @property
    def schema(self) -> GraphSchemaRecord:
        """Backwards-compatible accessor for the schema payload."""
        return self.schema_record


class GraphNodeFilters(BaseModel):
    """Current node list filters echoed back to callers."""

    type: str | None = None
    schema_name: str | None = None
    parent_id: str | None = None
    filter_dsl: str | None = None
    sort: str = "created_at_desc"


class GraphNodeRecord(BaseModel):
    """Stable node payload returned by admin graph-content APIs."""

    id: str
    type: str
    name: str | None = None
    owner_id: str | None = None
    parent_id: str | None = None
    schema_name: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    created_at: str
    updated_at: str
    version: int
    payload_size: int = 0
    payload_hash: str | None = None
    payload_mime: str | None = None
    payload_filename: str | None = None
    has_payload: bool = False


class GraphNodeList(BaseModel):
    """List response for graph nodes."""

    graph: dict[str, object]
    instance: dict[str, object]
    items: list[GraphNodeRecord] = Field(default_factory=list)
    total: int = 0
    limit: int = 50
    offset: int = 0
    filters: GraphNodeFilters = Field(default_factory=GraphNodeFilters)


class GraphNodeDetail(BaseModel):
    """Detail response for one graph node."""

    graph: dict[str, object]
    instance: dict[str, object]
    node_record: GraphNodeRecord = Field(serialization_alias="node")
    delete_blockers: "GraphNodeDeleteBlockers" = Field(
        default_factory=lambda: GraphNodeDeleteBlockers()
    )

    @property
    def node(self) -> GraphNodeRecord:
        """Backwards-compatible accessor for the node payload."""
        return self.node_record


class GraphNodeDeleteBlockers(BaseModel):
    """Delete preflight summary for one graph node."""

    child_count: int = 0
    incident_edge_count: int = 0
    sample_child_ids: list[str] = Field(default_factory=list)
    sample_edge_ids: list[str] = Field(default_factory=list)
    can_delete: bool = True


class GraphNodePayload(BaseModel):
    """Stable payload response for one graph node."""

    graph: dict[str, object]
    instance: dict[str, object]
    node_record: GraphNodeRecord = Field(serialization_alias="node")
    payload_base64: str
    encoding: str = "base64"
    filename: str

    @property
    def node(self) -> GraphNodeRecord:
        """Backwards-compatible accessor for the node payload."""
        return self.node_record


class GraphEdgeFilters(BaseModel):
    """Current edge list filters echoed back to callers."""

    type: str | None = None
    schema_name: str | None = None
    source_id: str | None = None
    target_id: str | None = None
    filter_dsl: str | None = None
    sort: str = "created_at_desc"


class GraphEdgeRecord(BaseModel):
    """Stable edge payload returned by admin graph-content APIs."""

    id: str
    type: str
    source_id: str
    target_id: str
    schema_name: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    created_at: str
    updated_at: str
    version: int


class GraphEdgeList(BaseModel):
    """List response for graph edges."""

    graph: dict[str, object]
    instance: dict[str, object]
    items: list[GraphEdgeRecord] = Field(default_factory=list)
    total: int = 0
    limit: int = 50
    offset: int = 0
    filters: GraphEdgeFilters = Field(default_factory=GraphEdgeFilters)


class GraphEdgeDetail(BaseModel):
    """Detail response for one graph edge."""

    graph: dict[str, object]
    instance: dict[str, object]
    edge_record: GraphEdgeRecord = Field(serialization_alias="edge")

    @property
    def edge(self) -> GraphEdgeRecord:
        """Backwards-compatible accessor for the edge payload."""
        return self.edge_record


class GraphViewerData(BaseModel):
    """Combined nodes and edges for the graph viewer (Cytoscape-oriented)."""

    graph: dict[str, object]
    instance: dict[str, object]
    elements: list[dict[str, object]] = Field(default_factory=list)
    node_count: int = 0
    edge_count: int = 0
    error: str | None = None


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
        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="view",
        )
        overview = GraphOverview(
            graph=self.serialize_graph(graph),
            instance=self.serialize_instance(instance),
        )
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
            logging.exception(
                "Failed to load live graph content counts",
                extra={"graph_id": graph_id, "instance_id": instance.id},
            )
            overview.content_status = "unavailable"
            overview.content_error = f"Could not load live graph content counts: {exc}"
        finally:
            await db.sqla_engine.dispose()

        return overview

    async def list_graph_schemas(
        self,
        *,
        graph_id: str,
        current_user: AdminUser | None,
        allow_local_system: bool = False,
        kind: str | None = None,
        include_json_schema: bool = False,
    ) -> GraphSchemaList:
        """Return the current schema registry for one managed graph."""
        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="view",
        )
        try:
            items: list[GraphSchemaRecord] = []
            clean_kind = self._normalize_optional_schema_kind(kind)
            for schema_name in sorted(await db.list_schemas(kind=clean_kind)):
                schema = await db.get_schema(schema_name)
                if schema is None:
                    continue
                items.append(
                    self._serialize_schema_record(
                        schema,
                        include_json_schema=include_json_schema,
                        usage=GraphSchemaUsage(),
                    )
                )
            return GraphSchemaList(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                items=items,
                total=len(items),
            )
        finally:
            await db.sqla_engine.dispose()

    async def get_graph_schema(
        self,
        *,
        graph_id: str,
        name: str,
        current_user: AdminUser | None,
        allow_local_system: bool = False,
    ) -> GraphSchemaDetail:
        """Return one graph schema plus usage detail."""
        clean_name = name.strip()
        if not clean_name:
            raise GraphContentValidationError("Schema name is required.")

        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="view",
        )
        try:
            schema = await db.get_schema(clean_name)
            if schema is None:
                raise GraphContentNotFoundError(f"Schema '{clean_name}' was not found.")
            return GraphSchemaDetail(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                schema_record=self._serialize_schema_record(
                    schema,
                    include_json_schema=True,
                    usage=await self._inspect_schema_usage(db, clean_name),
                ),
            )
        finally:
            await db.sqla_engine.dispose()

    async def create_graph_schema(
        self,
        *,
        graph_id: str,
        name: str,
        json_schema: dict[str, Any],
        current_user: AdminUser | None,
        kind: str = "node",
        allow_local_system: bool = False,
    ) -> GraphSchemaDetail:
        """Create one schema in a managed graph."""
        clean_name = self._validate_schema_name(name)
        clean_kind = self._validate_schema_kind(kind)
        self._validate_json_schema(json_schema)

        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="manage_schemas",
        )
        try:
            if await db.get_schema(clean_name) is not None:
                raise GraphContentConflictError(
                    f"Schema '{clean_name}' already exists."
                )
            try:
                schema = await db.register_schema(
                    clean_name,
                    json_schema,
                    kind=clean_kind,
                )
            except (SchemaBreakingChangeError, ValueError) as exc:
                raise GraphContentValidationError(str(exc)) from exc
            return GraphSchemaDetail(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                schema_record=self._serialize_schema_record(
                    schema,
                    include_json_schema=True,
                    usage=GraphSchemaUsage(),
                ),
            )
        finally:
            await db.sqla_engine.dispose()

    async def update_graph_schema(
        self,
        *,
        graph_id: str,
        name: str,
        json_schema: dict[str, Any],
        current_user: AdminUser | None,
        kind: str = "node",
        allow_local_system: bool = False,
    ) -> GraphSchemaDetail:
        """Update one schema in a managed graph when the change is non-breaking."""
        clean_name = self._validate_schema_name(name)
        clean_kind = self._validate_schema_kind(kind)
        self._validate_json_schema(json_schema)

        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="manage_schemas",
        )
        try:
            existing = await db.get_schema(clean_name)
            if existing is None:
                raise GraphContentNotFoundError(f"Schema '{clean_name}' was not found.")
            try:
                schema = await db.register_schema(
                    clean_name,
                    json_schema,
                    kind=clean_kind,
                )
            except SchemaBreakingChangeError as exc:
                raise GraphContentValidationError(
                    "Breaking schema changes are not supported here yet. "
                    f"Use a migration workflow for schema '{clean_name}'."
                ) from exc
            except ValueError as exc:
                raise GraphContentValidationError(str(exc)) from exc
            return GraphSchemaDetail(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                schema_record=self._serialize_schema_record(
                    schema,
                    include_json_schema=True,
                    usage=await self._inspect_schema_usage(db, clean_name),
                ),
            )
        finally:
            await db.sqla_engine.dispose()

    async def delete_graph_schema(
        self,
        *,
        graph_id: str,
        name: str,
        current_user: AdminUser | None,
        allow_local_system: bool = False,
    ) -> GraphSchemaDetail:
        """Delete one unused schema from a managed graph."""
        clean_name = self._validate_schema_name(name)

        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="manage_schemas",
        )
        try:
            schema = await db.get_schema(clean_name)
            if schema is None:
                raise GraphContentNotFoundError(f"Schema '{clean_name}' was not found.")
            usage = await self._inspect_schema_usage(db, clean_name)
            deleted = GraphSchemaDetail(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                schema_record=self._serialize_schema_record(
                    schema,
                    include_json_schema=True,
                    usage=usage,
                ),
            )
            try:
                await db.delete_schema(clean_name)
            except SchemaInUseError as exc:
                raise GraphContentConflictError(
                    self._format_schema_delete_blocker_message(clean_name, usage)
                ) from exc
            return deleted
        finally:
            await db.sqla_engine.dispose()

    async def list_graph_nodes(
        self,
        *,
        graph_id: str,
        current_user: AdminUser | None,
        allow_local_system: bool = False,
        type: str | None = None,
        schema_name: str | None = None,
        parent_id: str | None = None,
        filter_dsl: str | None = None,
        limit: int = 50,
        offset: int = 0,
        sort: str = "created_at_desc",
    ) -> GraphNodeList:
        """Return paginated node records for one managed graph."""
        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="view",
        )
        try:
            clean_filter_dsl = self._normalize_optional_text(filter_dsl)
            if clean_filter_dsl:
                try:
                    parsed_filter = FilterGroup.from_dsl(clean_filter_dsl)
                except ValueError as exc:
                    raise GraphContentValidationError(
                        f"Invalid filter (DSL): {exc}"
                    ) from exc
                filter_value: Filter | FilterGroup | None = parsed_filter
            else:
                filter_value = self._build_node_filter(
                    type=type,
                    schema_name=schema_name,
                    parent_id=parent_id,
                )
            query = SearchQuery(
                filter=filter_value,
                sort=[self._parse_node_sort(sort)],
                limit=self._validate_page_limit(limit),
                offset=self._validate_page_offset(offset),
            )
            page = await db.search_nodes(query)
            return GraphNodeList(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                items=[self._serialize_node_record(item) for item in page.items],
                total=page.total,
                limit=page.limit,
                offset=page.offset,
                filters=GraphNodeFilters(
                    type=self._normalize_optional_text(type),
                    schema_name=self._normalize_optional_text(schema_name),
                    parent_id=self._normalize_optional_text(parent_id),
                    filter_dsl=clean_filter_dsl,
                    sort=sort,
                ),
            )
        finally:
            await db.sqla_engine.dispose()

    async def get_graph_node(
        self,
        *,
        graph_id: str,
        node_id: str,
        current_user: AdminUser | None,
        allow_local_system: bool = False,
    ) -> GraphNodeDetail:
        """Return one graph node plus metadata."""
        clean_node_id = self._validate_node_id(node_id)
        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="view",
        )
        try:
            node = await db.get_node(clean_node_id)
            if node is None:
                raise GraphContentNotFoundError(
                    f"Node '{clean_node_id}' was not found."
                )
            return GraphNodeDetail(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                node_record=self._serialize_node_record(node),
                delete_blockers=await self._inspect_node_delete_blockers(
                    db, clean_node_id
                ),
            )
        finally:
            await db.sqla_engine.dispose()

    async def create_graph_node(
        self,
        *,
        graph_id: str,
        type: str,
        data: dict[str, Any],
        current_user: AdminUser | None,
        allow_local_system: bool = False,
        name: str | None = None,
        schema_name: str | None = None,
        owner_id: str | None = None,
        parent_id: str | None = None,
        tags: list[str] | None = None,
        payload: bytes | None = None,
        payload_mime: str | None = None,
        payload_filename: str | None = None,
        clear_payload: bool = False,
    ) -> GraphNodeDetail:
        """Create one node in a managed graph."""
        clean_type = self._validate_node_type(type)
        clean_name = self._normalize_optional_text(name)
        clean_schema_name = self._normalize_optional_text(schema_name)
        clean_owner_id = self._normalize_optional_text(owner_id)
        clean_parent_id = self._normalize_optional_text(parent_id)
        normalized_tags = self._normalize_tag_list(tags)
        clean_payload_mime = self._normalize_optional_text(payload_mime)
        clean_payload_filename = self._normalize_optional_text(payload_filename)
        self._validate_json_object(data, object_name="Node data")
        if clear_payload:
            raise GraphContentValidationError(
                "clear_payload cannot be used while creating a node."
            )

        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="manage_nodes",
        )
        try:
            try:
                node = await db.set_node(
                    NodeUpsert(
                        type=clean_type,
                        name=clean_name,
                        owner_id=clean_owner_id,
                        parent_id=clean_parent_id,
                        schema_name=clean_schema_name,
                        data=data,
                        tags=normalized_tags,
                        payload=payload,
                        payload_mime=clean_payload_mime,
                        payload_filename=clean_payload_filename,
                    )
                )
            except (SchemaNotFoundError, SchemaValidationError, ValueError) as exc:
                raise GraphContentValidationError(str(exc)) from exc
            return GraphNodeDetail(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                node_record=self._serialize_node_record(node),
                delete_blockers=GraphNodeDeleteBlockers(),
            )
        finally:
            await db.sqla_engine.dispose()

    async def update_graph_node(
        self,
        *,
        graph_id: str,
        node_id: str,
        type: str,
        data: dict[str, Any],
        current_user: AdminUser | None,
        allow_local_system: bool = False,
        name: str | None = None,
        schema_name: str | None = None,
        owner_id: str | None = None,
        parent_id: str | None = None,
        tags: list[str] | None = None,
        payload: bytes | None = None,
        payload_mime: str | None = None,
        payload_filename: str | None = None,
        clear_payload: bool = False,
    ) -> GraphNodeDetail:
        """Update one node in a managed graph."""
        clean_node_id = self._validate_node_id(node_id)
        clean_type = self._validate_node_type(type)
        clean_name = self._normalize_optional_text(name)
        clean_schema_name = self._normalize_optional_text(schema_name)
        clean_owner_id = self._normalize_optional_text(owner_id)
        clean_parent_id = self._normalize_optional_text(parent_id)
        normalized_tags = self._normalize_tag_list(tags)
        clean_payload_mime = self._normalize_optional_text(payload_mime)
        clean_payload_filename = self._normalize_optional_text(payload_filename)
        self._validate_json_object(data, object_name="Node data")
        if payload is not None and clear_payload:
            raise GraphContentValidationError(
                "Provide either payload bytes or clear_payload, not both."
            )

        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="manage_nodes",
        )
        try:
            existing = await db.get_node(clean_node_id)
            if existing is None:
                raise GraphContentNotFoundError(
                    f"Node '{clean_node_id}' was not found."
                )
            try:
                async with db.transaction():
                    node = await db.set_node(
                        NodeUpsert(
                            id=clean_node_id,
                            type=clean_type,
                            name=clean_name,
                            owner_id=clean_owner_id,
                            parent_id=clean_parent_id,
                            schema_name=clean_schema_name,
                            data=data,
                            tags=normalized_tags,
                            payload=payload,
                            payload_mime=clean_payload_mime,
                            payload_filename=clean_payload_filename,
                        )
                    )
                    if clear_payload:
                        node = await db.clear_node_payload(clean_node_id)
            except (SchemaNotFoundError, SchemaValidationError, ValueError) as exc:
                raise GraphContentValidationError(str(exc)) from exc
            return GraphNodeDetail(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                node_record=self._serialize_node_record(node),
                delete_blockers=await self._inspect_node_delete_blockers(
                    db, clean_node_id
                ),
            )
        finally:
            await db.sqla_engine.dispose()

    async def delete_graph_node(
        self,
        *,
        graph_id: str,
        node_id: str,
        current_user: AdminUser | None,
        allow_local_system: bool = False,
    ) -> GraphNodeDetail:
        """Delete one graph node when it has no child or edge blockers."""
        clean_node_id = self._validate_node_id(node_id)
        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="manage_nodes",
        )
        try:
            node = await db.get_node(clean_node_id)
            if node is None:
                raise GraphContentNotFoundError(
                    f"Node '{clean_node_id}' was not found."
                )
            blockers = await self._inspect_node_delete_blockers(db, clean_node_id)
            deleted = GraphNodeDetail(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                node_record=self._serialize_node_record(node),
                delete_blockers=blockers,
            )
            try:
                await db.delete_node(clean_node_id)
            except IntegrityError as exc:
                blockers = await self._inspect_node_delete_blockers(db, clean_node_id)
                raise GraphContentConflictError(
                    self._format_node_delete_blocker_message(clean_node_id, blockers)
                ) from exc
            return deleted
        finally:
            await db.sqla_engine.dispose()

    async def set_graph_node_payload(
        self,
        *,
        graph_id: str,
        node_id: str,
        payload: bytes,
        current_user: AdminUser | None,
        allow_local_system: bool = False,
        mime: str | None = None,
        payload_filename: str | None = None,
    ) -> GraphNodeDetail:
        """Upload or replace one graph node payload."""
        clean_node_id = self._validate_node_id(node_id)
        clean_mime = self._normalize_optional_text(mime)
        clean_payload_filename = self._normalize_optional_text(payload_filename)

        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="manage_nodes",
        )
        try:
            try:
                node = await db.set_node_payload(
                    clean_node_id,
                    payload,
                    mime=clean_mime,
                    filename=clean_payload_filename,
                )
            except ValueError as exc:
                raise GraphContentNotFoundError(
                    f"Node '{clean_node_id}' was not found."
                ) from exc
            return GraphNodeDetail(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                node_record=self._serialize_node_record(node),
                delete_blockers=await self._inspect_node_delete_blockers(
                    db, clean_node_id
                ),
            )
        finally:
            await db.sqla_engine.dispose()

    async def get_graph_node_payload(
        self,
        *,
        graph_id: str,
        node_id: str,
        current_user: AdminUser | None,
        allow_local_system: bool = False,
    ) -> GraphNodePayload:
        """Return one node payload encoded for JSON-based admin surfaces."""
        clean_node_id = self._validate_node_id(node_id)
        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="view",
        )
        try:
            node = await db.get_node_with_payload(clean_node_id)
            if node is None:
                raise GraphContentNotFoundError(
                    f"Node '{clean_node_id}' was not found."
                )
            if node.payload is None:
                raise GraphContentConflictError(
                    f"Node '{clean_node_id}' does not have a payload."
                )
            node_record = self._serialize_node_record(node)
            return GraphNodePayload(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                node_record=node_record,
                payload_base64=base64.b64encode(node.payload).decode("ascii"),
                filename=self._build_node_payload_filename(node_record),
            )
        finally:
            await db.sqla_engine.dispose()

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
        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="view",
        )
        try:
            clean_filter_dsl = self._normalize_optional_text(filter_dsl)
            if clean_filter_dsl:
                try:
                    parsed_filter = FilterGroup.from_dsl(clean_filter_dsl)
                except ValueError as exc:
                    raise GraphContentValidationError(
                        f"Invalid filter (DSL): {exc}"
                    ) from exc
                filter_value: Filter | FilterGroup | None = parsed_filter
            else:
                filter_value = self._build_edge_filter(
                    type=type,
                    schema_name=schema_name,
                    source_id=source_id,
                    target_id=target_id,
                )
            query = SearchQuery(
                filter=filter_value,
                sort=[self._parse_edge_sort(sort)],
                limit=self._validate_page_limit(limit),
                offset=self._validate_page_offset(offset),
            )
            page = await db.search_edges(query)
            return GraphEdgeList(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                items=[self._serialize_edge_record(item) for item in page.items],
                total=page.total,
                limit=page.limit,
                offset=page.offset,
                filters=GraphEdgeFilters(
                    type=self._normalize_optional_text(type),
                    schema_name=self._normalize_optional_text(schema_name),
                    source_id=self._normalize_optional_text(source_id),
                    target_id=self._normalize_optional_text(target_id),
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
        try:
            node_list = await self.list_graph_nodes(
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
            edge_list = await self.list_graph_edges(
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
                graph={},
                instance={},
                elements=[],
                node_count=0,
                edge_count=0,
                error=str(exc),
            )

        graph = node_list.graph
        instance = node_list.instance
        elements: list[dict[str, object]] = []

        for node in node_list.items:
            elements.append({
                "group": "nodes",
                "data": {
                    "id": node.id,
                    "label": node.name or node.id,
                    "type": node.type,
                },
            })
        for edge in edge_list.items:
            elements.append({
                "group": "edges",
                "data": {
                    "id": edge.id,
                    "source": edge.source_id,
                    "target": edge.target_id,
                    "label": edge.type,
                },
            })

        return GraphViewerData(
            graph=graph,
            instance=instance,
            elements=elements,
            node_count=len(node_list.items),
            edge_count=len(edge_list.items),
        )

    async def get_graph_edge(
        self,
        *,
        graph_id: str,
        edge_id: str,
        current_user: AdminUser | None,
        allow_local_system: bool = False,
    ) -> GraphEdgeDetail:
        """Return one graph edge plus metadata."""
        clean_edge_id = self._validate_edge_id(edge_id)
        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="view",
        )
        try:
            edge = await db.get_edge(clean_edge_id)
            if edge is None:
                raise GraphContentNotFoundError(
                    f"Edge '{clean_edge_id}' was not found."
                )
            return GraphEdgeDetail(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                edge_record=self._serialize_edge_record(edge),
            )
        finally:
            await db.sqla_engine.dispose()

    async def create_graph_edge(
        self,
        *,
        graph_id: str,
        type: str,
        source_id: str,
        target_id: str,
        data: dict[str, Any],
        current_user: AdminUser | None,
        allow_local_system: bool = False,
        schema_name: str | None = None,
        tags: list[str] | None = None,
    ) -> GraphEdgeDetail:
        """Create one edge in a managed graph."""
        clean_type = self._validate_edge_type(type)
        clean_source_id = self._validate_related_node_id(source_id, field_name="Source")
        clean_target_id = self._validate_related_node_id(target_id, field_name="Target")
        clean_schema_name = self._normalize_optional_text(schema_name)
        normalized_tags = self._normalize_tag_list(tags)
        self._validate_json_object(data, object_name="Edge data")

        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="manage_edges",
        )
        try:
            try:
                edge = await db.set_edge(
                    EdgeUpsert(
                        type=clean_type,
                        source_id=clean_source_id,
                        target_id=clean_target_id,
                        schema_name=clean_schema_name,
                        data=data,
                        tags=normalized_tags,
                    )
                )
            except IntegrityError as exc:
                raise GraphContentValidationError(
                    "Source and target nodes must exist before creating an edge."
                ) from exc
            except (SchemaNotFoundError, SchemaValidationError, ValueError) as exc:
                raise GraphContentValidationError(str(exc)) from exc
            return GraphEdgeDetail(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                edge_record=self._serialize_edge_record(edge),
            )
        finally:
            await db.sqla_engine.dispose()

    async def update_graph_edge(
        self,
        *,
        graph_id: str,
        edge_id: str,
        type: str,
        source_id: str,
        target_id: str,
        data: dict[str, Any],
        current_user: AdminUser | None,
        allow_local_system: bool = False,
        schema_name: str | None = None,
        tags: list[str] | None = None,
    ) -> GraphEdgeDetail:
        """Update one edge in a managed graph."""
        clean_edge_id = self._validate_edge_id(edge_id)
        clean_type = self._validate_edge_type(type)
        clean_source_id = self._validate_related_node_id(source_id, field_name="Source")
        clean_target_id = self._validate_related_node_id(target_id, field_name="Target")
        clean_schema_name = self._normalize_optional_text(schema_name)
        normalized_tags = self._normalize_tag_list(tags)
        self._validate_json_object(data, object_name="Edge data")

        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="manage_edges",
        )
        try:
            existing = await db.get_edge(clean_edge_id)
            if existing is None:
                raise GraphContentNotFoundError(
                    f"Edge '{clean_edge_id}' was not found."
                )
            try:
                edge = await db.set_edge(
                    EdgeUpsert(
                        id=clean_edge_id,
                        type=clean_type,
                        source_id=clean_source_id,
                        target_id=clean_target_id,
                        schema_name=clean_schema_name,
                        data=data,
                        tags=normalized_tags,
                    )
                )
            except IntegrityError as exc:
                raise GraphContentValidationError(
                    "Source and target nodes must exist before updating an edge."
                ) from exc
            except (SchemaNotFoundError, SchemaValidationError, ValueError) as exc:
                raise GraphContentValidationError(str(exc)) from exc
            return GraphEdgeDetail(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                edge_record=self._serialize_edge_record(edge),
            )
        finally:
            await db.sqla_engine.dispose()

    async def delete_graph_edge(
        self,
        *,
        graph_id: str,
        edge_id: str,
        current_user: AdminUser | None,
        allow_local_system: bool = False,
    ) -> GraphEdgeDetail:
        """Delete one edge from a managed graph."""
        clean_edge_id = self._validate_edge_id(edge_id)
        graph, instance, db = await self._open_graph(
            graph_id=graph_id,
            current_user=current_user,
            allow_local_system=allow_local_system,
            permission_kind="manage_edges",
        )
        try:
            edge = await db.get_edge(clean_edge_id)
            if edge is None:
                raise GraphContentNotFoundError(
                    f"Edge '{clean_edge_id}' was not found."
                )
            deleted = GraphEdgeDetail(
                graph=self.serialize_graph(graph),
                instance=self.serialize_instance(instance),
                edge_record=self._serialize_edge_record(edge),
            )
            await db.delete_edge(clean_edge_id)
            return deleted
        finally:
            await db.sqla_engine.dispose()

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

    def _serialize_schema_record(
        self,
        schema: Any,
        *,
        include_json_schema: bool,
        usage: GraphSchemaUsage | None = None,
    ) -> GraphSchemaRecord:
        """Project one core schema record into a stable admin response."""
        return GraphSchemaRecord(
            name=str(schema.name),
            kind=self._schema_kind_from_record(schema),
            version=str(schema.version),
            json_schema=schema.json_schema if include_json_schema else None,
            usage=usage or GraphSchemaUsage(),
        )

    def _serialize_node_record(self, node: Any) -> GraphNodeRecord:
        """Project one core node record into a stable admin response."""
        return GraphNodeRecord(
            id=str(node.id),
            type=str(node.type),
            name=node.name,
            owner_id=node.owner_id,
            parent_id=node.parent_id,
            schema_name=node.schema_name,
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

    def _serialize_edge_record(self, edge: Any) -> GraphEdgeRecord:
        """Project one core edge record into a stable admin response."""
        return GraphEdgeRecord(
            id=str(edge.id),
            type=str(edge.type),
            source_id=str(edge.source_id),
            target_id=str(edge.target_id),
            schema_name=edge.schema_name,
            data=dict(edge.data or {}),
            tags=list(edge.tags or []),
            created_at=edge.created_at.isoformat(),
            updated_at=edge.updated_at.isoformat(),
            version=int(edge.version),
        )

    def _validate_schema_name(self, name: str) -> str:
        clean_name = name.strip()
        if not clean_name:
            raise GraphContentValidationError("Schema name is required.")
        return clean_name

    def _validate_json_schema(self, json_schema: dict[str, Any]) -> None:
        if not isinstance(json_schema, dict):
            raise GraphContentValidationError("Schema JSON must be a JSON object.")

    def _validate_schema_kind(self, kind: str) -> str:
        clean_kind = kind.strip().lower()
        if clean_kind not in {"node", "edge"}:
            raise GraphContentValidationError(
                "Schema kind must be either 'node' or 'edge'."
            )
        return clean_kind

    def _normalize_optional_schema_kind(self, kind: str | None) -> str | None:
        if kind is None:
            return None
        clean_kind = kind.strip()
        if not clean_kind:
            return None
        return self._validate_schema_kind(clean_kind)

    def _validate_json_object(
        self,
        value: dict[str, Any],
        *,
        object_name: str,
    ) -> None:
        if not isinstance(value, dict):
            raise GraphContentValidationError(f"{object_name} must be a JSON object.")

    def _validate_node_type(self, value: str) -> str:
        clean_value = value.strip()
        if not clean_value:
            raise GraphContentValidationError("Node type is required.")
        return clean_value

    def _validate_node_id(self, value: str) -> str:
        clean_value = value.strip()
        if not clean_value:
            raise GraphContentValidationError("Node id is required.")
        return clean_value

    def _validate_edge_id(self, value: str) -> str:
        clean_value = value.strip()
        if not clean_value:
            raise GraphContentValidationError("Edge id is required.")
        return clean_value

    def _validate_edge_type(self, value: str) -> str:
        clean_value = value.strip()
        if not clean_value:
            raise GraphContentValidationError("Edge type is required.")
        return clean_value

    def _validate_related_node_id(self, value: str, *, field_name: str) -> str:
        clean_value = value.strip()
        if not clean_value:
            raise GraphContentValidationError(f"{field_name} node id is required.")
        return clean_value

    def _normalize_optional_text(self, value: str | None) -> str | None:
        if value is None:
            return None
        clean_value = value.strip()
        return clean_value or None

    def _schema_kind_from_record(self, schema: Any) -> str:
        kind = (schema.json_schema or {}).get("x-gpdb-kind")
        if kind not in {"node", "edge"}:
            raise GraphContentValidationError(
                f"Schema '{schema.name}' is missing valid kind metadata."
            )
        return str(kind)

    def _normalize_tag_list(self, tags: list[str] | None) -> list[str]:
        if not tags:
            return []
        normalized: list[str] = []
        for item in tags:
            clean_item = str(item).strip()
            if clean_item:
                normalized.append(clean_item)
        return normalized

    def _validate_page_limit(self, limit: int) -> int:
        if limit < 1:
            raise GraphContentValidationError("Limit must be at least 1.")
        if limit > 200:
            raise GraphContentValidationError("Limit cannot be greater than 200.")
        return limit

    def _validate_page_offset(self, offset: int) -> int:
        if offset < 0:
            raise GraphContentValidationError("Offset cannot be negative.")
        return offset

    def _parse_node_sort(self, sort: str) -> Sort:
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

    def _parse_edge_sort(self, sort: str) -> Sort:
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

    def _build_node_filter(
        self,
        *,
        type: str | None,
        schema_name: str | None,
        parent_id: str | None,
    ) -> FilterGroup | Filter | None:
        filters: list[Filter] = []
        clean_type = self._normalize_optional_text(type)
        clean_schema_name = self._normalize_optional_text(schema_name)
        clean_parent_id = self._normalize_optional_text(parent_id)
        if clean_type:
            filters.append(Filter(field="type", value=clean_type))
        if clean_schema_name:
            filters.append(Filter(field="schema_name", value=clean_schema_name))
        if clean_parent_id:
            filters.append(Filter(field="parent_id", value=clean_parent_id))
        if not filters:
            return None
        if len(filters) == 1:
            return filters[0]
        return FilterGroup(logic=Logic.AND, filters=filters)

    def _build_edge_filter(
        self,
        *,
        type: str | None,
        schema_name: str | None,
        source_id: str | None,
        target_id: str | None,
    ) -> FilterGroup | Filter | None:
        filters: list[Filter] = []
        clean_type = self._normalize_optional_text(type)
        clean_schema_name = self._normalize_optional_text(schema_name)
        clean_source_id = self._normalize_optional_text(source_id)
        clean_target_id = self._normalize_optional_text(target_id)
        if clean_type:
            filters.append(Filter(field="type", value=clean_type))
        if clean_schema_name:
            filters.append(Filter(field="schema_name", value=clean_schema_name))
        if clean_source_id:
            filters.append(Filter(field="source_id", value=clean_source_id))
        if clean_target_id:
            filters.append(Filter(field="target_id", value=clean_target_id))
        if not filters:
            return None
        if len(filters) == 1:
            return filters[0]
        return FilterGroup(logic=Logic.AND, filters=filters)

    def _format_schema_delete_blocker_message(
        self,
        schema_name: str,
        usage: GraphSchemaUsage,
    ) -> str:
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

    def _format_node_delete_blocker_message(
        self,
        node_id: str,
        blockers: GraphNodeDeleteBlockers,
    ) -> str:
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

    def _build_node_payload_filename(self, node: GraphNodeRecord) -> str:
        base_name = (node.payload_filename or node.name or node.id).strip() or node.id
        safe_name = base_name.replace("/", "_")
        return safe_name

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

    async def _open_graph(
        self,
        *,
        graph_id: str,
        current_user: AdminUser | None,
        allow_local_system: bool,
        permission_kind: str,
    ) -> tuple[ManagedGraph, ManagedInstance, GPGraph]:
        graph = await self._require_graph(graph_id)
        self._authorize_graph_access(
            instance_id=graph.instance_id,
            table_prefix=graph.table_prefix,
            permission_kind=permission_kind,
            current_user=current_user,
            allow_local_system=allow_local_system,
        )
        instance = await self._require_instance(graph.instance_id)
        return (
            graph,
            instance,
            GPGraph(
                self._resolve_instance_url(instance), table_prefix=graph.table_prefix
            ),
        )

    def _resolve_instance_url(self, instance: ManagedInstance) -> str:
        if instance.mode == "captive":
            if self._captive_url_factory is None:
                raise GraphContentNotReadyError(
                    "Captive graph access is not ready yet."
                )
            return self._captive_url_factory()
        return build_postgres_url(instance)

    async def _inspect_schema_usage(
        self,
        db: GPGraph,
        schema_name: str,
        *,
        sample_limit: int = 3,
    ) -> GraphSchemaUsage:
        node_page = await db.search_nodes(
            SearchQuery(
                filter=Filter(field="schema_name", value=schema_name),
                limit=max(1, sample_limit),
            )
        )
        edge_page = await db.search_edges(
            SearchQuery(
                filter=Filter(field="schema_name", value=schema_name),
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

    async def _inspect_node_delete_blockers(
        self,
        db: GPGraph,
        node_id: str,
        *,
        sample_limit: int = 3,
    ) -> GraphNodeDeleteBlockers:
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
            blockers.sample_edge_ids = [
                item.id for item in edge_page.items[:sample_limit]
            ]
        blockers.can_delete = not (blockers.child_count or blockers.incident_edge_count)
        return blockers
