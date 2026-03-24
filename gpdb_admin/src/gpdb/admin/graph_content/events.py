"""Event polling operations for graph-content service."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from gpdb import EventFilter
from gpdb.admin.store import AdminUser

from gpdb.admin.graph_content._helpers import (
    open_graph,
    require_admin_store,
    validate_page_limit,
    validate_page_offset,
)
from gpdb.admin.graph_content.models import (
    GraphChangeEventFilter,
    GraphChangeEventFilters,
    GraphChangeEventList,
    GraphChangeEventRecord,
)


def _to_core_event_filter(filter_model: GraphChangeEventFilter) -> EventFilter:
    """Convert admin event filter model into the core gpdb EventFilter."""
    return EventFilter(**filter_model.model_dump())


def _serialize_change_event(event: Any) -> GraphChangeEventRecord:
    """Project one core graph event into a stable admin response shape."""
    return GraphChangeEventRecord(
        kind=event.kind,
        table_prefix=event.table_prefix,
        occurred_at=event.occurred_at.isoformat(),
        node_id=getattr(event, "node_id", None),
        node_type=getattr(event, "node_type", None),
        edge_id=getattr(event, "edge_id", None),
        edge_type=getattr(event, "edge_type", None),
        source_id=getattr(event, "source_id", None),
        target_id=getattr(event, "target_id", None),
        source_node_type=getattr(event, "source_node_type", None),
        target_node_type=getattr(event, "target_node_type", None),
    )


async def list_graph_change_events(
    self,
    *,
    graph_id: str,
    since_time: datetime,
    event_filter: GraphChangeEventFilter | None = None,
    limit: int = 50,
    offset: int = 0,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> GraphChangeEventList:
    """Return paginated graph change events since one timestamp."""
    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="view",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        normalized_event_filter = (
            event_filter if event_filter is not None else GraphChangeEventFilter()
        )
        page = await db.list_change_events_since(
            since_time=since_time,
            filter=_to_core_event_filter(normalized_event_filter),
            limit=validate_page_limit(limit),
            offset=validate_page_offset(offset),
        )
        return GraphChangeEventList(
            items=[_serialize_change_event(item) for item in page.items],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
            filters=GraphChangeEventFilters(
                since_time=since_time.isoformat(),
                event_filter=normalized_event_filter,
            ),
        )
    finally:
        await db.sqla_engine.dispose()
