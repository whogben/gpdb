"""
Event listener registry, post-commit dispatch, and retroactive event queries.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, List

from sqlalchemy import or_, select

from gpdb.events import (
    EventFilter,
    GraphEvent,
    NodeCreatedEvent,
    NodeDeletedEvent,
    NodeDestinationEdgeCreatedEvent,
    NodeDestinationEdgeDeletedEvent,
    NodeDestinationEdgeUpdatedEvent,
    NodeOriginEdgeCreatedEvent,
    NodeOriginEdgeDeletedEvent,
    NodeOriginEdgeUpdatedEvent,
    NodeUpdatedEvent,
    build_star_expansion_sets,
    filter_events,
    graph_event_stable_sort_key,
)
from gpdb.models.base import _normalize_schema_kind
from gpdb.search.query import Page

GPDB_EVENTS_BUFFER_KEY = "_gpdb_events"

EventListenerHandler = Callable[
    [Any, str, List[GraphEvent]], Awaitable[None]
]


@dataclass
class _RegisteredEventListener:
    listener_id: str
    priority: int
    filter: EventFilter
    handler: EventListenerHandler
    seq: int


class EventListenersMixin:
    """Register async listeners; dispatch after successful DB commit."""

    def _invalidate_event_star_sets_cache(self) -> None:
        self._event_star_sets_cache = None

    async def _load_event_star_sets(
        self,
    ) -> tuple[dict[str, frozenset[str]], dict[str, frozenset[str]]]:
        async with self.sqla_sessionmaker() as session:
            async with session.begin():
                result = await session.execute(select(self._Schema))
                rows = []
                for s in result.scalars().all():
                    kind = _normalize_schema_kind(s.kind)
                    extends = list(s.extends or [])
                    rows.append((s.name, kind, extends))
        return build_star_expansion_sets(rows)

    async def _ensure_event_star_sets(
        self,
    ) -> tuple[dict[str, frozenset[str]], dict[str, frozenset[str]]]:
        if self._event_star_sets_cache is None:
            self._event_star_sets_cache = await self._load_event_star_sets()
        return self._event_star_sets_cache

    def _record_graph_events(self, session: Any, new_events: list[GraphEvent]) -> None:
        if not new_events:
            return
        buf: list[GraphEvent] = session.info.setdefault(GPDB_EVENTS_BUFFER_KEY, [])
        buf.extend(new_events)

    async def _dispatch_committed_events(self, session: Any) -> None:
        events = session.info.pop(GPDB_EVENTS_BUFFER_KEY, None)
        if not events:
            return
        node_sets, edge_sets = await self._ensure_event_star_sets()
        ordered = sorted(
            self._event_listeners,
            key=lambda e: (-e.priority, e.seq),
        )
        for ent in ordered:
            filtered = filter_events(
                events,
                ent.filter,
                node_star_sets=node_sets,
                edge_star_sets=edge_sets,
            )
            if filtered:
                await ent.handler(self, self.table_prefix, filtered)

    def register_event_listener(
        self,
        handler: EventListenerHandler,
        *,
        listener_id: str | None = None,
        filter: EventFilter | None = None,
        priority: int = 0,
    ) -> str:
        """
        Register an async listener called after each successful write transaction.

        If ``listener_id`` is omitted, ``str(id(handler))`` is used.

        Listener signature: ``async def fn(graph, table_prefix, events) -> None``.

        If a listener raises after commit, the exception propagates to the caller;
        data is already persisted.
        """
        lid = listener_id if listener_id is not None else str(id(handler))
        if any(e.listener_id == lid for e in self._event_listeners):
            raise ValueError(f"Event listener id already registered: {lid!r}")
        self._event_listener_seq += 1
        self._event_listeners.append(
            _RegisteredEventListener(
                listener_id=lid,
                priority=priority,
                filter=filter if filter is not None else EventFilter(),
                handler=handler,
                seq=self._event_listener_seq,
            )
        )
        return lid

    def unregister_event_listener(self, listener_id: str) -> None:
        before = len(self._event_listeners)
        self._event_listeners[:] = [
            e for e in self._event_listeners if e.listener_id != listener_id
        ]
        if len(self._event_listeners) == before:
            raise KeyError(listener_id)

    def update_event_listener(
        self,
        listener_id: str,
        *,
        filter: EventFilter | None = None,
        priority: int | None = None,
    ) -> None:
        for ent in self._event_listeners:
            if ent.listener_id == listener_id:
                if filter is not None:
                    patch = filter.model_dump(exclude_unset=True)
                    ent.filter = ent.filter.model_copy(update=patch)
                if priority is not None:
                    ent.priority = priority
                return
        raise KeyError(listener_id)

    async def list_change_events_since(
        self,
        since_time: datetime,
        filter: EventFilter,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> Page[GraphEvent]:
        """
        Return a page of change events for rows with ``updated_at > since_time``.

        Events are sorted by a stable total order: ``occurred_at`` ascending (create → row
        ``created_at``, update → ``updated_at``, delete → ``deleted_at``), then ``kind``,
        then the row identifier: ``edge_id`` when ``kind`` contains ``_edge_``, otherwise
        ``node_id`` (see ``graph_event_stable_sort_key``). Filtering uses the same rules as
        live listeners.

        Rows are classified from timestamps only (payload vs data updates are not distinguished).
        Tombstoned rows produce delete events. Edge rows produce paired origin and destination
        events.

        Node rows are loaded in one query: ``updated_at > since_time`` plus any node id that
        appears as an edge endpoint among changed edges (so endpoint types are available even
        when those nodes did not change in the window).

        Raises:
            ValueError: If ``limit < 1`` or ``offset < 0``.
        """
        if since_time.tzinfo is None:
            since_time = since_time.replace(tzinfo=timezone.utc)
        if limit < 1:
            raise ValueError("limit must be at least 1")
        if offset < 0:
            raise ValueError("offset cannot be negative")

        async with self.sqla_sessionmaker() as session:
            async with session.begin():
                e_stmt = select(self._Edge).where(self._Edge.updated_at > since_time)
                e_res = await session.execute(e_stmt)
                edge_orms = list(e_res.scalars().all())

                endpoint_ids: set[str] = set()
                for e in edge_orms:
                    if e.source_id:
                        endpoint_ids.add(e.source_id)
                    if e.target_id:
                        endpoint_ids.add(e.target_id)

                if endpoint_ids:
                    n_stmt = select(self._Node).where(
                        or_(
                            self._Node.updated_at > since_time,
                            self._Node.id.in_(endpoint_ids),
                        )
                    )
                else:
                    n_stmt = select(self._Node).where(
                        self._Node.updated_at > since_time
                    )
                n_res = await session.execute(n_stmt)
                all_node_rows = list(n_res.scalars().all())

        node_orms = [n for n in all_node_rows if n.updated_at > since_time]
        node_types_map: dict[str, str | None] = {
            n.id: n.type for n in all_node_rows
        }

        merged: list[GraphEvent] = []

        prefix = self.table_prefix
        for orm in node_orms:
            if orm.deleted_at is not None:
                ev = NodeDeletedEvent(
                    table_prefix=prefix,
                    occurred_at=orm.deleted_at,
                    node_id=orm.id,
                    node_type=orm.type,
                )
            elif orm.created_at > since_time:
                ev = NodeCreatedEvent(
                    table_prefix=prefix,
                    occurred_at=orm.created_at,
                    node_id=orm.id,
                    node_type=orm.type,
                )
            else:
                ev = NodeUpdatedEvent(
                    table_prefix=prefix,
                    occurred_at=orm.updated_at,
                    node_id=orm.id,
                    node_type=orm.type,
                )
            merged.append(ev)

        for eorm in edge_orms:
            st = node_types_map.get(eorm.source_id) if eorm.source_id else None
            tt = node_types_map.get(eorm.target_id) if eorm.target_id else None
            if eorm.deleted_at is not None:
                o = NodeOriginEdgeDeletedEvent(
                    table_prefix=prefix,
                    occurred_at=eorm.deleted_at,
                    edge_id=eorm.id,
                    edge_type=eorm.type,
                    source_id=eorm.source_id or "",
                    target_id=eorm.target_id or "",
                    source_node_type=st,
                    target_node_type=tt,
                )
                d = NodeDestinationEdgeDeletedEvent(
                    table_prefix=prefix,
                    occurred_at=eorm.deleted_at,
                    edge_id=eorm.id,
                    edge_type=eorm.type,
                    source_id=eorm.source_id or "",
                    target_id=eorm.target_id or "",
                    source_node_type=st,
                    target_node_type=tt,
                )
                merged.append(o)
                merged.append(d)
            elif eorm.created_at > since_time:
                o = NodeOriginEdgeCreatedEvent(
                    table_prefix=prefix,
                    occurred_at=eorm.created_at,
                    edge_id=eorm.id,
                    edge_type=eorm.type,
                    source_id=eorm.source_id or "",
                    target_id=eorm.target_id or "",
                    source_node_type=st,
                    target_node_type=tt,
                )
                d = NodeDestinationEdgeCreatedEvent(
                    table_prefix=prefix,
                    occurred_at=eorm.created_at,
                    edge_id=eorm.id,
                    edge_type=eorm.type,
                    source_id=eorm.source_id or "",
                    target_id=eorm.target_id or "",
                    source_node_type=st,
                    target_node_type=tt,
                )
                merged.append(o)
                merged.append(d)
            else:
                o = NodeOriginEdgeUpdatedEvent(
                    table_prefix=prefix,
                    occurred_at=eorm.updated_at,
                    edge_id=eorm.id,
                    edge_type=eorm.type,
                    source_id=eorm.source_id or "",
                    target_id=eorm.target_id or "",
                    source_node_type=st,
                    target_node_type=tt,
                )
                d = NodeDestinationEdgeUpdatedEvent(
                    table_prefix=prefix,
                    occurred_at=eorm.updated_at,
                    edge_id=eorm.id,
                    edge_type=eorm.type,
                    source_id=eorm.source_id or "",
                    target_id=eorm.target_id or "",
                    source_node_type=st,
                    target_node_type=tt,
                )
                merged.append(o)
                merged.append(d)

        merged.sort(key=graph_event_stable_sort_key)
        node_sets, edge_sets = await self._ensure_event_star_sets()
        filtered = filter_events(
            merged,
            filter,
            node_star_sets=node_sets,
            edge_star_sets=edge_sets,
        )
        total = len(filtered)
        page_items = filtered[offset : offset + limit]
        return Page(
            items=page_items,
            total=total,
            limit=limit,
            offset=offset,
        )
