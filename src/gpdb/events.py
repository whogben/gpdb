"""
Graph change events, filters, and type-matching for event listeners.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field

from gpdb.models.base import SchemaKind

# ---------------------------------------------------------------------------
# Event models (discriminated by ``kind``)
# ---------------------------------------------------------------------------


class GraphEventBase(BaseModel):
    """Shared fields for all graph events."""

    table_prefix: str
    occurred_at: datetime
    model_config = ConfigDict(frozen=True)


class NodeCreatedEvent(GraphEventBase):
    kind: Literal["node_created"] = "node_created"
    node_id: str
    node_type: str | None


class NodeUpdatedEvent(GraphEventBase):
    kind: Literal["node_updated"] = "node_updated"
    node_id: str
    node_type: str | None


class NodeDeletedEvent(GraphEventBase):
    kind: Literal["node_deleted"] = "node_deleted"
    node_id: str
    node_type: str | None


class NodeOriginEdgeCreatedEvent(GraphEventBase):
    kind: Literal["node_origin_edge_created"] = "node_origin_edge_created"
    edge_id: str
    edge_type: str | None
    source_id: str
    target_id: str
    source_node_type: str | None
    target_node_type: str | None


class NodeOriginEdgeUpdatedEvent(GraphEventBase):
    kind: Literal["node_origin_edge_updated"] = "node_origin_edge_updated"
    edge_id: str
    edge_type: str | None
    source_id: str
    target_id: str
    source_node_type: str | None
    target_node_type: str | None


class NodeOriginEdgeDeletedEvent(GraphEventBase):
    kind: Literal["node_origin_edge_deleted"] = "node_origin_edge_deleted"
    edge_id: str
    edge_type: str | None
    source_id: str
    target_id: str
    source_node_type: str | None
    target_node_type: str | None


class NodeDestinationEdgeCreatedEvent(GraphEventBase):
    kind: Literal["node_destination_edge_created"] = "node_destination_edge_created"
    edge_id: str
    edge_type: str | None
    source_id: str
    target_id: str
    source_node_type: str | None
    target_node_type: str | None


class NodeDestinationEdgeUpdatedEvent(GraphEventBase):
    kind: Literal["node_destination_edge_updated"] = "node_destination_edge_updated"
    edge_id: str
    edge_type: str | None
    source_id: str
    target_id: str
    source_node_type: str | None
    target_node_type: str | None


class NodeDestinationEdgeDeletedEvent(GraphEventBase):
    kind: Literal["node_destination_edge_deleted"] = "node_destination_edge_deleted"
    edge_id: str
    edge_type: str | None
    source_id: str
    target_id: str
    source_node_type: str | None
    target_node_type: str | None


GraphEvent = Annotated[
    Union[
        NodeCreatedEvent,
        NodeUpdatedEvent,
        NodeDeletedEvent,
        NodeOriginEdgeCreatedEvent,
        NodeOriginEdgeUpdatedEvent,
        NodeOriginEdgeDeletedEvent,
        NodeDestinationEdgeCreatedEvent,
        NodeDestinationEdgeUpdatedEvent,
        NodeDestinationEdgeDeletedEvent,
    ],
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# Event filter
# ---------------------------------------------------------------------------


class EventFilter(BaseModel):
    """
    Select which events a listener receives.

    For each event category, ``None`` means include (default). ``False`` excludes.
    ``True`` explicitly includes (equivalent to ``None`` for matching).

    Type lists are optional; when set, an event must match at least one pattern
    in the relevant category. A pattern ending with ``*`` matches that base type
    and all registered descendant schema types for the same kind (node vs edge).
    """

    node_created: bool | None = None
    node_updated: bool | None = None
    node_deleted: bool | None = None
    node_origin_edge_created: bool | None = None
    node_origin_edge_updated: bool | None = None
    node_origin_edge_deleted: bool | None = None
    node_destination_edge_created: bool | None = None
    node_destination_edge_updated: bool | None = None
    node_destination_edge_deleted: bool | None = None

    node_types: list[str] | None = None
    edge_types: list[str] | None = None
    origin_types: list[str] | None = None
    destination_types: list[str] | None = None

    model_config = ConfigDict(extra="forbid")


def _kind_enabled(filter: EventFilter, field: str) -> bool:
    v = getattr(filter, field)
    return v is not False


def _actual_matches_patterns(
    actual: str | None,
    patterns: list[str] | None,
    star_sets: dict[str, frozenset[str]],
) -> bool:
    if patterns is None:
        return True
    if actual is None:
        return False
    for p in patterns:
        if p.endswith("*"):
            base = p[:-1]
            allowed = star_sets.get(base, frozenset({base}))
            if actual in allowed:
                return True
        elif actual == p:
            return True
    return False


def event_matches_filter(
    event: GraphEvent,
    filter: EventFilter,
    *,
    node_star_sets: dict[str, frozenset[str]],
    edge_star_sets: dict[str, frozenset[str]],
) -> bool:
    """Return whether ``event`` passes ``filter`` (kind flags + type patterns)."""
    k = event.kind
    if k == "node_created" and not _kind_enabled(filter, "node_created"):
        return False
    if k == "node_updated" and not _kind_enabled(filter, "node_updated"):
        return False
    if k == "node_deleted" and not _kind_enabled(filter, "node_deleted"):
        return False
    if k == "node_origin_edge_created" and not _kind_enabled(
        filter, "node_origin_edge_created"
    ):
        return False
    if k == "node_origin_edge_updated" and not _kind_enabled(
        filter, "node_origin_edge_updated"
    ):
        return False
    if k == "node_origin_edge_deleted" and not _kind_enabled(
        filter, "node_origin_edge_deleted"
    ):
        return False
    if k == "node_destination_edge_created" and not _kind_enabled(
        filter, "node_destination_edge_created"
    ):
        return False
    if k == "node_destination_edge_updated" and not _kind_enabled(
        filter, "node_destination_edge_updated"
    ):
        return False
    if k == "node_destination_edge_deleted" and not _kind_enabled(
        filter, "node_destination_edge_deleted"
    ):
        return False

    if k in ("node_created", "node_updated"):
        return _actual_matches_patterns(
            event.node_type, filter.node_types, node_star_sets
        )
    if k == "node_deleted":
        return _actual_matches_patterns(
            event.node_type, filter.node_types, node_star_sets
        )

    if k.startswith("node_origin_edge_") or k.startswith("node_destination_edge_"):
        if not _actual_matches_patterns(
            event.edge_type, filter.edge_types, edge_star_sets
        ):
            return False
        if not _actual_matches_patterns(
            event.source_node_type, filter.origin_types, node_star_sets
        ):
            return False
        if not _actual_matches_patterns(
            event.target_node_type, filter.destination_types, node_star_sets
        ):
            return False
        return True

    return True


def build_star_expansion_sets(
    schema_rows: list[tuple[str, SchemaKind, list[str]]],
) -> tuple[dict[str, frozenset[str]], dict[str, frozenset[str]]]:
    """
    For each registered schema name (per kind), compute the set of type names
    that match ``name*`` (the name itself plus all transitive descendants).
    """
    node_children: dict[str, list[str]] = {}
    edge_children: dict[str, list[str]] = {}
    node_names: set[str] = set()
    edge_names: set[str] = set()

    for name, kind, extends in schema_rows:
        if kind == "node":
            node_names.add(name)
        else:
            edge_names.add(name)
        for parent in extends:
            if kind == "node":
                node_children.setdefault(parent, []).append(name)
            else:
                edge_children.setdefault(parent, []).append(name)

    def closure(all_names: set[str], children: dict[str, list[str]]) -> dict[str, frozenset[str]]:
        result: dict[str, frozenset[str]] = {}
        for base in all_names:
            seen: set[str] = set()
            stack = [base]
            while stack:
                cur = stack.pop()
                if cur in seen:
                    continue
                seen.add(cur)
                for ch in children.get(cur, []):
                    if ch not in seen:
                        stack.append(ch)
            result[base] = frozenset(seen)
        return result

    node_sets = closure(node_names, node_children)
    edge_sets = closure(edge_names, edge_children)
    return node_sets, edge_sets


def filter_events(
    events: list[GraphEvent],
    filter: EventFilter,
    *,
    node_star_sets: dict[str, frozenset[str]],
    edge_star_sets: dict[str, frozenset[str]],
) -> list[GraphEvent]:
    return [
        e
        for e in events
        if event_matches_filter(
            e, filter, node_star_sets=node_star_sets, edge_star_sets=edge_star_sets
        )
    ]


def graph_event_stable_sort_key(event: GraphEvent) -> tuple[datetime, str, str]:
    """
    Total-order key for change events (ascending).

    Orders by ``occurred_at`` (naive values are treated as UTC), then ``kind``
    (lexicographic), then the row id: ``edge_id`` when ``kind`` contains
    ``_edge_``, otherwise ``node_id``. This disambiguates bulk writes that share
    the same timestamp and fixes ordering between paired origin/destination
    edge events.
    """
    occurred = event.occurred_at
    if occurred.tzinfo is None:
        occurred = occurred.replace(tzinfo=timezone.utc)
    entity = event.edge_id if "_edge_" in event.kind else event.node_id
    return (occurred, event.kind, entity)
