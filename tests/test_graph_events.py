import pytest
import pytest_asyncio
from datetime import datetime, timedelta, timezone

from gpdb import (
    EdgeUpsert,
    EventFilter,
    GPGraph,
    NodeCreatedEvent,
    NodeDeletedEvent,
    NodeDestinationEdgeCreatedEvent,
    NodeDestinationEdgeDeletedEvent,
    NodeDestinationEdgeUpdatedEvent,
    NodeOriginEdgeCreatedEvent,
    NodeOriginEdgeDeletedEvent,
    NodeOriginEdgeUpdatedEvent,
    NodeUpdatedEvent,
    NodeUpsert,
    SchemaRef,
    SchemaUpsert,
    SchemaValidationError,
    graph_event_stable_sort_key,
)


@pytest_asyncio.fixture
async def db(pg_server):
    url = pg_server.get_uri()
    g = GPGraph(url)
    await g.create_tables()
    yield g
    await g.drop_tables()
    await g.sqla_engine.dispose()


@pytest.mark.asyncio
async def test_node_lifecycle_events(db: GPGraph):
    received: list = []

    async def listen(graph, prefix, events):
        received.extend(events)

    db.register_event_listener(listen)

    created_list = await db.set_nodes([NodeUpsert(type="__default__", data={"n": 1})])
    nid = created_list[0].id

    assert len(received) == 1
    assert isinstance(received[0], NodeCreatedEvent)
    assert received[0].node_id == nid
    assert received[0].table_prefix == ""

    await db.set_nodes([NodeUpsert(id=nid, type="__default__", data={"n": 2})])
    assert len(received) == 2
    assert isinstance(received[1], NodeUpdatedEvent)

    await db.delete_nodes([nid])
    assert len(received) == 3
    assert isinstance(received[2], NodeDeletedEvent)
    assert received[2].node_id == nid


@pytest.mark.asyncio
async def test_payload_updates_emit_node_updated(db: GPGraph):
    received: list = []

    async def listen(graph, prefix, events):
        received.extend(events)

    db.register_event_listener(listen)
    n = (await db.set_nodes([NodeUpsert(type="__default__", data={})]))[0]

    received.clear()
    await db.set_node_payload(n.id, b"x", mime="application/octet-stream")
    assert len(received) == 1
    assert isinstance(received[0], NodeUpdatedEvent)

    received.clear()
    await db.clear_node_payload(n.id)
    assert len(received) == 1
    assert isinstance(received[0], NodeUpdatedEvent)


@pytest.mark.asyncio
async def test_edge_create_emits_origin_and_destination(db: GPGraph):
    received: list = []

    async def listen(graph, prefix, events):
        received.extend(events)

    db.register_event_listener(listen)

    a = (await db.set_nodes([NodeUpsert(type="__default__", data={})]))[0]
    b = (await db.set_nodes([NodeUpsert(type="__default__", data={})]))[0]
    received.clear()

    edges = await db.set_edges(
        [EdgeUpsert(type="__default__", source_id=a.id, target_id=b.id, data={})]
    )
    eid = edges[0].id

    assert len(received) == 2
    assert isinstance(received[0], NodeOriginEdgeCreatedEvent)
    assert isinstance(received[1], NodeDestinationEdgeCreatedEvent)
    assert received[0].edge_id == eid
    assert received[1].edge_id == eid


@pytest.mark.asyncio
async def test_edge_update_emits_origin_and_destination(db: GPGraph):
    received: list = []

    async def listen(graph, prefix, events):
        received.extend(events)

    db.register_event_listener(listen)

    a = (await db.set_nodes([NodeUpsert(type="__default__", data={})]))[0]
    b = (await db.set_nodes([NodeUpsert(type="__default__", data={})]))[0]

    edges = await db.set_edges(
        [EdgeUpsert(type="__default__", source_id=a.id, target_id=b.id, data={"k": "v1"})]
    )
    eid = edges[0].id

    received.clear()
    await db.set_edges(
        [EdgeUpsert(id=eid, type="__default__", source_id=a.id, target_id=b.id, data={"k": "v2"})]
    )

    assert len(received) == 2
    assert isinstance(received[0], NodeOriginEdgeUpdatedEvent)
    assert isinstance(received[1], NodeDestinationEdgeUpdatedEvent)
    assert received[0].edge_id == eid
    assert received[1].edge_id == eid


@pytest.mark.asyncio
async def test_edge_type_filters(db: GPGraph):
    person = {
        "type": "object",
        "x-gpdb-kind": "node",
        "properties": {"name": {"type": "string"}},
    }
    follows = {
        "type": "object",
        "x-gpdb-kind": "edge",
        "properties": {},
    }
    await db.set_schemas(
        [
            SchemaUpsert(name="person", json_schema=person, kind="node"),
            SchemaUpsert(name="follows", json_schema=follows, kind="edge"),
        ]
    )

    alice = (await db.set_nodes([NodeUpsert(type="person", data={"name": "a"})]))[0]
    bob = (await db.set_nodes([NodeUpsert(type="person", data={"name": "b"})]))[0]

    received: list = []

    async def listen(graph, prefix, events):
        received.extend(events)

    db.register_event_listener(
        listen,
        filter=EventFilter(edge_types=["follows"], origin_types=["person"]),
    )

    await db.set_edges(
        [EdgeUpsert(type="follows", source_id=alice.id, target_id=bob.id, data={})]
    )
    assert len(received) == 2

    db.unregister_event_listener(str(id(listen)))
    received.clear()
    db.register_event_listener(
        listen,
        filter=EventFilter(edge_types=["follows"], origin_types=["other"]),
    )
    await db.set_edges(
        [EdgeUpsert(type="follows", source_id=alice.id, target_id=bob.id, data={})]
    )
    assert len(received) == 0


@pytest.mark.asyncio
async def test_delete_events_match_type_filters(db: GPGraph):
    person = {
        "type": "object",
        "x-gpdb-kind": "node",
        "properties": {"name": {"type": "string"}},
    }
    follows = {
        "type": "object",
        "x-gpdb-kind": "edge",
        "properties": {},
    }
    await db.set_schemas(
        [
            SchemaUpsert(name="person", json_schema=person, kind="node"),
            SchemaUpsert(name="follows", json_schema=follows, kind="edge"),
        ]
    )
    alice = (await db.set_nodes([NodeUpsert(type="person", data={"name": "a"})]))[0]
    bob = (await db.set_nodes([NodeUpsert(type="person", data={"name": "b"})]))[0]
    edge = (
        await db.set_edges(
            [EdgeUpsert(type="follows", source_id=alice.id, target_id=bob.id, data={})]
        )
    )[0]

    received: list = []

    async def listen(graph, prefix, events):
        received.extend(events)

    await db.delete_edges([edge.id])

    db.register_event_listener(
        listen,
        filter=EventFilter(
            node_deleted=True,
            node_types=["person"],
        ),
    )
    await db.delete_nodes([alice.id])
    assert len(received) == 1
    assert isinstance(received[0], NodeDeletedEvent)
    assert received[0].node_type == "person"

    db.unregister_event_listener(str(id(listen)))
    received.clear()
    charlie = (await db.set_nodes([NodeUpsert(type="person", data={"name": "c"})]))[0]
    edge2 = (
        await db.set_edges(
            [EdgeUpsert(type="follows", source_id=bob.id, target_id=charlie.id, data={})]
        )
    )[0]
    db.register_event_listener(
        listen,
        filter=EventFilter(
            node_origin_edge_deleted=True,
            node_destination_edge_deleted=True,
            edge_types=["follows"],
        ),
    )
    await db.delete_edges([edge2.id])
    assert len(received) == 2
    assert isinstance(received[0], NodeOriginEdgeDeletedEvent)
    assert isinstance(received[1], NodeDestinationEdgeDeletedEvent)
    assert received[0].edge_type == "follows"
    assert received[1].edge_type == "follows"


@pytest.mark.asyncio
async def test_node_type_star_inheritance_filter(db: GPGraph):
    base = {
        "type": "object",
        "x-gpdb-kind": "node",
        "properties": {"a": {"type": "integer"}},
    }
    child = {
        "type": "object",
        "x-gpdb-kind": "node",
        "properties": {"b": {"type": "string"}},
    }
    await db.set_schemas(
        [
            SchemaUpsert(name="base_t", json_schema=base, kind="node"),
            SchemaUpsert(name="child_t", json_schema=child, kind="node", extends=["base_t"]),
        ]
    )

    received: list = []

    async def listen(graph, prefix, events):
        received.extend(events)

    db.register_event_listener(listen, filter=EventFilter(node_types=["base_t"]))
    await db.set_nodes([NodeUpsert(type="child_t", data={"a": 1, "b": "x"})])
    assert len(received) == 0

    db.unregister_event_listener(str(id(listen)))
    db.register_event_listener(listen, filter=EventFilter(node_types=["base_t*"]))
    await db.set_nodes([NodeUpsert(type="child_t", data={"a": 2, "b": "y"})])
    assert len(received) == 1
    assert isinstance(received[0], NodeCreatedEvent)


@pytest.mark.asyncio
async def test_event_filter_excludes_node_created(db: GPGraph):
    received: list = []

    async def listen(graph, prefix, events):
        received.extend(events)

    db.register_event_listener(listen, filter=EventFilter(node_created=False))

    n = (await db.set_nodes([NodeUpsert(type="__default__", data={})]))[0]
    assert len(received) == 0

    await db.set_nodes([NodeUpsert(id=n.id, type="__default__", data={"x": 1})])
    assert len(received) == 1
    assert isinstance(received[0], NodeUpdatedEvent)


@pytest.mark.asyncio
async def test_listener_priority_order(db: GPGraph):
    order: list[str] = []

    async def low(_g, _p, _e):
        order.append("low")

    async def high(_g, _p, _e):
        order.append("high")

    db.register_event_listener(low, listener_id="low", priority=0)
    db.register_event_listener(high, listener_id="high", priority=10)

    await db.set_nodes([NodeUpsert(type="__default__", data={})])
    assert order == ["high", "low"]


@pytest.mark.asyncio
async def test_transaction_single_dispatch(db: GPGraph):
    batches: list[list] = []

    async def listen(graph, prefix, events):
        batches.append(list(events))

    db.register_event_listener(listen)

    async with db.transaction():
        await db.set_nodes([NodeUpsert(type="__default__", data={"i": 1})])
        await db.set_nodes([NodeUpsert(type="__default__", data={"i": 2})])

    assert len(batches) == 1
    assert len(batches[0]) == 2
    assert all(isinstance(e, NodeCreatedEvent) for e in batches[0])


@pytest.mark.asyncio
async def test_transaction_rollback_no_dispatch(db: GPGraph):
    count = 0

    async def listen(graph, prefix, events):
        nonlocal count
        count += 1

    db.register_event_listener(listen)

    with pytest.raises(RuntimeError, match="abort"):
        async with db.transaction():
            await db.set_nodes([NodeUpsert(type="__default__", data={})])
            raise RuntimeError("abort")

    assert count == 0


@pytest.mark.asyncio
async def test_standalone_set_nodes_dispatches(db: GPGraph):
    count = 0

    async def listen(graph, prefix, events):
        nonlocal count
        count += 1

    db.register_event_listener(listen)
    await db.set_nodes([NodeUpsert(type="__default__", data={})])
    assert count == 1


@pytest.mark.asyncio
async def test_register_duplicate_listener_id(db: GPGraph):
    async def a(g, p, e):
        pass

    async def b(g, p, e):
        pass

    db.register_event_listener(a, listener_id="x")
    with pytest.raises(ValueError, match="already registered"):
        db.register_event_listener(b, listener_id="x")


@pytest.mark.asyncio
async def test_list_change_events_since_sorted(db: GPGraph):
    t0 = datetime.now(timezone.utc) - timedelta(seconds=1)
    await db.set_nodes([NodeUpsert(type="__default__", data={"k": "a"})])
    await db.set_nodes([NodeUpsert(type="__default__", data={"k": "b"})])
    page = await db.list_change_events_since(t0, EventFilter(), limit=500, offset=0)
    kinds = [e.kind for e in page.items]
    assert kinds.count("node_created") >= 2
    assert page.total >= 2
    keys = [graph_event_stable_sort_key(e) for e in page.items]
    assert keys == sorted(keys)


@pytest.mark.asyncio
async def test_list_change_events_since_pagination(db: GPGraph):
    t0 = datetime.now(timezone.utc) - timedelta(seconds=1)
    for i in range(5):
        await db.set_nodes([NodeUpsert(type="__default__", data={"i": i})])

    collected = []
    offset = 0
    limit = 2
    while True:
        page = await db.list_change_events_since(
            t0, EventFilter(), limit=limit, offset=offset
        )
        assert page.limit == limit
        assert page.offset == offset
        collected.extend(page.items)
        if len(page.items) < limit:
            break
        offset += limit

    assert page.total == len(collected)
    mono = await db.list_change_events_since(
        t0, EventFilter(), limit=500, offset=0
    )
    assert [graph_event_stable_sort_key(e) for e in collected] == [
        graph_event_stable_sort_key(e) for e in mono.items
    ]


@pytest.mark.asyncio
async def test_list_change_events_since_edge_pair_stable_order(db: GPGraph):
    t0 = datetime.now(timezone.utc) - timedelta(seconds=1)
    a = (await db.set_nodes([NodeUpsert(type="__default__", data={})]))[0]
    b = (await db.set_nodes([NodeUpsert(type="__default__", data={})]))[0]
    edges = await db.set_edges(
        [EdgeUpsert(type="__default__", source_id=a.id, target_id=b.id, data={})]
    )
    page = await db.list_change_events_since(t0, EventFilter(), limit=50, offset=0)
    eid = edges[0].id
    edge_evs = [
        e for e in page.items if "_edge_" in e.kind and getattr(e, "edge_id", "") == eid
    ]
    assert len(edge_evs) == 2
    assert edge_evs[0].occurred_at == edge_evs[1].occurred_at
    keys = [graph_event_stable_sort_key(e) for e in edge_evs]
    assert keys == sorted(keys)
    assert keys[0][1] == "node_destination_edge_created"
    assert keys[1][1] == "node_origin_edge_created"


@pytest.mark.asyncio
async def test_list_change_events_since_limit_offset_validation(db: GPGraph):
    t0 = datetime.now(timezone.utc)
    with pytest.raises(ValueError, match="limit"):
        await db.list_change_events_since(t0, EventFilter(), limit=0)
    with pytest.raises(ValueError, match="offset"):
        await db.list_change_events_since(t0, EventFilter(), offset=-1)


@pytest.mark.asyncio
async def test_migrate_schema_dispatches_updates(db: GPGraph):
    person_schema_v1 = {
        "type": "object",
        "x-gpdb-kind": "node",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.set_schemas([SchemaUpsert(name="person", json_schema=person_schema_v1, kind="node")])

    n = (
        await db.set_nodes([NodeUpsert(type="person", data={"name": "Alice", "age": 30})])
    )[0]

    captured: list = []

    async def listen(graph, prefix, events):
        captured.extend(events)

    db.register_event_listener(listen)

    person_schema_v2 = {
        "type": "object",
        "x-gpdb-kind": "node",
        "properties": {
            "name": {"type": "string"},
            "age_years": {"type": "integer"},
        },
        "required": ["name"],
    }

    def migrate_age_to_age_years(old_data):
        new_data = old_data.copy()
        if "age" in new_data:
            new_data["age_years"] = new_data.pop("age")
        return new_data

    await db.migrate_schema(
        name="person",
        migration_func=migrate_age_to_age_years,
        new_schema=person_schema_v2,
        kind="node",
    )

    node_updates = [e for e in captured if isinstance(e, NodeUpdatedEvent)]
    assert any(e.node_id == n.id for e in node_updates)


@pytest.mark.asyncio
async def test_migrate_schema_validation_failure_no_dispatch(db: GPGraph):
    person_schema_v1 = {
        "type": "object",
        "x-gpdb-kind": "node",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.set_schemas([SchemaUpsert(name="person", json_schema=person_schema_v1, kind="node")])

    await db.set_nodes([NodeUpsert(type="person", data={"name": "Alice", "age": 30})])

    count = 0

    async def listen(graph, prefix, events):
        nonlocal count
        count += len(events)

    db.register_event_listener(listen)

    bad_v2 = {
        "type": "object",
        "x-gpdb-kind": "node",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name", "age_years"],
    }

    def bad_migrate(d):
        return {"name": d["name"]}

    with pytest.raises(SchemaValidationError):
        await db.migrate_schema(
            name="person",
            migration_func=bad_migrate,
            new_schema=bad_v2,
            kind="node",
        )

    assert count == 0


@pytest.mark.asyncio
async def test_update_event_listener_filter(db: GPGraph):
    received: list = []

    async def listen(graph, prefix, events):
        received.extend(events)

    lid = db.register_event_listener(
        listen, filter=EventFilter(node_created=False, node_updated=False)
    )
    n = (await db.set_nodes([NodeUpsert(type="__default__", data={})]))[0]
    assert len(received) == 0

    db.update_event_listener(lid, filter=EventFilter(node_updated=True))
    await db.set_nodes([NodeUpsert(id=n.id, type="__default__", data={"z": 1})])
    assert len(received) == 1
