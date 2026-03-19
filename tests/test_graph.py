import pytest
import pytest_asyncio
from unittest.mock import patch

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import StaleDataError

from gpdb import (
    GPGraph,
    EdgeRead,
    EdgeUpsert,
    NodeRead,
    NodeReadWithPayload,
    NodeUpsert,
    SearchQuery,
    Filter,
    Op,
)


@pytest_asyncio.fixture
async def db(pg_server):
    """
    Creates a GraphDB instance connected to the temporary Postgres server.
    Creates tables before the test and drops them after to ensure isolation.
    """
    url = pg_server.get_uri()
    db = GPGraph(url)

    # Initialize schema
    await db.create_tables()

    yield db

    # Cleanup
    await db.drop_tables()

    await db.sqla_engine.dispose()


# --- Tests ---


@pytest.mark.asyncio
async def test_node_crud(db: GPGraph):
    # 1. Create
    node = NodeUpsert(type="test", data={"foo": "bar"})
    created_list = await db.set_nodes([node])
    assert len(created_list) == 1
    created = created_list[0]
    assert created.id is not None
    assert created.version == 1

    # 2. Read
    fetched_list = await db.get_nodes([created.id])
    assert len(fetched_list) == 1
    fetched = fetched_list[0]
    assert fetched.id == created.id
    assert fetched.data == {"foo": "bar"}

    # 3. Update
    updated_node = NodeUpsert(id=fetched.id, type="test", data={"foo": "baz"})
    updated_list = await db.set_nodes([updated_node])
    assert len(updated_list) == 1
    updated = updated_list[0]
    assert updated.data == {"foo": "baz"}
    assert updated.version == 2

    # 4. Delete
    await db.delete_nodes([created.id])
    with pytest.raises(ValueError, match="Node ids not found"):
        await db.get_nodes([created.id])


@pytest.mark.asyncio
async def test_set_nodes_update_omitted_data_and_tags_preserves_existing(db: GPGraph):
    node_list = await db.set_nodes(
        [NodeUpsert(type="test", data={"foo": "bar"}, tags=["t1", "t2"])]
    )
    node = node_list[0]

    # Omit `data` and `tags` in the update DTO; existing values must remain.
    updated_list = await db.set_nodes([NodeUpsert(id=node.id, type="test")])
    updated = updated_list[0]

    assert updated.data == {"foo": "bar"}
    assert updated.tags == ["t1", "t2"]


@pytest.mark.asyncio
async def test_set_edges_update_omitted_data_and_tags_preserves_existing(db: GPGraph):
    n1_list = await db.set_nodes([NodeUpsert(type="test", data={"a": 1})])
    n2_list = await db.set_nodes([NodeUpsert(type="test", data={"b": 2})])
    n1 = n1_list[0]
    n2 = n2_list[0]

    edge_list = await db.set_edges(
        [
            EdgeUpsert(
                type="link",
                source_id=n1.id,
                target_id=n2.id,
                data={"weight": 10},
                tags=["urgent"],
            )
        ]
    )
    edge = edge_list[0]

    updated_list = await db.set_edges(
        [
            EdgeUpsert(
                id=edge.id,
                type="link",
                source_id=n1.id,
                target_id=n2.id,
            )
        ]
    )
    updated = updated_list[0]

    assert updated.data == {"weight": 10}
    assert updated.tags == ["urgent"]


@pytest.mark.asyncio
async def test_payload(db: GPGraph):
    payload = b"some binary data"
    node = NodeUpsert(type="test", payload=payload, payload_filename="blob.bin")
    created_list = await db.set_nodes([node])
    assert len(created_list) == 1
    created = created_list[0]

    # Fetch without payload (default behavior of get_nodes)
    fetched_list = await db.get_nodes([created.id])
    assert len(fetched_list) == 1
    fetched = fetched_list[0]

    # Fetch with payload
    fetched_full_list = await db.get_node_payloads([created.id])
    fetched_full = fetched_full_list[0]
    assert fetched_full.payload == payload

    # Check metadata auto-population
    assert fetched_full.payload_size == len(payload)
    assert fetched_full.payload_hash is not None
    assert fetched_full.payload_mime == "application/octet-stream"
    assert fetched_full.payload_filename == "blob.bin"


@pytest.mark.asyncio
async def test_zero_byte_payload(db: GPGraph):
    node = NodeUpsert(type="test", payload=b"", payload_filename="empty.txt")
    created_list = await db.set_nodes([node])
    assert len(created_list) == 1
    created = created_list[0]

    fetched_full_list = await db.get_node_payloads([created.id])
    fetched_full = fetched_full_list[0]
    assert fetched_full.payload == b""
    assert fetched_full.payload_size == 0
    assert fetched_full.payload_hash is not None
    assert fetched_full.payload_filename == "empty.txt"


@pytest.mark.asyncio
async def test_get_node_payloads_bulk(db: GPGraph):
    """Test bulk retrieval of node payloads."""
    # Create multiple nodes with payloads
    n1_list = await db.set_nodes([NodeUpsert(type="test", payload=b"payload1", payload_filename="file1.txt")])
    n2_list = await db.set_nodes([NodeUpsert(type="test", payload=b"payload2", payload_filename="file2.txt")])
    n3_list = await db.set_nodes([NodeUpsert(type="test", payload=b"payload3", payload_filename="file3.txt")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]
    
    # Fetch all three in bulk
    results = await db.get_node_payloads([n1.id, n2.id, n3.id])
    assert len(results) == 3
    assert results[0].id == n1.id
    assert results[0].payload == b"payload1"
    assert results[1].id == n2.id
    assert results[1].payload == b"payload2"
    assert results[2].id == n3.id
    assert results[2].payload == b"payload3"


@pytest.mark.asyncio
async def test_get_node_payloads_without_payload(db: GPGraph):
    """Test that nodes without payload are still returned with id filled."""
    # Create a node without payload
    node_list = await db.set_nodes([NodeUpsert(type="test")])
    node = node_list[0]
    
    # Fetch it with get_node_payloads
    results = await db.get_node_payloads([node.id])
    assert len(results) == 1
    assert results[0].id == node.id
    assert results[0].payload is None
    assert results[0].payload_size == 0


@pytest.mark.asyncio
async def test_get_node_payloads_duplicate_ids(db: GPGraph):
    """Test that duplicate ids are rejected."""
    node_list = await db.set_nodes([NodeUpsert(type="test")])
    node = node_list[0]
    
    with pytest.raises(ValueError, match="Duplicate node ids provided"):
        await db.get_node_payloads([node.id, node.id])


@pytest.mark.asyncio
async def test_get_node_payloads_missing_id(db: GPGraph):
    """Test that missing ids cause the entire call to fail."""
    node_list = await db.set_nodes([NodeUpsert(type="test")])
    node = node_list[0]
    
    with pytest.raises(ValueError, match="Node\\(s\\) not found"):
        await db.get_node_payloads([node.id, "nonexistent-id"])


@pytest.mark.asyncio
async def test_get_node_payloads_preserves_order(db: GPGraph):
    """Test that results are returned in input order."""
    n1_list = await db.set_nodes([NodeUpsert(type="test", payload=b"a")])
    n2_list = await db.set_nodes([NodeUpsert(type="test", payload=b"b")])
    n3_list = await db.set_nodes([NodeUpsert(type="test", payload=b"c")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]
    
    # Request in reverse order
    results = await db.get_node_payloads([n3.id, n1.id, n2.id])
    assert results[0].id == n3.id
    assert results[1].id == n1.id
    assert results[2].id == n2.id


@pytest.mark.asyncio
async def test_edge_crud(db: GPGraph):
    n1_list = await db.set_nodes([NodeUpsert(type="test")])
    n2_list = await db.set_nodes([NodeUpsert(type="test")])
    n1 = n1_list[0]
    n2 = n2_list[0]

    edge = EdgeUpsert(source_id=n1.id, target_id=n2.id, type="link")
    created = (await db.set_edges([edge]))[0]
    assert created.id is not None

    fetched = (await db.get_edges([created.id]))[0]
    assert fetched.source_id == n1.id
    assert fetched.target_id == n2.id

    # Update edge
    updated_edge = EdgeUpsert(
        id=fetched.id,
        type="link",
        source_id=n1.id,
        target_id=n2.id,
        data={"weight": 10},
    )
    updated = (await db.set_edges([updated_edge]))[0]
    assert updated.data == {"weight": 10}

    await db.delete_edges([created.id])
    with pytest.raises(ValueError, match="Edge ids not found"):
        await db.get_edges([created.id])


@pytest.mark.asyncio
async def test_get_edges_bulk(db: GPGraph):
    """
    Test bulk get_edges operation with multiple edges.
    """
    # Create multiple edges
    n1_list = await db.set_nodes([NodeUpsert(type="test")])
    n2_list = await db.set_nodes([NodeUpsert(type="test")])
    n3_list = await db.set_nodes([NodeUpsert(type="test")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]

    edge1 = (await db.set_edges([EdgeUpsert(source_id=n1.id, target_id=n2.id, type="link1")]))[0]
    edge2 = (await db.set_edges([EdgeUpsert(source_id=n2.id, target_id=n3.id, type="link2")]))[0]
    edge3 = (await db.set_edges([EdgeUpsert(source_id=n1.id, target_id=n3.id, type="link3")]))[0]

    # Fetch all three edges in bulk
    edges = await db.get_edges([edge1.id, edge2.id, edge3.id])
    assert len(edges) == 3
    assert edges[0].id == edge1.id
    assert edges[1].id == edge2.id
    assert edges[2].id == edge3.id

    # Verify all edges have their identity fields
    for edge in edges:
        assert edge.id is not None
        assert edge.type is not None
        assert edge.source_id is not None
        assert edge.target_id is not None


@pytest.mark.asyncio
async def test_get_edges_duplicate_rejection(db: GPGraph):
    """
    Test that duplicate edge ids are rejected.
    """
    n1_list = await db.set_nodes([NodeUpsert(type="test")])
    n2_list = await db.set_nodes([NodeUpsert(type="test")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    edge = (await db.set_edges([EdgeUpsert(source_id=n1.id, target_id=n2.id, type="link")]))[0]

    # Try to fetch with duplicate ids
    with pytest.raises(ValueError, match="Duplicate edge ids provided"):
        await db.get_edges([edge.id, edge.id])


@pytest.mark.asyncio
async def test_get_edges_missing_id_failure(db: GPGraph):
    """
    Test that missing edge ids cause the entire call to fail.
    """
    n1_list = await db.set_nodes([NodeUpsert(type="test")])
    n2_list = await db.set_nodes([NodeUpsert(type="test")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    edge = (await db.set_edges([EdgeUpsert(source_id=n1.id, target_id=n2.id, type="link")]))[0]

    # Try to fetch with a non-existent id
    with pytest.raises(ValueError, match="Edge ids not found"):
        await db.get_edges([edge.id, "nonexistent-id"])


@pytest.mark.asyncio
async def test_get_edges_preserves_order(db: GPGraph):
    """
    Test that get_edges preserves input order in returned results.
    """
    n1_list = await db.set_nodes([NodeUpsert(type="test")])
    n2_list = await db.set_nodes([NodeUpsert(type="test")])
    n3_list = await db.set_nodes([NodeUpsert(type="test")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]

    edge1 = (await db.set_edges([EdgeUpsert(source_id=n1.id, target_id=n2.id, type="link1")]))[0]
    edge2 = (await db.set_edges([EdgeUpsert(source_id=n2.id, target_id=n3.id, type="link2")]))[0]
    edge3 = (await db.set_edges([EdgeUpsert(source_id=n1.id, target_id=n3.id, type="link3")]))[0]

    # Fetch in reverse order
    edges = await db.get_edges([edge3.id, edge1.id, edge2.id])
    assert len(edges) == 3
    assert edges[0].id == edge3.id
    assert edges[1].id == edge1.id
    assert edges[2].id == edge2.id


@pytest.mark.asyncio
async def test_referential_integrity(db: GPGraph):
    """
    Verify we cannot delete nodes that are referenced by other nodes or edges.
    """
    n1_list = await db.set_nodes([NodeUpsert(type="test")])
    n2_list = await db.set_nodes([NodeUpsert(type="test")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    edge = (await db.set_edges([EdgeUpsert(source_id=n1.id, target_id=n2.id, type="link")]))[0]

    # Try to delete source
    with pytest.raises(IntegrityError):
        await db.delete_nodes([n1.id])

    # Try to delete target
    with pytest.raises(IntegrityError):
        await db.delete_nodes([n2.id])

    # Delete edge first
    await db.delete_edges([edge.id])
    # Now nodes can be deleted
    await db.delete_nodes([n1.id, n2.id])


@pytest.mark.asyncio
async def test_set_edges_bulk_create(db: GPGraph):
    """
    Test bulk creation of multiple edges.
    """
    # Create nodes
    n1_list = await db.set_nodes([NodeUpsert(type="test")])
    n2_list = await db.set_nodes([NodeUpsert(type="test")])
    n3_list = await db.set_nodes([NodeUpsert(type="test")])
    n4_list = await db.set_nodes([NodeUpsert(type="test")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]
    n4 = n4_list[0]

    # Create multiple edges in bulk
    edges = [
        EdgeUpsert(source_id=n1.id, target_id=n2.id, type="link1"),
        EdgeUpsert(source_id=n2.id, target_id=n3.id, type="link2"),
        EdgeUpsert(source_id=n3.id, target_id=n4.id, type="link3"),
    ]
    results = await db.set_edges(edges)

    assert len(results) == 3
    assert all(e.id is not None for e in results)
    assert results[0].type == "link1"
    assert results[1].type == "link2"
    assert results[2].type == "link3"


@pytest.mark.asyncio
async def test_set_edges_bulk_update(db: GPGraph):
    """
    Test bulk update of multiple edges.
    """
    # Create nodes and edges
    n1_list = await db.set_nodes([NodeUpsert(type="test")])
    n2_list = await db.set_nodes([NodeUpsert(type="test")])
    n3_list = await db.set_nodes([NodeUpsert(type="test")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]

    edge1 = (await db.set_edges([EdgeUpsert(source_id=n1.id, target_id=n2.id, type="link", data={"weight": 1})]))[0]
    edge2 = (await db.set_edges([EdgeUpsert(source_id=n2.id, target_id=n3.id, type="link", data={"weight": 2})]))[0]

    # Update both edges in bulk
    updated_edges = [
        EdgeUpsert(id=edge1.id, source_id=n1.id, target_id=n2.id, type="link", data={"weight": 10}),
        EdgeUpsert(id=edge2.id, source_id=n2.id, target_id=n3.id, type="link", data={"weight": 20}),
    ]
    results = await db.set_edges(updated_edges)

    assert len(results) == 2
    assert results[0].data["weight"] == 10
    assert results[1].data["weight"] == 20


@pytest.mark.asyncio
async def test_set_edges_duplicate_ids(db: GPGraph):
    """
    Test that duplicate edge ids are rejected.
    """
    n1_list = await db.set_nodes([NodeUpsert(type="test")])
    n2_list = await db.set_nodes([NodeUpsert(type="test")])
    n1 = n1_list[0]
    n2 = n2_list[0]

    # Try to create edges with duplicate ids
    edges = [
        EdgeUpsert(id="duplicate-id", source_id=n1.id, target_id=n2.id, type="link1"),
        EdgeUpsert(id="duplicate-id", source_id=n1.id, target_id=n2.id, type="link2"),
    ]
    with pytest.raises(ValueError, match="Duplicate edge ids provided"):
        await db.set_edges(edges)


@pytest.mark.asyncio
async def test_set_edges_atomic_failure(db: GPGraph):
    """
    Test that set_edges fails atomically when one edge cannot be created.
    """
    from gpdb import SchemaUpsert, SchemaValidationError

    n1_list = await db.set_nodes([NodeUpsert(type="test")])
    n2_list = await db.set_nodes([NodeUpsert(type="test")])
    n3_list = await db.set_nodes([NodeUpsert(type="test")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]

    # Create a schema that will fail validation
    await db.set_schemas([SchemaUpsert(
        name="strict_edge",
        kind="edge",
        json_schema={"type": "object", "required": ["weight"], "properties": {"weight": {"type": "number"}}}
    )])

    # Try to create edges where one will fail validation
    edges = [
        EdgeUpsert(source_id=n1.id, target_id=n2.id, type="link1", schema_name="strict_edge", data={"weight": 1}),
        EdgeUpsert(source_id=n2.id, target_id=n3.id, type="link2", schema_name="strict_edge", data={"invalid": "data"}),
    ]
    with pytest.raises(SchemaValidationError):
        await db.set_edges(edges)

    # Verify no edges were created (atomic failure)
    all_edges = await db.search_edges(SearchQuery(filter=Filter(field="type", op=Op.IN, value=["link1", "link2"])))
    assert all_edges.total == 0


@pytest.mark.asyncio
async def test_transaction_rollback(db: GPGraph):
    """
    Verify that exceptions inside a transaction block roll back changes.
    """
    node_list = await db.set_nodes([NodeUpsert(type="test")])
    node = node_list[0]

    try:
        async with db.transaction():
            # This should be rolled back
            await db.delete_nodes([node.id])
            raise RuntimeError("Oops")
    except RuntimeError:
        pass

    # Verify node still exists
    fetched_list = await db.get_nodes([node.id])
    assert len(fetched_list) == 1


@pytest.mark.asyncio
async def test_node_hierarchy(db: GPGraph):
    """
    Verify parent/child relationships and constraints.
    """
    # Create parent
    parent_list = await db.set_nodes([NodeUpsert(type="folder", name="root")])
    parent = parent_list[0]

    # Create child
    child1_list = await db.set_nodes([
        NodeUpsert(type="file", name="config", parent_id=parent.id)
    ])
    child1 = child1_list[0]

    # Verify child relationship
    assert child1.parent_id == parent.id

    # 1. Test Uniqueness (same parent, same name)
    with pytest.raises(IntegrityError):
        await db.set_nodes([NodeUpsert(type="file", name="config", parent_id=parent.id)])

    # 2. Test Null Names (should be allowed multiple times)
    child2_list = await db.set_nodes([NodeUpsert(type="file", name=None, parent_id=parent.id)])
    child3_list = await db.set_nodes([NodeUpsert(type="file", name=None, parent_id=parent.id)])
    child2 = child2_list[0]
    child3 = child3_list[0]
    assert child2.id != child3.id

    # 3. Test Delete Restriction (cannot delete parent with children)
    with pytest.raises(IntegrityError):
        await db.delete_nodes([parent.id])

    # Cleanup children
    await db.delete_nodes([child1.id, child2.id, child3.id])

    # Now parent can be deleted
    await db.delete_nodes([parent.id])
    with pytest.raises(ValueError, match="Node ids not found"):
        await db.get_nodes([parent.id])


@pytest.mark.asyncio
async def test_get_node_child(db: GPGraph):
    """
    Verify get_node_child functionality.
    """
    # Create parent
    parent_list = await db.set_nodes([NodeUpsert(type="folder", name="root")])
    parent = parent_list[0]

    # Create child with payload
    payload = b"child payload"
    child_list = await db.set_nodes([
        NodeUpsert(type="file", name="config", parent_id=parent.id, payload=payload)
    ])
    child = child_list[0]

    # Test fetch success without payload
    fetched = await db.get_node_child(parent.id, "config")
    assert fetched is not None
    assert fetched.id == child.id

    # Test fetch failure (wrong name)
    assert await db.get_node_child(parent.id, "wrong_name") is None

    # Test fetch failure (wrong parent)
    assert await db.get_node_child("wrong_parent", "config") is None


@pytest.mark.asyncio
async def test_get_node_payload(db: GPGraph):
    """
    Test get_node_payload and set_node_payload methods.
    """
    # Create node without payload
    node_list = await db.set_nodes([NodeUpsert(type="test")])
    node = node_list[0]

    # Get payload (should be None)
    assert await db.get_node_payload(node.id) is None

    # Set payload (returns updated NodeRead)
    payload = b"new payload data"
    updated = await db.set_node_payload(
        node.id, payload, "text/plain", filename="notes.txt"
    )
    assert updated.payload_size == len(payload)
    assert updated.payload_mime == "text/plain"
    assert updated.payload_filename == "notes.txt"

    # Get payload
    fetched_payload = await db.get_node_payload(node.id)
    assert fetched_payload == payload

    # Verify node metadata via get_node_payloads
    node_with_payload_list = await db.get_node_payloads([node.id])
    node_with_payload = node_with_payload_list[0]
    assert node_with_payload.payload == payload
    assert node_with_payload.payload_filename == "notes.txt"


@pytest.mark.asyncio
async def test_clear_node_payload(db: GPGraph):
    node_list = await db.set_nodes([
        NodeUpsert(type="test", payload=b"payload", payload_filename="notes.txt")
    ])
    node = node_list[0]

    cleared = await db.clear_node_payload(node.id)
    assert cleared.payload_size == 0
    assert cleared.payload_hash is None
    assert cleared.payload_mime is None
    assert cleared.payload_filename is None

    node_with_payload_list = await db.get_node_payloads([node.id])
    node_with_payload = node_with_payload_list[0]
    assert node_with_payload.payload is None


# --- Side Table Tests ---


@pytest.mark.asyncio
async def test_side_table_isolation(pg_server):
    """
    Verify that side tables are isolated from main tables.
    """
    url = pg_server.get_uri()

    # Main DB (default tables)
    main = GPGraph(url)
    await main.create_tables()

    # Side table DB (prefixed tables)
    scratch = GPGraph(url, table_prefix="scratch")
    await scratch.create_tables()

    # Create nodes in each
    main_node_list = await main.set_nodes([NodeUpsert(type="test", data={"src": "main"})])
    main_node = main_node_list[0]
    scratch_node_list = await scratch.set_nodes([
        NodeUpsert(type="test", data={"src": "scratch"})
    ])
    scratch_node = scratch_node_list[0]

    # Verify isolation - search on main doesn't see scratch
    main_results = await main.search_nodes(SearchQuery(filter="type eq test"))
    assert len(main_results.items) == 1
    assert main_results.items[0].data["src"] == "main"

    # Verify isolation - search on scratch doesn't see main
    scratch_results = await scratch.search_nodes(SearchQuery(filter="type eq test"))
    assert len(scratch_results.items) == 1
    assert scratch_results.items[0].data["src"] == "scratch"

    # Drop scratch tables, main still works
    await scratch.drop_tables()
    fetched_list = await main.get_nodes([main_node.id])
    assert len(fetched_list) == 1

    # Cleanup
    await main.drop_tables()
    await main.sqla_engine.dispose()
    await scratch.sqla_engine.dispose()


@pytest.mark.asyncio
async def test_side_table_edges(pg_server):
    """
    Verify that edges in side tables reference nodes in the same table.
    """
    url = pg_server.get_uri()

    scratch = GPGraph(url, table_prefix="scratch2")
    await scratch.create_tables()

    # Create nodes and edge in scratch table
    n1_list = await scratch.set_nodes([NodeUpsert(type="test")])
    n2_list = await scratch.set_nodes([NodeUpsert(type="test")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    edge = (await scratch.set_edges(
        [EdgeUpsert(source_id=n1.id, target_id=n2.id, type="link")]
    ))[0]

    # Verify edge exists
    fetched = (await scratch.get_edges([edge.id]))[0]
    assert fetched.source_id == n1.id

    # Cleanup
    await scratch.drop_tables()
    await scratch.sqla_engine.dispose()


@pytest.mark.asyncio
async def test_drop_tables_for_prefix(pg_server):
    """
    Verify that drop_tables_for_prefix correctly drops prefixed tables.
    """
    url = pg_server.get_uri()

    # Create a GPGraph with a specific prefix
    view_db = GPGraph(url, table_prefix="view_test123")
    await view_db.create_tables()

    # Create a node to verify tables exist
    node_list = await view_db.set_nodes([NodeUpsert(type="test", data={"src": "view"})])
    node = node_list[0]
    assert node.id is not None

    # Verify node exists
    fetched_list = await view_db.get_nodes([node.id])
    assert len(fetched_list) == 1

    # Drop tables using the prefix (must be within transaction)
    async with view_db.transaction():
        await view_db.drop_tables_for_prefix("view_test123")

    # Verify tables are gone by trying to create a new GPGraph with same prefix
    # This should work without errors since tables were dropped
    view_db2 = GPGraph(url, table_prefix="view_test123")
    await view_db2.create_tables()

    # Cleanup
    await view_db2.drop_tables()
    await view_db2.sqla_engine.dispose()
    await view_db.sqla_engine.dispose()


# --- ID collision retry (integration: real DB, patched ID to force collision) ---


@pytest.mark.asyncio
async def test_node_id_collision_retry_succeeds(db: GPGraph):
    """When generate_id returns an existing id, set_nodes retries with a new id and succeeds."""
    collide_id = "col-xx-xxxx"
    unique_id = "uni-yy-yyyy"
    with patch("gpdb.graph_nodes.generate_id", side_effect=[collide_id, unique_id]):
        await db.set_nodes([NodeUpsert(id=collide_id, type="test", data={})])
        created_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    created = created_list[0]
    assert created.id == unique_id
    fetched_list = await db.get_nodes([unique_id])
    assert fetched_list[0].data == {"x": 1}


@pytest.mark.asyncio
async def test_node_id_collision_retry_exhausted(db: GPGraph):
    """When generate_id always returns the same existing id, set_nodes raises after max attempts."""
    same_id = "same-id-always"
    with patch("gpdb.graph_nodes.generate_id", return_value=same_id):
        await db.set_nodes([NodeUpsert(id=same_id, type="test", data={})])
        with pytest.raises(
            RuntimeError,
            match="Failed to generate unique node ID after 10 attempts",
        ):
            await db.set_nodes([NodeUpsert(type="test", data={"y": 1})])


# --- Bulk get_nodes tests ---


@pytest.mark.asyncio
async def test_get_nodes_multiple(db: GPGraph):
    """Test getting multiple nodes at once."""
    # Create multiple nodes
    node1_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    node2_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 2})])
    node3_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 3})])
    node1 = node1_list[0]
    node2 = node2_list[0]
    node3 = node3_list[0]

    # Fetch all three
    fetched = await db.get_nodes([node1.id, node2.id, node3.id])
    assert len(fetched) == 3
    assert fetched[0].id == node1.id
    assert fetched[0].data == {"x": 1}
    assert fetched[1].id == node2.id
    assert fetched[1].data == {"x": 2}
    assert fetched[2].id == node3.id
    assert fetched[2].data == {"x": 3}


@pytest.mark.asyncio
async def test_get_nodes_preserves_order(db: GPGraph):
    """Test that get_nodes preserves input order."""
    # Create multiple nodes
    node1_list = await db.set_nodes([NodeUpsert(type="test", data={"order": 1})])
    node2_list = await db.set_nodes([NodeUpsert(type="test", data={"order": 2})])
    node3_list = await db.set_nodes([NodeUpsert(type="test", data={"order": 3})])
    node1 = node1_list[0]
    node2 = node2_list[0]
    node3 = node3_list[0]

    # Fetch in reverse order
    fetched = await db.get_nodes([node3.id, node1.id, node2.id])
    assert len(fetched) == 3
    assert fetched[0].id == node3.id
    assert fetched[1].id == node1.id
    assert fetched[2].id == node2.id


@pytest.mark.asyncio
async def test_get_nodes_duplicate_ids(db: GPGraph):
    """Test that get_nodes rejects duplicate ids."""
    node_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    node = node_list[0]

    with pytest.raises(ValueError, match="Duplicate node ids provided"):
        await db.get_nodes([node.id, node.id])


@pytest.mark.asyncio
async def test_get_nodes_missing_id(db: GPGraph):
    """Test that get_nodes fails when any requested id is missing."""
    node_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    node = node_list[0]

    with pytest.raises(ValueError, match="Node ids not found"):
        await db.get_nodes([node.id, "nonexistent-id"])


@pytest.mark.asyncio
async def test_get_nodes_empty_list(db: GPGraph):
    """Test that get_nodes returns empty list for empty input."""
    fetched = await db.get_nodes([])
    assert fetched == []


@pytest.mark.asyncio
async def test_get_nodes_single_item(db: GPGraph):
    """Test that get_nodes works correctly with a single item."""
    node_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    node = node_list[0]

    fetched = await db.get_nodes([node.id])
    assert len(fetched) == 1
    assert fetched[0].id == node.id
    assert fetched[0].data == {"x": 1}


@pytest.mark.asyncio
async def test_delete_edges_bulk(db: GPGraph):
    """Test bulk deletion of multiple edges."""
    # Create nodes
    n1_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    n2_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 2})])
    n3_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 3})])
    n4_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 4})])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]
    n4 = n4_list[0]

    # Create edges
    e1 = (await db.set_edges([EdgeUpsert(type="link", source_id=n1.id, target_id=n2.id)]))[0]
    e2 = (await db.set_edges([EdgeUpsert(type="link", source_id=n2.id, target_id=n3.id)]))[0]
    e3 = (await db.set_edges([EdgeUpsert(type="link", source_id=n3.id, target_id=n4.id)]))[0]

    # Delete multiple edges at once
    await db.delete_edges([e1.id, e2.id, e3.id])

    # Verify all edges are deleted
    with pytest.raises(ValueError, match="Edge ids not found"):
        await db.get_edges([e1.id, e2.id, e3.id])


@pytest.mark.asyncio
async def test_delete_edges_duplicate_ids(db: GPGraph):
    """Test that duplicate edge IDs are rejected."""
    # Create nodes and edge
    n1_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    n2_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 2})])
    n1 = n1_list[0]
    n2 = n2_list[0]
    e1 = (await db.set_edges([EdgeUpsert(type="link", source_id=n1.id, target_id=n2.id)]))[0]

    # Try to delete with duplicate IDs
    with pytest.raises(ValueError, match="Duplicate edge ids provided"):
        await db.delete_edges([e1.id, e1.id])

    # Verify edge still exists
    fetched = await db.get_edges([e1.id])
    assert len(fetched) == 1
    assert fetched[0].id == e1.id


@pytest.mark.asyncio
async def test_delete_edges_missing_id(db: GPGraph):
    """Test that missing edge IDs cause the entire batch to fail."""
    # Create nodes and edge
    n1_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    n2_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 2})])
    n1 = n1_list[0]
    n2 = n2_list[0]
    e1 = (await db.set_edges([EdgeUpsert(type="link", source_id=n1.id, target_id=n2.id)]))[0]

    # Try to delete with a non-existent ID
    with pytest.raises(ValueError, match="Edge ids not found"):
        await db.delete_edges([e1.id, "nonexistent-id"])

    # Verify edge still exists (atomic all-or-nothing)
    fetched = await db.get_edges([e1.id])
    assert len(fetched) == 1
    assert fetched[0].id == e1.id


@pytest.mark.asyncio
async def test_delete_edges_single_item(db: GPGraph):
    """Test that delete_edges works correctly with a single item."""
    # Create nodes and edge
    n1_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    n2_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 2})])
    n1 = n1_list[0]
    n2 = n2_list[0]
    e1 = (await db.set_edges([EdgeUpsert(type="link", source_id=n1.id, target_id=n2.id)]))[0]

    # Delete single edge using bulk method
    await db.delete_edges([e1.id])

    # Verify edge is deleted
    with pytest.raises(ValueError, match="Edge ids not found"):
        await db.get_edges([e1.id])


@pytest.mark.asyncio
async def test_delete_edges_empty_list(db: GPGraph):
    """Test that delete_edges works with empty list."""
    # Should not raise any error
    await db.delete_edges([])


@pytest.mark.asyncio
async def test_delete_nodes_multiple(db: GPGraph):
    """Test that delete_nodes works correctly with multiple nodes."""
    # Create multiple nodes
    n1_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    n2_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 2})])
    n3_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 3})])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]

    # Delete all three nodes
    await db.delete_nodes([n1.id, n2.id, n3.id])

    # Verify all nodes are deleted
    with pytest.raises(ValueError, match="Node ids not found"):
        await db.get_nodes([n1.id, n2.id, n3.id])


@pytest.mark.asyncio
async def test_delete_nodes_single(db: GPGraph):
    """Test that delete_nodes works correctly with a single item."""
    # Create node
    n1_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    n1 = n1_list[0]

    # Delete single node using bulk method
    await db.delete_nodes([n1.id])

    # Verify node is deleted
    with pytest.raises(ValueError, match="Node ids not found"):
        await db.get_nodes([n1.id])


@pytest.mark.asyncio
async def test_delete_nodes_missing_id_fails_atomic(db: GPGraph):
    """Test that delete_nodes fails the entire batch if any requested id is missing."""
    n1_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    n2_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 2})])
    n1 = n1_list[0]
    n2 = n2_list[0]

    with pytest.raises(ValueError, match="Node ids not found"):
        await db.delete_nodes([n1.id, "nonexistent-id"])

    # Verify the operation did not partially delete.
    fetched = await db.get_nodes([n1.id, n2.id])
    assert len(fetched) == 2


@pytest.mark.asyncio
async def test_delete_nodes_empty_list(db: GPGraph):
    """Test that delete_nodes works with empty list."""
    # Should not raise any error
    await db.delete_nodes([])


@pytest.mark.asyncio
async def test_delete_nodes_duplicates(db: GPGraph):
    """Test that delete_nodes rejects duplicate ids."""
    # Create node
    n1_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    n1 = n1_list[0]

    # Try to delete with duplicate id
    with pytest.raises(ValueError, match="Duplicate node ids"):
        await db.delete_nodes([n1.id, n1.id])

    # Verify node still exists
    fetched = await db.get_nodes([n1.id])
    assert len(fetched) == 1


@pytest.mark.asyncio
async def test_delete_nodes_atomic_failure(db: GPGraph):
    """Test that delete_nodes fails atomically when one node cannot be deleted."""
    # Create nodes
    n1_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 1})])
    n2_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 2})])
    n3_list = await db.set_nodes([NodeUpsert(type="test", data={"x": 3})])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]

    # Create an edge from n1 to n2
    edge = (await db.set_edges([EdgeUpsert(type="link", source_id=n1.id, target_id=n2.id)]))[0]

    # Try to delete all three nodes - should fail because n1 and n2 have an edge
    with pytest.raises(IntegrityError):
        await db.delete_nodes([n1.id, n2.id, n3.id])

    # Verify all nodes still exist (atomic failure)
    fetched = await db.get_nodes([n1.id, n2.id, n3.id])
    assert len(fetched) == 3

    # Clean up
    await db.delete_edges([edge.id])
    await db.delete_nodes([n1.id, n2.id, n3.id])
