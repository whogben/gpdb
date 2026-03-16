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
    created = await db.set_node(node)
    assert created.id is not None
    assert created.version == 1

    # 2. Read
    fetched = await db.get_node(created.id)
    assert fetched is not None
    assert fetched.id == created.id
    assert fetched.data == {"foo": "bar"}

    # 3. Update
    updated_node = NodeUpsert(id=fetched.id, type="test", data={"foo": "baz"})
    updated = await db.set_node(updated_node)
    assert updated.data == {"foo": "baz"}
    assert updated.version == 2

    # 4. Delete
    await db.delete_node(created.id)
    assert await db.get_node(created.id) is None


@pytest.mark.asyncio
async def test_payload(db: GPGraph):
    payload = b"some binary data"
    node = NodeUpsert(type="test", payload=payload, payload_filename="blob.bin")
    created = await db.set_node(node)

    # Fetch without payload (default behavior of get_node)
    fetched = await db.get_node(created.id)
    assert fetched is not None

    # Fetch with payload
    fetched_full = await db.get_node_with_payload(created.id)
    assert fetched_full.payload == payload

    # Check metadata auto-population
    assert fetched_full.payload_size == len(payload)
    assert fetched_full.payload_hash is not None
    assert fetched_full.payload_mime == "application/octet-stream"
    assert fetched_full.payload_filename == "blob.bin"


@pytest.mark.asyncio
async def test_zero_byte_payload(db: GPGraph):
    node = NodeUpsert(type="test", payload=b"", payload_filename="empty.txt")
    created = await db.set_node(node)

    fetched_full = await db.get_node_with_payload(created.id)
    assert fetched_full.payload == b""
    assert fetched_full.payload_size == 0
    assert fetched_full.payload_hash is not None
    assert fetched_full.payload_filename == "empty.txt"


@pytest.mark.asyncio
async def test_edge_crud(db: GPGraph):
    n1 = await db.set_node(NodeUpsert(type="test"))
    n2 = await db.set_node(NodeUpsert(type="test"))

    edge = EdgeUpsert(source_id=n1.id, target_id=n2.id, type="link")
    created = await db.set_edge(edge)
    assert created.id is not None

    fetched = await db.get_edge(created.id)
    assert fetched is not None
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
    updated = await db.set_edge(updated_edge)
    assert updated.data == {"weight": 10}

    await db.delete_edge(created.id)
    assert await db.get_edge(created.id) is None


@pytest.mark.asyncio
async def test_referential_integrity(db: GPGraph):
    """
    Verify we cannot delete nodes that are referenced by other nodes or edges.
    """
    n1 = await db.set_node(NodeUpsert(type="test"))
    n2 = await db.set_node(NodeUpsert(type="test"))
    edge = await db.set_edge(EdgeUpsert(source_id=n1.id, target_id=n2.id, type="link"))

    # Try to delete source
    with pytest.raises(IntegrityError):
        await db.delete_node(n1.id)

    # Try to delete target
    with pytest.raises(IntegrityError):
        await db.delete_node(n2.id)

    # Delete edge first
    await db.delete_edge(edge.id)
    # Now nodes can be deleted
    await db.delete_node(n1.id)
    await db.delete_node(n2.id)


@pytest.mark.asyncio
async def test_transaction_rollback(db: GPGraph):
    """
    Verify that exceptions inside a transaction block roll back changes.
    """
    node = await db.set_node(NodeUpsert(type="test"))

    try:
        async with db.transaction():
            # This should be rolled back
            await db.delete_node(node.id)
            raise RuntimeError("Oops")
    except RuntimeError:
        pass

    # Verify node still exists
    assert await db.get_node(node.id) is not None


@pytest.mark.asyncio
async def test_node_hierarchy(db: GPGraph):
    """
    Verify parent/child relationships and constraints.
    """
    # Create parent
    parent = await db.set_node(NodeUpsert(type="folder", name="root"))

    # Create child
    child1 = await db.set_node(
        NodeUpsert(type="file", name="config", parent_id=parent.id)
    )

    # Verify child relationship
    assert child1.parent_id == parent.id

    # 1. Test Uniqueness (same parent, same name)
    with pytest.raises(IntegrityError):
        await db.set_node(NodeUpsert(type="file", name="config", parent_id=parent.id))

    # 2. Test Null Names (should be allowed multiple times)
    child2 = await db.set_node(NodeUpsert(type="file", name=None, parent_id=parent.id))
    child3 = await db.set_node(NodeUpsert(type="file", name=None, parent_id=parent.id))
    assert child2.id != child3.id

    # 3. Test Delete Restriction (cannot delete parent with children)
    with pytest.raises(IntegrityError):
        await db.delete_node(parent.id)

    # Cleanup children
    await db.delete_node(child1.id)
    await db.delete_node(child2.id)
    await db.delete_node(child3.id)

    # Now parent can be deleted
    await db.delete_node(parent.id)
    assert await db.get_node(parent.id) is None


@pytest.mark.asyncio
async def test_get_node_child(db: GPGraph):
    """
    Verify get_node_child functionality.
    """
    # Create parent
    parent = await db.set_node(NodeUpsert(type="folder", name="root"))

    # Create child with payload
    payload = b"child payload"
    child = await db.set_node(
        NodeUpsert(type="file", name="config", parent_id=parent.id, payload=payload)
    )

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
    node = await db.set_node(NodeUpsert(type="test"))

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

    # Verify node metadata via get_node_with_payload
    node_with_payload = await db.get_node_with_payload(node.id)
    assert node_with_payload.payload == payload
    assert node_with_payload.payload_filename == "notes.txt"


@pytest.mark.asyncio
async def test_clear_node_payload(db: GPGraph):
    node = await db.set_node(
        NodeUpsert(type="test", payload=b"payload", payload_filename="notes.txt")
    )

    cleared = await db.clear_node_payload(node.id)
    assert cleared.payload_size == 0
    assert cleared.payload_hash is None
    assert cleared.payload_mime is None
    assert cleared.payload_filename is None

    node_with_payload = await db.get_node_with_payload(node.id)
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
    main_node = await main.set_node(NodeUpsert(type="test", data={"src": "main"}))
    scratch_node = await scratch.set_node(
        NodeUpsert(type="test", data={"src": "scratch"})
    )

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
    assert await main.get_node(main_node.id) is not None

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
    n1 = await scratch.set_node(NodeUpsert(type="test"))
    n2 = await scratch.set_node(NodeUpsert(type="test"))
    edge = await scratch.set_edge(
        EdgeUpsert(source_id=n1.id, target_id=n2.id, type="link")
    )

    # Verify edge exists
    fetched = await scratch.get_edge(edge.id)
    assert fetched is not None
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
    node = await view_db.set_node(NodeUpsert(type="test", data={"src": "view"}))
    assert node.id is not None

    # Verify node exists
    fetched = await view_db.get_node(node.id)
    assert fetched is not None

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
    """When generate_id returns an existing id, set_node retries with a new id and succeeds."""
    collide_id = "col-xx-xxxx"
    unique_id = "uni-yy-yyyy"
    with patch("gpdb.graph.generate_id", side_effect=[collide_id, unique_id]):
        await db.set_node(NodeUpsert(id=collide_id, type="test", data={}))
        created = await db.set_node(NodeUpsert(type="test", data={"x": 1}))
    assert created.id == unique_id
    assert (await db.get_node(unique_id)).data == {"x": 1}


@pytest.mark.asyncio
async def test_node_id_collision_retry_exhausted(db: GPGraph):
    """When generate_id always returns the same existing id, set_node raises after max attempts."""
    same_id = "same-id-always"
    with patch("gpdb.graph.generate_id", return_value=same_id):
        await db.set_node(NodeUpsert(id=same_id, type="test", data={}))
        with pytest.raises(
            RuntimeError,
            match="Failed to generate unique node ID after 10 attempts",
        ):
            await db.set_node(NodeUpsert(type="test", data={"y": 1}))
