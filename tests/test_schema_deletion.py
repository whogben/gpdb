import pytest
import pytest_asyncio
from gpdb import GPGraph, NodeUpsert, EdgeUpsert


# --- Tests ---


@pytest.mark.asyncio
async def test_delete_schema_blocked_when_referenced(db: GPGraph):
    """
    Test that deleting a schema fails if any nodes or edges reference it.
    """
    from gpdb import SchemaInUseError

    # Register a schema
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    }
    await db.register_schema(name="person_delete", schema=person_schema)

    # Create a node that references the schema
    node = NodeUpsert(
        type="person", schema_name="person_delete", data={"name": "Alice"}
    )
    await db.set_node(node)

    # Try to delete the schema (should fail)
    with pytest.raises(SchemaInUseError):
        await db.delete_schema("person_delete")


@pytest.mark.asyncio
async def test_delete_schema_blocked_when_referenced_by_edge(db: GPGraph):
    """
    Test that deleting a schema fails if any edges reference it.
    """
    from gpdb import SchemaInUseError

    # Register a schema for edges
    relationship_schema = {
        "type": "object",
        "properties": {
            "weight": {"type": "number"},
        },
        "required": ["weight"],
    }
    await db.register_schema(
        name="relationship_delete",
        schema=relationship_schema,
        kind="edge",
    )

    # Create two nodes
    node1 = NodeUpsert(type="test", data={"label": "A"})
    node2 = NodeUpsert(type="test", data={"label": "B"})
    result1 = await db.set_node(node1)
    result2 = await db.set_node(node2)

    # Create an edge that references the schema
    edge = EdgeUpsert(
        source_id=result1.id,
        target_id=result2.id,
        type="connected",
        schema_name="relationship_delete",
        data={"weight": 0.5},
    )
    await db.set_edge(edge)

    # Try to delete the schema (should fail)
    with pytest.raises(SchemaInUseError):
        await db.delete_schema("relationship_delete")


@pytest.mark.asyncio
async def test_delete_schema_success_when_unused(db: GPGraph):
    """
    Test that deleting a schema succeeds when no nodes/edges reference it.
    """
    # Register a schema
    unused_schema = {
        "type": "object",
        "properties": {
            "value": {"type": "string"},
        },
    }
    await db.register_schema(name="unused", schema=unused_schema)

    # Verify schema exists
    schema = await db.get_schema("unused")
    assert schema is not None

    # Delete the schema (should succeed)
    await db.delete_schema("unused")

    # Verify schema no longer exists
    schema = await db.get_schema("unused")
    assert schema is None


@pytest.mark.asyncio
async def test_update_node_preserves_schema(db: GPGraph):
    """
    Test that updating a node without providing schema_name preserves the existing schema.
    Per requirements: "If you don't pass schema_name in the update, the system preserves
    the existing schema and validates against it automatically."
    """
    from gpdb import SchemaValidationError

    # Register a schema
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.register_schema(name="person_preserve", schema=person_schema)

    # Create a node with schema_name
    node = NodeUpsert(
        type="person", schema_name="person_preserve", data={"name": "Alice", "age": 30}
    )
    result = await db.set_node(node)
    assert result.schema_name == "person_preserve"

    # Update the node without providing schema_name
    # The existing schema should be preserved and validation should still apply
    updated_node = NodeUpsert(
        id=result.id, type="person", data={"name": "Alice", "age": 31}
    )
    updated_result = await db.set_node(updated_node)

    # Verify schema_name is preserved
    assert updated_result.schema_name == "person_preserve"

    # Verify data was updated
    assert updated_result.data["age"] == 31

    # Verify validation still applies (try invalid data)
    invalid_node = NodeUpsert(id=result.id, type="person", data={"age": 32})
    with pytest.raises(SchemaValidationError):
        await db.set_node(invalid_node)
