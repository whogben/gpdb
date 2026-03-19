import pytest
import pytest_asyncio
from gpdb import GPGraph, NodeUpsert, EdgeUpsert, SchemaUpsert, SchemaRef


# --- Tests ---


@pytest.mark.asyncio
async def test_delete_schemas_blocked_when_referenced(db: GPGraph):
    """
    Test that deleting schemas fails if any nodes or edges reference them.
    """
    from gpdb import SchemaInUseError

    # Register schemas
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    }
    await db.set_schemas([SchemaUpsert(name="person_delete", json_schema=person_schema, kind="node")])

    unused_schema = {
        "type": "object",
        "properties": {
            "value": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="unused_delete", json_schema=unused_schema, kind="node")])

    # Create a node that references the first schema
    node = NodeUpsert(
        type="person", schema_name="person_delete", data={"name": "Alice"}
    )
    await db.set_nodes([node])

    # Try to delete both schemas (should fail because person_delete is in use)
    with pytest.raises(SchemaInUseError):
        await db.delete_schemas([SchemaRef(name="person_delete", kind="node"), SchemaRef(name="unused_delete", kind="node")])

    # Verify unused_delete still exists (atomic all-or-nothing)
    schemas = await db.get_schemas([SchemaRef(name="unused_delete", kind="node")])
    assert len(schemas) == 1


@pytest.mark.asyncio
async def test_delete_schemas_blocked_when_referenced_by_edge(db: GPGraph):
    """
    Test that deleting schemas fails if any edges reference them.
    """
    from gpdb import SchemaInUseError

    # Register schemas for edges
    relationship_schema = {
        "type": "object",
        "properties": {
            "weight": {"type": "number"},
        },
        "required": ["weight"],
    }
    await db.set_schemas(
        [SchemaUpsert(
            name="relationship_delete", json_schema=relationship_schema, kind="edge"
        )]
    )

    unused_schema = {
        "type": "object",
        "properties": {
            "value": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="unused_edge_delete", json_schema=unused_schema, kind="edge")])

    # Create two nodes
    node1 = NodeUpsert(type="test", data={"label": "A"})
    node2 = NodeUpsert(type="test", data={"label": "B"})
    result1_list = await db.set_nodes([node1])
    result2_list = await db.set_nodes([node2])
    result1 = result1_list[0]
    result2 = result2_list[0]

    # Create an edge that references the schema
    edge = EdgeUpsert(
        source_id=result1.id,
        target_id=result2.id,
        type="connected",
        schema_name="relationship_delete",
        data={"weight": 0.5},
    )
    await db.set_edges([edge])

    # Try to delete both schemas (should fail because relationship_delete is in use)
    with pytest.raises(SchemaInUseError):
        await db.delete_schemas([SchemaRef(name="relationship_delete", kind="edge"), SchemaRef(name="unused_edge_delete", kind="edge")])

    # Verify unused_edge_delete still exists (atomic all-or-nothing)
    schemas = await db.get_schemas([SchemaRef(name="unused_edge_delete", kind="edge")])
    assert len(schemas) == 1


@pytest.mark.asyncio
async def test_delete_schemas_success_when_unused(db: GPGraph):
    """
    Test that deleting multiple schemas succeeds when no nodes/edges reference them.
    """
    # Register schemas
    unused_schema1 = {
        "type": "object",
        "properties": {
            "value": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="unused1", json_schema=unused_schema1, kind="node")])

    unused_schema2 = {
        "type": "object",
        "properties": {
            "count": {"type": "integer"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="unused2", json_schema=unused_schema2, kind="node")])

    # Verify schemas exist
    schemas = await db.get_schemas([SchemaRef(name="unused1", kind="node"), SchemaRef(name="unused2", kind="node")])
    assert len(schemas) == 2

    # Delete the schemas (should succeed)
    await db.delete_schemas([SchemaRef(name="unused1", kind="node"), SchemaRef(name="unused2", kind="node")])

    # Verify schemas no longer exist
    from gpdb import SchemaNotFoundError
    with pytest.raises(SchemaNotFoundError):
        await db.get_schemas([SchemaRef(name="unused1", kind="node"), SchemaRef(name="unused2", kind="node")])


@pytest.mark.asyncio
async def test_delete_schemas_missing_name_fails_atomic(db: GPGraph):
    """
    Test that delete_schemas fails the entire batch if any requested schema name is missing.
    """
    from gpdb import SchemaNotFoundError

    unused_schema = {
        "type": "object",
        "properties": {
            "value": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="unused_missing_check", json_schema=unused_schema, kind="node")])

    with pytest.raises(SchemaNotFoundError):
        await db.delete_schemas([SchemaRef(name="unused_missing_check", kind="node"), SchemaRef(name="does_not_exist", kind="node")])

    # Verify the existing schema was not deleted.
    schemas = await db.get_schemas([SchemaRef(name="unused_missing_check", kind="node")])
    assert len(schemas) == 1


@pytest.mark.asyncio
async def test_delete_schemas_rejects_duplicates(db: GPGraph):
    """
    Test that deleting schemas rejects duplicate names.
    """
    # Register a schema
    unused_schema = {
        "type": "object",
        "properties": {
            "value": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="unused", json_schema=unused_schema, kind="node")])

    # Try to delete with duplicate names (should fail)
    with pytest.raises(ValueError, match="Duplicate schema refs provided"):
        await db.delete_schemas([SchemaRef(name="unused", kind="node"), SchemaRef(name="unused", kind="node")])

    # Verify schema still exists
    schemas = await db.get_schemas([SchemaRef(name="unused", kind="node")])
    assert len(schemas) == 1


@pytest.mark.asyncio
async def test_delete_schemas_single_item(db: GPGraph):
    """
    Test that deleting a single schema works via the bulk method.
    """
    # Register a schema
    unused_schema = {
        "type": "object",
        "properties": {
            "value": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="unused_single", json_schema=unused_schema, kind="node")])

    # Verify schema exists
    schemas = await db.get_schemas([SchemaRef(name="unused_single", kind="node")])
    assert len(schemas) == 1

    # Delete the schema (should succeed)
    await db.delete_schemas([SchemaRef(name="unused_single", kind="node")])

    # Verify schema no longer exists
    from gpdb import SchemaNotFoundError
    with pytest.raises(SchemaNotFoundError):
        await db.get_schemas([SchemaRef(name="unused_single", kind="node")])


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
    await db.set_schemas([SchemaUpsert(name="person_preserve", json_schema=person_schema, kind="node")])

    # Create a node with schema_name
    node = NodeUpsert(
        type="person", schema_name="person_preserve", data={"name": "Alice", "age": 30}
    )
    result_list = await db.set_nodes([node])
    result = result_list[0]
    assert result.schema_name == "person_preserve"

    # Update the node without providing schema_name
    # The existing schema should be preserved and validation should still apply
    updated_node = NodeUpsert(
        id=result.id, type="person", data={"name": "Alice", "age": 31}
    )
    updated_result_list = await db.set_nodes([updated_node])
    updated_result = updated_result_list[0]

    # Verify schema_name is preserved
    assert updated_result.schema_name == "person_preserve"

    # Verify data was updated
    assert updated_result.data["age"] == 31

    # Verify validation still applies (try invalid data)
    invalid_node = NodeUpsert(id=result.id, type="person", data={"age": 32})
    with pytest.raises(SchemaValidationError):
        await db.set_nodes([invalid_node])
