import pytest
import pytest_asyncio
from gpdb import GPGraph, NodeUpsert, EdgeUpsert, SchemaUpsert, SchemaRef


# --- Tests ---


@pytest.mark.asyncio
async def test_list_schemas(db: GPGraph):
    """
    Test that list_schemas() returns all registered schema names.
    """
    # Register multiple schemas
    schema1 = {"type": "object", "properties": {"name": {"type": "string"}}}
    schema2 = {"type": "object", "properties": {"value": {"type": "integer"}}}
    schema3 = {"type": "object", "properties": {"flag": {"type": "boolean"}}}

    await db.set_schemas([
        SchemaUpsert(name="schema1", json_schema=schema1, kind="node"),
        SchemaUpsert(name="schema2", json_schema=schema2, kind="node"),
        SchemaUpsert(name="schema3", json_schema=schema3, kind="node"),
    ])

    # List all schemas
    schemas = await db.list_schemas()
    node_schemas = await db.list_schemas(kind="node")
    edge_schemas = await db.list_schemas(kind="edge")

    # Verify all schemas are returned
    assert isinstance(schemas, list)
    schema_names = [s.name for s in schemas]
    assert "schema1" in schema_names
    assert "schema2" in schema_names
    assert "schema3" in schema_names
    assert {s.name for s in node_schemas} == {"schema1", "schema2", "schema3", "__default__"}
    assert [s.name for s in edge_schemas] == ["__default__"]


@pytest.mark.asyncio
async def test_schema_version_tracking(db: GPGraph):
    """
    Test that the version column is properly updated when schemas change.
    """
    # Register initial schema
    person_schema_v1 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    }
    await db.set_schemas(
        [SchemaUpsert(name="person_version", json_schema=person_schema_v1, kind="node")]
    )

    # Verify version is 1
    schemas = await db.get_schemas([SchemaRef(name="person_version", kind="node")])
    assert schemas[0].version == "1.0.0"

    # Update schema with optional field (minor change)
    person_schema_v2 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.set_schemas(
        [SchemaUpsert(name="person_version", json_schema=person_schema_v2, kind="node")]
    )

    # Verify version is now 1.1.0 (minor bump)
    schemas = await db.get_schemas([SchemaRef(name="person_version", kind="node")])
    assert schemas[0].version == "1.1.0"

    # Update schema with another optional field (minor change)
    person_schema_v3 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
            "email": {"type": "string"},
        },
        "required": ["name"],
    }
    await db.set_schemas(
        [SchemaUpsert(name="person_version", json_schema=person_schema_v3, kind="node")]
    )

    # Verify version is now 1.2.0 (minor bump)
    schemas = await db.get_schemas([SchemaRef(name="person_version", kind="node")])
    assert schemas[0].version == "1.2.0"


@pytest.mark.asyncio
async def test_edge_schema_validation_persistence(db: GPGraph):
    """
    Test that edges also properly persist and validate type on updates.
    Similar to node test - if type is not provided in update,
    the existing type should be preserved.
    """
    from gpdb import SchemaValidationError

    # Register a schema for edges
    relationship_schema = {
        "type": "object",
        "properties": {
            "weight": {"type": "number"},
            "label": {"type": "string"},
        },
        "required": ["weight"],
    }
    await db.set_schemas(
        [SchemaUpsert(
            name="relationship_persist", json_schema=relationship_schema, kind="edge"
        )]
    )

    # Create two nodes
    node1 = NodeUpsert(type="__default__", data={"label": "A"})
    node2 = NodeUpsert(type="__default__", data={"label": "B"})
    result1_list = await db.set_nodes([node1])
    result2_list = await db.set_nodes([node2])
    result1 = result1_list[0]
    result2 = result2_list[0]

    # Create an edge with type
    edge = EdgeUpsert(
        source_id=result1.id,
        target_id=result2.id,
        type="relationship_persist",
        data={"weight": 0.5, "label": "friend"},
    )
    edge_result = (await db.set_edges([edge]))[0]
    assert edge_result.type == "relationship_persist"

    # Update the edge with the same type
    # The existing type should be preserved and validation should still apply
    updated_edge = EdgeUpsert(
        id=edge_result.id,
        source_id=result1.id,
        target_id=result2.id,
        type="relationship_persist",
        data={"weight": 0.8, "label": "close friend"},
    )
    updated_result = (await db.set_edges([updated_edge]))[0]

    # Verify type is preserved
    assert updated_result.type == "relationship_persist"

    # Verify data was updated
    assert updated_result.data["weight"] == 0.8
    assert updated_result.data["label"] == "close friend"

    # Verify validation still applies (try invalid data - missing required field)
    invalid_edge = EdgeUpsert(
        id=edge_result.id,
        source_id=result1.id,
        target_id=result2.id,
        type="relationship_persist",
        data={"label": "friend"},
    )
    with pytest.raises(SchemaValidationError):
        await db.set_edges([invalid_edge])
