import pytest
import pytest_asyncio
from gpdb import GPGraph, NodeUpsert, EdgeUpsert, SchemaUpsert
from pydantic import BaseModel


# --- Tests ---


@pytest.mark.asyncio
async def test_node_validation_success(db: GPGraph):
    """
    Test that a node with valid data passes validation against a registered schema.
    """
    from gpdb import SchemaValidationError

    # Register a simple schema
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    }
    await db.set_schemas(
        [SchemaUpsert(name="person_validation", json_schema=person_schema)]
    )

    # Create a node with schema_name and valid data
    node = NodeUpsert(
        type="person",
        schema_name="person_validation",
        data={"name": "Alice"},
    )
    result_list = await db.set_nodes([node])
    result = result_list[0]

    assert result is not None
    assert result.schema_name == "person_validation"


@pytest.mark.asyncio
async def test_node_validation_failure(db: GPGraph):
    """
    Test that creating a node with invalid data (missing required field) raises a SchemaValidationError.
    """
    from gpdb import SchemaValidationError

    # Register a schema with required field
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.set_schemas([SchemaUpsert(name="person", json_schema=person_schema)])

    # Try to create a node with missing required field
    node = NodeUpsert(
        type="person",
        schema_name="person",
        data={"age": 30},
    )

    with pytest.raises(SchemaValidationError):
        await db.set_nodes([node])


@pytest.mark.asyncio
async def test_validator_caching(db: GPGraph):
    """
    Test that validators are cached - calling validation multiple times should use the cached validator.
    """
    from gpdb import SchemaValidationError

    # Register a schema
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    }
    await db.set_schemas([SchemaUpsert(name="person_cache", json_schema=person_schema)])

    # Create multiple nodes with the same schema
    node1 = NodeUpsert(
        type="person", schema_name="person_cache", data={"name": "Alice"}
    )
    node2 = NodeUpsert(type="person", schema_name="person_cache", data={"name": "Bob"})

    await db.set_nodes([node1])
    await db.set_nodes([node2])

    # Check that validators are cached
    assert hasattr(db, "_validators")
    assert "person_cache" in db._validators


@pytest.mark.asyncio
async def test_pydantic_to_json_schema(db: GPGraph):
    """
    Test that a Pydantic model can be passed to set_schema and gets converted to JSON Schema.
    """

    # Define a simple Pydantic model
    class PersonModel(BaseModel):
        name: str
        age: int

    # Register the Pydantic model as a schema
    await db.set_schemas([SchemaUpsert(name="person_pydantic", json_schema=PersonModel)])

    # Retrieve the schema and verify it's a dict (JSON Schema format)
    retrieved = await db.get_schemas(["person_pydantic"])
    assert len(retrieved) == 1
    assert isinstance(retrieved[0].json_schema, dict)
    assert "properties" in retrieved[0].json_schema
    assert "name" in retrieved[0].json_schema["properties"]
    assert "age" in retrieved[0].json_schema["properties"]


@pytest.mark.asyncio
async def test_nested_pydantic_model(db: GPGraph):
    """
    Test that nested Pydantic models work correctly with $ref inlining.
    """
    from gpdb import SchemaValidationError

    # Define nested Pydantic models
    class AddressModel(BaseModel):
        street: str
        city: str
        zip: str

    class PersonWithAddressModel(BaseModel):
        name: str
        age: int
        address: AddressModel

    # Register the nested Pydantic model as a schema
    await db.set_schemas(
        [SchemaUpsert(name="person_with_address", json_schema=PersonWithAddressModel)]
    )

    # Create a node with nested data
    node = NodeUpsert(
        type="person_with_address",
        schema_name="person_with_address",
        data={
            "name": "Alice",
            "age": 30,
            "address": {"street": "123 Main St", "city": "Springfield", "zip": "12345"},
        },
    )
    result_list = await db.set_nodes([node])
    result = result_list[0]

    # Verify the node was created successfully
    assert result is not None
    assert result.data["name"] == "Alice"
    assert result.data["age"] == 30
    assert result.data["address"]["street"] == "123 Main St"
    assert result.data["address"]["city"] == "Springfield"
    assert result.data["address"]["zip"] == "12345"

    # Verify validation works - missing required field in nested object should fail
    invalid_node = NodeUpsert(
        type="person_with_address",
        schema_name="person_with_address",
        data={
            "name": "Bob",
            "age": 25,
            "address": {
                "street": "456 Oak Ave"
                # Missing city and zip
            },
        },
    )
    with pytest.raises(SchemaValidationError):
        await db.set_nodes([invalid_node])


@pytest.mark.asyncio
async def test_edge_validation(db: GPGraph):
    """
    Test that edges also support schema validation - register a schema and create an edge with schema_name.
    """
    from gpdb import SchemaValidationError

    # Register a schema for edges
    relationship_schema = {
        "type": "object",
        "properties": {
            "weight": {"type": "number"},
        },
        "required": ["weight"],
    }
    await db.set_schemas(
        [SchemaUpsert(name="relationship", json_schema=relationship_schema, kind="edge")]
    )

    # Create two nodes
    node1 = NodeUpsert(type="test", data={"label": "A"})
    node2 = NodeUpsert(type="test", data={"label": "B"})
    result1_list = await db.set_nodes([node1])
    result2_list = await db.set_nodes([node2])
    result1 = result1_list[0]
    result2 = result2_list[0]

    # Create an edge with schema_name and valid data
    edge = EdgeUpsert(
        source_id=result1.id,
        target_id=result2.id,
        type="connected",
        schema_name="relationship",
        data={"weight": 0.5},
    )
    edge_result = (await db.set_edges([edge]))[0]

    assert edge_result is not None
    assert edge_result.schema_name == "relationship"


@pytest.mark.asyncio
async def test_node_cannot_use_edge_schema(db: GPGraph):
    from gpdb import SchemaKindMismatchError

    relationship_schema = {
        "type": "object",
        "properties": {
            "weight": {"type": "number"},
        },
        "required": ["weight"],
    }
    await db.set_schemas(
        [SchemaUpsert(
            name="edge_only_relationship", json_schema=relationship_schema, kind="edge"
        )]
    )

    with pytest.raises(SchemaKindMismatchError):
        await db.set_nodes([
            NodeUpsert(
                type="test",
                schema_name="edge_only_relationship",
                data={"weight": 0.5},
            )
        ])


@pytest.mark.asyncio
async def test_edge_cannot_use_node_schema(db: GPGraph):
    from gpdb import SchemaKindMismatchError

    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    }
    await db.set_schemas(
        [SchemaUpsert(name="node_only_person", json_schema=person_schema, kind="node")]
    )

    node1_list = await db.set_nodes([NodeUpsert(type="test", data={"label": "A"})])
    node2_list = await db.set_nodes([NodeUpsert(type="test", data={"label": "B"})])
    node1 = node1_list[0]
    node2 = node2_list[0]

    with pytest.raises(SchemaKindMismatchError):
        await db.set_edges(
            [EdgeUpsert(
                source_id=node1.id,
                target_id=node2.id,
                type="connected",
                schema_name="node_only_person",
                data={"name": "invalid"},
            )]
        )
