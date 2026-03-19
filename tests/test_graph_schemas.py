import pytest
import pytest_asyncio
from sqlalchemy import inspect, select, text
from gpdb import GPGraph, NodeUpsert, SchemaUpsert
from test_helpers import schema_with_kind


# --- Tests ---


@pytest.mark.asyncio
async def test_basic_schema_registration(db: GPGraph):
    """
    Test that GPGraph.set_schemas() can register a JSON schema
    and that it gets stored in the database with the correct columns.
    """
    # Define a simple JSON schema
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }

    # Register the schema
    result = await db.set_schemas([SchemaUpsert(name="person", json_schema=person_schema)])

    # Verify schema was stored in database
    async with db.sqla_sessionmaker() as session:
        db_result = await session.execute(
            select(db.SchemaTable).where(db.SchemaTable.name == "person")
        )
        schema_record = db_result.scalar_one()

        assert schema_record is not None
        assert schema_record.name == "person"
        assert schema_record.version == "1.0.0"
        assert schema_record.json_schema == schema_with_kind(person_schema)
        assert schema_record.created_at is not None
        assert len(result) == 1
        assert result[0].name == "person"


@pytest.mark.asyncio
async def test_schema_name_column_exists(db: GPGraph):
    """
    Verify that nodes and edges have a schema_name column after table creation.
    """
    # Check nodes table has schema_name column
    async with db.sqla_engine.begin() as conn:
        nodes_columns = await conn.run_sync(
            lambda sync_conn: [
                col["name"]
                for col in inspect(sync_conn).get_columns(db.NodeTable.__tablename__)
            ]
        )
        assert "schema_name" in nodes_columns

        edges_columns = await conn.run_sync(
            lambda sync_conn: [
                col["name"]
                for col in inspect(sync_conn).get_columns(db.EdgeTable.__tablename__)
            ]
        )
        assert "schema_name" in edges_columns


@pytest.mark.asyncio
async def test_retrieve_registered_schema(db: GPGraph):
    """
    Test that a registered schema can be retrieved by name.
    """
    # Define and register a schema
    address_schema = {
        "type": "object",
        "properties": {
            "street": {"type": "string"},
            "city": {"type": "string"},
            "zip": {"type": "string"},
        },
    }

    await db.set_schemas([SchemaUpsert(name="address", json_schema=address_schema)])

    # Retrieve the schema
    retrieved = await db.get_schemas(["address"])

    assert len(retrieved) == 1
    assert retrieved[0].name == "address"
    assert retrieved[0].version == "1.0.0"
    assert retrieved[0].json_schema == schema_with_kind(address_schema)


@pytest.mark.asyncio
async def test_retrieve_multiple_schemas(db: GPGraph):
    """
    Test that multiple registered schemas can be retrieved in a single call.
    """
    # Define and register multiple schemas
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
    }

    address_schema = {
        "type": "object",
        "properties": {
            "street": {"type": "string"},
            "city": {"type": "string"},
        },
    }

    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema),
        SchemaUpsert(name="address", json_schema=address_schema),
    ])

    # Retrieve multiple schemas
    retrieved = await db.get_schemas(["person", "address"])

    assert len(retrieved) == 2
    assert retrieved[0].name == "person"
    assert retrieved[0].version == "1.0.0"
    assert retrieved[1].name == "address"
    assert retrieved[1].version == "1.0.0"


@pytest.mark.asyncio
async def test_retrieve_schemas_preserves_order(db: GPGraph):
    """
    Test that get_schemas returns results in the same order as input names.
    """
    # Define and register multiple schemas
    schema_a = {"type": "object", "properties": {"a": {"type": "string"}}}
    schema_b = {"type": "object", "properties": {"b": {"type": "string"}}}
    schema_c = {"type": "object", "properties": {"c": {"type": "string"}}}

    await db.set_schemas([
        SchemaUpsert(name="schema_a", json_schema=schema_a),
        SchemaUpsert(name="schema_b", json_schema=schema_b),
        SchemaUpsert(name="schema_c", json_schema=schema_c),
    ])

    # Retrieve in a different order
    retrieved = await db.get_schemas(["schema_c", "schema_a", "schema_b"])

    assert len(retrieved) == 3
    assert retrieved[0].name == "schema_c"
    assert retrieved[1].name == "schema_a"
    assert retrieved[2].name == "schema_b"


@pytest.mark.asyncio
async def test_retrieve_schemas_duplicate_names(db: GPGraph):
    """
    Test that get_schemas rejects duplicate names in the input.
    """
    # Define and register a schema
    schema = {"type": "object", "properties": {"x": {"type": "string"}}}
    await db.set_schemas([SchemaUpsert(name="test_schema", json_schema=schema)])

    # Try to retrieve with duplicate names
    with pytest.raises(ValueError) as exc_info:
        await db.get_schemas(["test_schema", "test_schema"])

    assert "Duplicate schema names provided" in str(exc_info.value)


@pytest.mark.asyncio
async def test_retrieve_schemas_missing_schema(db: GPGraph):
    """
    Test that get_schemas raises SchemaNotFoundError when any requested schema is missing.
    """
    from gpdb import SchemaNotFoundError

    # Define and register one schema
    schema = {"type": "object", "properties": {"x": {"type": "string"}}}
    await db.set_schemas([SchemaUpsert(name="existing_schema", json_schema=schema)])

    # Try to retrieve with a missing schema
    with pytest.raises(SchemaNotFoundError) as exc_info:
        await db.get_schemas(["existing_schema", "missing_schema"])

    assert "missing_schema" in str(exc_info.value)


@pytest.mark.asyncio
async def test_schema_not_found_error(db: GPGraph):
    """
    Test that trying to set a node with a non-existent schema_name raises SchemaNotFoundError.
    """
    from gpdb import SchemaNotFoundError

    # Try to create a node with a schema that doesn't exist
    node = NodeUpsert(type="test", schema_name="nonexistent_schema")

    with pytest.raises(SchemaNotFoundError):
        await db.set_nodes([node])


@pytest.mark.asyncio
async def test_set_schemas_bulk_success(db: GPGraph):
    """
    Test that set_schemas can register multiple schemas in a single call.
    """
    # Define multiple schemas
    schema1 = {"type": "object", "properties": {"a": {"type": "string"}}}
    schema2 = {"type": "object", "properties": {"b": {"type": "integer"}}}
    schema3 = {"type": "object", "properties": {"c": {"type": "boolean"}}}

    # Register all schemas in bulk
    results = await db.set_schemas([
        SchemaUpsert(name="schema1", json_schema=schema1),
        SchemaUpsert(name="schema2", json_schema=schema2),
        SchemaUpsert(name="schema3", json_schema=schema3),
    ])

    # Verify all schemas were registered
    assert len(results) == 3
    assert results[0].name == "schema1"
    assert results[0].version == "1.0.0"
    assert results[1].name == "schema2"
    assert results[1].version == "1.0.0"
    assert results[2].name == "schema3"
    assert results[2].version == "1.0.0"


@pytest.mark.asyncio
async def test_set_schemas_duplicate_names(db: GPGraph):
    """
    Test that set_schemas rejects duplicate schema names in the input.
    """
    schema = {"type": "object", "properties": {"x": {"type": "string"}}}

    # Try to register schemas with duplicate names
    with pytest.raises(ValueError) as exc_info:
        await db.set_schemas([
            SchemaUpsert(name="duplicate", json_schema=schema),
            SchemaUpsert(name="duplicate", json_schema=schema),
        ])

    assert "Duplicate schema names are not allowed" in str(exc_info.value)


@pytest.mark.asyncio
async def test_set_schemas_atomic_failure(db: GPGraph):
    """
    Test that set_schemas fails atomically when one schema has an error.
    """
    from gpdb import SchemaBreakingChangeError, SchemaNotFoundError

    # First register a schema
    schema1 = {"type": "object", "properties": {"x": {"type": "string"}}}
    await db.set_schemas([SchemaUpsert(name="atomic_test", json_schema=schema1)])

    # Try to bulk update with a breaking change
    schema2 = {"type": "object", "properties": {"y": {"type": "integer"}}}
    schema3 = {"type": "object", "properties": {"z": {"type": "boolean"}}}

    # This should fail because we're trying to change the kind of atomic_test
    with pytest.raises(SchemaBreakingChangeError):
        await db.set_schemas([
            SchemaUpsert(name="atomic_test", json_schema=schema1, kind="edge"),
            SchemaUpsert(name="new_schema1", json_schema=schema2),
            SchemaUpsert(name="new_schema2", json_schema=schema3),
        ])

    # Verify that no new schemas were created (atomic failure)
    # Only atomic_test should exist, new_schema1 and new_schema2 should not
    retrieved = await db.get_schemas(["atomic_test"])
    assert len(retrieved) == 1
    assert retrieved[0].name == "atomic_test"

    # Verify new schemas don't exist
    with pytest.raises(SchemaNotFoundError):
        await db.get_schemas(["new_schema1"])
    with pytest.raises(SchemaNotFoundError):
        await db.get_schemas(["new_schema2"])


@pytest.mark.asyncio
async def test_schema_preserves_json_structure(db: GPGraph):
    """
    Test that schemas are stored exactly as provided, without modifications.
    """
    # Define a schema with $defs and $ref
    schema_with_refs = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "address": {"$ref": "#/$defs/Address"},
        },
        "$defs": {
            "Address": {
                "type": "object",
                "properties": {
                    "street": {"type": "string"},
                    "city": {"type": "string"},
                },
            }
        },
    }

    # Register the schema
    await db.set_schemas([SchemaUpsert(name="address_schema", json_schema=schema_with_refs)])

    # Retrieve and verify the schema is unchanged
    retrieved = await db.get_schemas(["address_schema"])
    assert len(retrieved) == 1
    assert retrieved[0].json_schema == schema_with_refs
    assert "$defs" in retrieved[0].json_schema
    assert "$ref" in retrieved[0].json_schema["properties"]["address"]


@pytest.mark.asyncio
async def test_schema_kind_stored_separately(db: GPGraph):
    """
    Test that kind is stored in a separate column, not in json_schema.
    """
    # Define a simple schema
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }

    # Register as an edge schema
    await db.set_schemas([SchemaUpsert(name="person_edge", json_schema=person_schema, kind="edge")])

    # Retrieve and verify
    retrieved = await db.get_schemas(["person_edge"])
    assert len(retrieved) == 1
    assert retrieved[0].kind == "edge"
    # Verify json_schema doesn't contain x-gpdb-kind
    assert "x-gpdb-kind" not in retrieved[0].json_schema
    # Verify json_schema is exactly as provided
    assert retrieved[0].json_schema == person_schema


@pytest.mark.asyncio
async def test_pydantic_model_schema_preserves_structure(db: GPGraph):
    """
    Test that Pydantic model schemas are stored with $defs preserved.
    """
    from pydantic import BaseModel

    # Define nested Pydantic models
    class Address(BaseModel):
        street: str
        city: str

    class Person(BaseModel):
        name: str
        address: Address

    # Register the Pydantic model
    await db.set_schemas([SchemaUpsert(name="person_model", json_schema=Person)])

    # Retrieve and verify $defs are preserved
    retrieved = await db.get_schemas(["person_model"])
    assert len(retrieved) == 1
    # Pydantic generates $defs for nested models
    assert "$defs" in retrieved[0].json_schema
    # Verify kind is stored separately
    assert retrieved[0].kind == "node"
    # Verify no x-gpdb-kind in json_schema
    assert "x-gpdb-kind" not in retrieved[0].json_schema
