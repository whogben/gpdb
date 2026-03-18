import pytest
import pytest_asyncio
from sqlalchemy import inspect, select, text
from gpdb import GPGraph, NodeUpsert, SchemaUpsert
from test_helpers import schema_with_kind


# --- Tests ---


@pytest.mark.asyncio
async def test_basic_schema_registration(db: GPGraph):
    """
    Test that GPGraph.register_schema() can register a JSON schema
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
    await db.register_schema(SchemaUpsert(name="person", json_schema=person_schema))

    # Verify schema was stored in database
    async with db.sqla_sessionmaker() as session:
        result = await session.execute(
            select(db.SchemaTable).where(db.SchemaTable.name == "person")
        )
        schema_record = result.scalar_one()

        assert schema_record is not None
        assert schema_record.name == "person"
        assert schema_record.version == "1.0.0"
        assert schema_record.json_schema == schema_with_kind(person_schema)
        assert schema_record.created_at is not None


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

    await db.register_schema(SchemaUpsert(name="address", json_schema=address_schema))

    # Retrieve the schema
    retrieved = await db.get_schema("address")

    assert retrieved is not None
    assert retrieved.name == "address"
    assert retrieved.version == "1.0.0"
    assert retrieved.json_schema == schema_with_kind(address_schema)


@pytest.mark.asyncio
async def test_schema_not_found_error(db: GPGraph):
    """
    Test that trying to set a node with a non-existent schema_name raises SchemaNotFoundError.
    """
    from gpdb import SchemaNotFoundError

    # Try to create a node with a schema that doesn't exist
    node = NodeUpsert(type="test", schema_name="nonexistent_schema")

    with pytest.raises(SchemaNotFoundError):
        await db.set_node(node)
