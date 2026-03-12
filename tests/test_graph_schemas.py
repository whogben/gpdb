import pytest
import pytest_asyncio
import shutil
import tempfile
from pathlib import Path
from pixeltable_pgserver import PostgresServer
from sqlalchemy import inspect, select, text
from gpdb import GPGraph, NodeUpsert, EdgeUpsert
from pydantic import BaseModel


# --- Fixtures ---


def _schema_with_kind(schema: dict, kind: str = "node") -> dict:
    return {**schema, "x-gpdb-kind": kind}


@pytest.fixture(scope="session")
def pg_server():
    """
    Starts a temporary PostgreSQL server for the test session.
    """
    # Create a temporary directory for pgdata
    try:
        # Try standard temp dir first
        pgdata_str = tempfile.mkdtemp()
        pgdata = Path(pgdata_str)
    except OSError:
        # Fallback to local directory if system temp is not accessible
        pgdata = Path("./.test_pgdata").resolve()
        if pgdata.exists():
            shutil.rmtree(pgdata)
        pgdata.mkdir(parents=True, exist_ok=True)

    server = PostgresServer(pgdata)
    with server:
        yield server

    # Cleanup data directory
    if pgdata.exists():
        shutil.rmtree(pgdata)


@pytest_asyncio.fixture
async def db(pg_server):
    """
    Creates a GraphDB instance connected to the temporary Postgres server.
    Creates tables before the test and drops them after to ensure isolation.
    """
    url = pg_server.get_uri()
    db = GPGraph(url, table_prefix="test_schema")

    # Manually drop tables using raw SQL to ensure clean state
    # This handles the case where tables exist with old schema (version as INTEGER)
    async with db.sqla_engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS test_schema_schemas CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS test_schema_edges CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS test_schema_nodes CASCADE"))

    # Clear model cache and metadata to ensure fresh table definitions
    from gpdb.graph import _model_cache, _Base

    _model_cache.clear()
    _Base.metadata.clear()

    # Initialize schema
    await db.create_tables()

    yield db

    # Cleanup
    await db.drop_tables()

    await db.sqla_engine.dispose()


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
    await db.register_schema(name="person", schema=person_schema)

    # Verify schema was stored in database
    async with db.sqla_sessionmaker() as session:
        result = await session.execute(
            select(db.SchemaTable).where(db.SchemaTable.name == "person")
        )
        schema_record = result.scalar_one()

        assert schema_record is not None
        assert schema_record.name == "person"
        assert schema_record.version == "1.0.0"
        assert schema_record.json_schema == _schema_with_kind(person_schema)
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

    await db.register_schema(name="address", schema=address_schema)

    # Retrieve the schema
    retrieved = await db.get_schema("address")

    assert retrieved is not None
    assert retrieved.name == "address"
    assert retrieved.version == "1.0.0"
    assert retrieved.json_schema == _schema_with_kind(address_schema)


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
    await db.register_schema(name="person_validation", schema=person_schema)

    # Create a node with schema_name and valid data
    node = NodeUpsert(
        type="person",
        schema_name="person_validation",
        data={"name": "Alice"},
    )
    result = await db.set_node(node)

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
    await db.register_schema(name="person", schema=person_schema)

    # Try to create a node with missing required field
    node = NodeUpsert(
        type="person",
        schema_name="person",
        data={"age": 30},
    )

    with pytest.raises(SchemaValidationError):
        await db.set_node(node)


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
    await db.register_schema(name="person_cache", schema=person_schema)

    # Create multiple nodes with the same schema
    node1 = NodeUpsert(
        type="person", schema_name="person_cache", data={"name": "Alice"}
    )
    node2 = NodeUpsert(type="person", schema_name="person_cache", data={"name": "Bob"})

    await db.set_node(node1)
    await db.set_node(node2)

    # Check that validators are cached
    assert hasattr(db, "_validators")
    assert "person_cache" in db._validators


@pytest.mark.asyncio
async def test_pydantic_to_json_schema(db: GPGraph):
    """
    Test that a Pydantic model can be passed to register_schema and gets converted to JSON Schema.
    """

    # Define a simple Pydantic model
    class PersonModel(BaseModel):
        name: str
        age: int

    # Register the Pydantic model as a schema
    await db.register_schema(name="person_pydantic", schema=PersonModel)

    # Retrieve the schema and verify it's a dict (JSON Schema format)
    retrieved = await db.get_schema("person_pydantic")
    assert retrieved is not None
    assert isinstance(retrieved.json_schema, dict)
    assert "properties" in retrieved.json_schema
    assert "name" in retrieved.json_schema["properties"]
    assert "age" in retrieved.json_schema["properties"]


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
    await db.register_schema(name="person_with_address", schema=PersonWithAddressModel)

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
    result = await db.set_node(node)

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
        await db.set_node(invalid_node)


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
    await db.register_schema(
        name="relationship",
        schema=relationship_schema,
        kind="edge",
    )

    # Create two nodes
    node1 = NodeUpsert(type="test", data={"label": "A"})
    node2 = NodeUpsert(type="test", data={"label": "B"})
    result1 = await db.set_node(node1)
    result2 = await db.set_node(node2)

    # Create an edge with schema_name and valid data
    edge = EdgeUpsert(
        source_id=result1.id,
        target_id=result2.id,
        type="connected",
        schema_name="relationship",
        data={"weight": 0.5},
    )
    edge_result = await db.set_edge(edge)

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
    await db.register_schema(
        name="edge_only_relationship",
        schema=relationship_schema,
        kind="edge",
    )

    with pytest.raises(SchemaKindMismatchError):
        await db.set_node(
            NodeUpsert(
                type="test",
                schema_name="edge_only_relationship",
                data={"weight": 0.5},
            )
        )


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
    await db.register_schema(name="node_only_person", schema=person_schema, kind="node")

    node1 = await db.set_node(NodeUpsert(type="test", data={"label": "A"}))
    node2 = await db.set_node(NodeUpsert(type="test", data={"label": "B"}))

    with pytest.raises(SchemaKindMismatchError):
        await db.set_edge(
            EdgeUpsert(
                source_id=node1.id,
                target_id=node2.id,
                type="connected",
                schema_name="node_only_person",
                data={"name": "invalid"},
            )
        )


@pytest.mark.asyncio
async def test_semver_patch_change(db: GPGraph):
    """
    Test that changing only descriptions/titles auto-increments patch version (e.g., 1.0.0 -> 1.0.1).
    Register schema v1, then update with only description change, verify version becomes 1.0.1.
    """
    # Register initial schema v1
    person_schema_v1 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.register_schema(name="person", schema=person_schema_v1)

    # Update with only description change (patch)
    person_schema_v2 = {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Person's full name"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.register_schema(name="person", schema=person_schema_v2)

    # Verify version incremented (patch: 1.0.0 -> 1.0.1)
    schema = await db.get_schema("person")
    assert schema.version == "1.0.1"


@pytest.mark.asyncio
async def test_semver_minor_change(db: GPGraph):
    """
    Test that adding an optional field auto-increments minor version (e.g., 1.0.0 -> 1.1.0).
    Old data should still validate against new schema.
    """
    from gpdb import SchemaValidationError

    # Register initial schema v1
    person_schema_v1 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    }
    await db.register_schema(name="person_minor", schema=person_schema_v1)

    # Create node with old schema
    node = NodeUpsert(type="person", schema_name="person_minor", data={"name": "Alice"})
    result = await db.set_node(node)

    # Update with optional field (minor change)
    person_schema_v2 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.register_schema(name="person_minor", schema=person_schema_v2)

    # Verify version incremented (minor: 1.0.0 -> 1.1.0)
    schema = await db.get_schema("person_minor")
    assert schema.version == "1.1.0"

    # Verify old data still validates
    node2 = NodeUpsert(type="person", schema_name="person_minor", data={"name": "Bob"})
    result2 = await db.set_node(node2)
    assert result2 is not None


@pytest.mark.asyncio
async def test_semver_major_change_detection(db: GPGraph):
    """
    Test that breaking changes (adding required field, removing field, changing type) are detected.
    register_schema should raise SchemaBreakingChangeError by default.
    """
    from gpdb import SchemaBreakingChangeError

    # Register initial schema v1
    person_schema_v1 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.register_schema(name="person", schema=person_schema_v1)

    # Test 1: Adding required field (breaking)
    person_schema_v2_required = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
            "email": {"type": "string"},
        },
        "required": ["name", "email"],
    }
    with pytest.raises(SchemaBreakingChangeError):
        await db.register_schema(name="person", schema=person_schema_v2_required)

    # Test 2: Removing field (breaking)
    person_schema_v2_removed = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    }
    with pytest.raises(SchemaBreakingChangeError):
        await db.register_schema(name="person", schema=person_schema_v2_removed)

    # Test 3: Changing type (breaking)
    person_schema_v2_type = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "string"},
        },
        "required": ["name"],
    }
    with pytest.raises(SchemaBreakingChangeError):
        await db.register_schema(name="person", schema=person_schema_v2_type)


@pytest.mark.asyncio
async def test_forced_update_bypasses_check(db: GPGraph):
    """
    Test that breaking changes are properly detected and fail.
    Breaking changes require explicit migration via migrate_schema().
    """
    from gpdb import SchemaBreakingChangeError

    # Register initial schema v1
    person_schema_v1 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    }
    await db.register_schema(name="person_force", schema=person_schema_v1)

    # Try breaking change (should fail)
    person_schema_v2 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "email": {"type": "string"},
        },
        "required": ["name", "email"],
    }
    with pytest.raises(SchemaBreakingChangeError):
        await db.register_schema(name="person_force", schema=person_schema_v2)

    # Verify schema was NOT updated
    schema = await db.get_schema("person_force")
    assert schema is not None
    assert "email" not in schema.json_schema["properties"]
    assert schema.version == "1.0.0"


@pytest.mark.asyncio
async def test_migrate_schema_success(db: GPGraph):
    """
    Test the migrate_schema method. Register v1 schema, create some nodes,
    then migrate to v2 with a migration function that transforms old data -> new data.
    Verify nodes are updated correctly.
    """
    # Register v1 schema
    person_schema_v1 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.register_schema(name="person", schema=person_schema_v1)

    # Create nodes with v1 schema
    node1 = NodeUpsert(
        type="person", schema_name="person", data={"name": "Alice", "age": 30}
    )
    node2 = NodeUpsert(
        type="person", schema_name="person", data={"name": "Bob", "age": 25}
    )
    result1 = await db.set_node(node1)
    result2 = await db.set_node(node2)

    # Define v2 schema with breaking change (age -> age_years)
    person_schema_v2 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age_years": {"type": "integer"},
        },
        "required": ["name"],
    }

    # Define migration function
    def migrate_age_to_age_years(old_data):
        new_data = old_data.copy()
        if "age" in new_data:
            new_data["age_years"] = new_data.pop("age")
        return new_data

    # Migrate schema
    await db.migrate_schema(
        name="person",
        migration_func=migrate_age_to_age_years,
        new_schema=person_schema_v2,
    )

    # Verify nodes were migrated
    migrated_node1 = await db.get_node(result1.id)
    migrated_node2 = await db.get_node(result2.id)

    assert "age_years" in migrated_node1.data
    assert "age" not in migrated_node1.data
    assert migrated_node1.data["age_years"] == 30

    assert "age_years" in migrated_node2.data
    assert "age" not in migrated_node2.data
    assert migrated_node2.data["age_years"] == 25

    # Verify schema was updated
    schema = await db.get_schema("person")
    assert schema.version == "2.0.0"
    assert "age_years" in schema.json_schema["properties"]

    # Verify migrated data validates against new schema
    # Try to update a migrated node - should work since data is valid
    update_node = NodeUpsert(
        id=result1.id, type="person", schema_name="person", data=migrated_node1.data
    )
    updated = await db.set_node(update_node)
    assert updated is not None


@pytest.mark.asyncio
async def test_migrate_schema_validates_data(db: GPGraph):
    """
    Test that migrate_schema validates the migrated data against the new schema.
    If migration produces invalid data, it should fail.
    """
    from gpdb import SchemaValidationError

    # Register v1 schema
    person_schema_v1 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.register_schema(name="person_validate", schema=person_schema_v1)

    # Create a node with v1 schema
    node = NodeUpsert(
        type="person_validate",
        schema_name="person_validate",
        data={"name": "Alice", "age": 30},
    )
    result = await db.set_node(node)

    # Define v2 schema with required field
    person_schema_v2 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age_years": {"type": "integer"},
        },
        "required": ["name", "age_years"],  # age_years is now required
    }

    # Define migration function that produces invalid data (missing required field)
    def bad_migration(old_data):
        new_data = old_data.copy()
        if "age" in new_data:
            new_data["age_years"] = new_data.pop("age")
        # Oops! Forgot to handle case where age is None/missing
        # This will produce invalid data if age is missing
        return new_data

    # Try to migrate with a function that produces invalid data for some nodes
    # First, create a node without age to trigger the bug
    node_no_age = NodeUpsert(
        type="person_validate",
        schema_name="person_validate",
        data={"name": "Bob"},  # No age field
    )
    await db.set_node(node_no_age)

    # Migration should fail because bad_migration produces invalid data
    with pytest.raises(SchemaValidationError):
        await db.migrate_schema(
            name="person_validate",
            migration_func=bad_migration,
            new_schema=person_schema_v2,
        )


@pytest.mark.asyncio
async def test_migrate_schema_transaction(db: GPGraph):
    """
    Test that migration runs in a transaction - if it fails halfway, no changes are persisted.
    """
    # Register v1 schema
    person_schema_v1 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.register_schema(name="person_transaction", schema=person_schema_v1)

    # Create nodes with v1 schema
    node1 = NodeUpsert(
        type="person",
        schema_name="person_transaction",
        data={"name": "Alice", "age": 30},
    )
    node2 = NodeUpsert(
        type="person", schema_name="person_transaction", data={"name": "Bob", "age": 25}
    )
    result1 = await db.set_node(node1)
    result2 = await db.set_node(node2)

    # Store original data for verification
    original_node1 = await db.get_node(result1.id)
    original_data = original_node1.data.copy()

    # Define v2 schema
    person_schema_v2 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age_years": {"type": "integer"},
        },
        "required": ["name"],
    }

    # Define migration function that fails halfway
    def failing_migration(old_data):
        new_data = old_data.copy()
        if "age" in new_data:
            new_data["age_years"] = new_data.pop("age")
        # Fail on second node
        if old_data.get("name") == "Bob":
            raise ValueError("Migration failed")
        return new_data

    # Try to migrate (should fail and rollback)
    with pytest.raises(ValueError, match="Migration failed"):
        await db.migrate_schema(
            name="person_transaction",
            migration_func=failing_migration,
            new_schema=person_schema_v2,
        )

    # Verify no changes were persisted (transaction rolled back)
    node1_after = await db.get_node(result1.id)
    node2_after = await db.get_node(result2.id)

    # Data should be unchanged
    assert node1_after.data == original_data
    assert "age" in node2_after.data
    assert "age_years" not in node2_after.data

    # Schema should still be v1
    schema = await db.get_schema("person_transaction")
    assert schema.version == "1.0.0"
    assert "age" in schema.json_schema["properties"]
    assert "age_years" not in schema.json_schema["properties"]


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


@pytest.mark.asyncio
async def test_list_schemas(db: GPGraph):
    """
    Test that list_schemas() returns all registered schema names.
    """
    # Register multiple schemas
    schema1 = {"type": "object", "properties": {"name": {"type": "string"}}}
    schema2 = {"type": "object", "properties": {"value": {"type": "integer"}}}
    schema3 = {"type": "object", "properties": {"flag": {"type": "boolean"}}}

    await db.register_schema(name="schema1", schema=schema1)
    await db.register_schema(name="schema2", schema=schema2)
    await db.register_schema(name="schema3", schema=schema3)

    # List all schemas
    schemas = await db.list_schemas()
    node_schemas = await db.list_schemas(kind="node")
    edge_schemas = await db.list_schemas(kind="edge")

    # Verify all schemas are returned
    assert isinstance(schemas, list)
    assert "schema1" in schemas
    assert "schema2" in schemas
    assert "schema3" in schemas
    assert set(node_schemas) == {"schema1", "schema2", "schema3"}
    assert edge_schemas == []


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
    await db.register_schema(name="person_version", schema=person_schema_v1)

    # Verify version is 1
    schema = await db.get_schema("person_version")
    assert schema.version == "1.0.0"

    # Update schema with optional field (minor change)
    person_schema_v2 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.register_schema(name="person_version", schema=person_schema_v2)

    # Verify version is now 1.1.0 (minor bump)
    schema = await db.get_schema("person_version")
    assert schema.version == "1.1.0"

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
    await db.register_schema(name="person_version", schema=person_schema_v3)

    # Verify version is now 1.2.0 (minor bump)
    schema = await db.get_schema("person_version")
    assert schema.version == "1.2.0"


@pytest.mark.asyncio
async def test_edge_schema_validation_persistence(db: GPGraph):
    """
    Test that edges also properly persist and validate schema_name on updates.
    Similar to node test - if schema_name is not provided in update,
    the existing schema should be preserved.
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
    await db.register_schema(
        name="relationship_persist",
        schema=relationship_schema,
        kind="edge",
    )

    # Create two nodes
    node1 = NodeUpsert(type="test", data={"label": "A"})
    node2 = NodeUpsert(type="test", data={"label": "B"})
    result1 = await db.set_node(node1)
    result2 = await db.set_node(node2)

    # Create an edge with schema_name
    edge = EdgeUpsert(
        source_id=result1.id,
        target_id=result2.id,
        type="connected",
        schema_name="relationship_persist",
        data={"weight": 0.5, "label": "friend"},
    )
    edge_result = await db.set_edge(edge)
    assert edge_result.schema_name == "relationship_persist"

    # Update the edge without providing schema_name
    # The existing schema should be preserved and validation should still apply
    updated_edge = EdgeUpsert(
        id=edge_result.id,
        source_id=result1.id,
        target_id=result2.id,
        type="connected",
        data={"weight": 0.8, "label": "close friend"},
    )
    updated_result = await db.set_edge(updated_edge)

    # Verify schema_name is preserved
    assert updated_result.schema_name == "relationship_persist"

    # Verify data was updated
    assert updated_result.data["weight"] == 0.8
    assert updated_result.data["label"] == "close friend"

    # Verify validation still applies (try invalid data - missing required field)
    invalid_edge = EdgeUpsert(
        id=edge_result.id,
        source_id=result1.id,
        target_id=result2.id,
        type="connected",
        data={"label": "friend"},
    )
    with pytest.raises(SchemaValidationError):
        await db.set_edge(invalid_edge)
