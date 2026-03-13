import pytest
import pytest_asyncio
from gpdb import GPGraph, NodeUpsert, EdgeUpsert, SchemaValidationError


# --- Tests ---


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
