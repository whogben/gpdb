import pytest
import pytest_asyncio
from gpdb import GPGraph, NodeUpsert, EdgeUpsert, SchemaValidationError, SchemaUpsert


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
    await db.set_schemas([SchemaUpsert(name="person", json_schema=person_schema_v1)])

    # Create nodes with v1 schema
    node1 = NodeUpsert(
        type="person", schema_name="person", data={"name": "Alice", "age": 30}
    )
    node2 = NodeUpsert(
        type="person", schema_name="person", data={"name": "Bob", "age": 25}
    )
    result1_list = await db.set_nodes([node1])
    result2_list = await db.set_nodes([node2])
    result1 = result1_list[0]
    result2 = result2_list[0]

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
    migrated_nodes = await db.get_nodes([result1.id, result2.id])
    migrated_node1 = migrated_nodes[0]
    migrated_node2 = migrated_nodes[1]

    assert "age_years" in migrated_node1.data
    assert "age" not in migrated_node1.data
    assert migrated_node1.data["age_years"] == 30

    assert "age_years" in migrated_node2.data
    assert "age" not in migrated_node2.data
    assert migrated_node2.data["age_years"] == 25

    # Verify schema was updated
    schemas = await db.get_schemas(["person"])
    assert schemas[0].version == "2.0.0"
    assert "age_years" in schemas[0].json_schema["properties"]

    # Verify migrated data validates against new schema
    # Try to update a migrated node - should work since data is valid
    update_node = NodeUpsert(
        id=result1.id, type="person", schema_name="person", data=migrated_node1.data
    )
    updated_list = await db.set_nodes([update_node])
    updated = updated_list[0]
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
    await db.set_schemas(
        [SchemaUpsert(name="person_validate", json_schema=person_schema_v1)]
    )

    # Create a node with v1 schema
    node = NodeUpsert(
        type="person_validate",
        schema_name="person_validate",
        data={"name": "Alice", "age": 30},
    )
    result_list = await db.set_nodes([node])
    result = result_list[0]

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
    await db.set_nodes([node_no_age])

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
    await db.set_schemas(
        [SchemaUpsert(name="person_transaction", json_schema=person_schema_v1)]
    )

    # Create nodes with v1 schema
    node1 = NodeUpsert(
        type="person",
        schema_name="person_transaction",
        data={"name": "Alice", "age": 30},
    )
    node2 = NodeUpsert(
        type="person", schema_name="person_transaction", data={"name": "Bob", "age": 25}
    )
    result1_list = await db.set_nodes([node1])
    result2_list = await db.set_nodes([node2])
    result1 = result1_list[0]
    result2 = result2_list[0]

    # Store original data for verification
    original_nodes = await db.get_nodes([result1.id])
    original_node1 = original_nodes[0]
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
    nodes_after = await db.get_nodes([result1.id, result2.id])
    node1_after = nodes_after[0]
    node2_after = nodes_after[1]

    # Data should be unchanged
    assert node1_after.data == original_data
    assert "age" in node2_after.data
    assert "age_years" not in node2_after.data

    # Schema should still be v1
    schemas = await db.get_schemas(["person_transaction"])
    assert schemas[0].version == "1.0.0"
    assert "age" in schemas[0].json_schema["properties"]
    assert "age_years" not in schemas[0].json_schema["properties"]
