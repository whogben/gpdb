import pytest
import pytest_asyncio
from gpdb import GPGraph, NodeUpsert, EdgeUpsert, SchemaValidationError, SchemaUpsert, SchemaRef


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
    await db.set_schemas([SchemaUpsert(name="person", json_schema=person_schema_v1, kind="node")])

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
        kind="node",
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
    schemas = await db.get_schemas([SchemaRef(name="person", kind="node")])
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
        [SchemaUpsert(name="person_validate", json_schema=person_schema_v1, kind="node")]
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
            kind="node",
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
        [SchemaUpsert(name="person_transaction", json_schema=person_schema_v1, kind="node")]
    )

    # Create nodes with v1 schema
    node1 = NodeUpsert(
        type="person_transaction",
        schema_name="person_transaction",
        data={"name": "Alice", "age": 30},
    )
    node2 = NodeUpsert(
        type="person_transaction", schema_name="person_transaction", data={"name": "Bob", "age": 25}
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
            kind="node",
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
    schemas = await db.get_schemas([SchemaRef(name="person_transaction", kind="node")])
    assert schemas[0].version == "1.0.0"
    assert "age" in schemas[0].json_schema["properties"]
    assert "age_years" not in schemas[0].json_schema["properties"]


@pytest.mark.asyncio
async def test_migrate_schema_with_multiple_descendants(db: GPGraph):
    """
    Test that migrating a parent schema migrates all descendant nodes.
    """
    # Create parent schema
    parent_schema = {
        "type": "object",
        "properties": {
            "old_field": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="parent_multi_desc", json_schema=parent_schema, kind="node")])

    # Create child A extending parent
    child_a_schema = {
        "type": "object",
        "properties": {
            "child_a_field": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="child_a", json_schema=child_a_schema, kind="node", extends=["parent_multi_desc"])])

    # Create child B extending parent
    child_b_schema = {
        "type": "object",
        "properties": {
            "child_b_field": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="child_b", json_schema=child_b_schema, kind="node", extends=["parent_multi_desc"])])

    # Create nodes of all types
    parent_node = NodeUpsert(
        type="parent_multi_desc",
        data={"old_field": "parent_value"},
    )
    parent_result_list = await db.set_nodes([parent_node])
    parent_result = parent_result_list[0]

    child_a_node = NodeUpsert(
        type="child_a",
        data={
            "old_field": "child_a_value",
            "child_a_field": "child_a_extra",
        },
    )
    child_a_result_list = await db.set_nodes([child_a_node])
    child_a_result = child_a_result_list[0]

    child_b_node = NodeUpsert(
        type="child_b",
        data={
            "old_field": "child_b_value",
            "child_b_field": "child_b_extra",
        },
    )
    child_b_result_list = await db.set_nodes([child_b_node])
    child_b_result = child_b_result_list[0]

    # Migrate parent schema
    new_parent_schema = {
        "type": "object",
        "properties": {
            "new_field": {"type": "string"},
        },
    }

    def migrate_func(old_data):
        new_data = old_data.copy()
        if "old_field" in new_data:
            new_data["new_field"] = new_data.pop("old_field")
        return new_data

    await db.migrate_schema(
        name="parent_multi_desc",
        migration_func=migrate_func,
        new_schema=new_parent_schema,
        kind="node",
    )

    # Verify all nodes were migrated
    migrated_parent = await db.get_nodes([parent_result.id])
    assert len(migrated_parent) == 1
    assert "new_field" in migrated_parent[0].data
    assert "old_field" not in migrated_parent[0].data
    assert migrated_parent[0].data["new_field"] == "parent_value"

    migrated_child_a = await db.get_nodes([child_a_result.id])
    assert len(migrated_child_a) == 1
    assert "new_field" in migrated_child_a[0].data
    assert "old_field" not in migrated_child_a[0].data
    assert migrated_child_a[0].data["new_field"] == "child_a_value"
    assert migrated_child_a[0].data["child_a_field"] == "child_a_extra"

    migrated_child_b = await db.get_nodes([child_b_result.id])
    assert len(migrated_child_b) == 1
    assert "new_field" in migrated_child_b[0].data
    assert "old_field" not in migrated_child_b[0].data
    assert migrated_child_b[0].data["new_field"] == "child_b_value"
    assert migrated_child_b[0].data["child_b_field"] == "child_b_extra"

    # Verify schema was updated
    schemas = await db.get_schemas([SchemaRef(name="parent_multi_desc", kind="node")])
    assert schemas[0].version == "2.0.0"
    assert "new_field" in schemas[0].json_schema["properties"]


@pytest.mark.asyncio
async def test_migrate_schema_descendant_rollback_on_validation_failure(db: GPGraph):
    """
    Test that migrating a parent schema rolls back all descendant nodes on validation failure.
    """
    # Create parent schema
    parent_schema = {
        "type": "object",
        "properties": {
            "old_field": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="parent_rollback_desc", json_schema=parent_schema, kind="node")])

    # Create child extending parent
    child_schema = {
        "type": "object",
        "properties": {
            "child_field": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="child_rollback_desc", json_schema=child_schema, kind="node", extends=["parent_rollback_desc"])])

    # Create nodes
    parent_node = NodeUpsert(
        type="parent_rollback_desc",
        data={"old_field": "parent_value"},
    )
    parent_result_list = await db.set_nodes([parent_node])
    parent_result = parent_result_list[0]

    child_node = NodeUpsert(
        type="child_rollback_desc",
        data={
            "old_field": "child_value",
            "child_field": "child_extra",
        },
    )
    child_result_list = await db.set_nodes([child_node])
    child_result = child_result_list[0]

    # Store original data
    original_parent = await db.get_nodes([parent_result.id])
    original_child = await db.get_nodes([child_result.id])
    original_parent_data = original_parent[0].data.copy()
    original_child_data = original_child[0].data.copy()

    # Try to migrate with a function that produces invalid data
    new_parent_schema = {
        "type": "object",
        "properties": {
            "new_field": {"type": "string"},
        },
        "required": ["new_field"],  # Now required
    }

    def bad_migrate_func(old_data):
        new_data = old_data.copy()
        if "old_field" in new_data:
            new_data["new_field"] = new_data.pop("old_field")
        # Oops! If old_field is missing, new_field won't be set
        return new_data

    # Create a node without old_field to trigger validation failure
    node_no_field = NodeUpsert(
        type="parent_rollback_desc",
        data={},  # No old_field
    )
    await db.set_nodes([node_no_field])

    # Migration should fail and rollback all nodes
    from gpdb import SchemaValidationError

    with pytest.raises(SchemaValidationError):
        await db.migrate_schema(
            name="parent_rollback_desc",
            migration_func=bad_migrate_func,
            new_schema=new_parent_schema,
            kind="node",
        )

    # Verify no changes were persisted for any nodes
    parent_after = await db.get_nodes([parent_result.id])
    child_after = await db.get_nodes([child_result.id])

    assert parent_after[0].data == original_parent_data
    assert child_after[0].data == original_child_data

    # Schema should still be v1
    schemas = await db.get_schemas([SchemaRef(name="parent_rollback_desc", kind="node")])
    assert schemas[0].version == "1.0.0"
    assert "old_field" in schemas[0].json_schema["properties"]


@pytest.mark.asyncio
async def test_migrate_grandparent_recomputes_leaf_effective(db: GPGraph):
    """
    Migrating a root ancestor updates stored effective_json_schema for multi-level
    descendants (not only direct children).
    """
    await db.set_schemas(
        [
            SchemaUpsert(
                name="mg_g",
                json_schema={"type": "object", "properties": {"old_g": {"type": "string"}}},
                kind="node",
            ),
        ]
    )
    await db.set_schemas(
        [
            SchemaUpsert(
                name="mg_p",
                json_schema={"type": "object", "properties": {"p": {"type": "string"}}},
                kind="node",
                extends=["mg_g"],
            ),
        ]
    )
    await db.set_schemas(
        [
            SchemaUpsert(
                name="mg_c",
                json_schema={"type": "object", "properties": {"c": {"type": "string"}}},
                kind="node",
                extends=["mg_p"],
            ),
        ]
    )

    node_list = await db.set_nodes(
        [NodeUpsert(type="mg_c", data={"old_g": "gv", "p": "pv", "c": "cv"})]
    )
    nid = node_list[0].id

    new_g = {"type": "object", "properties": {"new_g": {"type": "string"}}}

    def migrate_func(old_data):
        new_data = old_data.copy()
        if "old_g" in new_data:
            new_data["new_g"] = new_data.pop("old_g")
        return new_data

    await db.migrate_schema(
        name="mg_g",
        migration_func=migrate_func,
        new_schema=new_g,
        kind="node",
    )

    updated = await db.get_nodes([nid])
    assert updated[0].data == {"new_g": "gv", "p": "pv", "c": "cv"}

    leaf = await db.get_schemas([SchemaRef(name="mg_c", kind="node")])
    eff = leaf[0].effective_json_schema
    assert eff is not None
    assert "new_g" in eff["properties"]
    assert "p" in eff["properties"]
    assert "c" in eff["properties"]
