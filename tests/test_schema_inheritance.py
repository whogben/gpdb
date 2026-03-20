"""
Tests for schema inheritance functionality.

Tests cover:
- Disjoint parent/child create with effective schema validation
- Child reusing parent top-level key raises SchemaInheritanceError
- Two parents with overlapping keys raises error
- Cycle detection in extends
- Unknown parent / self-extend errors
- Update parent adds key that collides with co-parent's key
- effective_json_schema null for roots, non-null for children
- migrate_schema: nodes of descendant types migrated
- Batch set_schemas creates parent+child in one call
"""

import pytest
import pytest_asyncio
from gpdb import GPGraph, NodeUpsert, EdgeUpsert, SchemaUpsert, SchemaRef
from gpdb.schema_inheritance import build_inheritance_graph, topological_sort


def test_topological_sort_parents_before_children():
    reg = {
        "A": {"json_schema": {}, "extends": []},
        "B": {"json_schema": {}, "extends": ["A"]},
        "C": {"json_schema": {}, "extends": ["B"]},
    }
    g = build_inheritance_graph(reg)
    order = topological_sort(g)
    assert order.index("A") < order.index("B") < order.index("C")


def test_topological_sort_diamond_parents_before_child():
    reg = {
        "root": {"json_schema": {}, "extends": []},
        "L": {"json_schema": {}, "extends": ["root"]},
        "R": {"json_schema": {}, "extends": ["root"]},
        "D": {"json_schema": {}, "extends": ["L", "R"]},
    }
    g = build_inheritance_graph(reg)
    order = topological_sort(g)
    assert order.index("root") < order.index("L")
    assert order.index("root") < order.index("R")
    assert order.index("L") < order.index("D")
    assert order.index("R") < order.index("D")


# --- Tests ---


@pytest.mark.asyncio
async def test_disjoint_parent_child_create(db: GPGraph):
    """
    Test creating parent and child schemas with disjoint properties.
    Child data should validate against merged effective schema.
    """
    # Create parent schema with some properties
    parent_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.set_schemas([SchemaUpsert(name="parent", json_schema=parent_schema, kind="node")])

    # Create child schema extending parent with disjoint properties
    child_schema = {
        "type": "object",
        "properties": {
            "email": {"type": "string"},
            "phone": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="child", json_schema=child_schema, kind="node", extends=["parent"])])

    # Verify effective_json_schema is computed correctly
    child_record = await db.get_schemas([SchemaRef(name="child", kind="node")])
    assert len(child_record) == 1
    assert child_record[0].effective_json_schema is not None
    effective = child_record[0].effective_json_schema

    # Should have all properties from both parent and child
    assert "properties" in effective
    assert "name" in effective["properties"]
    assert "age" in effective["properties"]
    assert "email" in effective["properties"]
    assert "phone" in effective["properties"]

    # Should have required fields from parent
    assert "required" in effective
    assert "name" in effective["required"]

    # Create node with child type and validate it uses effective schema
    node = NodeUpsert(
        type="child",
        data={
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com",
            "phone": "555-1234",
        },
    )
    result_list = await db.set_nodes([node])
    result = result_list[0]

    assert result is not None
    assert result.type == "child"
    assert result.data["name"] == "Alice"
    assert result.data["age"] == 30
    assert result.data["email"] == "alice@example.com"
    assert result.data["phone"] == "555-1234"


@pytest.mark.asyncio
async def test_child_reuses_parent_key_raises_error(db: GPGraph):
    """
    Test that child schema reusing parent top-level key raises SchemaInheritanceError.
    """
    from gpdb import SchemaInheritanceError

    # Create parent schema with property "email"
    parent_schema = {
        "type": "object",
        "properties": {
            "email": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="parent_email", json_schema=parent_schema, kind="node")])

    # Try to create child schema extending parent that also defines "email"
    child_schema = {
        "type": "object",
        "properties": {
            "email": {"type": "string"},  # Collision!
            "phone": {"type": "string"},
        },
    }

    with pytest.raises(SchemaInheritanceError) as exc_info:
        await db.set_schemas([SchemaUpsert(name="child_email", json_schema=child_schema, kind="node", extends=["parent_email"])])

    error_msg = str(exc_info.value)
    assert "parent_email" in error_msg
    assert "child_email" in error_msg
    assert "email" in error_msg


@pytest.mark.asyncio
async def test_two_parents_overlapping_keys_raises_error(db: GPGraph):
    """
    Test that two parents with overlapping keys raises error when creating child.
    """
    from gpdb import SchemaInheritanceError

    # Create parent A with property "name"
    parent_a_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="parent_a", json_schema=parent_a_schema, kind="node")])

    # Create parent B with property "name"
    parent_b_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},  # Collision!
            "age": {"type": "integer"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="parent_b", json_schema=parent_b_schema, kind="node")])

    # Try to create child C extending both A and B
    child_schema = {
        "type": "object",
        "properties": {
            "email": {"type": "string"},
        },
    }

    with pytest.raises(SchemaInheritanceError) as exc_info:
        await db.set_schemas([SchemaUpsert(name="child_c", json_schema=child_schema, kind="node", extends=["parent_a", "parent_b"])])

    error_msg = str(exc_info.value)
    assert "parent_a" in error_msg
    assert "parent_b" in error_msg
    assert "name" in error_msg


@pytest.mark.asyncio
async def test_cycle_in_extends_raises_error(db: GPGraph):
    """
    Test that cycle in extends raises SchemaInheritanceError.
    """
    from gpdb import SchemaInheritanceError

    schema_a = {"type": "object", "properties": {"a": {"type": "string"}}}
    schema_b = {"type": "object", "properties": {"b": {"type": "string"}}}

    # Create schema A first
    await db.set_schemas([SchemaUpsert(name="cycle_a", json_schema=schema_a, kind="node")])

    # Try to create schema B extending A, and then update A to extend B
    # This should fail when updating A to extend B
    await db.set_schemas([SchemaUpsert(name="cycle_b", json_schema=schema_b, kind="node", extends=["cycle_a"])])

    with pytest.raises(SchemaInheritanceError) as exc_info:
        await db.set_schemas([SchemaUpsert(name="cycle_a", json_schema=schema_a, kind="node", extends=["cycle_b"])])

    error_msg = str(exc_info.value)
    assert "cycle" in error_msg.lower()


@pytest.mark.asyncio
async def test_unknown_parent_raises_error(db: GPGraph):
    """
    Test that extending non-existent parent raises SchemaInheritanceError.
    """
    from gpdb import SchemaInheritanceError

    schema = {"type": "object", "properties": {"x": {"type": "string"}}}

    with pytest.raises(SchemaInheritanceError) as exc_info:
        await db.set_schemas([SchemaUpsert(name="child_unknown", json_schema=schema, kind="node", extends=["nonexistent_parent"])])

    error_msg = str(exc_info.value)
    assert "nonexistent_parent" in error_msg


@pytest.mark.asyncio
async def test_self_extend_raises_error(db: GPGraph):
    """
    Test that extending itself raises SchemaInheritanceError.
    """
    from gpdb import SchemaInheritanceError

    schema = {"type": "object", "properties": {"x": {"type": "string"}}}

    with pytest.raises(SchemaInheritanceError) as exc_info:
        await db.set_schemas([SchemaUpsert(name="self_extend", json_schema=schema, kind="node", extends=["self_extend"])])

    error_msg = str(exc_info.value)
    assert "cycle" in error_msg.lower()


@pytest.mark.asyncio
async def test_update_parent_adds_key_collides_with_co_parent(db: GPGraph):
    """
    Test that updating parent to add key that collides with co-parent's key raises error.
    """
    from gpdb import SchemaInheritanceError

    # Create parent A with property "x"
    parent_a_schema = {
        "type": "object",
        "properties": {
            "x": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="parent_x", json_schema=parent_a_schema, kind="node")])

    # Create parent B with property "y"
    parent_b_schema = {
        "type": "object",
        "properties": {
            "y": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="parent_y", json_schema=parent_b_schema, kind="node")])

    # Create child C extending both A and B
    child_schema = {
        "type": "object",
        "properties": {
            "z": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="child_z", json_schema=child_schema, kind="node", extends=["parent_x", "parent_y"])])

    # Try to update parent A to add property "y" (collision with parent B)
    updated_parent_a_schema = {
        "type": "object",
        "properties": {
            "x": {"type": "string"},
            "y": {"type": "string"},  # Collision!
        },
    }

    with pytest.raises(SchemaInheritanceError) as exc_info:
        await db.set_schemas([SchemaUpsert(name="parent_x", json_schema=updated_parent_a_schema, kind="node")])

    error_msg = str(exc_info.value)
    assert "parent_x" in error_msg
    assert "parent_y" in error_msg
    assert "y" in error_msg


@pytest.mark.asyncio
async def test_effective_json_schema_null_for_roots(db: GPGraph):
    """
    Test that effective_json_schema is null for root schemas (no extends).
    """
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}

    await db.set_schemas([SchemaUpsert(name="root_schema", json_schema=schema, kind="node")])

    # Verify effective_json_schema is null for root
    root_record = await db.get_schemas([SchemaRef(name="root_schema", kind="node")])
    assert len(root_record) == 1
    assert root_record[0].effective_json_schema is None


@pytest.mark.asyncio
async def test_effective_json_schema_non_null_for_children(db: GPGraph):
    """
    Test that effective_json_schema is non-null for child schemas.
    """
    parent_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([SchemaUpsert(name="parent_effective", json_schema=parent_schema, kind="node")])

    child_schema = {"type": "object", "properties": {"age": {"type": "integer"}}}
    await db.set_schemas([SchemaUpsert(name="child_effective", json_schema=child_schema, kind="node", extends=["parent_effective"])])

    # Verify effective_json_schema is non-null for child
    child_record = await db.get_schemas([SchemaRef(name="child_effective", kind="node")])
    assert len(child_record) == 1
    assert child_record[0].effective_json_schema is not None

    # Verify it contains both parent and child properties
    effective = child_record[0].effective_json_schema
    assert "name" in effective["properties"]
    assert "age" in effective["properties"]


@pytest.mark.asyncio
async def test_merge_required_fields_from_multiple_parents(db: GPGraph):
    """
    Test that required fields are merged from multiple parents.
    """
    parent_a_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    }
    await db.set_schemas([SchemaUpsert(name="parent_req_a", json_schema=parent_a_schema, kind="node")])

    parent_b_schema = {
        "type": "object",
        "properties": {
            "age": {"type": "integer"},
        },
        "required": ["age"],
    }
    await db.set_schemas([SchemaUpsert(name="parent_req_b", json_schema=parent_b_schema, kind="node")])

    child_schema = {
        "type": "object",
        "properties": {
            "email": {"type": "string"},
        },
        "required": ["email"],
    }
    await db.set_schemas([SchemaUpsert(name="child_req", json_schema=child_schema, kind="node", extends=["parent_req_a", "parent_req_b"])])

    # Verify required fields are merged
    child_record = await db.get_schemas([SchemaRef(name="child_req", kind="node")])
    assert len(child_record) == 1
    effective = child_record[0].effective_json_schema

    assert "required" in effective
    assert "name" in effective["required"]
    assert "age" in effective["required"]
    assert "email" in effective["required"]


@pytest.mark.asyncio
async def test_merge_properties_from_multiple_parents(db: GPGraph):
    """
    Test that properties are merged from multiple parents.
    """
    parent_a_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="parent_prop_a", json_schema=parent_a_schema, kind="node")])

    parent_b_schema = {
        "type": "object",
        "properties": {
            "email": {"type": "string"},
            "phone": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="parent_prop_b", json_schema=parent_b_schema, kind="node")])

    child_schema = {
        "type": "object",
        "properties": {
            "address": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="child_prop", json_schema=child_schema, kind="node", extends=["parent_prop_a", "parent_prop_b"])])

    # Verify all properties are merged
    child_record = await db.get_schemas([SchemaRef(name="child_prop", kind="node")])
    assert len(child_record) == 1
    effective = child_record[0].effective_json_schema

    assert "properties" in effective
    assert "name" in effective["properties"]
    assert "age" in effective["properties"]
    assert "email" in effective["properties"]
    assert "phone" in effective["properties"]
    assert "address" in effective["properties"]


@pytest.mark.asyncio
async def test_migrate_schema_nodes_of_descendant_types(db: GPGraph):
    """
    Test that migrate_schema migrates nodes of descendant types.
    """
    # Create parent schema with property "old_field"
    parent_schema = {
        "type": "object",
        "properties": {
            "old_field": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="parent_migrate", json_schema=parent_schema, kind="node")])

    # Create child schema extending parent with additional property
    child_schema = {
        "type": "object",
        "properties": {
            "child_field": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="child_migrate", json_schema=child_schema, kind="node", extends=["parent_migrate"])])

    # Create nodes of both parent and child types
    parent_node = NodeUpsert(
        type="parent_migrate",
        data={"old_field": "parent_value"},
    )
    parent_result_list = await db.set_nodes([parent_node])
    parent_result = parent_result_list[0]

    child_node = NodeUpsert(
        type="child_migrate",
        data={
            "old_field": "child_value",
            "child_field": "child_extra",
        },
    )
    child_result_list = await db.set_nodes([child_node])
    child_result = child_result_list[0]

    # Migrate parent schema (rename "old_field" to "new_field")
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
        name="parent_migrate",
        migration_func=migrate_func,
        new_schema=new_parent_schema,
        kind="node",
    )

    # Verify nodes of both parent and child types are migrated
    migrated_parent = await db.get_nodes([parent_result.id])
    assert len(migrated_parent) == 1
    assert "new_field" in migrated_parent[0].data
    assert "old_field" not in migrated_parent[0].data
    assert migrated_parent[0].data["new_field"] == "parent_value"

    migrated_child = await db.get_nodes([child_result.id])
    assert len(migrated_child) == 1
    assert "new_field" in migrated_child[0].data
    assert "old_field" not in migrated_child[0].data
    assert migrated_child[0].data["new_field"] == "child_value"
    assert migrated_child[0].data["child_field"] == "child_extra"

    # Verify validation uses descendant effective schema
    # Child node should still validate with both new_field and child_field
    update_child = NodeUpsert(
        id=child_result.id,
        type="child_migrate",
        data={
            "new_field": "updated_value",
            "child_field": "child_extra",
        },
    )
    updated_list = await db.set_nodes([update_child])
    assert len(updated_list) == 1


@pytest.mark.asyncio
async def test_migrate_schema_transaction_rollback_on_validation_failure(db: GPGraph):
    """
    Test that migrate_schema rolls back on validation failure.
    """
    # Create parent schema
    parent_schema = {
        "type": "object",
        "properties": {
            "old_field": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="parent_rollback", json_schema=parent_schema, kind="node")])

    # Create child schema
    child_schema = {
        "type": "object",
        "properties": {
            "child_field": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="child_rollback", json_schema=child_schema, kind="node", extends=["parent_rollback"])])

    # Create nodes
    parent_node = NodeUpsert(
        type="parent_rollback",
        data={"old_field": "parent_value"},
    )
    parent_result_list = await db.set_nodes([parent_node])
    parent_result = parent_result_list[0]

    child_node = NodeUpsert(
        type="child_rollback",
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
        type="parent_rollback",
        data={},  # No old_field
    )
    await db.set_nodes([node_no_field])

    # Migration should fail and rollback
    from gpdb import SchemaValidationError

    with pytest.raises(SchemaValidationError):
        await db.migrate_schema(
            name="parent_rollback",
            migration_func=bad_migrate_func,
            new_schema=new_parent_schema,
            kind="node",
        )

    # Verify no changes were persisted
    parent_after = await db.get_nodes([parent_result.id])
    child_after = await db.get_nodes([child_result.id])

    assert parent_after[0].data == original_parent_data
    assert child_after[0].data == original_child_data

    # Schema should still be v1
    schemas = await db.get_schemas([SchemaRef(name="parent_rollback", kind="node")])
    assert schemas[0].version == "1.0.0"
    assert "old_field" in schemas[0].json_schema["properties"]


@pytest.mark.asyncio
async def test_batch_set_schemas_creates_parent_and_child(db: GPGraph):
    """
    Test that batch set_schemas can create both parent and child in one call.
    """
    parent_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }

    child_schema = {
        "type": "object",
        "properties": {
            "age": {"type": "integer"},
        },
        "additionalProperties": False,
    }

    # Create both parent and child in a single batch
    results = await db.set_schemas([
        SchemaUpsert(name="batch_parent", json_schema=parent_schema, kind="node"),
        SchemaUpsert(name="batch_child", json_schema=child_schema, kind="node", extends=["batch_parent"]),
    ])

    assert len(results) == 2

    # Verify both were created successfully
    parent_record = await db.get_schemas([SchemaRef(name="batch_parent", kind="node")])
    assert len(parent_record) == 1
    assert parent_record[0].name == "batch_parent"
    assert parent_record[0].version == "1.0.0"

    child_record = await db.get_schemas([SchemaRef(name="batch_child", kind="node")])
    assert len(child_record) == 1
    assert child_record[0].name == "batch_child"
    assert child_record[0].version == "1.0.0"

    # Verify extends is set correctly
    assert child_record[0].extends == ["batch_parent"]

    # Effective schema must be merged in-batch (child-only + additionalProperties: false
    # would reject parent's "name" without a real effective merge).
    assert child_record[0].effective_json_schema is not None
    eff = child_record[0].effective_json_schema
    assert "name" in eff["properties"]
    assert "age" in eff["properties"]

    # Verify node can be created with child type (validates against effective schema)
    node = NodeUpsert(
        type="batch_child",
        data={
            "name": "Alice",
            "age": 30,
        },
    )
    result_list = await db.set_nodes([node])
    result = result_list[0]

    assert result is not None
    assert result.data["name"] == "Alice"
    assert result.data["age"] == 30


@pytest.mark.asyncio
async def test_batch_set_schemas_three_level_effective(db: GPGraph):
    """Grandparent, parent, and child created in one batch get correct effective merge."""
    await db.set_schemas(
        [
            SchemaUpsert(
                name="batch_gp",
                json_schema={"type": "object", "properties": {"ga": {"type": "string"}}},
                kind="node",
            ),
            SchemaUpsert(
                name="batch_mid",
                json_schema={"type": "object", "properties": {"mb": {"type": "string"}}},
                kind="node",
                extends=["batch_gp"],
            ),
            SchemaUpsert(
                name="batch_leaf",
                json_schema={"type": "object", "properties": {"lc": {"type": "string"}}},
                kind="node",
                extends=["batch_mid"],
            ),
        ]
    )

    leaf = await db.get_schemas([SchemaRef(name="batch_leaf", kind="node")])
    assert len(leaf) == 1
    assert leaf[0].effective_json_schema is not None
    props = leaf[0].effective_json_schema["properties"]
    assert "ga" in props and "mb" in props and "lc" in props

    node_list = await db.set_nodes(
        [
            NodeUpsert(
                type="batch_leaf",
                data={"ga": "x", "mb": "y", "lc": "z"},
            )
        ]
    )
    assert node_list[0].data == {"ga": "x", "mb": "y", "lc": "z"}


@pytest.mark.asyncio
async def test_update_schema_omitting_extends_preserves_extends(db: GPGraph):
    """On update, extends=None must not clear inheritance (merge-with-existing)."""
    await db.set_schemas(
        [SchemaUpsert(name="keep_p", json_schema={"type": "object", "properties": {"n": {"type": "string"}}}, kind="node")]
    )
    await db.set_schemas(
        [
            SchemaUpsert(
                name="keep_c",
                json_schema={"type": "object", "properties": {"a": {"type": "integer"}}},
                kind="node",
                extends=["keep_p"],
            )
        ]
    )

    await db.set_schemas(
        [
            SchemaUpsert(
                name="keep_c",
                json_schema={
                    "type": "object",
                    "properties": {"a": {"type": "integer"}, "x": {"type": "string"}},
                },
                kind="node",
            )
        ]
    )

    row = await db.get_schemas([SchemaRef(name="keep_c", kind="node")])
    assert row[0].extends == ["keep_p"]
    assert row[0].effective_json_schema is not None
    assert "n" in row[0].effective_json_schema["properties"]
    assert "x" in row[0].effective_json_schema["properties"]


@pytest.mark.asyncio
async def test_edge_schema_inheritance(db: GPGraph):
    """
    Test that schema inheritance works for edge schemas as well.
    """
    # Create parent edge schema
    parent_edge_schema = {
        "type": "object",
        "properties": {
            "weight": {"type": "number"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="parent_edge", json_schema=parent_edge_schema, kind="edge")])

    # Create child edge schema extending parent
    child_edge_schema = {
        "type": "object",
        "properties": {
            "label": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="child_edge", json_schema=child_edge_schema, kind="edge", extends=["parent_edge"])])

    # Verify effective_json_schema is computed correctly
    child_record = await db.get_schemas([SchemaRef(name="child_edge", kind="edge")])
    assert len(child_record) == 1
    assert child_record[0].effective_json_schema is not None
    effective = child_record[0].effective_json_schema

    assert "weight" in effective["properties"]
    assert "label" in effective["properties"]

    # Create nodes and edge with child type
    node1 = NodeUpsert(type="__default__", data={"label": "A"})
    node2 = NodeUpsert(type="__default__", data={"label": "B"})
    result1_list = await db.set_nodes([node1])
    result2_list = await db.set_nodes([node2])
    result1 = result1_list[0]
    result2 = result2_list[0]

    edge = EdgeUpsert(
        source_id=result1.id,
        target_id=result2.id,
        type="child_edge",
        data={
            "weight": 0.5,
            "label": "connected",
        },
    )
    edge_result = (await db.set_edges([edge]))[0]

    assert edge_result is not None
    assert edge_result.type == "child_edge"
    assert edge_result.data["weight"] == 0.5
    assert edge_result.data["label"] == "connected"


@pytest.mark.asyncio
async def test_multiple_levels_of_inheritance(db: GPGraph):
    """
    Test that multiple levels of inheritance work correctly.
    """
    # Create grandparent
    grandparent_schema = {
        "type": "object",
        "properties": {
            "field_a": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="grandparent", json_schema=grandparent_schema, kind="node")])

    # Create parent extending grandparent
    parent_schema = {
        "type": "object",
        "properties": {
            "field_b": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="parent_multi", json_schema=parent_schema, kind="node", extends=["grandparent"])])

    # Create child extending parent
    child_schema = {
        "type": "object",
        "properties": {
            "field_c": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="child_multi", json_schema=child_schema, kind="node", extends=["parent_multi"])])

    # Verify child's effective schema includes all fields
    child_record = await db.get_schemas([SchemaRef(name="child_multi", kind="node")])
    assert len(child_record) == 1
    effective = child_record[0].effective_json_schema

    assert "field_a" in effective["properties"]
    assert "field_b" in effective["properties"]
    assert "field_c" in effective["properties"]

    # Create node with child type
    node = NodeUpsert(
        type="child_multi",
        data={
            "field_a": "value_a",
            "field_b": "value_b",
            "field_c": "value_c",
        },
    )
    result_list = await db.set_nodes([node])
    result = result_list[0]

    assert result is not None
    assert result.data["field_a"] == "value_a"
    assert result.data["field_b"] == "value_b"
    assert result.data["field_c"] == "value_c"


@pytest.mark.asyncio
async def test_update_child_preserves_extends(db: GPGraph):
    """
    Test that updating a child schema preserves its extends relationship.
    """
    parent_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([SchemaUpsert(name="parent_update", json_schema=parent_schema, kind="node")])

    child_schema = {"type": "object", "properties": {"age": {"type": "integer"}}}
    await db.set_schemas([SchemaUpsert(name="child_update", json_schema=child_schema, kind="node", extends=["parent_update"])])

    # Update child schema (add new property)
    updated_child_schema = {
        "type": "object",
        "properties": {
            "age": {"type": "integer"},
            "email": {"type": "string"},
        },
    }
    await db.set_schemas([SchemaUpsert(name="child_update", json_schema=updated_child_schema, kind="node", extends=["parent_update"])])

    # Verify extends is preserved and effective schema is updated
    child_record = await db.get_schemas([SchemaRef(name="child_update", kind="node")])
    assert len(child_record) == 1
    assert child_record[0].extends == ["parent_update"]
    assert child_record[0].effective_json_schema is not None

    effective = child_record[0].effective_json_schema
    assert "name" in effective["properties"]  # From parent
    assert "age" in effective["properties"]  # From child
    assert "email" in effective["properties"]  # From updated child


@pytest.mark.asyncio
async def test_diamond_inheritance(db: GPGraph):
    """
    Test diamond inheritance pattern (A -> B, A -> C, B -> D, C -> D).
    """
    # Create root A
    schema_a = {"type": "object", "properties": {"a": {"type": "string"}}}
    await db.set_schemas([SchemaUpsert(name="diamond_a", json_schema=schema_a, kind="node")])

    # Create B and C extending A
    schema_b = {"type": "object", "properties": {"b": {"type": "string"}}}
    await db.set_schemas([SchemaUpsert(name="diamond_b", json_schema=schema_b, kind="node", extends=["diamond_a"])])

    schema_c = {"type": "object", "properties": {"c": {"type": "string"}}}
    await db.set_schemas([SchemaUpsert(name="diamond_c", json_schema=schema_c, kind="node", extends=["diamond_a"])])

    # Create D extending both B and C
    schema_d = {"type": "object", "properties": {"d": {"type": "string"}}}
    await db.set_schemas([SchemaUpsert(name="diamond_d", json_schema=schema_d, kind="node", extends=["diamond_b", "diamond_c"])])

    # Verify D's effective schema includes all properties
    d_record = await db.get_schemas([SchemaRef(name="diamond_d", kind="node")])
    assert len(d_record) == 1
    effective = d_record[0].effective_json_schema

    assert "a" in effective["properties"]  # From A (via B and C)
    assert "b" in effective["properties"]  # From B
    assert "c" in effective["properties"]  # From C
    assert "d" in effective["properties"]  # From D
