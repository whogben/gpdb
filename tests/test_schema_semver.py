import pytest
import pytest_asyncio
from sqlalchemy import text
from gpdb import GPGraph, NodeUpsert, SchemaBreakingChangeError, SchemaValidationError, SchemaUpsert


# --- Tests ---


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
    await db.register_schema(SchemaUpsert(name="person", json_schema=person_schema_v1))

    # Update with only description change (patch)
    person_schema_v2 = {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Person's full name"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.register_schema(SchemaUpsert(name="person", json_schema=person_schema_v2))

    # Verify version incremented (patch: 1.0.0 -> 1.0.1)
    schema = await db.get_schema("person")
    assert schema.version == "1.0.1"


@pytest.mark.asyncio
async def test_semver_minor_change(db: GPGraph):
    """
    Test that adding an optional field auto-increments minor version (e.g., 1.0.0 -> 1.1.0).
    Old data should still validate against new schema.
    """
    # Register initial schema v1
    person_schema_v1 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    }
    await db.register_schema(SchemaUpsert(name="person_minor", json_schema=person_schema_v1))

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
    await db.register_schema(SchemaUpsert(name="person_minor", json_schema=person_schema_v2))

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
    # Register initial schema v1
    person_schema_v1 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }
    await db.register_schema(SchemaUpsert(name="person", json_schema=person_schema_v1))

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
        await db.register_schema(SchemaUpsert(name="person", json_schema=person_schema_v2_required))

    # Test 2: Removing field (breaking)
    person_schema_v2_removed = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    }
    with pytest.raises(SchemaBreakingChangeError):
        await db.register_schema(SchemaUpsert(name="person", json_schema=person_schema_v2_removed))

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
        await db.register_schema(SchemaUpsert(name="person", json_schema=person_schema_v2_type))


@pytest.mark.asyncio
async def test_forced_update_bypasses_check(db: GPGraph):
    """
    Test that breaking changes are properly detected and fail.
    Breaking changes require explicit migration via migrate_schema().
    """
    # Register initial schema v1
    person_schema_v1 = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    }
    await db.register_schema(SchemaUpsert(name="person_force", json_schema=person_schema_v1))

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
        await db.register_schema(SchemaUpsert(name="person_force", json_schema=person_schema_v2))

    # Verify schema was NOT updated
    schema = await db.get_schema("person_force")
    assert schema is not None
    assert "email" not in schema.json_schema["properties"]
    assert schema.version == "1.0.0"
