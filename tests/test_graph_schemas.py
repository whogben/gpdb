import pytest
import pytest_asyncio
from sqlalchemy import inspect, select, text
from gpdb import GPGraph, NodeUpsert, SchemaUpsert, SchemaRef
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
    result = await db.set_schemas([SchemaUpsert(name="person", json_schema=person_schema, kind="node")])

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

    await db.set_schemas([SchemaUpsert(name="address", json_schema=address_schema, kind="node")])

    # Retrieve the schema
    retrieved = await db.get_schemas([SchemaRef(name="address", kind="node")])

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
        SchemaUpsert(name="person", json_schema=person_schema, kind="node"),
        SchemaUpsert(name="address", json_schema=address_schema, kind="node"),
    ])

    # Retrieve multiple schemas
    retrieved = await db.get_schemas([SchemaRef(name="person", kind="node"), SchemaRef(name="address", kind="node")])

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
        SchemaUpsert(name="schema_a", json_schema=schema_a, kind="node"),
        SchemaUpsert(name="schema_b", json_schema=schema_b, kind="node"),
        SchemaUpsert(name="schema_c", json_schema=schema_c, kind="node"),
    ])

    # Retrieve in a different order
    retrieved = await db.get_schemas([SchemaRef(name="schema_c", kind="node"), SchemaRef(name="schema_a", kind="node"), SchemaRef(name="schema_b", kind="node")])

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
    await db.set_schemas([SchemaUpsert(name="test_schema", json_schema=schema, kind="node")])

    # Try to retrieve with duplicate names
    with pytest.raises(ValueError) as exc_info:
        await db.get_schemas([SchemaRef(name="test_schema", kind="node"), SchemaRef(name="test_schema", kind="node")])

    assert "Duplicate schema refs provided" in str(exc_info.value)


@pytest.mark.asyncio
async def test_retrieve_schemas_missing_schema(db: GPGraph):
    """
    Test that get_schemas raises SchemaNotFoundError when any requested schema is missing.
    """
    from gpdb import SchemaNotFoundError

    # Define and register one schema
    schema = {"type": "object", "properties": {"x": {"type": "string"}}}
    await db.set_schemas([SchemaUpsert(name="existing_schema", json_schema=schema, kind="node")])

    # Try to retrieve with a missing schema
    with pytest.raises(SchemaNotFoundError) as exc_info:
        await db.get_schemas([SchemaRef(name="existing_schema", kind="node"), SchemaRef(name="missing_schema", kind="node")])

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
        SchemaUpsert(name="schema1", json_schema=schema1, kind="node"),
        SchemaUpsert(name="schema2", json_schema=schema2, kind="node"),
        SchemaUpsert(name="schema3", json_schema=schema3, kind="node"),
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
            SchemaUpsert(name="duplicate", json_schema=schema, kind="node"),
            SchemaUpsert(name="duplicate", json_schema=schema, kind="node"),
        ])

    assert "Duplicate schema names are not allowed" in str(exc_info.value)


@pytest.mark.asyncio
async def test_set_schemas_atomic_failure(db: GPGraph):
    """
    Test that set_schemas fails atomically when one schema has an error.
    """
    from gpdb import SchemaBreakingChangeError, SchemaNotFoundError

    # First register a schema with a required field
    schema1 = {"type": "object", "required": ["x"], "properties": {"x": {"type": "string"}}}
    await db.set_schemas([SchemaUpsert(name="atomic_test", json_schema=schema1, kind="node")])

    # Try to bulk update with a breaking change (removing required field)
    schema2 = {"type": "object", "properties": {"y": {"type": "integer"}}}
    schema3 = {"type": "object", "properties": {"z": {"type": "boolean"}}}

    # This should fail because we're removing a required field from atomic_test
    with pytest.raises(SchemaBreakingChangeError):
        await db.set_schemas([
            SchemaUpsert(name="atomic_test", json_schema=schema2, kind="node"),
            SchemaUpsert(name="new_schema1", json_schema=schema2, kind="node"),
            SchemaUpsert(name="new_schema2", json_schema=schema3, kind="node"),
        ])

    # Verify that no new schemas were created (atomic failure)
    # Only atomic_test should exist, new_schema1 and new_schema2 should not
    retrieved = await db.get_schemas([SchemaRef(name="atomic_test", kind="node")])
    assert len(retrieved) == 1
    assert retrieved[0].name == "atomic_test"

    # Verify new schemas don't exist
    with pytest.raises(SchemaNotFoundError):
        await db.get_schemas([SchemaRef(name="new_schema1", kind="node")])
    with pytest.raises(SchemaNotFoundError):
        await db.get_schemas([SchemaRef(name="new_schema2", kind="node")])


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
    await db.set_schemas([SchemaUpsert(name="address_schema", json_schema=schema_with_refs, kind="node")])

    # Retrieve and verify the schema is unchanged
    retrieved = await db.get_schemas([SchemaRef(name="address_schema", kind="node")])
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
    retrieved = await db.get_schemas([SchemaRef(name="person_edge", kind="edge")])
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
    await db.set_schemas([SchemaUpsert(name="person_model", json_schema=Person, kind="node")])

    # Retrieve and verify $defs are preserved
    retrieved = await db.get_schemas([SchemaRef(name="person_model", kind="node")])
    assert len(retrieved) == 1
    # Pydantic generates $defs for nested models
    assert "$defs" in retrieved[0].json_schema
    # Verify kind is stored separately
    assert retrieved[0].kind == "node"
    # Verify no x-gpdb-kind in json_schema
    assert "x-gpdb-kind" not in retrieved[0].json_schema


# --- Schema Display Metadata Tests (alias and svg_icon) ---


@pytest.mark.asyncio
async def test_create_schema_with_alias_only(db: GPGraph):
    """Test creating a schema with only an alias."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    
    result = await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", alias="Person")
    ])
    
    assert len(result) == 1
    assert result[0].alias == "Person"
    assert result[0].svg_icon is None


@pytest.mark.asyncio
async def test_create_schema_with_svg_icon_only(db: GPGraph):
    """Test creating a schema with only an svg_icon."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40"/></svg>'
    
    result = await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", svg_icon=svg)
    ])
    
    assert len(result) == 1
    assert result[0].alias is None
    # SVG is sanitized, so check for key elements rather than exact match
    assert "<svg" in result[0].svg_icon
    assert "<circle" in result[0].svg_icon
    assert 'cx="50"' in result[0].svg_icon
    assert 'cy="50"' in result[0].svg_icon
    assert 'r="40"' in result[0].svg_icon


@pytest.mark.asyncio
async def test_create_schema_with_both_alias_and_svg_icon(db: GPGraph):
    """Test creating a schema with both alias and svg_icon."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40"/></svg>'
    
    result = await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", alias="Person", svg_icon=svg)
    ])
    
    assert len(result) == 1
    assert result[0].alias == "Person"
    # SVG is sanitized, so check for key elements rather than exact match
    assert "<svg" in result[0].svg_icon
    assert "<circle" in result[0].svg_icon
    assert 'cx="50"' in result[0].svg_icon
    assert 'cy="50"' in result[0].svg_icon
    assert 'r="40"' in result[0].svg_icon


@pytest.mark.asyncio
async def test_create_schema_with_invalid_svg(db: GPGraph):
    """Test that creating a schema with invalid SVG raises ValueError."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    invalid_svg = '<script>alert("xss")</script>'
    
    with pytest.raises(ValueError) as exc_info:
        await db.set_schemas([
            SchemaUpsert(name="person", json_schema=schema, kind="node", svg_icon=invalid_svg)
        ])
    
    assert "not valid XML" in str(exc_info.value)


@pytest.mark.asyncio
async def test_create_schema_with_svg_over_size_limit(db: GPGraph):
    """Test that creating a schema with SVG over size limit raises ValueError."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    # Create an SVG that's over 20KB
    large_svg = '<svg xmlns="http://www.w3.org/2000/svg">' + '<circle/>' * 10000 + '</svg>'
    
    with pytest.raises(ValueError) as exc_info:
        await db.set_schemas([
            SchemaUpsert(name="person", json_schema=schema, kind="node", svg_icon=large_svg)
        ])
    
    assert "exceeds maximum size" in str(exc_info.value)


@pytest.mark.asyncio
async def test_update_schema_to_add_alias(db: GPGraph):
    """Test updating a schema to add an alias."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    
    # Create schema without alias
    await db.set_schemas([SchemaUpsert(name="person", json_schema=schema, kind="node")])
    
    # Update to add alias
    result = await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", alias="Person")
    ])
    
    assert len(result) == 1
    assert result[0].alias == "Person"


@pytest.mark.asyncio
async def test_update_schema_to_add_svg_icon(db: GPGraph):
    """Test updating a schema to add an svg_icon."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40"/></svg>'
    
    # Create schema without svg_icon
    await db.set_schemas([SchemaUpsert(name="person", json_schema=schema, kind="node")])
    
    # Update to add svg_icon
    result = await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", svg_icon=svg)
    ])
    
    assert len(result) == 1
    # SVG is sanitized, so check for key elements rather than exact match
    assert "<svg" in result[0].svg_icon
    assert "<circle" in result[0].svg_icon
    assert 'cx="50"' in result[0].svg_icon
    assert 'cy="50"' in result[0].svg_icon
    assert 'r="40"' in result[0].svg_icon


@pytest.mark.asyncio
async def test_update_schema_to_change_alias(db: GPGraph):
    """Test updating a schema to change its alias."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    
    # Create schema with alias
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", alias="Person")
    ])
    
    # Update to change alias
    result = await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", alias="Human")
    ])
    
    assert len(result) == 1
    assert result[0].alias == "Human"


@pytest.mark.asyncio
async def test_update_schema_to_change_svg_icon(db: GPGraph):
    """Test updating a schema to change its svg_icon."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    svg1 = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40"/></svg>'
    svg2 = '<svg xmlns="http://www.w3.org/2000/svg"><rect width="100" height="100"/></svg>'
    
    # Create schema with svg_icon
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", svg_icon=svg1)
    ])
    
    # Update to change svg_icon
    result = await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", svg_icon=svg2)
    ])
    
    assert len(result) == 1
    # SVG is sanitized, so check for key elements rather than exact match
    assert "<svg" in result[0].svg_icon
    assert "<rect" in result[0].svg_icon
    assert 'width="100"' in result[0].svg_icon
    assert 'height="100"' in result[0].svg_icon
    # Verify it's not the old circle
    assert "<circle" not in result[0].svg_icon


@pytest.mark.asyncio
async def test_update_schema_with_none_values_keeps_existing(db: GPGraph):
    """Test that updating a schema with None values keeps existing values."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40"/></svg>'
    
    # Create schema with both alias and svg_icon
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", alias="Person", svg_icon=svg)
    ])
    
    # Update with None values (should keep existing)
    result = await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", alias=None, svg_icon=None)
    ])
    
    assert len(result) == 1
    assert result[0].alias == "Person"
    # SVG is sanitized, so check for key elements rather than exact match
    assert "<svg" in result[0].svg_icon
    assert "<circle" in result[0].svg_icon
    assert 'cx="50"' in result[0].svg_icon
    assert 'cy="50"' in result[0].svg_icon
    assert 'r="40"' in result[0].svg_icon


@pytest.mark.asyncio
async def test_svg_sanitization_on_create(db: GPGraph):
    """Test that SVG is sanitized when creating a schema."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    # SVG with potentially dangerous content
    svg_with_script = '<svg xmlns="http://www.w3.org/2000/svg"><script>alert("xss")</script><circle cx="50" cy="50" r="40"/></svg>'
    
    result = await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", svg_icon=svg_with_script)
    ])
    
    # Script should be removed
    assert "<script>" not in result[0].svg_icon
    assert "<circle" in result[0].svg_icon


@pytest.mark.asyncio
async def test_svg_sanitization_on_update(db: GPGraph):
    """Test that SVG is sanitized when updating a schema."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    
    # Create schema without svg_icon
    await db.set_schemas([SchemaUpsert(name="person", json_schema=schema, kind="node")])
    
    # Update with SVG containing dangerous content
    svg_with_script = '<svg xmlns="http://www.w3.org/2000/svg"><script>alert("xss")</script><circle cx="50" cy="50" r="40"/></svg>'
    result = await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", svg_icon=svg_with_script)
    ])
    
    # Script should be removed
    assert "<script>" not in result[0].svg_icon
    assert "<circle" in result[0].svg_icon


@pytest.mark.asyncio
async def test_schema_display_cache_works_correctly(db: GPGraph):
    """Test that the schema display cache works correctly."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40"/></svg>'
    
    # Create schema with alias and svg_icon
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", alias="Person", svg_icon=svg)
    ])
    
    # Get display info (should cache it)
    ref = SchemaRef(name="person", kind="node")
    display_info1 = await db._get_schema_display_info(ref)
    
    assert display_info1["alias"] == "Person"
    # SVG is sanitized, so check for key elements rather than exact match
    assert "<svg" in display_info1["svg_icon"]
    assert "<circle" in display_info1["svg_icon"]
    assert 'cx="50"' in display_info1["svg_icon"]
    assert 'cy="50"' in display_info1["svg_icon"]
    assert 'r="40"' in display_info1["svg_icon"]
    
    # Get again (should use cache)
    display_info2 = await db._get_schema_display_info(ref)
    
    assert display_info2["alias"] == "Person"
    assert display_info2["svg_icon"] == display_info1["svg_icon"]
    assert display_info1 is display_info2  # Same object from cache


@pytest.mark.asyncio
async def test_cache_invalidated_on_schema_update(db: GPGraph):
    """Test that the cache is invalidated when a schema is updated."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    svg1 = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40"/></svg>'
    svg2 = '<svg xmlns="http://www.w3.org/2000/svg"><rect width="100" height="100"/></svg>'
    
    # Create schema with alias and svg_icon
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", alias="Person", svg_icon=svg1)
    ])
    
    # Get display info (should cache it)
    ref = SchemaRef(name="person", kind="node")
    display_info1 = await db._get_schema_display_info(ref)
    
    # Check for circle in first SVG
    assert "<circle" in display_info1["svg_icon"]
    assert 'cx="50"' in display_info1["svg_icon"]
    
    # Update schema with new svg_icon
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", svg_icon=svg2)
    ])
    
    # Get display info again (should fetch fresh data)
    display_info2 = await db._get_schema_display_info(ref)
    
    # Check for rect in second SVG
    assert "<rect" in display_info2["svg_icon"]
    assert 'width="100"' in display_info2["svg_icon"]
    assert display_info1 is not display_info2  # Different objects (cache was invalidated)


@pytest.mark.asyncio
async def test_cache_invalidated_on_schema_delete(db: GPGraph):
    """Test that the cache is invalidated when a schema is deleted."""
    schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle cx="50" cy="50" r="40"/></svg>'
    
    # Create schema with alias and svg_icon
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=schema, kind="node", alias="Person", svg_icon=svg)
    ])
    
    # Get display info (should cache it)
    ref = SchemaRef(name="person", kind="node")
    display_info1 = await db._get_schema_display_info(ref)
    
    assert display_info1["alias"] == "Person"
    
    # Delete schema
    await db.delete_schemas([ref])
    
    # Cache should be cleared
    cache_key = ("person", "node")
    assert cache_key not in db._schema_display_cache
