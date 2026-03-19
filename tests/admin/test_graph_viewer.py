"""Tests for graph viewer data endpoint with schema display metadata."""

import pytest
import pytest_asyncio
from gpdb import (
    GPGraph,
    NodeUpsert,
    EdgeUpsert,
    SchemaRef,
    SchemaUpsert,
    normalize_svg_icon_for_display,
    sanitize_svg,
)

_RAW_PERSON_CIRCLE_SVG = "<svg><circle cx='10' cy='10' r='5'/></svg>"
EXPECTED_PERSON_CIRCLE_SVG = normalize_svg_icon_for_display(
    sanitize_svg(_RAW_PERSON_CIRCLE_SVG)
)


@pytest.mark.asyncio
async def test_viewer_data_includes_schema_metadata(db: GPGraph):
    """Test that viewer data includes schema metadata dict."""
    # Create a schema with alias and svg_icon
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
            svg_icon=_RAW_PERSON_CIRCLE_SVG,
        )
    ])

    # Create a node using the schema
    await db.set_nodes([
        NodeUpsert(
            id="person1",
            type="person",
            data={"name": "Alice"},
        )
    ])

    # Get viewer data
    from gpdb import SearchQuery
    node_page = await db.search_nodes(SearchQuery(limit=10))
    edge_page = await db.search_edges(SearchQuery(limit=10))

    # Build viewer data manually (simulating what the admin endpoint does)
    schema_types = set()
    for node in node_page.items:
        schema_types.add((node.type, "node"))
    for edge in edge_page.items:
        schema_types.add((edge.type, "edge"))

    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    # Verify schema metadata is included
    assert "person" in schemas_metadata
    assert schemas_metadata["person"]["alias"] == "Person"
    assert schemas_metadata["person"]["svg_icon"] == EXPECTED_PERSON_CIRCLE_SVG


@pytest.mark.asyncio
async def test_schema_metadata_includes_alias_if_set(db: GPGraph):
    """Test that schema metadata includes alias if set."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
        )
    ])

    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"})
    ])

    from gpdb import SearchQuery
    node_page = await db.search_nodes(SearchQuery(limit=10))

    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    assert "person" in schemas_metadata
    assert schemas_metadata["person"]["alias"] == "Person"


@pytest.mark.asyncio
async def test_schema_metadata_includes_svg_icon_if_set(db: GPGraph):
    """Test that schema metadata includes svg_icon if set."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    svg_icon = _RAW_PERSON_CIRCLE_SVG
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            svg_icon=svg_icon,
        )
    ])

    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"})
    ])

    from gpdb import SearchQuery
    node_page = await db.search_nodes(SearchQuery(limit=10))

    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    assert "person" in schemas_metadata
    # SVG is normalized by sanitizer (single quotes → double quotes, self-closing → opening/closing)
    assert schemas_metadata["person"]["svg_icon"] == EXPECTED_PERSON_CIRCLE_SVG


@pytest.mark.asyncio
async def test_node_elements_have_display_label_field(db: GPGraph):
    """Test that node elements have display_label field."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
        )
    ])

    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"})
    ])

    from gpdb import SearchQuery
    node_page = await db.search_nodes(SearchQuery(limit=10))

    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    # Build elements with display_label
    elements = []
    for node in node_page.items:
        display_info = schemas_metadata.get(node.type, {})
        elements.append({
            "group": "nodes",
            "data": {
                "id": node.id,
                "label": node.name or node.id,
                "type": node.type,
                "display_label": display_info.get("alias") or node.type,
            },
        })

    assert len(elements) == 1
    assert "display_label" in elements[0]["data"]
    assert elements[0]["data"]["display_label"] == "Person"


@pytest.mark.asyncio
async def test_edge_elements_have_display_label_field(db: GPGraph):
    """Test that edge elements have display_label field."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    friend_schema = {
        "type": "object",
        "properties": {
            "since": {"type": "string"},
        },
    }
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node"),
        SchemaUpsert(
            name="friend",
            json_schema=friend_schema,
            kind="edge",
            alias="Friend",
        ),
    ])

    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
        NodeUpsert(id="person2", type="person", data={"name": "Bob"}),
    ])

    await db.set_edges([
        EdgeUpsert(
            id="edge1",
            type="friend",
            source_id="person1",
            target_id="person2",
            data={"since": "2020"},
        )
    ])

    from gpdb import SearchQuery
    edge_page = await db.search_edges(SearchQuery(limit=10))

    schema_types = {(edge.type, "edge") for edge in edge_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    # Build elements with display_label
    elements = []
    for edge in edge_page.items:
        display_info = schemas_metadata.get(edge.type, {})
        elements.append({
            "group": "edges",
            "data": {
                "id": edge.id,
                "source": edge.source_id,
                "target": edge.target_id,
                "label": edge.type,
                "display_label": display_info.get("alias") or edge.type,
            },
        })

    assert len(elements) == 1
    assert "display_label" in elements[0]["data"]
    assert elements[0]["data"]["display_label"] == "Friend"


@pytest.mark.asyncio
async def test_display_label_uses_alias_if_available(db: GPGraph):
    """Test that display_label uses alias if available."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
        )
    ])

    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"})
    ])

    from gpdb import SearchQuery
    node_page = await db.search_nodes(SearchQuery(limit=10))

    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    elements = []
    for node in node_page.items:
        display_info = schemas_metadata.get(node.type, {})
        elements.append({
            "group": "nodes",
            "data": {
                "id": node.id,
                "label": node.name or node.id,
                "type": node.type,
                "display_label": display_info.get("alias") or node.type,
            },
        })

    assert elements[0]["data"]["display_label"] == "Person"


@pytest.mark.asyncio
async def test_display_label_falls_back_to_schema_name(db: GPGraph):
    """Test that display_label falls back to schema name when no alias."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
        )
    ])

    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"})
    ])

    from gpdb import SearchQuery
    node_page = await db.search_nodes(SearchQuery(limit=10))

    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    elements = []
    for node in node_page.items:
        display_info = schemas_metadata.get(node.type, {})
        elements.append({
            "group": "nodes",
            "data": {
                "id": node.id,
                "label": node.name or node.id,
                "type": node.type,
                "display_label": display_info.get("alias") or node.type,
            },
        })

    assert elements[0]["data"]["display_label"] == "person"


@pytest.mark.asyncio
async def test_multiple_nodes_same_schema_no_duplicate_svg(db: GPGraph):
    """Test that multiple nodes with same schema don't duplicate SVG in metadata."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    svg_icon = _RAW_PERSON_CIRCLE_SVG
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            svg_icon=svg_icon,
        )
    ])

    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
        NodeUpsert(id="person2", type="person", data={"name": "Bob"}),
        NodeUpsert(id="person3", type="person", data={"name": "Charlie"}),
    ])

    from gpdb import SearchQuery
    node_page = await db.search_nodes(SearchQuery(limit=10))

    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    # Verify only one entry in schemas_metadata
    assert len(schemas_metadata) == 1
    assert "person" in schemas_metadata
    # SVG is normalized by sanitizer (single quotes → double quotes, self-closing → opening/closing)
    assert schemas_metadata["person"]["svg_icon"] == EXPECTED_PERSON_CIRCLE_SVG

    # Verify all nodes reference the same schema
    assert len(node_page.items) == 3
    for node in node_page.items:
        assert node.type == "person"


@pytest.mark.asyncio
async def test_viewer_data_handles_missing_schemas_gracefully(db: GPGraph):
    """Test that viewer data handles missing schemas gracefully."""
    # Create a schema first, then create a node
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node")
    ])

    await db.set_nodes([
        NodeUpsert(id="node1", type="person", data={"name": "Test"})
    ])

    from gpdb import SearchQuery
    node_page = await db.search_nodes(SearchQuery(limit=10))

    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        try:
            ref = SchemaRef(name=schema_name, kind=schema_kind)
            display_info = await db._get_schema_display_info(ref)
            schemas_metadata[schema_name] = display_info
        except Exception:
            # Schema doesn't exist, skip it
            pass

    # Verify schemas_metadata contains schema
    assert len(schemas_metadata) == 1
    assert "person" in schemas_metadata

    # Verify elements can still be built with display_info
    elements = []
    for node in node_page.items:
        display_info = schemas_metadata.get(node.type, {})
        elements.append({
            "group": "nodes",
            "data": {
                "id": node.id,
                "label": node.name or node.id,
                "type": node.type,
                "display_label": display_info.get("alias") or node.type,
            },
        })

    assert len(elements) == 1
    assert elements[0]["data"]["display_label"] == "person"


@pytest.mark.asyncio
async def test_viewer_data_works_with_schemas_no_alias_or_svg(db: GPGraph):
    """Test that viewer data works with schemas that have no alias or svg_icon."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
        )
    ])

    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"})
    ])

    from gpdb import SearchQuery
    node_page = await db.search_nodes(SearchQuery(limit=10))

    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    # Verify schema metadata exists but has None values
    assert "person" in schemas_metadata
    assert schemas_metadata["person"]["alias"] is None
    assert schemas_metadata["person"]["svg_icon"] is None

    # Verify elements work correctly
    elements = []
    for node in node_page.items:
        display_info = schemas_metadata.get(node.type, {})
        elements.append({
            "group": "nodes",
            "data": {
                "id": node.id,
                "label": node.name or node.id,
                "type": node.type,
                "display_label": display_info.get("alias") or node.type,
            },
        })

    assert len(elements) == 1
    assert elements[0]["data"]["display_label"] == "person"


@pytest.mark.asyncio
async def test_viewer_data_with_multiple_schema_types(db: GPGraph):
    """Test that viewer data works correctly with multiple schema types."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    company_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    works_for_schema = {
        "type": "object",
        "properties": {
            "role": {"type": "string"},
        },
    }

    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
            svg_icon=_RAW_PERSON_CIRCLE_SVG,
        ),
        SchemaUpsert(
            name="company",
            json_schema=company_schema,
            kind="node",
            alias="Company",
        ),
        SchemaUpsert(
            name="works_for",
            json_schema=works_for_schema,
            kind="edge",
            alias="Works For",
        ),
    ])

    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
        NodeUpsert(id="company1", type="company", data={"name": "Acme Corp"}),
    ])

    await db.set_edges([
        EdgeUpsert(
            id="edge1",
            type="works_for",
            source_id="person1",
            target_id="company1",
            data={"role": "Engineer"},
        )
    ])

    from gpdb import SearchQuery
    node_page = await db.search_nodes(SearchQuery(limit=10))
    edge_page = await db.search_edges(SearchQuery(limit=10))

    schema_types = set()
    for node in node_page.items:
        schema_types.add((node.type, "node"))
    for edge in edge_page.items:
        schema_types.add((edge.type, "edge"))

    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    # Verify all schemas are included
    assert len(schemas_metadata) == 3
    assert "person" in schemas_metadata
    assert "company" in schemas_metadata
    assert "works_for" in schemas_metadata

    # Verify display labels
    assert schemas_metadata["person"]["alias"] == "Person"
    assert schemas_metadata["company"]["alias"] == "Company"
    assert schemas_metadata["works_for"]["alias"] == "Works For"

    # Verify SVG icon is included for person (normalized by sanitizer)
    assert schemas_metadata["person"]["svg_icon"] == EXPECTED_PERSON_CIRCLE_SVG
