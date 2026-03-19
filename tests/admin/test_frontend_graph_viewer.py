"""Tests for frontend graph viewer rendering with schema display metadata."""

from urllib.parse import unquote

import pytest
import pytest_asyncio
from gpdb import GPGraph, NodeUpsert, EdgeUpsert, SchemaUpsert, SchemaRef
from gpdb.svg_sanitizer import svg_markup_to_cytoscape_data_uri


def _svg_to_viewer_data_uri(svg_data: str) -> str:
    """Match graph-viewer.js + server: percent-encoded UTF-8 data URIs for Cytoscape."""
    return svg_markup_to_cytoscape_data_uri(svg_data) or ""


def _decode_viewer_data_uri(uri: str) -> str:
    if ";base64," in uri:
        import base64

        return base64.b64decode(uri.split(",", 1)[1]).decode("utf-8")
    return unquote(uri.split(",", 1)[1])


@pytest.mark.asyncio
async def test_svg_icon_registry_builds_correctly(db: GPGraph):
    """Test that SVG icon registry builds correctly from schema metadata."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    svg_icon = "<svg><circle cx='10' cy='10' r='5'/></svg>"
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

    # Simulate building SVG registry (as done in frontend JavaScript)
    svg_registry = {}
    for schema_name, schema in schemas_metadata.items():
        if schema["svg_icon"]:
            svg_data = schema["svg_icon"]
            svg_registry[schema_name] = _svg_to_viewer_data_uri(svg_data)

    # Verify registry contains the schema
    assert "person" in svg_registry
    assert svg_registry["person"].startswith("data:image/svg+xml;charset=utf-8,")
    decoded_svg = _decode_viewer_data_uri(svg_registry["person"])
    assert "<circle" in decoded_svg
    assert 'cx="10"' in decoded_svg
    assert 'r="5"' in decoded_svg


@pytest.mark.asyncio
async def test_multiple_nodes_same_schema_share_svg_reference(db: GPGraph):
    """Test that multiple nodes with same schema share SVG reference."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    svg_icon = "<svg><circle cx='10' cy='10' r='5'/></svg>"
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

    # Build SVG registry
    svg_registry = {}
    for schema_name, schema in schemas_metadata.items():
        if schema["svg_icon"]:
            svg_data = schema["svg_icon"]
            svg_registry[schema_name] = _svg_to_viewer_data_uri(svg_data)

    # Verify only one entry in registry
    assert len(svg_registry) == 1
    # Verify all nodes reference the same schema
    assert len(node_page.items) == 3
    for node in node_page.items:
        assert node.type == "person"
        # All nodes would use the same SVG reference from the registry


@pytest.mark.asyncio
async def test_alias_displays_in_label(db: GPGraph):
    """Test that alias displays in label."""
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

    # Simulate applying display labels (as done in frontend JavaScript)
    for element in elements:
        display_label = element["data"]["display_label"]
        if display_label:
            element["data"]["label"] = display_label

    # Verify alias is used as label
    assert len(elements) == 1
    assert elements[0]["data"]["label"] == "Person"


@pytest.mark.asyncio
async def test_fallback_to_schema_name_when_no_alias(db: GPGraph):
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

    # Simulate applying display labels
    for element in elements:
        display_label = element["data"]["display_label"]
        if display_label:
            element["data"]["label"] = display_label

    # Verify schema name is used as fallback
    assert len(elements) == 1
    assert elements[0]["data"]["label"] == "person"


@pytest.mark.asyncio
async def test_fallback_to_default_styling_when_no_svg(db: GPGraph):
    """Test that nodes without SVG icon fall back to default styling."""
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

    # Build SVG registry
    svg_registry = {}
    for schema_name, schema in schemas_metadata.items():
        if schema["svg_icon"]:
            svg_data = schema["svg_icon"]
            svg_registry[schema_name] = _svg_to_viewer_data_uri(svg_data)

    # Verify no SVG in registry
    assert len(svg_registry) == 0

    # Simulate getting background-image for node (as done in frontend JavaScript)
    for node in node_page.items:
        schema = node.type
        background_image = svg_registry.get(schema, "")
        # Should be empty string when no SVG
        assert background_image == ""


@pytest.mark.asyncio
async def test_svg_icon_does_not_duplicate_in_dom(db: GPGraph):
    """Test that SVG icon doesn't duplicate in DOM (single registry entry)."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    svg_icon = "<svg><circle cx='10' cy='10' r='5'/></svg>"
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

    # Build SVG registry (should only have one entry)
    svg_registry = {}
    for schema_name, schema in schemas_metadata.items():
        if schema["svg_icon"]:
            svg_data = schema["svg_icon"]
            svg_registry[schema_name] = _svg_to_viewer_data_uri(svg_data)

    # Verify only one entry in registry (no duplication)
    assert len(svg_registry) == 1
    assert "person" in svg_registry

    # Verify all nodes would reference the same SVG
    for node in node_page.items:
        assert node.type == "person"
        # All nodes use the same SVG reference from registry
        assert svg_registry["person"] == svg_registry[node.type]


@pytest.mark.asyncio
async def test_display_label_updates_when_schema_metadata_changes(db: GPGraph):
    """Test that display_label updates when schema metadata changes."""
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

    # Apply display labels
    for element in elements:
        display_label = element["data"]["display_label"]
        if display_label:
            element["data"]["label"] = display_label

    # Verify initial alias
    assert elements[0]["data"]["label"] == "Person"

    # Update schema with new alias
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Human",
        )
    ])

    # Re-fetch schema metadata
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    # Rebuild elements with updated display_label
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

    # Apply updated display labels
    for element in elements:
        display_label = element["data"]["display_label"]
        if display_label:
            element["data"]["label"] = display_label

    # Verify updated alias
    assert elements[0]["data"]["label"] == "Human"


@pytest.mark.asyncio
async def test_svg_icon_updates_when_schema_metadata_changes(db: GPGraph):
    """Test that SVG icon updates when schema metadata changes."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    svg_icon1 = "<svg><circle cx='10' cy='10' r='5'/></svg>"
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            svg_icon=svg_icon1,
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

    # Build SVG registry
    svg_registry = {}
    for schema_name, schema in schemas_metadata.items():
        if schema["svg_icon"]:
            svg_data = schema["svg_icon"]
            svg_registry[schema_name] = _svg_to_viewer_data_uri(svg_data)

    # Verify initial SVG
    assert "person" in svg_registry
    decoded_svg = _decode_viewer_data_uri(svg_registry["person"])
    assert "<circle" in decoded_svg
    assert 'cx="10"' in decoded_svg

    # Update schema with new SVG
    svg_icon2 = "<svg><rect x='5' y='5' width='10' height='10'/></svg>"
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            svg_icon=svg_icon2,
        )
    ])

    # Re-fetch schema metadata
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    # Rebuild SVG registry
    svg_registry = {}
    for schema_name, schema in schemas_metadata.items():
        if schema["svg_icon"]:
            svg_data = schema["svg_icon"]
            svg_registry[schema_name] = _svg_to_viewer_data_uri(svg_data)

    # Verify updated SVG
    assert "person" in svg_registry
    decoded_svg = _decode_viewer_data_uri(svg_registry["person"])
    assert "<rect" in decoded_svg
    assert 'x="5"' in decoded_svg
    assert 'width="10"' in decoded_svg
    assert "<circle" not in decoded_svg


@pytest.mark.asyncio
async def test_display_label_applied_to_edges(db: GPGraph):
    """Test that display_label is applied to edges."""
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

    # Simulate applying display labels (as done in frontend JavaScript)
    for element in elements:
        display_label = element["data"]["display_label"]
        if display_label:
            element["data"]["label"] = display_label

    # Verify alias is used as label for edge
    assert len(elements) == 1
    assert elements[0]["data"]["label"] == "Friend"


@pytest.mark.asyncio
async def test_multiple_schema_types_with_different_icons(db: GPGraph):
    """Test that multiple schema types with different icons work correctly."""
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
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
            svg_icon="<svg><circle cx='10' cy='10' r='5'/></svg>",
        ),
        SchemaUpsert(
            name="company",
            json_schema=company_schema,
            kind="node",
            alias="Company",
            svg_icon="<svg><rect x='5' y='5' width='10' height='10'/></svg>",
        ),
    ])

    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
        NodeUpsert(id="company1", type="company", data={"name": "Acme Corp"}),
    ])

    from gpdb import SearchQuery
    node_page = await db.search_nodes(SearchQuery(limit=10))

    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    # Build SVG registry
    svg_registry = {}
    for schema_name, schema in schemas_metadata.items():
        if schema["svg_icon"]:
            svg_data = schema["svg_icon"]
            svg_registry[schema_name] = _svg_to_viewer_data_uri(svg_data)

    # Verify both schemas have SVGs
    assert len(svg_registry) == 2
    assert "person" in svg_registry
    assert "company" in svg_registry

    # Verify different SVGs
    assert svg_registry["person"] != svg_registry["company"]
    decoded_person_svg = _decode_viewer_data_uri(svg_registry["person"])
    decoded_company_svg = _decode_viewer_data_uri(svg_registry["company"])
    assert "<circle" in decoded_person_svg
    assert 'cx="10"' in decoded_person_svg
    assert "<rect" in decoded_company_svg
    assert 'width="10"' in decoded_company_svg

    # Verify display labels
    assert schemas_metadata["person"]["alias"] == "Person"
    assert schemas_metadata["company"]["alias"] == "Company"
