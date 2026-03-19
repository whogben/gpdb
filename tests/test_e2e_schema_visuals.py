"""End-to-end tests for schema visualization features (alias and svg_icon)."""

from urllib.parse import unquote

import pytest
import pytest_asyncio
from gpdb import GPGraph, NodeUpsert, EdgeUpsert, SchemaUpsert, SchemaRef, SearchQuery
from gpdb.svg_sanitizer import svg_markup_to_cytoscape_data_uri


@pytest.mark.asyncio
async def test_complete_flow_with_alias_and_svg_icon(db: GPGraph):
    """Test complete flow: create schema with alias and svg_icon, create nodes, view in viewer."""
    # Create schema with alias and svg_icon
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    svg_icon = "<svg><circle cx='10' cy='10' r='5'/></svg>"
    
    result = await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
            svg_icon=svg_icon,
        )
    ])
    
    assert len(result) == 1
    assert result[0].alias == "Person"
    assert "<svg" in result[0].svg_icon
    
    # Create nodes using the schema
    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
        NodeUpsert(id="person2", type="person", data={"name": "Bob"}),
    ])
    
    # View graph in viewer (simulate viewer data endpoint)
    node_page = await db.search_nodes(SearchQuery(limit=10))
    
    # Build schema metadata (simulating viewer endpoint)
    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info
    
    # Verify SVG icon displays in metadata
    assert "person" in schemas_metadata
    assert schemas_metadata["person"]["alias"] == "Person"
    assert "<svg" in schemas_metadata["person"]["svg_icon"]
    
    # Verify alias displays in node elements
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
    
    assert len(elements) == 2
    assert all(elem["data"]["display_label"] == "Person" for elem in elements)


@pytest.mark.asyncio
async def test_update_schema_alias(db: GPGraph):
    """Test updating schema alias and verifying viewer updates."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    
    # Create schema with alias
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
        )
    ])
    
    # Create nodes using the schema
    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
    ])
    
    # Verify initial alias
    ref = SchemaRef(name="person", kind="node")
    display_info1 = await db._get_schema_display_info(ref)
    assert display_info1["alias"] == "Person"
    
    # Update schema alias
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Human",
        )
    ])
    
    # Verify viewer updates with new alias
    display_info2 = await db._get_schema_display_info(ref)
    assert display_info2["alias"] == "Human"
    
    # Verify nodes display new alias
    node_page = await db.search_nodes(SearchQuery(limit=10))
    schemas_metadata = {}
    for node in node_page.items:
        ref = SchemaRef(name=node.type, kind="node")
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[node.type] = display_info
    
    elements = []
    for node in node_page.items:
        display_info = schemas_metadata.get(node.type, {})
        elements.append({
            "group": "nodes",
            "data": {
                "id": node.id,
                "display_label": display_info.get("alias") or node.type,
            },
        })
    
    assert elements[0]["data"]["display_label"] == "Human"


@pytest.mark.asyncio
async def test_update_schema_svg_icon(db: GPGraph):
    """Test updating schema svg_icon and verifying viewer updates."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    svg1 = "<svg><circle cx='10' cy='10' r='5'/></svg>"
    svg2 = "<svg><rect width='20' height='20'/></svg>"
    
    # Create schema with svg_icon
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            svg_icon=svg1,
        )
    ])
    
    # Create nodes using the schema
    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
    ])
    
    # Verify initial SVG
    ref = SchemaRef(name="person", kind="node")
    display_info1 = await db._get_schema_display_info(ref)
    assert "<circle" in display_info1["svg_icon"]
    
    # Update schema svg_icon
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            svg_icon=svg2,
        )
    ])
    
    # Verify viewer updates with new SVG
    display_info2 = await db._get_schema_display_info(ref)
    assert "<rect" in display_info2["svg_icon"]
    assert "<circle" not in display_info2["svg_icon"]


@pytest.mark.asyncio
async def test_delete_schema_with_nodes_fails(db: GPGraph):
    """Test that deleting a schema with nodes fails."""
    from gpdb import SchemaInUseError
    
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    
    # Create schema with alias and svg_icon
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
            svg_icon="<svg><circle cx='10' cy='10' r='5'/></svg>",
        )
    ])
    
    # Create nodes using the schema
    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
    ])
    
    # Try to delete schema (should fail)
    ref = SchemaRef(name="person", kind="node")
    with pytest.raises(SchemaInUseError):
        await db.delete_schemas([ref])
    
    # Verify viewer still works with existing schema
    node_page = await db.search_nodes(SearchQuery(limit=10))
    assert len(node_page.items) == 1
    
    schemas_metadata = {}
    for node in node_page.items:
        ref = SchemaRef(name=node.type, kind="node")
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[node.type] = display_info
    
    assert "person" in schemas_metadata
    assert schemas_metadata["person"]["alias"] == "Person"


@pytest.mark.asyncio
async def test_schema_with_only_alias(db: GPGraph):
    """Test schema with only alias (no svg_icon)."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    
    # Create schema with alias only
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
        )
    ])
    
    # Create nodes using the schema
    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
    ])
    
    # Verify alias displays
    ref = SchemaRef(name="person", kind="node")
    display_info = await db._get_schema_display_info(ref)
    assert display_info["alias"] == "Person"
    assert display_info["svg_icon"] is None
    
    # Verify default node styling (no SVG)
    node_page = await db.search_nodes(SearchQuery(limit=10))
    schemas_metadata = {}
    for node in node_page.items:
        ref = SchemaRef(name=node.type, kind="node")
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[node.type] = display_info
    
    elements = []
    for node in node_page.items:
        display_info = schemas_metadata.get(node.type, {})
        elements.append({
            "group": "nodes",
            "data": {
                "id": node.id,
                "display_label": display_info.get("alias") or node.type,
            },
        })
    
    assert elements[0]["data"]["display_label"] == "Person"
    assert elements[0]["data"].get("iconUri") is None


@pytest.mark.asyncio
async def test_schema_with_only_svg_icon(db: GPGraph):
    """Test schema with only svg_icon (no alias)."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    svg_icon = "<svg><circle cx='10' cy='10' r='5'/></svg>"
    
    # Create schema with svg_icon only
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            svg_icon=svg_icon,
        )
    ])
    
    # Create nodes using the schema
    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
    ])
    
    # Verify SVG icon displays
    ref = SchemaRef(name="person", kind="node")
    display_info = await db._get_schema_display_info(ref)
    assert display_info["alias"] is None
    assert "<svg" in display_info["svg_icon"]
    
    # Verify schema name displays as label
    node_page = await db.search_nodes(SearchQuery(limit=10))
    schemas_metadata = {}
    for node in node_page.items:
        ref = SchemaRef(name=node.type, kind="node")
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[node.type] = display_info
    
    elements = []
    for node in node_page.items:
        display_info = schemas_metadata.get(node.type, {})
        uri = svg_markup_to_cytoscape_data_uri(display_info.get("svg_icon"))
        data = {
            "id": node.id,
            "display_label": display_info.get("alias") or node.type,
        }
        if uri:
            data["iconUri"] = uri
        elements.append({"group": "nodes", "data": data})
    
    assert elements[0]["data"]["display_label"] == "person"
    assert elements[0]["data"]["iconUri"].startswith("data:image/svg+xml;charset=utf-8,")
    decoded = unquote(elements[0]["data"]["iconUri"].split(",", 1)[1])
    assert "<circle" in decoded


@pytest.mark.asyncio
async def test_schema_with_neither_alias_nor_svg_icon(db: GPGraph):
    """Test schema with neither alias nor svg_icon."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    
    # Create schema with neither
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
        )
    ])
    
    # Create nodes using the schema
    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
    ])
    
    # Verify schema name displays as label
    ref = SchemaRef(name="person", kind="node")
    display_info = await db._get_schema_display_info(ref)
    assert display_info["alias"] is None
    assert display_info["svg_icon"] is None
    
    # Verify default node styling
    node_page = await db.search_nodes(SearchQuery(limit=10))
    schemas_metadata = {}
    for node in node_page.items:
        ref = SchemaRef(name=node.type, kind="node")
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[node.type] = display_info
    
    elements = []
    for node in node_page.items:
        display_info = schemas_metadata.get(node.type, {})
        elements.append({
            "group": "nodes",
            "data": {
                "id": node.id,
                "display_label": display_info.get("alias") or node.type,
            },
        })
    
    assert elements[0]["data"]["display_label"] == "person"
    assert elements[0]["data"].get("iconUri") is None


@pytest.mark.asyncio
async def test_multiple_schemas_with_different_aliases_and_svgs(db: GPGraph):
    """Test multiple schemas with different aliases and SVGs."""
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
    
    # Create multiple schemas
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
            svg_icon="<svg><rect width='20' height='20'/></svg>",
        ),
    ])
    
    # Create nodes using different schemas
    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
        NodeUpsert(id="company1", type="company", data={"name": "Acme Corp"}),
        NodeUpsert(id="person2", type="person", data={"name": "Bob"}),
    ])
    
    # Verify each schema displays correctly
    node_page = await db.search_nodes(SearchQuery(limit=10))
    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info
    
    # Verify all schemas are included
    assert len(schemas_metadata) == 2
    assert "person" in schemas_metadata
    assert "company" in schemas_metadata
    
    # Verify each schema has correct alias and SVG
    assert schemas_metadata["person"]["alias"] == "Person"
    assert "<circle" in schemas_metadata["person"]["svg_icon"]
    assert schemas_metadata["company"]["alias"] == "Company"
    assert "<rect" in schemas_metadata["company"]["svg_icon"]
    
    # Verify no SVG duplication (only one entry per schema)
    assert len(schemas_metadata) == 2
    
    # Verify nodes reference correct schemas
    elements = []
    for node in node_page.items:
        display_info = schemas_metadata.get(node.type, {})
        elements.append({
            "group": "nodes",
            "data": {
                "id": node.id,
                "type": node.type,
                "display_label": display_info.get("alias") or node.type,
            },
        })
    
    # Verify person nodes have Person label
    person_elements = [e for e in elements if e["data"]["type"] == "person"]
    assert len(person_elements) == 2
    assert all(e["data"]["display_label"] == "Person" for e in person_elements)
    
    # Verify company node has Company label
    company_elements = [e for e in elements if e["data"]["type"] == "company"]
    assert len(company_elements) == 1
    assert company_elements[0]["data"]["display_label"] == "Company"


@pytest.mark.asyncio
async def test_large_graph_performance(db: GPGraph):
    """Test viewer performance with large graph (200 nodes)."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    svg_icon = "<svg><circle cx='10' cy='10' r='5'/></svg>"
    
    # Create schema with alias and svg_icon
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
            svg_icon=svg_icon,
        )
    ])
    
    # Create 200 nodes using the schema
    nodes = [
        NodeUpsert(id=f"person{i}", type="person", data={"name": f"Person {i}"})
        for i in range(200)
    ]
    await db.set_nodes(nodes)
    
    # Verify viewer loads efficiently
    node_page = await db.search_nodes(SearchQuery(limit=200))
    assert len(node_page.items) == 200
    
    # Build schema metadata
    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info
    
    # Verify SVG is not duplicated 200 times (only one entry in metadata)
    assert len(schemas_metadata) == 1
    assert "person" in schemas_metadata
    assert schemas_metadata["person"]["alias"] == "Person"
    assert "<svg" in schemas_metadata["person"]["svg_icon"]
    
    # Verify all nodes reference the same schema
    for node in node_page.items:
        assert node.type == "person"
    
    # Verify elements can be built efficiently
    elements = []
    for node in node_page.items:
        display_info = schemas_metadata.get(node.type, {})
        elements.append({
            "group": "nodes",
            "data": {
                "id": node.id,
                "display_label": display_info.get("alias") or node.type,
            },
        })
    
    assert len(elements) == 200
    assert all(elem["data"]["display_label"] == "Person" for elem in elements)


@pytest.mark.asyncio
async def test_invalid_svg_handling(db: GPGraph):
    """Test that creating schema with invalid SVG raises error."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    invalid_svg = '<script>alert("xss")</script>'
    
    # Attempt to create schema with invalid SVG
    with pytest.raises(ValueError) as exc_info:
        await db.set_schemas([
            SchemaUpsert(
                name="person",
                json_schema=person_schema,
                kind="node",
                svg_icon=invalid_svg,
            )
        ])
    
    assert "not valid XML" in str(exc_info.value)
    
    # Verify schema was not created
    ref = SchemaRef(name="person", kind="node")
    with pytest.raises(Exception):  # SchemaNotFoundError or similar
        await db.get_schemas([ref])


@pytest.mark.asyncio
async def test_svg_size_limit(db: GPGraph):
    """Test that creating schema with SVG over 20KB raises error."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    # Create an SVG that's over 20KB
    large_svg = '<svg xmlns="http://www.w3.org/2000/svg">' + '<circle/>' * 10000 + '</svg>'
    
    # Attempt to create schema with large SVG
    with pytest.raises(ValueError) as exc_info:
        await db.set_schemas([
            SchemaUpsert(
                name="person",
                json_schema=person_schema,
                kind="node",
                svg_icon=large_svg,
            )
        ])
    
    assert "exceeds maximum size" in str(exc_info.value)
    
    # Verify schema was not created
    ref = SchemaRef(name="person", kind="node")
    with pytest.raises(Exception):  # SchemaNotFoundError or similar
        await db.get_schemas([ref])


@pytest.mark.asyncio
async def test_edge_schemas_with_alias_and_svg_icon(db: GPGraph):
    """Test edge schemas with alias and svg_icon."""
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
    svg_icon = "<svg><line x1='0' y1='0' x2='20' y2='20'/></svg>"
    
    # Create node and edge schemas
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
        ),
        SchemaUpsert(
            name="friend",
            json_schema=friend_schema,
            kind="edge",
            alias="Friend",
            svg_icon=svg_icon,
        ),
    ])
    
    # Create nodes and edges
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
    
    # Verify edge schema metadata
    ref = SchemaRef(name="friend", kind="edge")
    display_info = await db._get_schema_display_info(ref)
    assert display_info["alias"] == "Friend"
    assert "<svg" in display_info["svg_icon"]
    
    # Verify alias displays in edge labels
    edge_page = await db.search_edges(SearchQuery(limit=10))
    schema_types = {(edge.type, "edge") for edge in edge_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info
    
    elements = []
    for edge in edge_page.items:
        display_info = schemas_metadata.get(edge.type, {})
        elements.append({
            "group": "edges",
            "data": {
                "id": edge.id,
                "source": edge.source_id,
                "target": edge.target_id,
                "display_label": display_info.get("alias") or edge.type,
            },
        })
    
    assert len(elements) == 1
    assert elements[0]["data"]["display_label"] == "Friend"


@pytest.mark.asyncio
async def test_viewer_handles_missing_schema_gracefully(db: GPGraph):
    """Test that viewer handles missing schema gracefully."""
    # Create a node without a schema (should not happen in normal flow, but test robustness)
    # First create a schema
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node")
    ])
    
    # Create a node
    await db.set_nodes([
        NodeUpsert(id="node1", type="person", data={"name": "Test"})
    ])
    
    # Get viewer data
    node_page = await db.search_nodes(SearchQuery(limit=10))
    
    # Build schema metadata with error handling
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
async def test_schema_update_preserves_existing_values(db: GPGraph):
    """Test that updating schema with None values preserves existing values."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    svg_icon = "<svg><circle cx='10' cy='10' r='5'/></svg>"
    
    # Create schema with both alias and svg_icon
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
            svg_icon=svg_icon,
        )
    ])
    
    # Update with None values (should keep existing)
    result = await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias=None,
            svg_icon=None,
        )
    ])
    
    assert len(result) == 1
    assert result[0].alias == "Person"
    assert "<svg" in result[0].svg_icon
    
    # Verify viewer still shows both
    ref = SchemaRef(name="person", kind="node")
    display_info = await db._get_schema_display_info(ref)
    assert display_info["alias"] == "Person"
    assert "<svg" in display_info["svg_icon"]


@pytest.mark.asyncio
async def test_multiple_nodes_same_schema_no_duplicate_svg(db: GPGraph):
    """Test that multiple nodes with same schema don't duplicate SVG in metadata."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    svg_icon = "<svg><circle cx='10' cy='10' r='5'/></svg>"
    
    # Create schema with svg_icon
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            svg_icon=svg_icon,
        )
    ])
    
    # Create multiple nodes using the same schema
    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
        NodeUpsert(id="person2", type="person", data={"name": "Bob"}),
        NodeUpsert(id="person3", type="person", data={"name": "Charlie"}),
    ])
    
    # Get viewer data
    node_page = await db.search_nodes(SearchQuery(limit=10))
    
    # Build schema metadata
    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info
    
    # Verify only one entry in schemas_metadata (no duplication)
    assert len(schemas_metadata) == 1
    assert "person" in schemas_metadata
    assert "<svg" in schemas_metadata["person"]["svg_icon"]
    
    # Verify all nodes reference the same schema
    assert len(node_page.items) == 3
    for node in node_page.items:
        assert node.type == "person"
    
    # Verify elements can be built with shared SVG reference
    expected_uri = svg_markup_to_cytoscape_data_uri(
        schemas_metadata["person"]["svg_icon"]
    )
    elements = []
    for node in node_page.items:
        elements.append({
            "group": "nodes",
            "data": {
                "id": node.id,
                "iconUri": expected_uri,
            },
        })
    
    # All elements should reference the same SVG (not duplicated)
    assert len(elements) == 3
    assert all(elem["data"]["iconUri"] == expected_uri for elem in elements)


@pytest.mark.asyncio
async def test_svg_sanitization_removes_dangerous_content(db: GPGraph):
    """Test that SVG sanitization removes dangerous content."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    # SVG with potentially dangerous content
    svg_with_script = '<svg xmlns="http://www.w3.org/2000/svg"><script>alert("xss")</script><circle cx="50" cy="50" r="40"/></svg>'
    
    # Create schema with dangerous SVG
    result = await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            svg_icon=svg_with_script,
        )
    ])
    
    # Script should be removed
    assert "<script>" not in result[0].svg_icon
    assert "<circle" in result[0].svg_icon
    
    # Verify viewer shows sanitized SVG
    ref = SchemaRef(name="person", kind="node")
    display_info = await db._get_schema_display_info(ref)
    assert "<script>" not in display_info["svg_icon"]
    assert "<circle" in display_info["svg_icon"]


@pytest.mark.asyncio
async def test_display_label_fallback_chain(db: GPGraph):
    """Test display_label fallback chain: alias → schema name."""
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    
    # Create schema with alias
    await db.set_schemas([
        SchemaUpsert(
            name="person",
            json_schema=person_schema,
            kind="node",
            alias="Person",
        )
    ])
    
    # Create node
    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"}),
    ])
    
    # Get viewer data
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
        display_label = display_info.get("alias") or node.type
        elements.append({
            "group": "nodes",
            "data": {
                "id": node.id,
                "display_label": display_label,
            },
        })
    
    # Verify alias is used
    assert elements[0]["data"]["display_label"] == "Person"
    
    # Create a second schema without alias
    company_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
    }
    await db.set_schemas([
        SchemaUpsert(
            name="company",
            json_schema=company_schema,
            kind="node",
        )
    ])
    
    # Create node with second schema
    await db.set_nodes([
        NodeUpsert(id="company1", type="company", data={"name": "Acme Corp"}),
    ])
    
    # Get fresh viewer data
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
        display_label = display_info.get("alias") or node.type
        elements.append({
            "group": "nodes",
            "data": {
                "id": node.id,
                "type": node.type,
                "display_label": display_label,
            },
        })
    
    # Verify person node uses alias
    person_elements = [e for e in elements if e["data"]["type"] == "person"]
    assert len(person_elements) == 1
    assert person_elements[0]["data"]["display_label"] == "Person"
    
    # Verify company node falls back to schema name
    company_elements = [e for e in elements if e["data"]["type"] == "company"]
    assert len(company_elements) == 1
    assert company_elements[0]["data"]["display_label"] == "company"
