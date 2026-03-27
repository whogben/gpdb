"""Tests for graph viewer data format (Gephi Lite)."""

from urllib.parse import unquote

import pytest
from gpdb import GPGraph, NodeUpsert, EdgeUpsert, SchemaUpsert, SchemaRef
from gpdb.svg_sanitizer import svg_to_data_uri
from gpdb.admin.graph_content.edges import _flatten_dict, _infer_field_type, _build_field_defs
from gpdb.admin.graph_content.models import GephiViewerData


def _decode_data_uri(uri: str) -> str:
    """Decode a percent-encoded SVG data URI back to SVG markup."""
    return unquote(uri.split(",", 1)[1])


@pytest.mark.asyncio
async def test_svg_icon_data_uri_builds_correctly(db: GPGraph):
    """SVG icon data URIs are built correctly from schema metadata."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    svg_icon = "<svg><circle cx='10' cy='10' r='5'/></svg>"
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node", svg_icon=svg_icon)
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

    # Build data URI from stored SVG
    svg_data = schemas_metadata["person"]["svg_icon"]
    uri = svg_to_data_uri(svg_data)
    assert uri is not None
    assert uri.startswith("data:image/svg+xml;charset=utf-8,")
    decoded = _decode_data_uri(uri)
    assert "<circle" in decoded
    assert 'cx="10"' in decoded


@pytest.mark.asyncio
async def test_multiple_nodes_same_schema_share_svg_reference(db: GPGraph):
    """Multiple nodes with same schema share the same SVG data URI."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    svg_icon = "<svg><circle cx='10' cy='10' r='5'/></svg>"
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node", svg_icon=svg_icon)
    ])
    await db.set_nodes([
        NodeUpsert(id="p1", type="person", data={"name": "Alice"}),
        NodeUpsert(id="p2", type="person", data={"name": "Bob"}),
        NodeUpsert(id="p3", type="person", data={"name": "Charlie"}),
    ])

    from gpdb import SearchQuery
    node_page = await db.search_nodes(SearchQuery(limit=10))

    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    # Only one schema entry
    assert len(schemas_metadata) == 1
    # All nodes reference the same schema
    assert len(node_page.items) == 3
    for node in node_page.items:
        assert node.type == "person"


@pytest.mark.asyncio
async def test_alias_used_as_display_label(db: GPGraph):
    """Schema alias is used as display_label."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node", alias="Person")
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

    display_info = schemas_metadata.get("person", {})
    display_label = display_info.get("alias") or "person"
    assert display_label == "Person"


@pytest.mark.asyncio
async def test_fallback_to_schema_name_when_no_alias(db: GPGraph):
    """display_label falls back to schema name when no alias."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node")
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

    display_info = schemas_metadata.get("person", {})
    display_label = display_info.get("alias") or "person"
    assert display_label == "person"


@pytest.mark.asyncio
async def test_no_svg_icon_when_not_set(db: GPGraph):
    """Nodes without SVG icon have no svg_icon_data_uri attribute."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node")
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

    assert schemas_metadata["person"]["svg_icon"] is None
    uri = svg_to_data_uri(schemas_metadata["person"]["svg_icon"])
    assert uri is None


@pytest.mark.asyncio
async def test_display_label_applied_to_edges(db: GPGraph):
    """display_label is applied to edges."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    friend_schema = {"type": "object", "properties": {"since": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node"),
        SchemaUpsert(name="friend", json_schema=friend_schema, kind="edge", alias="Friend"),
    ])
    await db.set_nodes([
        NodeUpsert(id="p1", type="person", data={"name": "Alice"}),
        NodeUpsert(id="p2", type="person", data={"name": "Bob"}),
    ])
    await db.set_edges([
        EdgeUpsert(id="e1", type="friend", source_id="p1", target_id="p2", data={"since": "2020"})
    ])

    from gpdb import SearchQuery
    edge_page = await db.search_edges(SearchQuery(limit=10))

    schema_types = {(edge.type, "edge") for edge in edge_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    display_info = schemas_metadata.get("friend", {})
    display_label = display_info.get("alias") or "friend"
    assert display_label == "Friend"


@pytest.mark.asyncio
async def test_multiple_schema_types_with_different_icons(db: GPGraph):
    """Multiple schema types with different icons produce distinct data URIs."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    company_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(
            name="person", json_schema=person_schema, kind="node",
            alias="Person", svg_icon="<svg><circle cx='10' cy='10' r='5'/></svg>",
        ),
        SchemaUpsert(
            name="company", json_schema=company_schema, kind="node",
            alias="Company", svg_icon="<svg><rect x='5' y='5' width='10' height='10'/></svg>",
        ),
    ])
    await db.set_nodes([
        NodeUpsert(id="p1", type="person", data={"name": "Alice"}),
        NodeUpsert(id="c1", type="company", data={"name": "Acme Corp"}),
    ])

    from gpdb import SearchQuery
    node_page = await db.search_nodes(SearchQuery(limit=10))

    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    uri_person = svg_to_data_uri(schemas_metadata["person"]["svg_icon"])
    uri_company = svg_to_data_uri(schemas_metadata["company"]["svg_icon"])
    assert uri_person != uri_company
    assert "<circle" in _decode_data_uri(uri_person)
    assert "<rect" in _decode_data_uri(uri_company)


@pytest.mark.asyncio
async def test_gephi_viewer_data_model_validates(db: GPGraph):
    """GephiViewerData model validates correctly with proper data."""
    data = GephiViewerData(
        nodeData={"n1": {"type": "person", "name": "Alice"}},
        edgeData={"e1": {"type": "friend", "source": "n1", "target": "n2"}},
        layout={},
        metadata={"title": "Test Graph"},
        nodeFields=[{"name": "type", "type": "category"}],
        edgeFields=[{"name": "type", "type": "category"}],
        fullGraph={
            "options": {"type": "mixed", "multi": True, "allowSelfLoops": True},
            "nodes": [{"key": "n1"}],
            "edges": [{"key": "e1", "source": "n1", "target": "n2"}],
        },
    )
    assert data.nodeData["n1"]["type"] == "person"
    assert data.fullGraph["nodes"][0]["key"] == "n1"
    assert data.error is None


@pytest.mark.asyncio
async def test_gephi_viewer_data_with_error(db: GPGraph):
    """GephiViewerData can carry an error message."""
    data = GephiViewerData(error="Something went wrong")
    assert data.error == "Something went wrong"
    assert data.nodeData == {}
    assert data.edgeData == {}
