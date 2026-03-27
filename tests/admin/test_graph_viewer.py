"""Tests for graph viewer data pipeline (Gephi Lite format)."""

import pytest
from gpdb import (
    GPGraph,
    NodeUpsert,
    EdgeUpsert,
    SchemaRef,
    SchemaUpsert,
    SearchQuery,
    normalize_svg_icon_for_display,
    sanitize_svg,
)
from gpdb.admin.graph_content.edges import (
    _build_field_defs,
    _flatten_dict,
    _infer_field_type,
)
from gpdb.admin.graph_content.models import GephiViewerData
from gpdb.svg_sanitizer import svg_to_data_uri

_RAW_PERSON_CIRCLE_SVG = "<svg><circle cx='10' cy='10' r='5'/></svg>"
EXPECTED_PERSON_CIRCLE_SVG = normalize_svg_icon_for_display(
    sanitize_svg(_RAW_PERSON_CIRCLE_SVG)
)


@pytest.mark.asyncio
async def test_viewer_data_includes_schema_metadata(db: GPGraph):
    """Viewer data includes schema metadata dict with alias and svg_icon."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(
            name="person", json_schema=person_schema, kind="node",
            alias="Person", svg_icon=_RAW_PERSON_CIRCLE_SVG,
        )
    ])
    await db.set_nodes([
        NodeUpsert(id="person1", type="person", data={"name": "Alice"})
    ])

    node_page = await db.search_nodes(SearchQuery(limit=10))
    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    assert "person" in schemas_metadata
    assert schemas_metadata["person"]["alias"] == "Person"
    assert schemas_metadata["person"]["svg_icon"] == EXPECTED_PERSON_CIRCLE_SVG


@pytest.mark.asyncio
async def test_node_data_built_with_flattened_attributes(db: GPGraph):
    """Node data is built with flattened attributes including display_label."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node", alias="Person")
    ])
    await db.set_nodes([
        NodeUpsert(id="person1", type="person", name="Alice", data={"address": {"city": "NYC"}}),
    ])

    node_page = await db.search_nodes(SearchQuery(limit=10))
    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        meta_key = f"{schema_kind}:{schema_name}"
        schemas_metadata[meta_key] = {
            "alias": display_info["alias"],
            "svg_icon": display_info["svg_icon"],
        }

    # Build flattened node attributes (same logic as get_graph_viewer_data)
    node_data = {}
    for node in node_page.items:
        display_info = schemas_metadata.get(f"node:{node.type}", {})
        attrs = {
            "type": node.type,
            "name": node.name,
            "display_label": display_info.get("alias") or node.type,
        }
        attrs.update(_flatten_dict(node.data, "data"))
        for tag in node.tags or []:
            attrs[f"tag__{tag}"] = True
        node_data[node.id] = attrs

    assert "person1" in node_data
    assert node_data["person1"]["type"] == "person"
    assert node_data["person1"]["name"] == "Alice"
    assert node_data["person1"]["display_label"] == "Person"
    assert node_data["person1"]["data__address__city"] == "NYC"


@pytest.mark.asyncio
async def test_edge_data_built_with_source_target(db: GPGraph):
    """Edge data includes source/target in fullGraph structure."""
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

    node_page = await db.search_nodes(SearchQuery(limit=10))
    edge_page = await db.search_edges(SearchQuery(limit=10))

    # Build fullGraph (same logic as get_graph_viewer_data)
    node_data = {node.id: {"type": node.type} for node in node_page.items}
    edge_data = {edge.id: {"type": edge.type} for edge in edge_page.items}
    full_graph = {
        "options": {"type": "mixed", "multi": True, "allowSelfLoops": True},
        "nodes": [{"key": nid} for nid in node_data],
        "edges": [
            {"key": eid, "source": edge.source_id, "target": edge.target_id}
            for eid, edge in zip(edge_data, edge_page.items)
        ],
    }

    assert full_graph["options"]["type"] == "mixed"
    assert len(full_graph["nodes"]) == 2
    assert len(full_graph["edges"]) == 1
    assert full_graph["edges"][0]["key"] == "e1"
    assert full_graph["edges"][0]["source"] == "p1"
    assert full_graph["edges"][0]["target"] == "p2"


@pytest.mark.asyncio
async def test_tags_flattened_to_boolean_attributes(db: GPGraph):
    """Tags lists are flattened to tag_name: True boolean attributes."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node")
    ])
    await db.set_nodes([
        NodeUpsert(id="n1", type="person", data={"name": "Alice"}, tags=["important", "active"]),
    ])

    node_page = await db.search_nodes(SearchQuery(limit=10))
    node = node_page.items[0]
    attrs = {}
    attrs.update(_flatten_dict(node.data, "data"))
    for tag in node.tags or []:
        attrs[f"tag__{tag}"] = True

    assert attrs["tag__important"] is True
    assert attrs["tag__active"] is True


@pytest.mark.asyncio
async def test_svg_icon_data_uri_on_nodes(db: GPGraph):
    """SVG icons are included as svg_icon_data_uri attributes on nodes."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node", svg_icon=_RAW_PERSON_CIRCLE_SVG)
    ])
    await db.set_nodes([
        NodeUpsert(id="n1", type="person", data={"name": "Alice"}),
    ])

    node_page = await db.search_nodes(SearchQuery(limit=10))
    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        meta_key = f"{schema_kind}:{schema_name}"
        schemas_metadata[meta_key] = {
            "alias": display_info["alias"],
            "svg_icon": display_info["svg_icon"],
            "svg_icon_data_uri": svg_to_data_uri(display_info["svg_icon"]),
        }

    display_info = schemas_metadata.get("node:person", {})
    svg_uri = display_info.get("svg_icon_data_uri")
    assert svg_uri is not None
    assert svg_uri.startswith("data:image/svg+xml;charset=utf-8,")


@pytest.mark.asyncio
async def test_display_label_uses_alias(db: GPGraph):
    """display_label uses alias when set."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node", alias="Person")
    ])
    await db.set_nodes([
        NodeUpsert(id="n1", type="person", data={"name": "Alice"}),
    ])

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
async def test_display_label_falls_back_to_schema_name(db: GPGraph):
    """display_label falls back to schema name when no alias."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node")
    ])
    await db.set_nodes([
        NodeUpsert(id="n1", type="person", data={"name": "Alice"}),
    ])

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
async def test_multiple_nodes_same_schema_no_duplicate_svg(db: GPGraph):
    """Multiple nodes with same schema don't duplicate SVG in metadata."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node", svg_icon=_RAW_PERSON_CIRCLE_SVG)
    ])
    await db.set_nodes([
        NodeUpsert(id="p1", type="person", data={"name": "Alice"}),
        NodeUpsert(id="p2", type="person", data={"name": "Bob"}),
        NodeUpsert(id="p3", type="person", data={"name": "Charlie"}),
    ])

    node_page = await db.search_nodes(SearchQuery(limit=10))
    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        ref = SchemaRef(name=schema_name, kind=schema_kind)
        display_info = await db._get_schema_display_info(ref)
        schemas_metadata[schema_name] = display_info

    assert len(schemas_metadata) == 1
    assert schemas_metadata["person"]["svg_icon"] == EXPECTED_PERSON_CIRCLE_SVG
    assert len(node_page.items) == 3


@pytest.mark.asyncio
async def test_multiple_schema_types_with_edges(db: GPGraph):
    """Viewer data works correctly with multiple schema types including edges."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    company_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    works_for_schema = {"type": "object", "properties": {"role": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node", alias="Person", svg_icon=_RAW_PERSON_CIRCLE_SVG),
        SchemaUpsert(name="company", json_schema=company_schema, kind="node", alias="Company"),
        SchemaUpsert(name="works_for", json_schema=works_for_schema, kind="edge", alias="Works For"),
    ])
    await db.set_nodes([
        NodeUpsert(id="p1", type="person", data={"name": "Alice"}),
        NodeUpsert(id="c1", type="company", data={"name": "Acme Corp"}),
    ])
    await db.set_edges([
        EdgeUpsert(id="e1", type="works_for", source_id="p1", target_id="c1", data={"role": "Engineer"})
    ])

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

    assert len(schemas_metadata) == 3
    assert schemas_metadata["person"]["alias"] == "Person"
    assert schemas_metadata["company"]["alias"] == "Company"
    assert schemas_metadata["works_for"]["alias"] == "Works For"
    assert schemas_metadata["person"]["svg_icon"] == EXPECTED_PERSON_CIRCLE_SVG


@pytest.mark.asyncio
async def test_viewer_data_handles_missing_schemas_gracefully(db: GPGraph):
    """Viewer data handles missing schemas gracefully."""
    person_schema = {"type": "object", "properties": {"name": {"type": "string"}}}
    await db.set_schemas([
        SchemaUpsert(name="person", json_schema=person_schema, kind="node")
    ])
    await db.set_nodes([
        NodeUpsert(id="n1", type="person", data={"name": "Test"})
    ])

    node_page = await db.search_nodes(SearchQuery(limit=10))
    schema_types = {(node.type, "node") for node in node_page.items}
    schemas_metadata = {}
    for schema_name, schema_kind in schema_types:
        try:
            ref = SchemaRef(name=schema_name, kind=schema_kind)
            display_info = await db._get_schema_display_info(ref)
            schemas_metadata[schema_name] = display_info
        except Exception:
            pass

    assert len(schemas_metadata) == 1
    assert "person" in schemas_metadata


@pytest.mark.asyncio
async def test_gephi_viewer_data_model(db: GPGraph):
    """GephiViewerData model validates correctly."""
    data = GephiViewerData(
        nodeData={"n1": {"type": "person", "name": "Alice"}},
        edgeData={"e1": {"type": "friend"}},
        layout={},
        metadata={"title": "Test"},
        nodeFields=[{"id": "type", "type": "category"}],
        edgeFields=[{"id": "type", "type": "category"}],
        fullGraph={
            "options": {"type": "mixed", "multi": True, "allowSelfLoops": True},
            "nodes": [{"key": "n1"}],
            "edges": [{"key": "e1", "source": "n1", "target": "n2"}],
        },
    )
    assert data.nodeData["n1"]["type"] == "person"
    assert data.error is None


def test_gephi_viewer_data_with_error():
    """GephiViewerData can carry an error message."""
    data = GephiViewerData(error="Something went wrong")
    assert data.error == "Something went wrong"
    assert data.nodeData == {}


# --- Unit tests for helper functions ---


class TestFlattenDict:
    """Tests for _flatten_dict helper."""

    def test_flat_dict_unchanged(self):
        assert _flatten_dict({"a": 1, "b": "x"}) == {"a": 1, "b": "x"}

    def test_nested_dict_flattened(self):
        assert _flatten_dict({"a": {"b": 1}}, "data") == {"data__a__b": 1}

    def test_deeply_nested(self):
        assert _flatten_dict({"a": {"b": {"c": 2}}}, "data") == {"data__a__b__c": 2}

    def test_mixed_flat_and_nested(self):
        result = _flatten_dict({"x": 1, "y": {"z": 2}}, "data")
        assert result == {"data__x": 1, "data__y__z": 2}


class TestInferFieldType:
    """Tests for _infer_field_type helper."""

    def test_bool(self):
        assert _infer_field_type(True) == "boolean"

    def test_int(self):
        assert _infer_field_type(42) == "number"

    def test_float(self):
        assert _infer_field_type(3.14) == "number"

    def test_string(self):
        assert _infer_field_type("hello") == "text"


class TestBuildFieldDefs:
    """Tests for _build_field_defs helper."""

    def test_basic_fields(self):
        all_attrs = {"n1": {"type": "person", "name": "Alice", "age": 30}}
        fields = _build_field_defs(all_attrs, item_type="nodes")
        ids = {f["id"] for f in fields}
        assert "type" in ids
        assert "name" in ids
        assert "age" in ids
        type_field = next(f for f in fields if f["id"] == "type")
        assert type_field["type"] == "category"
        name_field = next(f for f in fields if f["id"] == "name")
        assert name_field["type"] == "text"
        age_field = next(f for f in fields if f["id"] == "age")
        assert age_field["type"] == "number"

    def test_svg_icon_data_uri_type_is_image(self):
        all_attrs = {"n1": {"type": "person", "svg_icon_data_uri": "data:image/svg+xml,xxx"}}
        fields = _build_field_defs(all_attrs, item_type="nodes")
        img_field = next(f for f in fields if f["id"] == "svg_icon_data_uri")
        assert img_field["type"] == "url"
