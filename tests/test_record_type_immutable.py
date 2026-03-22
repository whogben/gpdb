"""Tests for immutable node and edge ``type`` after creation."""

import pytest

from gpdb import (
    EdgeUpsert,
    GPGraph,
    NodeUpsert,
    RecordTypeImmutableError,
    SchemaUpsert,
)
from test_helpers import schema_with_kind


@pytest.mark.asyncio
async def test_node_update_omitting_type_preserves_type_and_validates(db: GPGraph):
    await db.set_schemas(
        [
            SchemaUpsert(
                name="immutable_node_a",
                json_schema=schema_with_kind(
                    {
                        "type": "object",
                        "properties": {"x": {"type": "number"}},
                        "required": ["x"],
                    }
                ),
                kind="node",
            )
        ]
    )
    n = (
        await db.set_nodes(
            [NodeUpsert(type="immutable_node_a", data={"x": 1})]
        )
    )[0]
    updated = (await db.set_nodes([NodeUpsert(id=n.id, data={"x": 2})]))[0]
    assert updated.type == "immutable_node_a"
    assert updated.data == {"x": 2}


@pytest.mark.asyncio
async def test_node_update_explicit_same_type_allowed(db: GPGraph):
    await db.set_schemas(
        [
            SchemaUpsert(
                name="immutable_node_b",
                json_schema=schema_with_kind({"type": "object", "properties": {}}),
                kind="node",
            )
        ]
    )
    n = (
        await db.set_nodes(
            [NodeUpsert(type="immutable_node_b", data={"k": 1})]
        )
    )[0]
    updated = (
        await db.set_nodes(
            [NodeUpsert(id=n.id, type="immutable_node_b", data={"k": 2})]
        )
    )[0]
    assert updated.type == "immutable_node_b"
    assert updated.data == {"k": 2}


@pytest.mark.asyncio
async def test_node_type_change_raises(db: GPGraph):
    await db.set_schemas(
        [
            SchemaUpsert(
                name="immutable_node_c1",
                json_schema=schema_with_kind({"type": "object", "properties": {}}),
                kind="node",
            ),
            SchemaUpsert(
                name="immutable_node_c2",
                json_schema=schema_with_kind({"type": "object", "properties": {}}),
                kind="node",
            ),
        ]
    )
    n = (
        await db.set_nodes(
            [NodeUpsert(type="immutable_node_c1", data={"a": 1})]
        )
    )[0]
    with pytest.raises(RecordTypeImmutableError):
        await db.set_nodes(
            [NodeUpsert(id=n.id, type="immutable_node_c2", data={"a": 2})]
        )


@pytest.mark.asyncio
async def test_edge_update_omitting_type_preserves_type(db: GPGraph):
    await db.set_schemas(
        [
            SchemaUpsert(
                name="immutable_edge_a",
                json_schema=schema_with_kind(
                    {
                        "type": "object",
                        "properties": {"w": {"type": "number"}},
                        "required": ["w"],
                    },
                    kind="edge",
                ),
                kind="edge",
            )
        ]
    )
    n1 = (await db.set_nodes([NodeUpsert(type="__default__", data={"l": "1"})]))[0]
    n2 = (await db.set_nodes([NodeUpsert(type="__default__", data={"l": "2"})]))[0]
    e = (
        await db.set_edges(
            [
                EdgeUpsert(
                    type="immutable_edge_a",
                    source_id=n1.id,
                    target_id=n2.id,
                    data={"w": 1.0},
                )
            ]
        )
    )[0]
    updated = (
        await db.set_edges(
            [EdgeUpsert(id=e.id, source_id=n1.id, target_id=n2.id, data={"w": 2.0})]
        )
    )[0]
    assert updated.type == "immutable_edge_a"
    assert updated.data == {"w": 2.0}


@pytest.mark.asyncio
async def test_edge_update_explicit_same_type_allowed(db: GPGraph):
    await db.set_schemas(
        [
            SchemaUpsert(
                name="immutable_edge_b",
                json_schema=schema_with_kind(
                    {"type": "object", "properties": {}}, kind="edge"
                ),
                kind="edge",
            )
        ]
    )
    n1 = (await db.set_nodes([NodeUpsert(type="__default__", data={})]))[0]
    n2 = (await db.set_nodes([NodeUpsert(type="__default__", data={})]))[0]
    e = (
        await db.set_edges(
            [
                EdgeUpsert(
                    type="immutable_edge_b",
                    source_id=n1.id,
                    target_id=n2.id,
                    data={"x": 1},
                )
            ]
        )
    )[0]
    updated = (
        await db.set_edges(
            [
                EdgeUpsert(
                    id=e.id,
                    type="immutable_edge_b",
                    source_id=n1.id,
                    target_id=n2.id,
                    data={"x": 2},
                )
            ]
        )
    )[0]
    assert updated.type == "immutable_edge_b"


@pytest.mark.asyncio
async def test_edge_type_change_raises(db: GPGraph):
    await db.set_schemas(
        [
            SchemaUpsert(
                name="immutable_edge_c1",
                json_schema=schema_with_kind(
                    {"type": "object", "properties": {}}, kind="edge"
                ),
                kind="edge",
            ),
            SchemaUpsert(
                name="immutable_edge_c2",
                json_schema=schema_with_kind(
                    {"type": "object", "properties": {}}, kind="edge"
                ),
                kind="edge",
            ),
        ]
    )
    n1 = (await db.set_nodes([NodeUpsert(type="__default__", data={})]))[0]
    n2 = (await db.set_nodes([NodeUpsert(type="__default__", data={})]))[0]
    e = (
        await db.set_edges(
            [
                EdgeUpsert(
                    type="immutable_edge_c1",
                    source_id=n1.id,
                    target_id=n2.id,
                    data={},
                )
            ]
        )
    )[0]
    with pytest.raises(RecordTypeImmutableError):
        await db.set_edges(
            [
                EdgeUpsert(
                    id=e.id,
                    type="immutable_edge_c2",
                    source_id=n1.id,
                    target_id=n2.id,
                    data={},
                )
            ]
        )
