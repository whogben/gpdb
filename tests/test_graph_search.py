import pytest
import pytest_asyncio
from gpdb import (
    GPGraph,
    EdgeUpsert,
    NodeUpsert,
    NodeRead,
    Filter,
    FilterGroup,
    Logic,
    Op,
    SearchQuery,
)


@pytest_asyncio.fixture
async def db(pg_server):
    """
    Creates a GraphDB instance connected to the temporary Postgres server.
    """
    url = pg_server.get_uri()
    db = GPGraph(url)
    await db.create_tables()
    yield db
    await db.drop_tables()
    await db.sqla_engine.dispose()


# --- Tests ---


@pytest.mark.asyncio
async def test_search_basic(db: GPGraph):
    """Test basic equality search on standard columns."""
    # Create nodes
    n1_list = await db.set_nodes([NodeUpsert(type="__default__", name="alice")])
    n2_list = await db.set_nodes([NodeUpsert(type="__default__", name="bob")])
    n3_list = await db.set_nodes([NodeUpsert(type="__default__", name="admins")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]

    # Search for type="__default__"
    query = SearchQuery(filter=Filter(field="type", op=Op.EQ, value="__default__"))
    result = await db.search_nodes(query)

    assert result.total == 3
    assert len(result.items) == 3
    assert {n.name for n in result.items} == {"alice", "bob", "admins"}

    # Search for name="admins"
    query = SearchQuery(filter=Filter(field="name", op=Op.EQ, value="admins"))
    result = await db.search_nodes(query)
    assert result.total == 1
    assert result.items[0].id == n3.id


@pytest.mark.asyncio
async def test_search_jsonb(db: GPGraph):
    """Test search on JSONB fields."""
    # Create nodes with data
    n1_list = await db.set_nodes([NodeUpsert(type="__default__", data={"color": "red", "size": 10})])
    n2_list = await db.set_nodes([NodeUpsert(type="__default__", data={"color": "blue", "size": 20})])
    n3_list = await db.set_nodes([NodeUpsert(type="__default__", data={"color": "red", "size": 30})])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]

    # Search data.color = "red"
    query = SearchQuery(filter=Filter(field="data.color", op=Op.EQ, value="red"))
    result = await db.search_nodes(query)
    assert result.total == 2
    assert {n.id for n in result.items} == {n1.id, n3.id}

    # Search data.size = 20
    query = SearchQuery(filter=Filter(field="data.size", op=Op.EQ, value=20))
    result = await db.search_nodes(query)
    assert result.total == 1
    assert result.items[0].id == n2.id


@pytest.mark.asyncio
async def test_search_logic(db: GPGraph):
    """Test AND/OR logic."""
    n1_list = await db.set_nodes([NodeUpsert(type="__default__", data={"x": 1})])
    n2_list = await db.set_nodes([NodeUpsert(type="__default__", data={"x": 2})])
    n3_list = await db.set_nodes([NodeUpsert(type="__default__", data={"x": 1})])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]

    # (type=__default__ AND data.x=1)
    query = SearchQuery(
        filter=FilterGroup(
            logic=Logic.AND,
            filters=[
                Filter(field="type", op=Op.EQ, value="__default__"),
                Filter(field="data.x", op=Op.EQ, value=1),
            ],
        )
    )
    result = await db.search_nodes(query)
    assert result.total == 2
    ids = {n.id for n in result.items}
    assert ids == {n1.id, n3.id}

    # (data.x=1 OR data.x=2)
    query = SearchQuery(
        filter=FilterGroup(
            logic=Logic.OR,
            filters=[
                Filter(field="data.x", op=Op.EQ, value=1),
                Filter(field="data.x", op=Op.EQ, value=2),
            ],
        )
    )
    result = await db.search_nodes(query)
    assert result.total == 3


@pytest.mark.asyncio
async def test_search_paging(db: GPGraph):
    """Test pagination."""
    # Create 10 nodes
    nodes = []
    for i in range(10):
        node_list = await db.set_nodes([NodeUpsert(type="__default__", name=str(i))])
        nodes.append(node_list[0])

    # Page 1: limit=3, offset=0
    query = SearchQuery(
        filter=Filter(field="type", op=Op.EQ, value="__default__"), limit=3, offset=0
    )
    result = await db.search_nodes(query)
    assert result.total == 10
    assert len(result.items) == 3

    # Page 2: limit=3, offset=3
    query.offset = 3
    result = await db.search_nodes(query)
    assert len(result.items) == 3
    # Ensure distinct items from page 1? (depends on sort order, default is created_at desc)


@pytest.mark.asyncio
async def test_search_nested_json(db: GPGraph):
    """Test deeply nested JSON access."""
    n1_list = await db.set_nodes([NodeUpsert(type="__default__", data={"a": {"b": {"c": "found"}}})])
    n2_list = await db.set_nodes([NodeUpsert(type="__default__", data={"a": {"b": {"c": "other"}}})])
    n1 = n1_list[0]
    n2 = n2_list[0]

    query = SearchQuery(filter=Filter(field="data.a.b.c", op=Op.EQ, value="found"))
    result = await db.search_nodes(query)
    assert result.total == 1
    assert result.items[0].id == n1.id


@pytest.mark.asyncio
async def test_search_select_columns(db: GPGraph):
    """Test selecting specific top-level columns."""
    n1_list = await db.set_nodes([NodeUpsert(type="__default__", name="alice", data={"age": 30})])
    n1 = n1_list[0]

    query = SearchQuery(
        select=["id", "name"], filter=Filter(field="name", value="alice")
    )
    result = await db.search_nodes_projection(query)

    assert result.total == 1
    item = result.items[0]

    # Item should be a dict, not a NodeRead
    assert isinstance(item, dict)
    assert "id" in item
    assert item["name"] == "alice"
    assert "type" not in item  # Not selected
    assert "data" not in item  # Not selected


@pytest.mark.asyncio
async def test_search_select_json_fields(db: GPGraph):
    """Test selecting deep JSON fields."""
    n1_list = await db.set_nodes([
        NodeUpsert(
            type="__default__",
            data={"specs": {"color": "red", "weight": 10}, "active": True},
            tags=["imported", "active"],
        )
    ])
    n1 = n1_list[0]

    query = SearchQuery(select=["id", "data.specs.color", "tags"])
    result = await db.search_nodes_projection(query)

    assert result.total == 1
    item = result.items[0]

    assert item["id"] == n1.id
    assert item["data.specs.color"] == "red"
    assert item["tags"] == ["imported", "active"]

    # Ensure full data blob wasn't fetched (we can't easily verify network traffic here,
    # but we can verify structure)
    assert "data" not in item


@pytest.mark.asyncio
async def test_search_select_mixed(db: GPGraph):
    """Test selecting mix of standard columns and JSON fields."""
    n1_list = await db.set_nodes([NodeUpsert(type="__default__", name="foo", data={"val": 123})])
    n1 = n1_list[0]

    query = SearchQuery(select=["type", "data.val"])
    result = await db.search_nodes_projection(query)

    assert len(result.items) == 1
    item = result.items[0]

    assert item["type"] == "__default__"
    assert item["data.val"] == 123


@pytest.mark.asyncio
async def test_search_edges_projection_select_columns(db: GPGraph):
    """Test selecting specific columns on edges."""
    n1_list = await db.set_nodes([NodeUpsert(type="__default__", name="a")])
    n2_list = await db.set_nodes([NodeUpsert(type="__default__", name="b")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    e1 = (await db.set_edges(
        [EdgeUpsert(type="__default__", source_id=n1.id, target_id=n2.id, data={"weight": 2})]
    ))[0]

    query = SearchQuery(
        select=["id", "type", "source_id", "target_id"],
        filter=Filter(field="type", op=Op.EQ, value="__default__"),
    )
    result = await db.search_edges_projection(query)

    assert result.total == 1
    item = result.items[0]
    assert isinstance(item, dict)
    assert item["id"] == e1.id
    assert item["type"] == "__default__"
    assert item["source_id"] == n1.id
    assert item["target_id"] == n2.id
    assert "data" not in item
    assert "created_at" not in item


@pytest.mark.asyncio
async def test_search_edges_projection_select_data_and_tags(db: GPGraph):
    """Test selecting edge data and tags."""
    n1_list = await db.set_nodes([NodeUpsert(type="__default__", name="x")])
    n2_list = await db.set_nodes([NodeUpsert(type="__default__", name="y")])
    n1 = n1_list[0]
    n2 = n2_list[0]
    await db.set_edges(
        [EdgeUpsert(
            type="__default__",
            source_id=n1.id,
            target_id=n2.id,
            data={"score": 10, "meta": {"n": 1}},
            tags=["foo", "bar"],
        )]
    )

    query = SearchQuery(
        select=["id", "data.score", "data.meta.n", "tags"],
        filter=Filter(field="type", op=Op.EQ, value="__default__"),
    )
    result = await db.search_edges_projection(query)

    assert result.total == 1
    item = result.items[0]
    assert item["data.score"] == 10
    assert item["data.meta.n"] == 1
    assert item["tags"] == ["foo", "bar"]
    assert "source_id" not in item


@pytest.mark.asyncio
async def test_search_edges_projection_requires_select(db: GPGraph):
    """Test that search_edges_projection requires query.select."""
    with pytest.raises(ValueError, match="query.select is required"):
        await db.search_edges_projection(SearchQuery(limit=1))


@pytest.mark.asyncio
async def test_select_fallback(db: GPGraph):
    """Test that default behavior (no select) still returns models."""
    n1_list = await db.set_nodes([NodeUpsert(type="__default__")])
    n1 = n1_list[0]

    query = SearchQuery(limit=1)
    result = await db.search_nodes(query)

    assert len(result.items) == 1
    assert isinstance(result.items[0], NodeRead)


@pytest.mark.asyncio
async def test_search_dsl_integration(db: GPGraph):
    """Test that SearchQuery automatically parses DSL strings."""
    # Create test data
    n1_list = await db.set_nodes([
        NodeUpsert(type="__default__", data={"priority": "high", "score": 10}, tags=["urgent"])
    ])
    n2_list = await db.set_nodes([
        NodeUpsert(type="__default__", data={"priority": "low", "score": 5})
    ])
    n3_list = await db.set_nodes([NodeUpsert(type="__default__", data={"priority": "medium", "score": 20})])
    n1 = n1_list[0]
    n2 = n2_list[0]
    n3 = n3_list[0]

    # 1. Simple equality (data.priority:high)
    query = SearchQuery(filter="data.priority:high")
    result = await db.search_nodes(query)
    assert result.total == 1
    assert result.items[0].id == n1.id

    # 2. Implicit AND (type:__default__ data.priority:high)
    query = SearchQuery(filter="type:__default__ data.priority:high")
    result = await db.search_nodes(query)
    assert result.total == 1
    assert result.items[0].id == n1.id

    # 3. Explicit Operator (data.score > 5)
    query = SearchQuery(filter="data.score > 5")
    result = await db.search_nodes(query)
    assert result.total == 2
    ids = {n.id for n in result.items}
    assert ids == {n1.id, n3.id}

    # 4. OR Logic with grouping
    # (data.priority:medium OR data.priority:high)
    query = SearchQuery(filter="(data.priority:medium OR data.priority:high)")
    result = await db.search_nodes(query)
    assert result.total == 2
    ids = {n.id for n in result.items}
    assert ids == {n1.id, n3.id}

    # 5. IN operator
    # tags in (urgent, bug)
    query = SearchQuery(filter="tags in (urgent, bug)")
    result = await db.search_nodes(query)
    assert result.total == 1
    assert result.items[0].id == n1.id
