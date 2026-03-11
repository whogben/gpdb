# gpdb

A Python library for storing graphs of typed, schema-validated data in PostgreSQL. Nodes and edges carry arbitrary JSON payloads backed by Pydantic models, JSONB columns, and async SQLAlchemy.

## When to use it

Use gpdb when you need a lightweight graph-like data layer on top of Postgres without deploying a dedicated graph database. It's a good fit for applications that need:

- Typed nodes and edges with flexible JSON data
- Schema validation and versioned schema evolution
- Binary payload storage on nodes (files, images, embeddings)
- Filtered search with pagination over graph records
- Multi-tenant table isolation via prefixes
- An async-first Python API

## Install

```bash
pip install gpdb
```

Requires Python 3.9+ and a PostgreSQL database.

## Quick start

```python
from gpdb import GPGraph, NodeUpsert, EdgeUpsert, SearchQuery, Filter, Op

db = GPGraph("postgresql://user:pass@localhost/mydb")
await db.create_tables()

# Create nodes
alice = await db.set_node(NodeUpsert(type="user", name="alice", data={"role": "admin"}))
bob = await db.set_node(NodeUpsert(type="user", name="bob", data={"role": "member"}))

# Connect them with an edge
await db.set_edge(EdgeUpsert(type="follows", source_id=alice.id, target_id=bob.id))

# Search
result = await db.search_nodes(
    SearchQuery(filter=Filter(field="type", op=Op.EQ, value="user"), limit=10)
)
for node in result.items:
    print(node.name, node.data)
```

## Core concepts

### Nodes

Nodes are the primary records. Each has an `id`, `type`, optional `name`, and a `data` dict for arbitrary JSON. Nodes also support:

- **Parent-child hierarchy** — `parent_id` with a unique constraint on `(parent_id, name)`
- **Ownership** — optional `owner_id` for access control patterns
- **Binary payloads** — store bytes with auto-computed `payload_size`, `payload_hash`, and `payload_mime`
- **Tags** — a JSONB list for lightweight categorization

```python
node = await db.set_node(NodeUpsert(
    type="document",
    name="notes.md",
    parent_id=folder.id,
    data={"word_count": 350},
    tags=["draft", "personal"],
    payload=b"# My notes\n...",
    payload_mime="text/markdown",
))
```

Payloads are deferred by default — `get_node()` skips the blob, `get_node_with_payload()` includes it.

### Edges

Edges connect two nodes with a `source_id` and `target_id`, a `type`, and their own `data` and `tags`.

```python
edge = await db.set_edge(EdgeUpsert(
    type="authored",
    source_id=user.id,
    target_id=document.id,
    data={"timestamp": "2025-01-15"},
))
```

### Search

Search nodes or edges with filters, sorting, and pagination. Filters work on top-level columns and on nested JSONB paths using dot notation.

```python
# Programmatic filters
result = await db.search_nodes(SearchQuery(
    filter=FilterGroup(logic=Logic.AND, filters=[
        Filter(field="type", op=Op.EQ, value="user"),
        Filter(field="data.role", op=Op.EQ, value="admin"),
    ]),
    sort=[Sort(field="created_at", desc=True)],
    limit=25,
))

# Or use the DSL string syntax
result = await db.search_nodes(SearchQuery(filter="type = user and data.role = admin"))
```

Supported operators: `eq`, `gt`, `lt`, `contains` (ilike), `in`.

Field projections are available via `search_nodes_projection()` for returning only selected columns.

### Schemas

Register JSON schemas (or Pydantic models) to validate node/edge data on every write. Schemas are versioned with automatic semver bumps.

```python
from pydantic import BaseModel

class UserData(BaseModel):
    role: str
    email: str | None = None

await db.register_schema("user_data", UserData)

# This node's data will be validated against the schema
await db.set_node(NodeUpsert(
    type="user",
    schema_name="user_data",
    data={"role": "admin", "email": "a@b.com"},
))
```

Schema updates are classified automatically:
- **Patch** — description/title changes only
- **Minor** — new optional fields (backward compatible)
- **Major** — removed fields, type changes, or new required fields (rejected unless migrated)

Use `migrate_schema()` to atomically transform existing data and update the schema in one transaction.

### Domain models (ODM)

Subclass `NodeModel` or `EdgeModel` for strongly-typed domain objects that serialize to/from the graph.

```python
from gpdb import NodeModel

class User(NodeModel):
    node_type: str = "user"
    role: str = "member"
    email: str | None = None

user = User(role="admin", email="a@b.com")
created = await db.set_node(user.to_upsert())
loaded = User.from_read(await db.get_node(created.id))
print(loaded.role)  # "admin"
```

### Transactions

Wrap multiple operations in an atomic transaction.

```python
async with db.transaction():
    node = await db.set_node(NodeUpsert(type="account"))
    await db.set_edge(EdgeUpsert(type="owns", source_id=owner.id, target_id=node.id))
```

### Table prefixes

Isolate data into separate tables by passing a `table_prefix`. Each prefix gets its own `nodes`, `edges`, and `schemas` tables.

```python
main_db = GPGraph(url)
scratch = GPGraph(url, table_prefix="scratch")

await main_db.create_tables()   # creates: nodes, edges, schemas
await scratch.create_tables()   # creates: scratch_nodes, scratch_edges, scratch_schemas
```

## API reference

| Method | Description |
|---|---|
| `create_tables()` | Create tables (idempotent) |
| `drop_tables()` | Drop this instance's tables |
| `set_node(NodeUpsert)` | Create or update a node |
| `get_node(id)` | Get node without payload |
| `get_node_with_payload(id)` | Get node with payload |
| `get_node_payload(id)` | Get only the payload bytes |
| `set_node_payload(id, bytes)` | Set payload on existing node |
| `get_node_child(parent_id, name)` | Get child node by name |
| `delete_node(id)` | Delete a node |
| `search_nodes(SearchQuery)` | Search nodes with filters/sort/pagination |
| `search_nodes_projection(SearchQuery)` | Search with field projection |
| `set_edge(EdgeUpsert)` | Create or update an edge |
| `get_edge(id)` | Get an edge |
| `delete_edge(id)` | Delete an edge |
| `search_edges(SearchQuery)` | Search edges |
| `register_schema(name, schema)` | Register or update a JSON schema |
| `get_schema(name)` | Get a schema |
| `delete_schema(name)` | Delete a schema (fails if in use) |
| `list_schemas()` | List all schema names |
| `migrate_schema(name, func, schema)` | Atomically migrate data + schema |
| `transaction()` | Context manager for atomic operations |

## Dependencies

- [Pydantic](https://docs.pydantic.dev/) >= 2.0
- [SQLAlchemy](https://www.sqlalchemy.org/) >= 2.0 (async)
- [asyncpg](https://github.com/MagicStack/asyncpg) >= 0.29

## License

MIT
