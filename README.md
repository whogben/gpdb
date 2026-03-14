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

Choose the package that matches how you want to use GPDB:

- `pip install gpdb` — core graph database library only
- `pip install gpdb-admin` — installs the core library plus the `gpdb` admin command, web UI, REST API, and MCP server
- `pip install gpdb[dev]` — core development dependencies
- `pip install gpdb-admin[dev]` — full admin + development/test dependencies

Requires Python 3.9+.

The core `gpdb` package uses your PostgreSQL database. The optional `gpdb-admin` package adds an admin runtime on top of the core library.

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

## Admin add-on

`gpdb-admin` is an optional package that layers an admin runtime on top of the core graph library. When installed, it provides:

- the `gpdb` console command
- a browser-based admin app
- a REST API under `/api`
- an MCP server exposed over Streamable HTTP
- embeddable runtime for mounting into host applications

### Admin install

```bash
pip install gpdb-admin
```

Import the admin module as `gpdb.admin` when `gpdb-admin` is installed.

### Start the admin service

```bash
gpdb start
```

By default the admin service listens on `127.0.0.1:8747`. You can override the bind address at startup:

```bash
gpdb start --host 0.0.0.0 --port 9000
```

Once the service is running, the current runtime exposes:

- the admin web app at `/`
- a health endpoint at `/health`
- the REST status endpoint at `POST /api/status`
- the MCP endpoint at `/mcp/gpdb/mcp`

The CLI also exposes the current status command directly:

```bash
gpdb status
```

### First-run setup

On a fresh install, opening the admin web app takes you through initial owner setup. After the first owner account is created, the app requires login and uses an authenticated browser session for access to the admin pages.

### Configuration

`gpdb-admin` resolves its config file in this order:

1. `--config` or `-c`
2. `GPDB_CONFIG`
3. the default user config path for `gpdb`

The current file-backed config includes:

- `server.host`
- `server.port`
- `runtime.data_dir`
- `auth.session_secret`

At startup, `gpdb-admin` will generate and persist `auth.session_secret` automatically if it is missing and the selected config file is writable.

### Admin storage model

The admin runtime manages its own local data directory and starts a captive PostgreSQL instance for admin state. Admin identity data is stored using GPDB tables with the `admin` table prefix, separate from the application graph data you manage with the core library.

### Embedding admin in host applications

The admin runtime can be embedded into existing ToolAccess-based applications using the `AdminRuntime` container and `attach_admin_to_manager()` function. This allows you to mount the admin UI and APIs under custom prefixes within your own application's ServerManager.

```python
from gpdb.admin.entry import attach_admin_to_manager
from toolaccess import ServerManager

def build_main_manager() -> ServerManager:
    # Host creates its own manager
    manager = ServerManager(name="my-main-app")

    # Attach admin under /gpdb
    admin = attach_admin_to_manager(
        manager,
        http_root="/gpdb",
        api_path_prefix="/api",
        mcp_name="gpdb",
        cli_root_name=None,  # Host controls CLI
    )

    # Host can mount admin ToolServices into its own CLI
    # my_cli.mount(admin.graph_service)

    return manager
```

The `AdminRuntime` exposes the following ToolServices for host integration:

- `admin_service` — Admin tools (status, etc.)
- `graph_service` — Graph content tools
- `cli_api_key_service` — CLI API key management
- `mcp_api_key_service` — MCP API key management

**Mount point parameters:**

- `http_root` — Web UI mount prefix (e.g., `/gpdb`)
- `api_path_prefix` — REST API mount prefix (e.g., `/api`)
- `mcp_name` — MCP server name (e.g., `"gpdb"` or `"gpdb-admin"`)
- `cli_root_name` — CLI root command (set to `None` to skip CLI creation)

When embedded, the admin runtime shares the host's ServerManager lifecycle, and upgrading `gpdb-admin` automatically updates all embedded surfaces.

### Current scope

Today, `gpdb-admin` is an early-stage admin surface. It supports:

- service startup via `gpdb start`
- a first-run owner bootstrap flow
- login/logout for the admin web app
- the `status` command across CLI, REST, and MCP

It does not yet provide a full multi-user administration console or a broad admin API surface.

## Core concepts

### Nodes

Nodes are the primary records. Each has an `id`, `type`, optional `name`, and a `data` dict for arbitrary JSON. Nodes also support:

- **Parent-child hierarchy** — `parent_id` with a unique constraint on `(parent_id, name)`
- **Ownership** — optional `owner_id` for access control patterns
- **Binary payloads** — store bytes with auto-computed `payload_size`, `payload_hash`, plus optional `payload_mime` and `payload_filename`
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
    payload_filename="notes.md",
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

### Query DSL

The DSL string syntax supports natural comparison operators for filtering nodes and edges:

```python
# Equality (= or ==)
result = await db.search_nodes(SearchQuery(filter='name == "alice"'))
result = await db.search_nodes(SearchQuery(filter="type = user"))

# Not equal (!=)
result = await db.search_nodes(SearchQuery(filter='status != "deleted"'))

# Greater than (>) and greater than or equal (>=)
result = await db.search_nodes(SearchQuery(filter="age >= 18"))
result = await db.search_nodes(SearchQuery(filter="score > 100"))

# Less than (<) and less than or equal (<=)
result = await db.search_nodes(SearchQuery(filter="created_at < 2024-01-01"))
result = await db.search_nodes(SearchQuery(filter="price <= 50.00"))

# Contains (~) - case-insensitive substring match
result = await db.search_nodes(SearchQuery(filter='name ~ "john"'))

# In - match any value in a list
result = await db.search_nodes(SearchQuery(filter="type in (user, admin, guest)"))

# Combining conditions with and/or
result = await db.search_nodes(SearchQuery(filter='type = user and age >= 18'))
result = await db.search_nodes(SearchQuery(filter='status = active or role = admin'))

# Parentheses for grouping
result = await db.search_nodes(SearchQuery(filter='(type = user and active = true) or role = superuser'))
```

**Supported operators:**

| Operator | Aliases | Meaning |
|----------|---------|---------|
| `=` | `==`, `:`, `eq` | Equal |
| `!=` | `ne` | Not equal |
| `>` | `gt`, `after` | Greater than |
| `>=` | `gte` | Greater than or equal |
| `<` | `lt`, `before` | Less than |
| `<=` | `lte` | Less than or equal |
| `~` | `contains` | Contains (case-insensitive) |
| `in` | — | Match any value in list |

**JSON path filtering:**

Use dot notation to filter on nested JSONB data:

```python
result = await db.search_nodes(SearchQuery(filter="data.role = admin"))
result = await db.search_nodes(SearchQuery(filter="data.metadata.version >= 2"))
```

Field projections are available via `search_nodes_projection()` for returning only selected columns.

### Schemas

Register JSON schemas (or Pydantic models) to validate node or edge data on every write. Each schema is scoped to either nodes or edges and is versioned with automatic semver bumps.

```python
from pydantic import BaseModel

class UserData(BaseModel):
    role: str
    email: str | None = None

await db.register_schema("user_data", UserData, kind="node")

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
| `search_edges_projection(SearchQuery)` | Search edges with field projection |
| `register_schema(name, schema, kind="node")` | Register or update a node/edge JSON schema |
| `get_schema(name)` | Get a schema |
| `delete_schema(name)` | Delete a schema (fails if in use) |
| `list_schemas(kind=None)` | List all schema names, optionally filtered by kind |
| `migrate_schema(name, func, schema, kind=None)` | Atomically migrate data + schema |
| `transaction()` | Context manager for atomic operations |

## Dependencies

- [Pydantic](https://docs.pydantic.dev/) >= 2.0
- [SQLAlchemy](https://www.sqlalchemy.org/) >= 2.0 (async)
- [asyncpg](https://github.com/MagicStack/asyncpg) >= 0.29

## License

MIT
