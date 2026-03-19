"""
Reusable documentation strings for gpdb query/filter DSL and admin list sort codes.

These are imported by `gpdb-admin` parameter schemas so API/MCP OpenAPI docs stay
in sync with the core DSL implementation.
"""

FILTER_DSL_DESCRIPTION = """Filter DSL (string) for `SearchQuery(filter=...)`.

The DSL supports:

1) Comparisons: `field op value`
- Equality: `=` (aliases: `==`, `:`, `eq`)
- Not equal: `!=` (alias: `ne`)
- Greater than: `>` (aliases: `gt`, `after`)
- Greater than or equal: `>=` (alias: `gte`)
- Less than: `<` (aliases: `lt`, `before`)
- Less than or equal: `<=` (alias: `lte`)
- Contains: `~` (alias: `contains`) case-insensitive substring match
- In-list: `in` (e.g. `type in (user, admin, guest)`)

2) Boolean logic: combine expressions with:
- `and`
- `or`
- Parentheses for grouping, e.g. `(type = user and active = true) or role = superuser`

3) JSONB path filtering:
- Use dot notation to filter inside JSONB payloads, e.g.:
  - `data.role = "admin"`
  - `data.metadata.version >= 2`

Examples:
- `name == "alice"`
- `status != "deleted"`
- `age >= 18`
- `name ~ "john"`
- `type in (user, admin, guest)`
- `type = user and age >= 18`
- `(type = user and active = true) or role = superuser`
"""


NODE_LIST_SORT_DESCRIPTION = """Node list `sort` code.

This is a compact string that gpdb-admin maps to structured `SearchQuery.sort=[Sort(...)]`.
Allowed values (default: `created_at_desc`):
- `created_at_desc`: `created_at` descending (newest first)
- `created_at_asc`: `created_at` ascending (oldest first)
- `updated_at_desc`: `updated_at` descending (newest first)
- `updated_at_asc`: `updated_at` ascending (oldest first)
- `name_asc`: `name` ascending (A-Z)
- `name_desc`: `name` descending (Z-A)
"""


EDGE_LIST_SORT_DESCRIPTION = """Edge list `sort` code.

This is a compact string that gpdb-admin maps to structured `SearchQuery.sort=[Sort(...)]`.
Allowed values (default: `created_at_desc`):
- `created_at_desc`: `created_at` descending (newest first)
- `created_at_asc`: `created_at` ascending (oldest first)
- `updated_at_desc`: `updated_at` descending (newest first)
- `updated_at_asc`: `updated_at` ascending (oldest first)
- `type_asc`: `type` ascending (A-Z)
- `type_desc`: `type` descending (Z-A)
"""

