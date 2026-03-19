---
name: bulk-migration-audit
overview: Complete the unfinished bulk migration for graph CRUD operations
todos:
  - id: phase1-schema
    content: "Phase 1: Migrate Schema CRUD (update_graph_schemas)"
    status: pending
  - id: phase2-node
    content: "Phase 2: Migrate Node CRUD (5 methods: get/create/update/delete_graph_nodes)"
    status: pending
  - id: phase3-payload
    content: "Phase 3: Migrate Node Payload CRUD (2 methods: get/set_graph_node_payloads)"
    status: pending
  - id: phase4-edge
    content: "Phase 4: Migrate Edge CRUD (4 methods: get/create/update/delete_graph_edges)"
    status: pending
  - id: phase5-verify
    content: "Phase 5: Verify and cleanup (run tests, rg checks, delete temp test)"
    status: pending
isProject: false
---

## Goal

Complete the unfinished bulk migration from singular to plural/bulk methods, following the rules in [`plans/bulk_operation_procedure.md`](plans/bulk_operation_procedure.md):

1. **Replace everything** - remove old singular names entirely, don't keep both
2. **Pluralize names** - `get_graph_node` → `get_graph_nodes`
3. **Bulk semantics** - reject duplicates, fail entire batch on any missing/failure
4. **Single-item callers** - wrap into one-item batch, then unwrap result

## Current State (Already Migrated)

| Layer | Method | Status |
|-------|--------|--------|
| Service | `get_graph_schemas` | ✓ Bulk |
| Service | `create_graph_schemas` | ✓ Bulk |
| Service | `delete_graph_schemas` | ✓ Bulk |
| Tool | `graph_schemas_get` | ✓ Bulk |

## Migration Required

### Phase 1: Schema CRUD

| Layer | Current (singular) | Target (bulk) | Notes |
|-------|-------------------|---------------|-------|
| Service | `update_graph_schemas` | `update_graph_schemas` | Per-item optional fields |
| Tool | `graph_schemas_update` | `graph_schemas_update` | Bulk request shape |
| Web | Direct service calls | One-item batch wrap | See note below |

### Phase 2: Node CRUD

| Layer | Current (singular) | Target (bulk) | Notes |
|-------|-------------------|---------------|-------|
| Service | `get_graph_nodes` | `get_graph_nodes` | Fail if any missing |
| Tool | `graph_nodes_get` | `graph_nodes_get` | |
| Service | `create_graph_nodes` | `create_graph_nodes` | Atomic, duplicates rejected |
| Tool | `graph_nodes_create` | `graph_nodes_create` | |
| Service | `update_graph_nodes` | `update_graph_nodes` | Per-node optional fields |
| Tool | `graph_nodes_update` | `graph_nodes_update` | |
| Service | `delete_graph_nodes` | `delete_graph_nodes` | Fail entire batch if any fails |
| Tool | `graph_nodes_delete` | `graph_nodes_delete` | |
| Web | Direct service calls | One-item batch wrap | |

### Phase 3: Node Payload CRUD

| Layer | Current (singular) | Target (bulk) | Notes |
|-------|-------------------|---------------|-------|
| Service | `set_graph_node_payloads` | `set_graph_node_payloads` | Atomic batch |
| Tool | `graph_node_payloads_set` | `graph_node_payloads_set` | |
| Service | `get_graph_node_payloads` | `get_graph_node_payloads` | Fail if any missing |
| Tool | `graph_node_payloads_get` | `graph_node_payloads_get` | |

### Phase 4: Edge CRUD

| Layer | Current (singular) | Target (bulk) | Notes |
|-------|-------------------|---------------|-------|
| Service | `get_graph_edges` | `get_graph_edges` | Fail if any missing |
| Tool | `graph_edges_get` | `graph_edges_get` | |
| Service | `create_graph_edges` | `create_graph_edges` | Atomic, duplicates rejected |
| Tool | `graph_edges_create` | `graph_edges_create` | |
| Service | `update_graph_edges` | `update_graph_edges` | Per-edge optional fields |
| Tool | `graph_edges_update` | `graph_edges_update` | |
| Service | `delete_graph_edges` | `delete_graph_edges` | Fail entire batch if any fails |
| Tool | `graph_edges_delete` | `graph_edges_delete` | |
| Web | Direct service calls | One-item batch wrap | |

## Web Route Updates (Per Phase)

For each web route that calls service methods directly, apply the one-item batch wrap rule:

1. **Wrap single item** into a one-item list
2. **Call bulk service method** with the list
3. **Unwrap single result** from the returned list
4. **Handle errors** appropriately (batch failure = single failure for web)

Files to update:
- `gpdb_admin/src/gpdb/admin/web/routes/graph_schemas.py` - schema update
- `gpdb_admin/src/gpdb/admin/web/routes/graph_nodes.py` - node CRUD + payload
- `gpdb_admin/src/gpdb/admin/web/routes/graph_edges.py` - edge CRUD

## Acceptance Criteria

1. **`pytest gpdb_admin/tests/test_bulk_methods_migration.py` passes**
   - Verifies no old singular tool names remain in OpenAPI
   - Verifies all expected bulk plural tool names exist

2. **Repo-wide `rg` checks pass** (0 matches for legacy singular CRUD names):
   - Checks cover graph schemas/nodes/edges and node payloads CRUD.

3. **Delete temporary test**: `gpdb_admin/tests/test_bulk_methods_migration.py`

## Implementation Notes

- Service migrations follow `bulk_operation_procedure.md` Section 3
- Tool migrations follow `bulk_operation_procedure.md` Section 4
- Use Pydantic param extraction as needed for bulk request shapes
- Each phase should be completed fully before moving to the next
- Run the oracle test after each phase to catch regressions early
