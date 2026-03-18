# Plan: Fix JSON Schema Integrity

## Problem Statement

The system currently modifies client-provided JSON schemas in two problematic ways:

1. **Injection of `x-gpdb-kind`** - Kind is injected into the `json_schema` JSON field before storage
2. **`$ref` Inlining** - All `$ref` references are resolved and `$defs`/`$definitions` are removed

This violates the principle that `json_schema` should be an opaque, client-controlled object.

## Goals

1. Store `json_schema` exactly as provided by the client
2. Store `kind` as a separate field in the schema table (not embedded)
3. Remove `$ref` inlining - store schema structure as-is
4. Maintain validation functionality

---

## Implementation Steps

### Step 1: Add `kind` Column to Schema Table

**File**: `src/gpdb/graph.py`

- [ ] Add `kind` column to `_GPSchema` class (line ~639)
- [ ] Make it nullable initially for migration
- [ ] Index the column for querying

```python
# Current (line ~639)
class _GPSchema(_Base):
    __tablename__ = "schemas"
    name: Mapped[str] = mapped_column(String, primary_key=True)
    version: Mapped[str] = mapped_column(String, default="1.0.0")
    json_schema: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = ...

# Add
kind: Mapped[Optional[str]] = mapped_column(String, nullable=True, index=True)
```

### Step 2: Update `_prepare_schema_registration`

**File**: `src/gpdb/graph.py` (line ~1124)

- [ ] Remove line 1155: `json_schema[_SCHEMA_KIND_FIELD] = resolved_kind`
- [ ] Instead, set `resolved_kind` on the schema record directly
- [ ] Remove `_extract_schema_kind` call (or make it read-only for validation)

### Step 3: Remove `$ref` Inlining

**File**: `src/gpdb/graph.py` (line ~1067)

- [ ] Remove call to `self._inline_refs(json_schema)` at line 1137
- [ ] Keep the method for potential future opt-in use, or remove entirely
- [ ] Update or remove `_inline_refs` method

### Step 4: Update Kind Extraction Logic

**File**: `src/gpdb/graph.py`

- [ ] When `SchemaUpsert` has `kind` field, use that directly
- [ ] If `kind` is not provided but `x-gpdb-kind` exists in JSON, extract and store separately (for migration path)
- [ ] Remove dependency on reading kind from `json_schema`

### Step 5: Update Schema Retrieval

**File**: `src/gpdb/graph.py`

- [ ] Update `get_schemas()` to return `kind` as separate field
- [ ] Ensure returned object has both `json_schema` (unchanged) and `kind`

### Step 6: Update Validation Logic

**File**: `src/gpdb/graph.py`

- [ ] Update `_get_validator` to work with schemas that may have `$ref`
- [ ] Ensure jsonschema library handles `$ref` properly (it does natively)
- [ ] Update `_validate_data` to use stored `kind` for validation checks

### Step 7: Update Tests

**File**: `tests/test_helpers.py`

- [ ] Remove `schema_with_kind()` helper or make it a no-op
- [ ] Update tests that expect `x-gpdb-kind` in retrieved schemas

**Files**: `tests/test_graph_schemas.py`, `tests/test_schema_validation.py`

- [ ] Update assertions to expect `json_schema` unchanged
- [ ] Add test: verify dict schema is returned exactly as provided
- [ ] Add test: verify `$ref` and `$defs` are preserved
- [ ] Add test: verify Pydantic model schemas work (with `$defs`)

### Step 8: Migration Path for Existing Data

- [ ] Create migration script to:
  1. Add `kind` column to schemas table
  2. Extract `x-gpdb-kind` from existing `json_schema` into `kind` column
  3. Remove `x-gpdb-kind` from stored `json_schema` (restore original)
- [ ] Document migration steps

---

## Code Locations to Modify

| Location | Change |
|----------|--------|
| `src/gpdb/graph.py:639-650` | Add `kind` column to `_GPSchema` |
| `src/gpdb/graph.py:1067-1113` | Keep `_inline_refs` method but stop calling it |
| `src/gpdb/graph.py:1137` | Remove `_inline_refs` call |
| `src/gpdb/graph.py:1155` | Remove `x-gpdb-kind` injection |
| `src/gpdb/graph.py:94-115` | Update `_extract_schema_kind` to be read-only or remove |
| `tests/test_helpers.py:4` | Update `schema_with_kind` helper |
| Various test files | Update assertions |

---

## Backward Compatibility Considerations

1. **Existing schemas with `x-gpdb-kind`**: Migration will extract and store separately
2. **Clients relying on `x-gpdb-kind`**: Will need to update to read `kind` separately
3. **Pydantic models**: Still work - `model_json_schema()` output will be stored as-is

---

## Success Criteria

- [ ] Client passes `{"type": "object", "$defs": {...}}`, retrieves exactly that
- [ ] No `x-gpdb-kind` field in stored or retrieved `json_schema`
- [ ] `kind` available as separate field on schema objects
- [ ] Validation still works with schemas containing `$ref`
- [ ] Existing tests pass (or updated appropriately)
