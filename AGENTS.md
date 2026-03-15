References:
- Temporary references go in /temp_refs, these aren't checked into Git, use this for plans, notes, etc.

Working on the Project:
- Read the README.md so you have project context.
- Study the project's existing code to understand and maintain the established patterns.

`gpdb_admin/src/gpdb/admin/entry.py`:
- When adding new admin graph tools, follow the existing pattern:
  - Put the core behavior in the shared services (for example `graph_content.py`).
  - In `entry.py`, expose three thin wrappers per operation (REST, CLI, MCP) that:
    - handle auth/context (`Request`, CLI trust, MCP `Context`),
    - then delegate into shared helpers like `_call_graph_content(...)`,
    - and keep the REST/OpenAPI, CLI, and MCP signatures and return types stable.
  - Use a single Pydantic param model that contains all the params for the method, ensuring they will be sent in the body for FastAPI.
  - Expose REST tool endpoints with POST for every operation (including creates and updates). Do not use PUT; keep a single method for all tool invocations.

Tool parameterization (all future tools):
- Use exactly ONE Pydantic model per operation for that operation’s parameters. That same model is used for REST (body), CLI, and MCP; do not introduce separate or duplicate param shapes.
- For update operations: require only identity fields (e.g. `graph_id`, `node_id`, `instance_id`, `name` for schema). Make every other field optional (`... | None = Field(None, ...)`). Omitted fields must be left unchanged by the service (merge-with-existing). This ensures callers can update a single field without knowing or re-sending the rest of the resource, and avoids accidental overwrites (e.g. clearing tags when only changing the name).

Project Layout:
- `pip install gpdb` for the main package (graph database only; no admin code, no CLI).
- `pip install gpdb-admin` for the `gpdb` console command and `gpdb.admin` module (depends on gpdb).
- `pip install gpdb[dev]` for core dev deps; `pip install gpdb-admin[dev]` for full dev (includes gpdb[dev] + admin + test deps).

Testing:
- Always run tests against the .venv at the project root, never with system Python.
- From repo root with both packages installed: `pytest` runs core and admin tests. Install with `pip install -e . -e ./gpdb_admin[dev]` for development.

Admin Web App:
- Keep the human-facing web UI in `gpdb_admin/src/gpdb/admin/web`; do not add web code to the core `gpdb` package.
- Put server-rendered page routes in `web/routes`, shared templates in `web/templates`, and static assets in `web/static`.
- Keep `toolaccess` tool endpoints under `/api` and use the mounted web app for browser pages at `/`.

Admin Config:
- Keep file-backed admin config in `gpdb_admin/src/gpdb/admin/config.py`; resolve config path from CLI `--config`, then `GPDB_CONFIG`, then the default user config path.
- Use `ResolvedConfig` for runtime values and `ConfigStore` for file reads/writes; only persist file-backed values, not env or CLI overrides.

ToolAccess Integration:
- ToolAccess is a project we also control. When encountering problems that would require workarounds or code that would be better off in ToolAccess, do NOT implement local workarounds in gpdb-admin.
- Instead, stop and inform the user that we need to get upstream changes in ToolAccess first, and wait until those changes are done before proceeding with gpdb-admin work.

Code Style:
- NEVER lazy import inside a function. Whenever there is an "import" that is not at the top level, move it to the top level and organize it alphabetically by group: standard, 3rd party, project.  (The only exception is inside tests, where a lazy import may be appropriate.)