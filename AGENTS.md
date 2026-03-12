References:
- Temporary references go in /temp_refs, these aren't checked into Git, use this for plans, notes, etc.

Working on the Project:
- Read the README.md so you have project context.
- Study the project's existing code to understand and maintain the established patterns.

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