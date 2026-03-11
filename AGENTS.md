Project Layout:
- `pip install gpdb` for the main package (graph database only; no admin code, no CLI).
- `pip install gpdb-admin` for the `gpdb` console command and `gpdb.admin` module (depends on gpdb).
- `pip install gpdb[dev]` for core dev deps; `pip install gpdb-admin[dev]` for full dev (includes gpdb[dev] + admin + test deps).

Testing:
- Always run tests against the .venv at the project root, never with system Python.
- From repo root with both packages installed: `pytest` runs core and admin tests. Install with `pip install -e . -e ./gpdb_admin[dev]` for development.