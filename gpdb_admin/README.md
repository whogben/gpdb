# gpdb-admin

Optional admin add-on for [gpdb](https://github.com/whogben/gpdb). Provides the `gpdb` console command, REST API, and MCP server.

- `pip install gpdb` — core graph database only (no admin code, no CLI).
- `pip install gpdb-admin` — installs gpdb and adds the `gpdb` command and `gpdb.admin` module.
- `pip install gpdb-admin[dev]` — full dev setup (gpdb with dev deps + admin + test deps).

Import the admin module as `gpdb.admin` when gpdb-admin is installed.
