References:
- Temporary references go in /temp_refs, these aren't checked into Git, use this for plans, notes, etc.

Working on the Project:
- Read the README.md so you have project context.
- Study the project's existing code to understand and maintain the established patterns.
- Never use Python or terminal scripts for file editing unless its the only option - ALWAYS prefer the built in tools for working with files provided by your IDE and agent harness. (This is because the built in tools can be automatically executed, but if you use a custom python command in the terminal, it stops the entire process until the user can approve it. So NEVER use custom terminal commands just for file reading/editing unless you need to perform some task not available via the MCP tools. Search is the only exception, you can grep.)
    - DO NOT USE CAT it always registers as a separate command it is not possible to pre-approve because Kilo Code has a bug where it can't tell the difference between the arguments and the command.

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

Admin Web UI System & Principles:
- Browser support & responsiveness:
  - All UI must remain usable on current Chrome, Safari, and Firefox on both desktop and mobile.
  - Pages must be fully usable in a vertical, portrait mobile browser; never rely on hover-only affordances.
- Visual style:
  - Favor a clean, minimal aesthetic with only as much visual complexity as needed to convey hierarchy and affordances.
  - Icons can stand alone where clear; avoid redundant button labels.
  - If explanatory text is longer than a short sentence, hide it behind an info affordance (e.g. “i” button → modal/popover) instead of inlining paragraphs in the layout.
- Space efficiency:
  - Default layouts should maximize space for core content and data, keeping shared chrome (titlebar, nav, profile, filters) as compact as possible.
  - Prefer collapsible panels, drawers, or modals for filters and optional controls so they are not always occupying vertical space.
  - Use borders and padding sparingly; prioritize information density over “breathing room” when the two conflict.
- Reusable component system:
  - Implement common UI patterns as shared templates/partials and CSS/JS components (not one-off copies per page).
  - Changes to global elements (titlebar, navigation menu, profile menu, information strap, etc.) should be done in a single place and reused across pages.
- Common shell elements:
  - Titlebar:
    - Minimal bar at the top of the screen with a left nav toggle, a center title (default “gpdb”), and a right profile toggle.
    - Keep height small; it should feel like an app chrome band, not a “hero” header.
  - Navigation menu:
    - Left-side sliding menu providing global navigation: graph selector plus graph-scoped pages (viewer, nodes, edges, schemas) and instances.
    - The selected graph should persist across page reloads; graph selection is treated as navigation.
    - On wide viewports, the nav can remain open as a persistent sidebar; on narrow/mobile it behaves like an overlay drawer that covers content and dismisses when navigating or tapping outside.
  - Profile menu:
    - Right-side sliding menu with user-specific actions: username display, API keys, sign out, and any other account-level items.
    - Same overlay/dismiss behaviors as the nav menu (tap background or navigate to close).
  - Information strap:
    - A narrow notification “strap” appears just under the titlebar for ephemeral success/error feedback and brief status messages.
    - Success and error states use distinct color treatments (e.g. green vs. red) and can be dismissed via an explicit close control or by navigating away.
    - The strap pushes page content downward when shown rather than overlaying it; multiple messages may stack with newest last.
- Behaviors & motion:
  - Modals, nav, and profile drawers:
    - Content area is opaque; the scrim/overlay uses a translucent background to maintain context of what’s underneath.
    - Tapping/clicking the scrim dismisses the element unless the specific flow must be modal-only.
  - Animation:
    - Use short, subtle animations only when they help users understand spatial relationships (e.g. nav sliding in from the left, straps sliding down from under the titlebar, modals fading in/out).
    - Avoid decorative or slow animations that distract from primary workflows.
- Theming:
  - Maintain first-class light and dark theme support; respect the user’s OS/device theme preference by default.
  - Any new components should inherit from the shared CSS variables and theme tokens used by the existing shell.

Admin Config:
- Keep file-backed admin config in `gpdb_admin/src/gpdb/admin/config.py`; resolve data dir from CLI `--data-dir`/`-d`, then `GPDB_DATA_DIR`, then the default user data dir. Config file is always `{data_dir}/admin.toml`.
- Use `ResolvedConfig` for runtime values and `ConfigStore` for file reads/writes; only persist file-backed values, not env or CLI overrides.

ToolAccess Integration:
- ToolAccess is a project we also control. When encountering problems that would require workarounds or code that would be better off in ToolAccess, do NOT implement local workarounds in gpdb-admin.
- Instead, stop and inform the user that we need to get upstream changes in ToolAccess first, and wait until those changes are done before proceeding with gpdb-admin work.

Code Style:
- NEVER lazy import inside a function. Whenever there is an "import" that is not at the top level, move it to the top level and organize it alphabetically by group: standard, 3rd party, project.  (The only exception is inside tests, where a lazy import may be appropriate.)
- NEVER USE REGEX OR OTHER PATTERN MATCHES TO MAKE EDITS. You simply cannot control what will be edited. You must always READ EVERYTHING AND MANUALLY EDIT ALL OCCURANCES when making updates to avoid unexpected changes.