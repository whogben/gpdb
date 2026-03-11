"""Entry point for the `gpdb` console command."""

from __future__ import annotations

import argparse
import secrets
import sys

import uvicorn
from toolaccess import CLIServer, OpenAPIServer, SSEMCPServer, ServerManager, ToolService
from toolaccess.toolaccess import MountableApp

from gpdb.admin.config import ConfigStore, ResolvedConfig, extract_config_arg
from gpdb.admin.runtime import AdminServices, create_admin_lifespan
from gpdb.admin.web import create_web_app


def status() -> str:
    """Return the current status of the GPDB admin service."""
    return "OK"


def create_manager(
    resolved_config: ResolvedConfig | None = None,
    config_store: ConfigStore | None = None,
) -> ServerManager:
    """Build the combined admin runtime."""
    if config_store is None:
        config_store = ConfigStore.from_sources()
    if resolved_config is None:
        resolved_config = _ensure_runtime_config(config_store)

    services = AdminServices(
        resolved_config=resolved_config,
        config_store=config_store,
    )
    admin_service = ToolService("admin", [status])

    rest_api = OpenAPIServer(path_prefix="/api", title="GPDB Admin API")
    rest_api.mount(admin_service)

    mcp_server = SSEMCPServer("gpdb")
    mcp_server.mount(admin_service)

    cli = CLIServer("gpdb")
    cli.mount(admin_service)

    web_app = MountableApp(
        create_web_app(
            resolved_config=resolved_config,
            config_store=config_store,
            services=services,
        ),
        path_prefix="",
        name="web",
    )

    manager = ServerManager(
        name="gpdb-admin",
        lifespan=create_admin_lifespan(services),
    )
    manager.app.state.config = resolved_config
    manager.app.state.config_store = config_store
    manager.app.state.services = services
    manager.add_server(web_app)
    manager.add_server(rest_api)
    manager.add_server(mcp_server)
    manager.add_server(cli)
    return manager


def bootstrap_runtime(argv: list[str] | None = None) -> tuple[ServerManager, ResolvedConfig, list[str]]:
    """Resolve config and create the runtime manager."""
    cli_args = list(sys.argv[1:] if argv is None else argv)
    config_arg, remaining_args = extract_config_arg(cli_args)
    config_store = ConfigStore.from_sources(cli_path=config_arg)
    resolved_config = _ensure_runtime_config(config_store)
    manager = create_manager(resolved_config=resolved_config, config_store=config_store)
    return manager, resolved_config, remaining_args


def main(argv: list[str] | None = None):
    manager, resolved_config, remaining_args = bootstrap_runtime(argv)
    if remaining_args and remaining_args[0] == "start":
        _run_start_command(manager, resolved_config, remaining_args[1:])
        return
    manager.cli(["gpdb", *remaining_args])


def _run_start_command(
    manager: ServerManager,
    resolved_config: ResolvedConfig,
    argv: list[str],
) -> None:
    """Run the HTTP server using config-backed defaults."""
    parser = argparse.ArgumentParser(prog="gpdb start")
    parser.add_argument("--host", default=resolved_config.server.host)
    parser.add_argument("--port", type=int, default=resolved_config.server.port)
    args = parser.parse_args(argv)

    print(f"Using config file: {resolved_config.location.path} ({resolved_config.location.source.value})")
    print(f"Config writable: {'yes' if resolved_config.location.writable else 'no'}")
    print("🚀 gpdb-admin Server Starting...")
    print("---------------------------------------------------")
    print(f"📋 OpenAPI:           http://{args.host}:{args.port}/docs")
    for mcp_name in manager.mcp_servers:
        print(f"🤖 MCP Server:        http://{args.host}:{args.port}/mcp/{mcp_name}/sse")
    for server in manager.active_servers.values():
        if isinstance(server, MountableApp):
            prefix = server.path_prefix if server.path_prefix else "/"
            print(f"🌐 Web App ({server.name}): http://{args.host}:{args.port}{prefix}")
    print("---------------------------------------------------")
    uvicorn.run(manager.app, host=args.host, port=args.port)


def _ensure_runtime_config(config_store: ConfigStore) -> ResolvedConfig:
    """Ensure required runtime secrets exist before the app starts."""
    resolved_config = config_store.load()
    if resolved_config.auth.session_secret:
        return resolved_config

    updated = resolved_config.file_config.model_copy(deep=True)
    updated.auth.session_secret = secrets.token_urlsafe(32)
    config_store.save(updated)
    return config_store.load()


if __name__ == "__main__":
    main()
