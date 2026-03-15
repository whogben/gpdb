"""Entry point for the `gpdb` console command."""

from __future__ import annotations

import argparse
import logging
import secrets
import sys
from dataclasses import dataclass
from typing import Any, Callable

import uvicorn

from gpdb.admin.auth import (
    extract_bearer_token,
    generate_api_key,
    hash_api_key_secret,
    verify_api_key_secret,
)
from gpdb.admin.config import ConfigStore, ResolvedConfig, extract_data_dir_arg
from gpdb.admin.context import (
    _build_mcp_principal_resolver,
    _build_rest_principal_resolver,
)
from gpdb.admin.graph_content import (
    GraphDetail,
    GraphEdgeDetail,
    GraphEdgeList,
    GraphList,
    GraphNodeDetail,
    GraphNodeList,
    GraphNodePayload,
    GraphOverview,
    GraphSchemaDetail,
    GraphSchemaList,
    InstanceDetail,
    InstanceList,
)
from gpdb.admin.runtime import AdminServices, create_admin_lifespan
from gpdb.admin.servers import (
    AuthMCPServer,
    CLIServer,
    OpenAPIServer,
    _AdminAPIKeyTokenVerifier,
    _invoke_tool_raw,
)
from gpdb.admin.tools import (
    _build_cli_api_key_tools,
    _build_graph_content_service,
    _build_mcp_api_key_tools,
)
from gpdb.admin.web import create_web_app
from toolaccess import (
    AccessPolicy,
    MountableApp,
    ServerManager,
    ToolService,
)

REST_API_PUBLIC_PATHS = frozenset(
    {
        "/docs",
        "/openapi.json",
        "/redoc",
        "/docs/oauth2-redirect",
    }
)

logger = logging.getLogger(__name__)


@dataclass
class AdminRuntime:
    """Container for all admin runtime components."""

    services: AdminServices
    resolved_config: ResolvedConfig
    config_store: ConfigStore
    lifespan: Callable
    web_app: MountableApp
    rest_api: OpenAPIServer
    mcp_server: AuthMCPServer
    cli_server: CLIServer | None
    admin_service: ToolService
    graph_service: ToolService
    cli_api_key_service: ToolService
    mcp_api_key_service: ToolService


def create_admin_runtime(
    *,
    config_store: ConfigStore | None = None,
    resolved_config: ResolvedConfig | None = None,
    http_root: str = "",
    api_path_prefix: str = "/api",
    mcp_name: str = "gpdb",
    cli_root_name: str | None = "gpdb",
) -> AdminRuntime:
    """Build admin runtime without creating a ServerManager.

    Args:
        config_store: Config store (created from sources if None)
        resolved_config: Resolved config (derived from store if None)
        http_root: Web UI mount prefix (e.g., "/gpdb")
        api_path_prefix: REST API mount prefix (e.g., "/api")
        mcp_name: MCP server name (e.g., "gpdb" or "gpdb-admin")
        cli_root_name: CLI root command (None to skip CLI creation)
    """
    if config_store is None:
        config_store = ConfigStore.from_sources()
    if resolved_config is None:
        resolved_config = _ensure_runtime_config(config_store)

    services = AdminServices(
        resolved_config=resolved_config,
        config_store=config_store,
    )
    lifespan = create_admin_lifespan(services)

    admin_service = ToolService("admin", [status])
    graph_service = _build_graph_content_service(services)
    cli_api_key_service = ToolService("admin-cli", _build_cli_api_key_tools(services))
    mcp_api_key_service = ToolService("admin-mcp", _build_mcp_api_key_tools(services))

    fastapi_app = create_web_app(
        resolved_config=resolved_config,
        config_store=config_store,
        services=services,
        http_root=http_root,
    )
    web_app = MountableApp(fastapi_app, path_prefix=http_root, name="web")

    rest_prefix = f"{http_root.rstrip('/')}{api_path_prefix}"
    rest_api = OpenAPIServer(
        path_prefix=rest_prefix,
        title="GPDB Admin API",
        principal_resolver=_build_rest_principal_resolver(),
    )
    rest_api.mount(admin_service)
    rest_api.mount(graph_service)
    _install_api_key_auth(rest_api, services)

    mcp_server = AuthMCPServer(
        mcp_name,
        auth_provider=_AdminAPIKeyTokenVerifier(services),
        principal_resolver=_build_mcp_principal_resolver(services),
    )
    mcp_server.mount(admin_service)
    mcp_server.mount(graph_service)
    mcp_server.mount(mcp_api_key_service)

    cli_server = None
    if cli_root_name is not None:
        cli_server = CLIServer(cli_root_name)
        cli_server.mount(admin_service)
        cli_server.mount(graph_service)
        cli_server.mount(cli_api_key_service)

    return AdminRuntime(
        services=services,
        resolved_config=resolved_config,
        config_store=config_store,
        lifespan=lifespan,
        web_app=web_app,
        rest_api=rest_api,
        mcp_server=mcp_server,
        cli_server=cli_server,
        admin_service=admin_service,
        graph_service=graph_service,
        cli_api_key_service=cli_api_key_service,
        mcp_api_key_service=mcp_api_key_service,
    )


def attach_admin_to_manager(
    manager: ServerManager,
    *,
    http_root: str = "/gpdb",
    api_path_prefix: str = "/api",
    mcp_name: str = "gpdb",
    cli_root_name: str | None = None,
    config_store: ConfigStore | None = None,
    resolved_config: ResolvedConfig | None = None,
) -> AdminRuntime:
    """Attach admin runtime to an existing ServerManager.

    This is the primary integration point for host applications.
    CLI is not attached by default (host typically has its own).
    """
    runtime = create_admin_runtime(
        config_store=config_store,
        resolved_config=resolved_config,
        http_root=http_root,
        api_path_prefix=api_path_prefix,
        mcp_name=mcp_name,
        cli_root_name=cli_root_name,
    )

    manager.add_server(runtime.web_app)
    manager.add_server(runtime.rest_api)
    manager.add_server(runtime.mcp_server)
    if runtime.cli_server:
        manager.add_server(runtime.cli_server)

    return runtime


def status() -> str:
    """Return the current status of the GPDB admin service."""
    return "OK"


def create_manager(
    resolved_config: ResolvedConfig | None = None,
    config_store: ConfigStore | None = None,
) -> ServerManager:
    """Create standalone admin ServerManager (backwards compatible)."""
    runtime = create_admin_runtime(
        config_store=config_store,
        resolved_config=resolved_config,
        http_root="",
        api_path_prefix="/api",
        mcp_name="gpdb",
        cli_root_name="gpdb",
    )

    manager = ServerManager(name="gpdb-admin", lifespan=runtime.lifespan)
    manager.add_server(runtime.web_app)
    manager.add_server(runtime.rest_api)
    manager.add_server(runtime.mcp_server)
    if runtime.cli_server:
        manager.add_server(runtime.cli_server)

    manager.app.state.config = runtime.resolved_config
    manager.app.state.config_store = runtime.config_store
    manager.app.state.services = runtime.services
    manager.app.state.admin_runtime = runtime
    return manager


def bootstrap_runtime(
    argv: list[str] | None = None,
) -> tuple[ServerManager, ResolvedConfig, list[str]]:
    """Resolve config and create the runtime manager."""
    cli_args = list(sys.argv[1:] if argv is None else argv)
    data_dir_arg, remaining_args = extract_data_dir_arg(cli_args)
    config_store = ConfigStore.from_sources(cli_data_dir=data_dir_arg)
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

    print(
        f"Using data dir: {resolved_config.location.data_dir} ({resolved_config.location.source.value})"
    )
    print(f"Config writable: {'yes' if resolved_config.location.writable else 'no'}")
    print("🚀 gpdb-admin Server Starting...")
    print("---------------------------------------------------")
    print(f"📋 OpenAPI:           http://{args.host}:{args.port}/api/docs")
    for mcp_name in manager.mcp_servers:
        print(
            f"🤖 MCP Server:        http://{args.host}:{args.port}/mcp/{mcp_name}/mcp"
        )
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


def _install_api_key_auth(rest_api: OpenAPIServer, services: AdminServices) -> None:
    """Require bearer API keys for protected REST routes under `/api`."""
    from fastapi import Request
    from fastapi.responses import JSONResponse

    @rest_api.app.middleware("http")
    async def require_api_key(request: Request, call_next):
        if request.url.path in REST_API_PUBLIC_PATHS:
            return await call_next(request)
        token = extract_bearer_token(request.headers.get("authorization"))
        if token is None or services.admin_store is None:
            return _unauthorized_response()
        authenticated = await services.admin_store.authenticate_api_key(
            api_key_token=token,
            verify_secret=verify_api_key_secret,
        )
        if authenticated is None:
            return _unauthorized_response()
        user, api_key = authenticated
        request.state.current_user = user
        request.state.current_api_key = api_key
        return await call_next(request)


def _unauthorized_response() -> JSONResponse:
    """Return the standard bearer-token auth failure response."""
    from fastapi.responses import JSONResponse

    return JSONResponse(
        {"detail": "Bearer API key required."},
        status_code=401,
        headers={"WWW-Authenticate": "Bearer"},
    )


if __name__ == "__main__":
    main()
