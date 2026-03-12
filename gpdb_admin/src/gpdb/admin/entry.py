"""Entry point for the `gpdb` console command."""

from __future__ import annotations

import argparse
import secrets
import sys

import uvicorn
from fastapi import Request
from fastapi.responses import JSONResponse
from fastmcp import FastMCP
from fastmcp.server.auth import AccessToken, TokenVerifier
from toolaccess import CLIServer, OpenAPIServer, SSEMCPServer as ToolaccessSSEMCPServer, ServerManager, ToolService
from toolaccess.toolaccess import MountableApp

from gpdb.admin.auth import extract_bearer_token, verify_api_key_secret
from gpdb.admin.config import ConfigStore, ResolvedConfig, extract_config_arg
from gpdb.admin.runtime import AdminServices, create_admin_lifespan
from gpdb.admin.web import create_web_app

REST_API_PUBLIC_PATHS = frozenset(
    {
        "/docs",
        "/openapi.json",
        "/redoc",
        "/docs/oauth2-redirect",
    }
)


class SSEMCPServer(ToolaccessSSEMCPServer):
    """Toolaccess-compatible MCP server with optional bearer auth."""

    def __init__(self, name: str = "default", auth_provider: TokenVerifier | None = None):
        self.name = name
        self.mcp = FastMCP(name, auth=auth_provider)


class _AdminAPIKeyTokenVerifier(TokenVerifier):
    """FastMCP bearer-token verifier backed by the admin API key store."""

    def __init__(self, services: AdminServices):
        super().__init__()
        self._services = services

    async def verify_token(self, token: str) -> AccessToken | None:
        admin_store = self._services.admin_store
        if admin_store is None:
            return None
        authenticated = await admin_store.authenticate_api_key(
            api_key_token=token,
            verify_secret=verify_api_key_secret,
        )
        if authenticated is None:
            return None
        user, api_key = authenticated
        return AccessToken(
            token=token,
            client_id=api_key.key_id,
            scopes=["gpdb-admin"],
            claims={
                "user_id": user.id,
                "username": user.username,
                "display_name": user.display_name,
                "is_owner": user.is_owner,
                "api_key_id": api_key.id,
                "api_key_label": api_key.label,
            },
        )


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
    _install_api_key_auth(rest_api, services)

    mcp_server = SSEMCPServer("gpdb", auth_provider=_AdminAPIKeyTokenVerifier(services))
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


def _install_api_key_auth(rest_api: OpenAPIServer, services: AdminServices) -> None:
    """Require bearer API keys for protected REST routes under `/api`."""

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
    return JSONResponse(
        {"detail": "Bearer API key required."},
        status_code=401,
        headers={"WWW-Authenticate": "Bearer"},
    )


if __name__ == "__main__":
    main()
