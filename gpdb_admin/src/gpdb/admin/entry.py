"""Entry point for the `gpdb` console command."""

from __future__ import annotations

import argparse
import base64
import json
import secrets
import sys
from dataclasses import asdict

import uvicorn
from fastapi import Request
from fastapi.responses import JSONResponse
from fastmcp import Context, FastMCP
from fastmcp.server.auth import AccessToken, TokenVerifier
from mcp.server.auth.middleware.auth_context import get_access_token
from toolaccess import (
    CLIServer,
    OpenAPIServer,
    SSEMCPServer as ToolaccessSSEMCPServer,
    ServerManager,
    ToolService,
)
from toolaccess import ToolDefinition
from toolaccess.toolaccess import MountableApp

from gpdb.admin.auth import (
    extract_bearer_token,
    generate_api_key,
    hash_api_key_secret,
    verify_api_key_secret,
)
from gpdb.admin.config import ConfigStore, ResolvedConfig, extract_config_arg
from gpdb.admin.graph_content import (
    GraphEdgeDetail,
    GraphEdgeList,
    GraphNodeDetail,
    GraphNodeList,
    GraphNodePayload,
    GraphOverview,
    GraphSchemaDetail,
    GraphSchemaList,
)
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

    def __init__(
        self, name: str = "default", auth_provider: TokenVerifier | None = None
    ):
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
    rest_graph_service = ToolService(
        "admin-graph-api", _build_rest_graph_content_tools(services)
    )
    cli_graph_service = ToolService(
        "admin-graph-cli", _build_cli_graph_content_tools(services)
    )
    mcp_graph_service = ToolService(
        "admin-graph-mcp", _build_mcp_graph_content_tools(services)
    )
    cli_api_key_service = ToolService("admin-cli", _build_cli_api_key_tools(services))
    mcp_api_key_service = ToolService("admin-mcp", _build_mcp_api_key_tools(services))

    rest_api = OpenAPIServer(path_prefix="/api", title="GPDB Admin API")
    rest_api.mount(admin_service)
    rest_api.mount(rest_graph_service)
    _install_api_key_auth(rest_api, services)

    mcp_server = SSEMCPServer("gpdb", auth_provider=_AdminAPIKeyTokenVerifier(services))
    mcp_server.mount(admin_service)
    mcp_server.mount(mcp_graph_service)
    mcp_server.mount(mcp_api_key_service)

    cli = CLIServer("gpdb")
    cli.mount(admin_service)
    cli.mount(cli_graph_service)
    cli.mount(cli_api_key_service)

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


def bootstrap_runtime(
    argv: list[str] | None = None,
) -> tuple[ServerManager, ResolvedConfig, list[str]]:
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

    print(
        f"Using config file: {resolved_config.location.path} ({resolved_config.location.source.value})"
    )
    print(f"Config writable: {'yes' if resolved_config.location.writable else 'no'}")
    print("🚀 gpdb-admin Server Starting...")
    print("---------------------------------------------------")
    print(f"📋 OpenAPI:           http://{args.host}:{args.port}/api/docs")
    for mcp_name in manager.mcp_servers:
        print(
            f"🤖 MCP Server:        http://{args.host}:{args.port}/mcp/{mcp_name}/sse"
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


async def _call_graph_content(
    services: AdminServices,
    method_name: str,
    *,
    current_user=None,
    allow_local_system: bool = False,
    **kwargs,
):
    """Invoke one graph-content service method with shared access controls."""
    graph_content = _require_graph_content(services)
    method = getattr(graph_content, method_name)
    return await method(
        current_user=current_user,
        allow_local_system=allow_local_system,
        **kwargs,
    )


def _emit_cli_model(result, *, by_alias: bool = False):
    """Serialize one Pydantic result for CLI output."""
    return _emit_cli_result(result.model_dump(mode="json", by_alias=by_alias))


def _build_rest_graph_content_tools(services: AdminServices) -> list[ToolDefinition]:
    """Build authenticated REST tools for graph-content access."""

    async def graph_overview(graph_id: str, request: Request) -> GraphOverview:
        """Return one managed graph overview for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "get_graph_overview",
            graph_id=graph_id,
            current_user=_require_rest_user(request),
        )

    async def graph_schema_list(
        graph_id: str,
        request: Request,
        kind: str = "",
    ) -> GraphSchemaList:
        """List graph schemas for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "list_graph_schemas",
            graph_id=graph_id,
            kind=kind,
            current_user=_require_rest_user(request),
        )

    async def graph_schema_get(
        graph_id: str, name: str, request: Request
    ) -> GraphSchemaDetail:
        """Return one graph schema for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "get_graph_schema",
            graph_id=graph_id,
            name=name,
            current_user=_require_rest_user(request),
        )

    async def graph_schema_create(
        graph_id: str,
        name: str,
        json_schema: dict[str, object],
        request: Request,
        kind: str = "node",
    ) -> GraphSchemaDetail:
        """Create one graph schema for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "create_graph_schema",
            graph_id=graph_id,
            name=name,
            json_schema=_coerce_json_object_argument(
                json_schema, argument_name="json_schema"
            ),
            kind=kind,
            current_user=_require_rest_user(request),
        )

    async def graph_schema_update(
        graph_id: str,
        name: str,
        json_schema: dict[str, object],
        request: Request,
        kind: str = "node",
    ) -> GraphSchemaDetail:
        """Update one graph schema for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "update_graph_schema",
            graph_id=graph_id,
            name=name,
            json_schema=_coerce_json_object_argument(
                json_schema, argument_name="json_schema"
            ),
            kind=kind,
            current_user=_require_rest_user(request),
        )

    async def graph_schema_delete(
        graph_id: str,
        name: str,
        request: Request,
    ) -> GraphSchemaDetail:
        """Delete one graph schema for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "delete_graph_schema",
            graph_id=graph_id,
            name=name,
            current_user=_require_rest_user(request),
        )

    async def graph_node_list(
        graph_id: str,
        request: Request,
        type: str = "",
        schema_name: str = "",
        parent_id: str = "",
        limit: int = 50,
        offset: int = 0,
        sort: str = "created_at_desc",
    ) -> GraphNodeList:
        """List graph nodes for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "list_graph_nodes",
            graph_id=graph_id,
            type=type,
            schema_name=schema_name,
            parent_id=parent_id,
            limit=limit,
            offset=offset,
            sort=sort,
            current_user=_require_rest_user(request),
        )

    async def graph_node_get(
        graph_id: str,
        node_id: str,
        request: Request,
    ) -> GraphNodeDetail:
        """Return one graph node for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "get_graph_node",
            graph_id=graph_id,
            node_id=node_id,
            current_user=_require_rest_user(request),
        )

    async def graph_node_create(
        graph_id: str,
        type: str,
        data: dict[str, object],
        request: Request,
        name: str = "",
        schema_name: str = "",
        owner_id: str = "",
        parent_id: str = "",
        tags: str = "",
        payload_base64: str | None = None,
        payload_mime: str = "",
        payload_filename: str = "",
    ) -> GraphNodeDetail:
        """Create one graph node for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "create_graph_node",
            graph_id=graph_id,
            type=type,
            name=name,
            schema_name=schema_name,
            owner_id=owner_id,
            parent_id=parent_id,
            tags=_coerce_tags_argument(tags),
            data=_coerce_json_object_argument(data, argument_name="data"),
            payload=_coerce_optional_payload_base64_argument(payload_base64),
            payload_mime=payload_mime,
            payload_filename=payload_filename,
            current_user=_require_rest_user(request),
        )

    async def graph_node_update(
        graph_id: str,
        node_id: str,
        type: str,
        data: dict[str, object],
        request: Request,
        name: str = "",
        schema_name: str = "",
        owner_id: str = "",
        parent_id: str = "",
        tags: str = "",
        payload_base64: str | None = None,
        payload_mime: str = "",
        payload_filename: str = "",
        clear_payload: bool = False,
    ) -> GraphNodeDetail:
        """Update one graph node for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "update_graph_node",
            graph_id=graph_id,
            node_id=node_id,
            type=type,
            name=name,
            schema_name=schema_name,
            owner_id=owner_id,
            parent_id=parent_id,
            tags=_coerce_tags_argument(tags),
            data=_coerce_json_object_argument(data, argument_name="data"),
            payload=_coerce_optional_payload_base64_argument(payload_base64),
            payload_mime=payload_mime,
            payload_filename=payload_filename,
            clear_payload=clear_payload,
            current_user=_require_rest_user(request),
        )

    async def graph_node_delete(
        graph_id: str,
        node_id: str,
        request: Request,
    ) -> GraphNodeDetail:
        """Delete one graph node for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "delete_graph_node",
            graph_id=graph_id,
            node_id=node_id,
            current_user=_require_rest_user(request),
        )

    async def graph_node_payload_get(
        graph_id: str,
        node_id: str,
        request: Request,
    ) -> GraphNodePayload:
        """Return one graph node payload for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "get_graph_node_payload",
            graph_id=graph_id,
            node_id=node_id,
            current_user=_require_rest_user(request),
        )

    async def graph_node_payload_set(
        graph_id: str,
        node_id: str,
        payload_base64: str,
        request: Request,
        mime: str = "",
        payload_filename: str = "",
    ) -> GraphNodeDetail:
        """Set one graph node payload for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "set_graph_node_payload",
            graph_id=graph_id,
            node_id=node_id,
            payload=_coerce_payload_base64_argument(payload_base64),
            mime=mime,
            payload_filename=payload_filename,
            current_user=_require_rest_user(request),
        )

    async def graph_edge_list(
        graph_id: str,
        request: Request,
        type: str = "",
        schema_name: str = "",
        source_id: str = "",
        target_id: str = "",
        limit: int = 50,
        offset: int = 0,
        sort: str = "created_at_desc",
    ) -> GraphEdgeList:
        """List graph edges for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "list_graph_edges",
            graph_id=graph_id,
            type=type,
            schema_name=schema_name,
            source_id=source_id,
            target_id=target_id,
            limit=limit,
            offset=offset,
            sort=sort,
            current_user=_require_rest_user(request),
        )

    async def graph_edge_get(
        graph_id: str,
        edge_id: str,
        request: Request,
    ) -> GraphEdgeDetail:
        """Return one graph edge for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "get_graph_edge",
            graph_id=graph_id,
            edge_id=edge_id,
            current_user=_require_rest_user(request),
        )

    async def graph_edge_create(
        graph_id: str,
        type: str,
        source_id: str,
        target_id: str,
        data: dict[str, object],
        request: Request,
        schema_name: str = "",
        tags: str = "",
    ) -> GraphEdgeDetail:
        """Create one graph edge for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "create_graph_edge",
            graph_id=graph_id,
            type=type,
            source_id=source_id,
            target_id=target_id,
            schema_name=schema_name,
            tags=_coerce_tags_argument(tags),
            data=_coerce_json_object_argument(data, argument_name="data"),
            current_user=_require_rest_user(request),
        )

    async def graph_edge_update(
        graph_id: str,
        edge_id: str,
        type: str,
        source_id: str,
        target_id: str,
        data: dict[str, object],
        request: Request,
        schema_name: str = "",
        tags: str = "",
    ) -> GraphEdgeDetail:
        """Update one graph edge for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "update_graph_edge",
            graph_id=graph_id,
            edge_id=edge_id,
            type=type,
            source_id=source_id,
            target_id=target_id,
            schema_name=schema_name,
            tags=_coerce_tags_argument(tags),
            data=_coerce_json_object_argument(data, argument_name="data"),
            current_user=_require_rest_user(request),
        )

    async def graph_edge_delete(
        graph_id: str,
        edge_id: str,
        request: Request,
    ) -> GraphEdgeDetail:
        """Delete one graph edge for the authenticated REST user."""
        return await _call_graph_content(
            services,
            "delete_graph_edge",
            graph_id=graph_id,
            edge_id=edge_id,
            current_user=_require_rest_user(request),
        )

    return [
        ToolDefinition(graph_overview, "graph_overview", http_method="GET"),
        ToolDefinition(graph_schema_list, "graph_schema_list", http_method="GET"),
        ToolDefinition(graph_schema_get, "graph_schema_get", http_method="GET"),
        ToolDefinition(graph_schema_create, "graph_schema_create"),
        ToolDefinition(graph_schema_update, "graph_schema_update"),
        ToolDefinition(graph_schema_delete, "graph_schema_delete"),
        ToolDefinition(graph_node_list, "graph_node_list", http_method="GET"),
        ToolDefinition(graph_node_get, "graph_node_get", http_method="GET"),
        ToolDefinition(graph_node_create, "graph_node_create"),
        ToolDefinition(graph_node_update, "graph_node_update"),
        ToolDefinition(graph_node_delete, "graph_node_delete"),
        ToolDefinition(
            graph_node_payload_get, "graph_node_payload_get", http_method="GET"
        ),
        ToolDefinition(graph_node_payload_set, "graph_node_payload_set"),
        ToolDefinition(graph_edge_list, "graph_edge_list", http_method="GET"),
        ToolDefinition(graph_edge_get, "graph_edge_get", http_method="GET"),
        ToolDefinition(graph_edge_create, "graph_edge_create"),
        ToolDefinition(graph_edge_update, "graph_edge_update"),
        ToolDefinition(graph_edge_delete, "graph_edge_delete"),
    ]


def _build_cli_graph_content_tools(services: AdminServices) -> list[ToolDefinition]:
    """Build trusted local CLI commands for graph-content access."""

    async def graph_overview(graph_id: str) -> dict[str, object]:
        """Return one managed graph overview for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "get_graph_overview",
            graph_id=graph_id,
            allow_local_system=True,
        )
        return _emit_cli_model(result)

    async def graph_schema_list(graph_id: str, kind: str = "") -> dict[str, object]:
        """List graph schemas for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "list_graph_schemas",
            graph_id=graph_id,
            allow_local_system=True,
            kind=kind,
        )
        return _emit_cli_model(result)

    async def graph_schema_get(graph_id: str, name: str) -> dict[str, object]:
        """Return one graph schema for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "get_graph_schema",
            graph_id=graph_id,
            name=name,
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    async def graph_schema_create(
        graph_id: str,
        name: str,
        json_schema: str,
        kind: str = "node",
    ) -> dict[str, object]:
        """Create one graph schema for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "create_graph_schema",
            graph_id=graph_id,
            name=name,
            json_schema=_coerce_json_object_argument(
                json_schema, argument_name="json_schema"
            ),
            kind=kind,
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    async def graph_schema_update(
        graph_id: str,
        name: str,
        json_schema: str,
        kind: str = "node",
    ) -> dict[str, object]:
        """Update one graph schema for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "update_graph_schema",
            graph_id=graph_id,
            name=name,
            json_schema=_coerce_json_object_argument(
                json_schema, argument_name="json_schema"
            ),
            kind=kind,
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    async def graph_schema_delete(
        graph_id: str,
        name: str,
    ) -> dict[str, object]:
        """Delete one graph schema for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "delete_graph_schema",
            graph_id=graph_id,
            name=name,
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    async def graph_node_list(
        graph_id: str,
        type: str = "",
        schema_name: str = "",
        parent_id: str = "",
        limit: int = 50,
        offset: int = 0,
        sort: str = "created_at_desc",
    ) -> dict[str, object]:
        """List graph nodes for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "list_graph_nodes",
            graph_id=graph_id,
            allow_local_system=True,
            type=type,
            schema_name=schema_name,
            parent_id=parent_id,
            limit=limit,
            offset=offset,
            sort=sort,
        )
        return _emit_cli_model(result)

    async def graph_node_get(graph_id: str, node_id: str) -> dict[str, object]:
        """Return one graph node for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "get_graph_node",
            graph_id=graph_id,
            node_id=node_id,
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    async def graph_node_create(
        graph_id: str,
        type: str,
        data: str,
        name: str = "",
        schema_name: str = "",
        owner_id: str = "",
        parent_id: str = "",
        tags: str = "",
        payload_base64: str | None = None,
        payload_mime: str = "",
        payload_filename: str = "",
    ) -> dict[str, object]:
        """Create one graph node for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "create_graph_node",
            graph_id=graph_id,
            type=type,
            name=name,
            schema_name=schema_name,
            owner_id=owner_id,
            parent_id=parent_id,
            tags=_coerce_tags_argument(tags),
            data=_coerce_json_object_argument(data, argument_name="data"),
            payload=_coerce_optional_payload_base64_argument(payload_base64),
            payload_mime=payload_mime,
            payload_filename=payload_filename,
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    async def graph_node_update(
        graph_id: str,
        node_id: str,
        type: str,
        data: str,
        name: str = "",
        schema_name: str = "",
        owner_id: str = "",
        parent_id: str = "",
        tags: str = "",
        payload_base64: str | None = None,
        payload_mime: str = "",
        payload_filename: str = "",
        clear_payload: bool = False,
    ) -> dict[str, object]:
        """Update one graph node for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "update_graph_node",
            graph_id=graph_id,
            node_id=node_id,
            type=type,
            name=name,
            schema_name=schema_name,
            owner_id=owner_id,
            parent_id=parent_id,
            tags=_coerce_tags_argument(tags),
            data=_coerce_json_object_argument(data, argument_name="data"),
            payload=_coerce_optional_payload_base64_argument(payload_base64),
            payload_mime=payload_mime,
            payload_filename=payload_filename,
            clear_payload=clear_payload,
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    async def graph_node_delete(graph_id: str, node_id: str) -> dict[str, object]:
        """Delete one graph node for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "delete_graph_node",
            graph_id=graph_id,
            node_id=node_id,
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    async def graph_node_payload_get(graph_id: str, node_id: str) -> dict[str, object]:
        """Return one graph node payload for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "get_graph_node_payload",
            graph_id=graph_id,
            node_id=node_id,
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    async def graph_node_payload_set(
        graph_id: str,
        node_id: str,
        payload_base64: str,
        mime: str = "",
        payload_filename: str = "",
    ) -> dict[str, object]:
        """Set one graph node payload for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "set_graph_node_payload",
            graph_id=graph_id,
            node_id=node_id,
            payload=_coerce_payload_base64_argument(payload_base64),
            mime=mime,
            payload_filename=payload_filename,
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    async def graph_edge_list(
        graph_id: str,
        type: str = "",
        schema_name: str = "",
        source_id: str = "",
        target_id: str = "",
        limit: int = 50,
        offset: int = 0,
        sort: str = "created_at_desc",
    ) -> dict[str, object]:
        """List graph edges for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "list_graph_edges",
            graph_id=graph_id,
            allow_local_system=True,
            type=type,
            schema_name=schema_name,
            source_id=source_id,
            target_id=target_id,
            limit=limit,
            offset=offset,
            sort=sort,
        )
        return _emit_cli_model(result)

    async def graph_edge_get(graph_id: str, edge_id: str) -> dict[str, object]:
        """Return one graph edge for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "get_graph_edge",
            graph_id=graph_id,
            edge_id=edge_id,
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    async def graph_edge_create(
        graph_id: str,
        type: str,
        source_id: str,
        target_id: str,
        data: str,
        schema_name: str = "",
        tags: str = "",
    ) -> dict[str, object]:
        """Create one graph edge for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "create_graph_edge",
            graph_id=graph_id,
            type=type,
            source_id=source_id,
            target_id=target_id,
            schema_name=schema_name,
            tags=_coerce_tags_argument(tags),
            data=_coerce_json_object_argument(data, argument_name="data"),
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    async def graph_edge_update(
        graph_id: str,
        edge_id: str,
        type: str,
        source_id: str,
        target_id: str,
        data: str,
        schema_name: str = "",
        tags: str = "",
    ) -> dict[str, object]:
        """Update one graph edge for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "update_graph_edge",
            graph_id=graph_id,
            edge_id=edge_id,
            type=type,
            source_id=source_id,
            target_id=target_id,
            schema_name=schema_name,
            tags=_coerce_tags_argument(tags),
            data=_coerce_json_object_argument(data, argument_name="data"),
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    async def graph_edge_delete(graph_id: str, edge_id: str) -> dict[str, object]:
        """Delete one graph edge for trusted local CLI use."""
        result = await _call_graph_content(
            services,
            "delete_graph_edge",
            graph_id=graph_id,
            edge_id=edge_id,
            allow_local_system=True,
        )
        return _emit_cli_model(result, by_alias=True)

    return [
        ToolDefinition(graph_overview, "graph_overview"),
        ToolDefinition(graph_schema_list, "graph_schema_list"),
        ToolDefinition(graph_schema_get, "graph_schema_get"),
        ToolDefinition(graph_schema_create, "graph_schema_create"),
        ToolDefinition(graph_schema_update, "graph_schema_update"),
        ToolDefinition(graph_schema_delete, "graph_schema_delete"),
        ToolDefinition(graph_node_list, "graph_node_list"),
        ToolDefinition(graph_node_get, "graph_node_get"),
        ToolDefinition(graph_node_create, "graph_node_create"),
        ToolDefinition(graph_node_update, "graph_node_update"),
        ToolDefinition(graph_node_delete, "graph_node_delete"),
        ToolDefinition(graph_node_payload_get, "graph_node_payload_get"),
        ToolDefinition(graph_node_payload_set, "graph_node_payload_set"),
        ToolDefinition(graph_edge_list, "graph_edge_list"),
        ToolDefinition(graph_edge_get, "graph_edge_get"),
        ToolDefinition(graph_edge_create, "graph_edge_create"),
        ToolDefinition(graph_edge_update, "graph_edge_update"),
        ToolDefinition(graph_edge_delete, "graph_edge_delete"),
    ]


def _build_mcp_graph_content_tools(services: AdminServices) -> list[ToolDefinition]:
    """Build authenticated MCP tools for graph-content access."""

    async def graph_overview(graph_id: str, ctx: Context) -> GraphOverview:
        """Return one managed graph overview for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "get_graph_overview",
            graph_id=graph_id,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_schema_list(
        graph_id: str,
        ctx: Context,
        kind: str = "",
    ) -> GraphSchemaList:
        """List graph schemas for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "list_graph_schemas",
            graph_id=graph_id,
            kind=kind,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_schema_get(
        graph_id: str, name: str, ctx: Context
    ) -> GraphSchemaDetail:
        """Return one graph schema for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "get_graph_schema",
            graph_id=graph_id,
            name=name,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_schema_create(
        graph_id: str,
        name: str,
        json_schema: dict[str, object],
        ctx: Context,
        kind: str = "node",
    ) -> GraphSchemaDetail:
        """Create one graph schema for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "create_graph_schema",
            graph_id=graph_id,
            name=name,
            json_schema=_coerce_json_object_argument(
                json_schema, argument_name="json_schema"
            ),
            kind=kind,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_schema_update(
        graph_id: str,
        name: str,
        json_schema: dict[str, object],
        ctx: Context,
        kind: str = "node",
    ) -> GraphSchemaDetail:
        """Update one graph schema for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "update_graph_schema",
            graph_id=graph_id,
            name=name,
            json_schema=_coerce_json_object_argument(
                json_schema, argument_name="json_schema"
            ),
            kind=kind,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_schema_delete(
        graph_id: str,
        name: str,
        ctx: Context,
    ) -> GraphSchemaDetail:
        """Delete one graph schema for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "delete_graph_schema",
            graph_id=graph_id,
            name=name,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_node_list(
        graph_id: str,
        ctx: Context,
        type: str = "",
        schema_name: str = "",
        parent_id: str = "",
        limit: int = 50,
        offset: int = 0,
        sort: str = "created_at_desc",
    ) -> GraphNodeList:
        """List graph nodes for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "list_graph_nodes",
            graph_id=graph_id,
            type=type,
            schema_name=schema_name,
            parent_id=parent_id,
            limit=limit,
            offset=offset,
            sort=sort,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_node_get(
        graph_id: str,
        node_id: str,
        ctx: Context,
    ) -> GraphNodeDetail:
        """Return one graph node for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "get_graph_node",
            graph_id=graph_id,
            node_id=node_id,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_node_create(
        graph_id: str,
        type: str,
        data: dict[str, object],
        ctx: Context,
        name: str = "",
        schema_name: str = "",
        owner_id: str = "",
        parent_id: str = "",
        tags: str = "",
        payload_base64: str | None = None,
        payload_mime: str = "",
        payload_filename: str = "",
    ) -> GraphNodeDetail:
        """Create one graph node for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "create_graph_node",
            graph_id=graph_id,
            type=type,
            name=name,
            schema_name=schema_name,
            owner_id=owner_id,
            parent_id=parent_id,
            tags=_coerce_tags_argument(tags),
            data=_coerce_json_object_argument(data, argument_name="data"),
            payload=_coerce_optional_payload_base64_argument(payload_base64),
            payload_mime=payload_mime,
            payload_filename=payload_filename,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_node_update(
        graph_id: str,
        node_id: str,
        type: str,
        data: dict[str, object],
        ctx: Context,
        name: str = "",
        schema_name: str = "",
        owner_id: str = "",
        parent_id: str = "",
        tags: str = "",
        payload_base64: str | None = None,
        payload_mime: str = "",
        payload_filename: str = "",
        clear_payload: bool = False,
    ) -> GraphNodeDetail:
        """Update one graph node for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "update_graph_node",
            graph_id=graph_id,
            node_id=node_id,
            type=type,
            name=name,
            schema_name=schema_name,
            owner_id=owner_id,
            parent_id=parent_id,
            tags=_coerce_tags_argument(tags),
            data=_coerce_json_object_argument(data, argument_name="data"),
            payload=_coerce_optional_payload_base64_argument(payload_base64),
            payload_mime=payload_mime,
            payload_filename=payload_filename,
            clear_payload=clear_payload,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_node_delete(
        graph_id: str,
        node_id: str,
        ctx: Context,
    ) -> GraphNodeDetail:
        """Delete one graph node for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "delete_graph_node",
            graph_id=graph_id,
            node_id=node_id,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_node_payload_get(
        graph_id: str,
        node_id: str,
        ctx: Context,
    ) -> GraphNodePayload:
        """Return one graph node payload for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "get_graph_node_payload",
            graph_id=graph_id,
            node_id=node_id,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_node_payload_set(
        graph_id: str,
        node_id: str,
        payload_base64: str,
        ctx: Context,
        mime: str = "",
        payload_filename: str = "",
    ) -> GraphNodeDetail:
        """Set one graph node payload for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "set_graph_node_payload",
            graph_id=graph_id,
            node_id=node_id,
            payload=_coerce_payload_base64_argument(payload_base64),
            mime=mime,
            payload_filename=payload_filename,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_edge_list(
        graph_id: str,
        ctx: Context,
        type: str = "",
        schema_name: str = "",
        source_id: str = "",
        target_id: str = "",
        limit: int = 50,
        offset: int = 0,
        sort: str = "created_at_desc",
    ) -> GraphEdgeList:
        """List graph edges for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "list_graph_edges",
            graph_id=graph_id,
            type=type,
            schema_name=schema_name,
            source_id=source_id,
            target_id=target_id,
            limit=limit,
            offset=offset,
            sort=sort,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_edge_get(
        graph_id: str,
        edge_id: str,
        ctx: Context,
    ) -> GraphEdgeDetail:
        """Return one graph edge for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "get_graph_edge",
            graph_id=graph_id,
            edge_id=edge_id,
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_edge_create(
        graph_id: str,
        type: str,
        source_id: str,
        target_id: str,
        data: dict[str, object],
        ctx: Context,
        schema_name: str = "",
        tags: str = "",
    ) -> GraphEdgeDetail:
        """Create one graph edge for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "create_graph_edge",
            graph_id=graph_id,
            type=type,
            source_id=source_id,
            target_id=target_id,
            schema_name=schema_name,
            tags=_coerce_tags_argument(tags),
            data=_coerce_json_object_argument(data, argument_name="data"),
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_edge_update(
        graph_id: str,
        edge_id: str,
        type: str,
        source_id: str,
        target_id: str,
        data: dict[str, object],
        ctx: Context,
        schema_name: str = "",
        tags: str = "",
    ) -> GraphEdgeDetail:
        """Update one graph edge for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "update_graph_edge",
            graph_id=graph_id,
            edge_id=edge_id,
            type=type,
            source_id=source_id,
            target_id=target_id,
            schema_name=schema_name,
            tags=_coerce_tags_argument(tags),
            data=_coerce_json_object_argument(data, argument_name="data"),
            current_user=await _require_mcp_user(services, ctx),
        )

    async def graph_edge_delete(
        graph_id: str,
        edge_id: str,
        ctx: Context,
    ) -> GraphEdgeDetail:
        """Delete one graph edge for the authenticated MCP user."""
        return await _call_graph_content(
            services,
            "delete_graph_edge",
            graph_id=graph_id,
            edge_id=edge_id,
            current_user=await _require_mcp_user(services, ctx),
        )

    return [
        ToolDefinition(graph_overview, "graph_overview"),
        ToolDefinition(graph_schema_list, "graph_schema_list"),
        ToolDefinition(graph_schema_get, "graph_schema_get"),
        ToolDefinition(graph_schema_create, "graph_schema_create"),
        ToolDefinition(graph_schema_update, "graph_schema_update"),
        ToolDefinition(graph_schema_delete, "graph_schema_delete"),
        ToolDefinition(graph_node_list, "graph_node_list"),
        ToolDefinition(graph_node_get, "graph_node_get"),
        ToolDefinition(graph_node_create, "graph_node_create"),
        ToolDefinition(graph_node_update, "graph_node_update"),
        ToolDefinition(graph_node_delete, "graph_node_delete"),
        ToolDefinition(graph_node_payload_get, "graph_node_payload_get"),
        ToolDefinition(graph_node_payload_set, "graph_node_payload_set"),
        ToolDefinition(graph_edge_list, "graph_edge_list"),
        ToolDefinition(graph_edge_get, "graph_edge_get"),
        ToolDefinition(graph_edge_create, "graph_edge_create"),
        ToolDefinition(graph_edge_update, "graph_edge_update"),
        ToolDefinition(graph_edge_delete, "graph_edge_delete"),
    ]


def _build_cli_api_key_tools(services: AdminServices) -> list[ToolDefinition]:
    """Build trusted local CLI commands for API key management."""

    async def api_key_list(username: str) -> list[dict[str, object]]:
        """List API keys for one local admin user."""
        user = await _require_user_by_username(services, username)
        result = [
            _serialize_api_key(item)
            for item in await services.admin_store.list_api_keys_for_user(user.id)
        ]
        return _emit_cli_result(result)

    async def api_key_create(username: str, label: str) -> dict[str, object]:
        """Create an API key for one local admin user."""
        user = await _require_user_by_username(services, username)
        result = await _create_api_key_for_user(services, user_id=user.id, label=label)
        return _emit_cli_result(result)

    async def api_key_reveal(username: str, key_id: str) -> dict[str, object]:
        """Reveal one API key owned by the named local user."""
        user = await _require_user_by_username(services, username)
        result = await _reveal_api_key_for_user(
            services, user_id=user.id, key_id=key_id
        )
        return _emit_cli_result(result)

    async def api_key_revoke(username: str, key_id: str) -> dict[str, object]:
        """Revoke one API key owned by the named local user."""
        user = await _require_user_by_username(services, username)
        result = await _revoke_api_key_for_user(
            services, user_id=user.id, key_id=key_id
        )
        return _emit_cli_result(result)

    return [
        ToolDefinition(api_key_list, "api_key_list"),
        ToolDefinition(api_key_create, "api_key_create"),
        ToolDefinition(api_key_reveal, "api_key_reveal"),
        ToolDefinition(api_key_revoke, "api_key_revoke"),
    ]


def _build_mcp_api_key_tools(services: AdminServices) -> list[ToolDefinition]:
    """Build authenticated MCP tools for current-user API key management."""

    async def api_key_list_me(ctx: Context) -> list[dict[str, object]]:
        """List API keys for the authenticated MCP user."""
        user = await _require_mcp_user(services, ctx)
        return [
            _serialize_api_key(item)
            for item in await services.admin_store.list_api_keys_for_user(user.id)
        ]

    async def api_key_create_me(label: str, ctx: Context) -> dict[str, object]:
        """Create an API key for the authenticated MCP user."""
        user = await _require_mcp_user(services, ctx)
        return await _create_api_key_for_user(services, user_id=user.id, label=label)

    async def api_key_reveal_me(key_id: str, ctx: Context) -> dict[str, object]:
        """Reveal one API key owned by the authenticated MCP user."""
        user = await _require_mcp_user(services, ctx)
        return await _reveal_api_key_for_user(services, user_id=user.id, key_id=key_id)

    async def api_key_revoke_me(key_id: str, ctx: Context) -> dict[str, object]:
        """Revoke one API key owned by the authenticated MCP user."""
        user = await _require_mcp_user(services, ctx)
        return await _revoke_api_key_for_user(services, user_id=user.id, key_id=key_id)

    return [
        ToolDefinition(api_key_list_me, "api_key_list"),
        ToolDefinition(api_key_create_me, "api_key_create"),
        ToolDefinition(api_key_reveal_me, "api_key_reveal"),
        ToolDefinition(api_key_revoke_me, "api_key_revoke"),
    ]


async def _require_user_by_username(services: AdminServices, username: str):
    """Return an active admin user by username or raise a friendly error."""
    admin_store = services.admin_store
    if admin_store is None:
        raise RuntimeError("Admin store is not ready yet.")
    user = await admin_store.get_user_by_username(username.strip())
    if user is None or not user.is_active:
        raise ValueError(f"User '{username}' was not found.")
    return user


async def _require_mcp_user(services: AdminServices, ctx: Context):
    """Return the authenticated MCP user for the current request."""
    admin_store = services.admin_store
    if admin_store is None:
        raise RuntimeError("Admin store is not ready yet.")
    access_token = get_access_token()
    if access_token is None:
        raise RuntimeError("Authenticated MCP API key required.")
    user_id = str(access_token.claims.get("user_id", "")).strip()
    if not user_id:
        raise RuntimeError("Authenticated MCP token is missing a user id.")
    user = await admin_store.get_user_by_id(user_id)
    if user is None or not user.is_active:
        raise RuntimeError("Authenticated MCP user is no longer active.")
    return user


async def _create_api_key_for_user(
    services: AdminServices,
    *,
    user_id: str,
    label: str,
) -> dict[str, object]:
    """Create one API key for a target user and return revealable metadata."""
    admin_store = services.admin_store
    if admin_store is None:
        raise RuntimeError("Admin store is not ready yet.")
    clean_label = label.strip()
    if not clean_label:
        raise ValueError("API key label is required.")
    generated = generate_api_key()
    api_key = await admin_store.create_api_key(
        user_id=user_id,
        label=clean_label,
        key_id=generated.key_id,
        preview=generated.preview,
        secret_hash=hash_api_key_secret(generated.secret),
        key_value=generated.token,
    )
    result = _serialize_api_key(api_key)
    result["api_key"] = generated.token
    return result


async def _reveal_api_key_for_user(
    services: AdminServices,
    *,
    user_id: str,
    key_id: str,
) -> dict[str, object]:
    """Reveal one API key if it belongs to the target user."""
    api_key = await _require_owned_api_key(services, user_id=user_id, key_id=key_id)
    revealed = await services.admin_store.reveal_api_key(api_key.id)
    if revealed is None:
        raise ValueError(f"API key '{key_id}' was not found.")
    result = _serialize_api_key(api_key)
    result["api_key"] = revealed
    return result


async def _revoke_api_key_for_user(
    services: AdminServices,
    *,
    user_id: str,
    key_id: str,
) -> dict[str, object]:
    """Revoke one API key if it belongs to the target user."""
    admin_store = services.admin_store
    if admin_store is None:
        raise RuntimeError("Admin store is not ready yet.")
    api_key = await _require_owned_api_key(services, user_id=user_id, key_id=key_id)
    updated = await admin_store.revoke_api_key(api_key.id)
    if updated is None:
        raise ValueError(f"API key '{key_id}' was not found.")
    return _serialize_api_key(updated)


async def _require_owned_api_key(
    services: AdminServices,
    *,
    user_id: str,
    key_id: str,
):
    """Return one API key when it belongs to the requested user."""
    admin_store = services.admin_store
    if admin_store is None:
        raise RuntimeError("Admin store is not ready yet.")
    api_key = await admin_store.get_api_key_by_key_id(key_id.strip())
    if api_key is None or api_key.user_id != user_id:
        raise ValueError(f"API key '{key_id}' was not found.")
    return api_key


def _serialize_api_key(api_key) -> dict[str, object]:
    """Project one API key dataclass into a tool-friendly dict."""
    return asdict(api_key)


def _require_graph_content(services: AdminServices):
    """Return the shared graph-content service once startup has completed."""
    graph_content = services.graph_content
    if graph_content is None:
        raise RuntimeError("Graph content service is not ready yet.")
    return graph_content


def _require_rest_user(request: Request):
    """Return the authenticated REST user set by API-key middleware."""
    current_user = getattr(request.state, "current_user", None)
    if current_user is None:
        raise RuntimeError("Authenticated REST API key required.")
    return current_user


def _emit_cli_result(result):
    """Print a JSON-formatted CLI result and return it for tests."""
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def _coerce_json_object_argument(raw_value, *, argument_name: str) -> dict[str, object]:
    """Accept either a parsed dict or JSON text and return a JSON object."""
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            raise ValueError(f"{argument_name} is required.")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{argument_name} must be valid JSON: {exc.msg}.") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"{argument_name} must be a JSON object.")
        return parsed
    raise ValueError(f"{argument_name} must be a JSON object.")


def _coerce_tags_argument(raw_value) -> list[str]:
    """Accept blank values, comma-delimited text, or a string list."""
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [str(item).strip() for item in raw_value if str(item).strip()]
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return []
        return [item.strip() for item in text.split(",") if item.strip()]
    raise ValueError("tags must be blank, comma-delimited text, or a list of strings.")


def _coerce_payload_base64_argument(raw_value) -> bytes:
    """Accept a base64-encoded payload body and return decoded bytes."""
    if not isinstance(raw_value, str):
        raise ValueError("payload_base64 must be a base64 string.")
    text = raw_value.strip()
    try:
        return base64.b64decode(text, validate=True)
    except ValueError as exc:
        raise ValueError("payload_base64 must be valid base64.") from exc


def _coerce_optional_payload_base64_argument(raw_value) -> bytes | None:
    """Accept an optional base64-encoded payload body and return decoded bytes."""
    if raw_value is None:
        return None
    return _coerce_payload_base64_argument(raw_value)


if __name__ == "__main__":
    main()
