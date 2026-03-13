"""Entry point for the `gpdb` console command."""

from __future__ import annotations

import argparse
import asyncio
import base64
import inspect
import json
import logging
import secrets
import sys
from dataclasses import asdict
from typing import Any, Union, get_args, get_origin

import uvicorn
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from fastmcp import FastMCP
from fastmcp.server.auth import AccessToken, TokenVerifier
from mcp.server.auth.middleware.auth_context import get_access_token
from toolaccess import (
    AccessPolicy,
    CLIServer as ToolaccessCLIServer,
    InvocationContext,
    MountableApp,
    OpenAPIServer as ToolaccessOpenAPIServer,
    PydanticJsonRenderer,
    Principal,
    ServerManager,
    StreamableHTTPMCPServer as ToolaccessStreamableHTTPMCPServer,
    SurfaceSpec,
    ToolDefinition,
    ToolService,
    get_public_signature,
    get_surface_spec,
    inject_context,
)
from toolaccess.definition import get_cli_signature
from toolaccess.codecs import ArgumentCodec
from toolaccess.pipeline import (
    call_user_func,
    decode_args,
    invoke_tool,
    render_result,
    resolve_principal,
    validate_access,
)

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

logger = logging.getLogger(__name__)

GRAPH_TOOL_ACCESS = AccessPolicy(
    require_authenticated=True,
    allow_anonymous=False,
)

CLI_JSON_RENDERER = PydanticJsonRenderer(indent=2, sort_keys=True)
CLI_ALIAS_JSON_RENDERER = PydanticJsonRenderer(
    by_alias=True,
    indent=2,
    sort_keys=True,
)


async def _invoke_tool_raw(
    tool: ToolDefinition,
    raw_args: dict[str, Any],
    ctx: InvocationContext,
    *,
    context_param_name: str | None = None,
    surface_resolver=None,
):
    """Run ToolAccess resolution/validation/decoding without rendering."""
    ctx.principal = await resolve_principal(tool, ctx, surface_resolver)
    await validate_access(tool.access, ctx)
    decoded_args = decode_args(tool.codecs, raw_args, ctx)
    return await call_user_func(tool.func, decoded_args, ctx, context_param_name)


class OpenAPIServer(ToolaccessOpenAPIServer):
    """ToolAccess OpenAPI server that hides injected invocation context."""

    def _add_route(self, tool: ToolDefinition):
        surface_spec = get_surface_spec(tool, "rest")
        http_method = surface_spec.http_method or "POST"
        router = {
            "GET": self.app.get,
            "POST": self.app.post,
            "PUT": self.app.put,
            "DELETE": self.app.delete,
            "PATCH": self.app.patch,
        }.get(http_method, self.app.post)

        public_sig, annotations, context_param = get_public_signature(tool.func)
        request_param = inspect.Parameter(
            "request",
            inspect.Parameter.KEYWORD_ONLY,
            default=inspect.Parameter.empty,
            annotation=Request,
        )
        route_sig = public_sig.replace(
            parameters=[*public_sig.parameters.values(), request_param]
        )
        annotations["request"] = Request

        if inspect.iscoroutinefunction(tool.func):

            async def route_handler(*args, request: Request, **kwargs):
                ctx = InvocationContext(
                    surface="rest",
                    principal=None,
                    raw_request=request,
                )
                try:
                    return await invoke_tool(
                        tool=tool,
                        raw_args=kwargs,
                        ctx=ctx,
                        context_param_name=context_param,
                        surface_resolver=self.principal_resolver,
                    )
                except PermissionError as exc:
                    raise HTTPException(status_code=403, detail=str(exc)) from exc
                except (ValueError, KeyError, TypeError) as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
                except Exception as exc:
                    logger.exception("Unexpected REST tool error for %s", tool.name)
                    raise HTTPException(status_code=500, detail=str(exc)) from exc

        else:

            def route_handler(*args, request: Request, **kwargs):
                ctx = InvocationContext(
                    surface="rest",
                    principal=None,
                    raw_request=request,
                )
                try:
                    return asyncio.run(
                        invoke_tool(
                            tool=tool,
                            raw_args=kwargs,
                            ctx=ctx,
                            context_param_name=context_param,
                            surface_resolver=self.principal_resolver,
                        )
                    )
                except PermissionError as exc:
                    raise HTTPException(status_code=403, detail=str(exc)) from exc
                except (ValueError, KeyError, TypeError) as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
                except Exception as exc:
                    logger.exception("Unexpected REST tool error for %s", tool.name)
                    raise HTTPException(status_code=500, detail=str(exc)) from exc

        route_handler.__signature__ = route_sig
        route_handler.__annotations__ = annotations
        route_handler.__doc__ = tool.description
        route_handler.__name__ = tool.name
        router(f"/{tool.name}", name=tool.name, description=tool.description)(route_handler)


class SSEMCPServer(ToolaccessStreamableHTTPMCPServer):
    """ToolAccess-compatible MCP server with optional bearer auth."""

    def __init__(
        self,
        name: str = "default",
        auth_provider: TokenVerifier | None = None,
        principal_resolver=None,
    ):
        super().__init__(name=name, principal_resolver=principal_resolver)
        self.name = name
        self.mcp = FastMCP(name, auth=auth_provider)

    def _wrap_for_mcp(self, tool: ToolDefinition):
        public_sig, annotations, context_param = get_public_signature(tool.func)

        def process_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
            processed: dict[str, Any] = {}
            for key, value in kwargs.items():
                if not isinstance(value, str):
                    processed[key] = value
                    continue
                parameter = public_sig.parameters.get(key)
                should_skip = False
                if parameter is not None:
                    annotation = parameter.annotation
                    if annotation is str:
                        should_skip = True
                    else:
                        origin = get_origin(annotation)
                        if origin is Union:
                            args = get_args(annotation)
                            non_none = [arg for arg in args if arg is not type(None)]
                            if len(non_none) == 1 and non_none[0] is str:
                                should_skip = True
                if should_skip:
                    processed[key] = value
                    continue
                try:
                    processed[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    processed[key] = value
            return processed

        if inspect.iscoroutinefunction(tool.func):

            async def async_wrapper(*args, **kwargs):
                return await invoke_tool(
                    tool=tool,
                    raw_args=process_kwargs(kwargs),
                    ctx=InvocationContext(surface="mcp", principal=None),
                    context_param_name=context_param,
                    surface_resolver=self.principal_resolver,
                )

            async_wrapper.__signature__ = public_sig
            async_wrapper.__annotations__ = annotations
            return async_wrapper

        def sync_wrapper(*args, **kwargs):
            return asyncio.run(
                invoke_tool(
                    tool=tool,
                    raw_args=process_kwargs(kwargs),
                    ctx=InvocationContext(surface="mcp", principal=None),
                    context_param_name=context_param,
                    surface_resolver=self.principal_resolver,
                )
            )

        sync_wrapper.__signature__ = public_sig
        sync_wrapper.__annotations__ = annotations
        return sync_wrapper


class CLIServer(ToolaccessCLIServer):
    """ToolAccess CLI server that hides injected context."""

    def _add_command(self, app, tool: ToolDefinition):
        public_sig, annotations, context_param = get_cli_signature(tool.func)

        async def _run_tool(kwargs: dict[str, Any]) -> tuple[Any, InvocationContext]:
            ctx = InvocationContext(
                surface="cli",
                principal=Principal(
                    kind="local",
                    is_authenticated=True,
                    is_trusted_local=True,
                ),
            )
            result = await _invoke_tool_raw(
                tool=tool,
                raw_args=kwargs,
                ctx=ctx,
                context_param_name=context_param,
                surface_resolver=self.principal_resolver,
            )
            return result, ctx

        def cli_wrapper(**kwargs):
            async def runner():
                return await _run_tool(kwargs)

            async def runner_with_lifespan():
                if self.manager and self.manager.lifespan_ctx:
                    app = self.manager.app
                    services = getattr(app.state, "services", None)
                    if isinstance(services, AdminServices):
                        if getattr(app.state, "admin_lifespan_active", False):
                            return await runner()
                        admin_lifespan = create_admin_lifespan(services)
                        async with admin_lifespan(app):
                            return await runner()
                    else:
                        async with self.manager.lifespan_ctx(app):
                            return await runner()
                return await runner()

            try:
                raw_result, ctx = asyncio.run(runner_with_lifespan())
            except KeyboardInterrupt:
                return None

            if isinstance(raw_result, str):
                print(raw_result)
                return raw_result

            rendered = render_result(
                tool,
                raw_result,
                ctx,
                surface_default_renderer=self.default_renderer,
            )
            if isinstance(rendered, str):
                print(rendered)
                try:
                    return json.loads(rendered)
                except json.JSONDecodeError:
                    return rendered

            print(rendered)
            return rendered

        cli_wrapper.__signature__ = public_sig
        cli_wrapper.__annotations__ = annotations
        app.command(name=tool.name, help=tool.description)(cli_wrapper)


class _JsonObjectArgumentCodec(ArgumentCodec):
    """Use gpdb's strict JSON-object coercion within ToolAccess."""

    def decode(self, value: Any, *, parameter_name: str, ctx: InvocationContext):
        return _coerce_json_object_argument(value, argument_name=parameter_name)


class _TagsArgumentCodec(ArgumentCodec):
    """Use gpdb's existing tags coercion rules within ToolAccess."""

    def decode(self, value: Any, *, parameter_name: str, ctx: InvocationContext):
        return _coerce_tags_argument(value)


class _PayloadBase64ArgumentCodec(ArgumentCodec):
    """Use gpdb's payload validation rules within ToolAccess."""

    def __init__(self, *, optional: bool = False):
        self._optional = optional

    def decode(self, value: Any, *, parameter_name: str, ctx: InvocationContext):
        if self._optional:
            return _coerce_optional_payload_base64_argument(value)
        return _coerce_payload_base64_argument(value)


JSON_OBJECT_CODEC = _JsonObjectArgumentCodec()
TAGS_CODEC = _TagsArgumentCodec()
PAYLOAD_BASE64_CODEC = _PayloadBase64ArgumentCodec()
OPTIONAL_PAYLOAD_BASE64_CODEC = _PayloadBase64ArgumentCodec(optional=True)


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
    graph_service = _build_graph_content_service(services)
    cli_api_key_service = ToolService("admin-cli", _build_cli_api_key_tools(services))
    mcp_api_key_service = ToolService("admin-mcp", _build_mcp_api_key_tools(services))

    rest_api = OpenAPIServer(
        path_prefix="/api",
        title="GPDB Admin API",
        principal_resolver=_build_rest_principal_resolver(),
    )
    rest_api.mount(admin_service)
    rest_api.mount(graph_service)
    _install_api_key_auth(rest_api, services)

    mcp_server = SSEMCPServer(
        "gpdb",
        auth_provider=_AdminAPIKeyTokenVerifier(services),
        principal_resolver=_build_mcp_principal_resolver(services),
    )
    mcp_server.mount(admin_service)
    mcp_server.mount(graph_service)
    mcp_server.mount(mcp_api_key_service)

    cli = CLIServer("gpdb")
    cli.mount(admin_service)
    cli.mount(graph_service)
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


def _build_rest_principal_resolver():
    """Resolve the authenticated REST principal from request state."""

    async def resolve(ctx: InvocationContext) -> Principal | None:
        request = ctx.raw_request
        if request is None:
            return None
        current_user = getattr(request.state, "current_user", None)
        if current_user is None:
            return None
        current_api_key = getattr(request.state, "current_api_key", None)
        ctx.state["current_user"] = current_user
        if current_api_key is not None:
            ctx.state["current_api_key"] = current_api_key
        claims = {"is_owner": current_user.is_owner}
        if current_api_key is not None:
            claims["api_key_id"] = current_api_key.id
            claims["api_key_label"] = current_api_key.label
        return Principal(
            kind="api_key",
            id=current_user.id,
            name=current_user.username,
            claims=claims,
            is_authenticated=True,
        )

    return resolve


def _build_mcp_principal_resolver(services: AdminServices):
    """Resolve the authenticated MCP principal from the FastMCP auth context."""

    async def resolve(ctx: InvocationContext) -> Principal | None:
        admin_store = services.admin_store
        if admin_store is None:
            raise RuntimeError("Admin store is not ready yet.")
        access_token = get_access_token()
        if access_token is None:
            return None
        user_id = str(access_token.claims.get("user_id", "")).strip()
        if not user_id:
            raise RuntimeError("Authenticated MCP token is missing a user id.")
        user = await admin_store.get_user_by_id(user_id)
        if user is None or not user.is_active:
            raise RuntimeError("Authenticated MCP user is no longer active.")
        ctx.state["current_user"] = user
        ctx.state["access_token"] = access_token
        return Principal(
            kind="api_key",
            id=user.id,
            name=user.username,
            claims=dict(access_token.claims),
            is_authenticated=True,
        )

    return resolve


def _ctx_current_user(ctx: InvocationContext):
    """Return the resolved current user from one invocation context."""
    return ctx.state.get("current_user")


def _require_context_user(ctx: InvocationContext):
    """Return the resolved current user or raise a friendly error."""
    current_user = _ctx_current_user(ctx)
    if current_user is None:
        raise RuntimeError("Authenticated user required.")
    return current_user


def _ctx_allow_local_system(ctx: InvocationContext) -> bool:
    """Return whether the current invocation represents trusted local access."""
    principal = ctx.principal
    return bool(principal is not None and principal.is_trusted_local)


async def _call_graph_content_from_context(
    services: AdminServices,
    method_name: str,
    ctx: InvocationContext,
    **kwargs,
):
    """Invoke one graph-content method using the resolved tool context."""
    return await _call_graph_content(
        services,
        method_name,
        current_user=_ctx_current_user(ctx),
        allow_local_system=_ctx_allow_local_system(ctx),
        **kwargs,
    )


def _graph_surface_specs(
    *,
    http_method: str = "POST",
    cli_renderer=CLI_JSON_RENDERER,
) -> dict[str, SurfaceSpec]:
    """Return the standard REST/MCP/CLI surface configuration for graph tools."""
    return {
        "rest": SurfaceSpec(http_method=http_method),
        "mcp": SurfaceSpec(),
        "cli": SurfaceSpec(renderer=cli_renderer),
    }


def _build_graph_content_service(services: AdminServices) -> ToolService:
    """Build graph-content tools once and expose them on all surfaces."""
    service = ToolService("admin-graph")

    @service.tool(
        name="graph_overview",
        surfaces=_graph_surface_specs(http_method="GET"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_overview(
        graph_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphOverview:
        """Return one managed graph overview for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_overview",
            ctx,
            graph_id=graph_id,
        )

    @service.tool(
        name="graph_schema_list",
        surfaces=_graph_surface_specs(http_method="GET"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schema_list(
        graph_id: str,
        kind: str = "",
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaList:
        """List graph schemas for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "list_graph_schemas",
            ctx,
            graph_id=graph_id,
            kind=kind,
        )

    @service.tool(
        name="graph_schema_get",
        surfaces=_graph_surface_specs(
            http_method="GET",
            cli_renderer=CLI_ALIAS_JSON_RENDERER,
        ),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schema_get(
        graph_id: str,
        name: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaDetail:
        """Return one graph schema for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_schema",
            ctx,
            graph_id=graph_id,
            name=name,
        )

    @service.tool(
        name="graph_schema_create",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={"json_schema": JSON_OBJECT_CODEC},
    )
    async def graph_schema_create(
        graph_id: str,
        name: str,
        json_schema: dict[str, object],
        kind: str = "node",
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaDetail:
        """Create one graph schema for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph_schema",
            ctx,
            graph_id=graph_id,
            name=name,
            json_schema=json_schema,
            kind=kind,
        )

    @service.tool(
        name="graph_schema_update",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={"json_schema": JSON_OBJECT_CODEC},
    )
    async def graph_schema_update(
        graph_id: str,
        name: str,
        json_schema: dict[str, object],
        kind: str = "node",
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaDetail:
        """Update one graph schema for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph_schema",
            ctx,
            graph_id=graph_id,
            name=name,
            json_schema=json_schema,
            kind=kind,
        )

    @service.tool(
        name="graph_schema_delete",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_schema_delete(
        graph_id: str,
        name: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphSchemaDetail:
        """Delete one graph schema for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph_schema",
            ctx,
            graph_id=graph_id,
            name=name,
        )

    @service.tool(
        name="graph_node_list",
        surfaces=_graph_surface_specs(http_method="GET"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_list(
        graph_id: str,
        type: str = "",
        schema_name: str = "",
        parent_id: str = "",
        limit: int = 50,
        offset: int = 0,
        sort: str = "created_at_desc",
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeList:
        """List graph nodes for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "list_graph_nodes",
            ctx,
            graph_id=graph_id,
            type=type,
            schema_name=schema_name,
            parent_id=parent_id,
            limit=limit,
            offset=offset,
            sort=sort,
        )

    @service.tool(
        name="graph_node_get",
        surfaces=_graph_surface_specs(
            http_method="GET",
            cli_renderer=CLI_ALIAS_JSON_RENDERER,
        ),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_get(
        graph_id: str,
        node_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Return one graph node for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_node",
            ctx,
            graph_id=graph_id,
            node_id=node_id,
        )

    @service.tool(
        name="graph_node_create",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={
            "data": JSON_OBJECT_CODEC,
            "tags": TAGS_CODEC,
            "payload_base64": OPTIONAL_PAYLOAD_BASE64_CODEC,
        },
    )
    async def graph_node_create(
        graph_id: str,
        type: str,
        data: dict[str, object],
        name: str = "",
        schema_name: str = "",
        owner_id: str = "",
        parent_id: str = "",
        tags: str = "",
        payload_base64: str | None = None,
        payload_mime: str = "",
        payload_filename: str = "",
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Create one graph node for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph_node",
            ctx,
            graph_id=graph_id,
            type=type,
            name=name,
            schema_name=schema_name,
            owner_id=owner_id,
            parent_id=parent_id,
            tags=tags,
            data=data,
            payload=payload_base64,
            payload_mime=payload_mime,
            payload_filename=payload_filename,
        )

    @service.tool(
        name="graph_node_update",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={
            "data": JSON_OBJECT_CODEC,
            "tags": TAGS_CODEC,
            "payload_base64": OPTIONAL_PAYLOAD_BASE64_CODEC,
        },
    )
    async def graph_node_update(
        graph_id: str,
        node_id: str,
        type: str,
        data: dict[str, object],
        name: str = "",
        schema_name: str = "",
        owner_id: str = "",
        parent_id: str = "",
        tags: str = "",
        payload_base64: str | None = None,
        payload_mime: str = "",
        payload_filename: str = "",
        clear_payload: bool = False,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Update one graph node for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph_node",
            ctx,
            graph_id=graph_id,
            node_id=node_id,
            type=type,
            name=name,
            schema_name=schema_name,
            owner_id=owner_id,
            parent_id=parent_id,
            tags=tags,
            data=data,
            payload=payload_base64,
            payload_mime=payload_mime,
            payload_filename=payload_filename,
            clear_payload=clear_payload,
        )

    @service.tool(
        name="graph_node_delete",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_delete(
        graph_id: str,
        node_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Delete one graph node for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph_node",
            ctx,
            graph_id=graph_id,
            node_id=node_id,
        )

    @service.tool(
        name="graph_node_payload_get",
        surfaces=_graph_surface_specs(
            http_method="GET",
            cli_renderer=CLI_ALIAS_JSON_RENDERER,
        ),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_node_payload_get(
        graph_id: str,
        node_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodePayload:
        """Return one graph node payload for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_node_payload",
            ctx,
            graph_id=graph_id,
            node_id=node_id,
        )

    @service.tool(
        name="graph_node_payload_set",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={"payload_base64": PAYLOAD_BASE64_CODEC},
    )
    async def graph_node_payload_set(
        graph_id: str,
        node_id: str,
        payload_base64: str,
        mime: str = "",
        payload_filename: str = "",
        ctx: InvocationContext = inject_context(),
    ) -> GraphNodeDetail:
        """Set one graph node payload for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "set_graph_node_payload",
            ctx,
            graph_id=graph_id,
            node_id=node_id,
            payload=payload_base64,
            mime=mime,
            payload_filename=payload_filename,
        )

    @service.tool(
        name="graph_edge_list",
        surfaces=_graph_surface_specs(http_method="GET"),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edge_list(
        graph_id: str,
        type: str = "",
        schema_name: str = "",
        source_id: str = "",
        target_id: str = "",
        limit: int = 50,
        offset: int = 0,
        sort: str = "created_at_desc",
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeList:
        """List graph edges for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "list_graph_edges",
            ctx,
            graph_id=graph_id,
            type=type,
            schema_name=schema_name,
            source_id=source_id,
            target_id=target_id,
            limit=limit,
            offset=offset,
            sort=sort,
        )

    @service.tool(
        name="graph_edge_get",
        surfaces=_graph_surface_specs(
            http_method="GET",
            cli_renderer=CLI_ALIAS_JSON_RENDERER,
        ),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edge_get(
        graph_id: str,
        edge_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeDetail:
        """Return one graph edge for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "get_graph_edge",
            ctx,
            graph_id=graph_id,
            edge_id=edge_id,
        )

    @service.tool(
        name="graph_edge_create",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={"data": JSON_OBJECT_CODEC, "tags": TAGS_CODEC},
    )
    async def graph_edge_create(
        graph_id: str,
        type: str,
        source_id: str,
        target_id: str,
        data: dict[str, object],
        schema_name: str = "",
        tags: str = "",
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeDetail:
        """Create one graph edge for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "create_graph_edge",
            ctx,
            graph_id=graph_id,
            type=type,
            source_id=source_id,
            target_id=target_id,
            schema_name=schema_name,
            tags=tags,
            data=data,
        )

    @service.tool(
        name="graph_edge_update",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
        codecs={"data": JSON_OBJECT_CODEC, "tags": TAGS_CODEC},
    )
    async def graph_edge_update(
        graph_id: str,
        edge_id: str,
        type: str,
        source_id: str,
        target_id: str,
        data: dict[str, object],
        schema_name: str = "",
        tags: str = "",
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeDetail:
        """Update one graph edge for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "update_graph_edge",
            ctx,
            graph_id=graph_id,
            edge_id=edge_id,
            type=type,
            source_id=source_id,
            target_id=target_id,
            schema_name=schema_name,
            tags=tags,
            data=data,
        )

    @service.tool(
        name="graph_edge_delete",
        surfaces=_graph_surface_specs(cli_renderer=CLI_ALIAS_JSON_RENDERER),
        access=GRAPH_TOOL_ACCESS,
    )
    async def graph_edge_delete(
        graph_id: str,
        edge_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> GraphEdgeDetail:
        """Delete one graph edge for the authenticated caller."""
        return await _call_graph_content_from_context(
            services,
            "delete_graph_edge",
            ctx,
            graph_id=graph_id,
            edge_id=edge_id,
        )

    return service


def _build_cli_api_key_tools(services: AdminServices) -> list[ToolDefinition]:
    """Build trusted local CLI commands for API key management."""

    async def api_key_list(username: str) -> list[dict[str, object]]:
        """List API keys for one local admin user."""
        user = await _require_user_by_username(services, username)
        return [
            _serialize_api_key(item)
            for item in await services.admin_store.list_api_keys_for_user(user.id)
        ]

    async def api_key_create(username: str, label: str) -> dict[str, object]:
        """Create an API key for one local admin user."""
        user = await _require_user_by_username(services, username)
        return await _create_api_key_for_user(services, user_id=user.id, label=label)

    async def api_key_reveal(username: str, key_id: str) -> dict[str, object]:
        """Reveal one API key owned by the named local user."""
        user = await _require_user_by_username(services, username)
        return await _reveal_api_key_for_user(
            services, user_id=user.id, key_id=key_id
        )

    async def api_key_revoke(username: str, key_id: str) -> dict[str, object]:
        """Revoke one API key owned by the named local user."""
        user = await _require_user_by_username(services, username)
        return await _revoke_api_key_for_user(
            services, user_id=user.id, key_id=key_id
        )

    return [
        ToolDefinition(api_key_list, "api_key_list"),
        ToolDefinition(api_key_create, "api_key_create"),
        ToolDefinition(api_key_reveal, "api_key_reveal"),
        ToolDefinition(api_key_revoke, "api_key_revoke"),
    ]


def _build_mcp_api_key_tools(services: AdminServices) -> list[ToolDefinition]:
    """Build authenticated MCP tools for current-user API key management."""

    async def api_key_list_me(
        ctx: InvocationContext = inject_context(),
    ) -> list[dict[str, object]]:
        """List API keys for the authenticated MCP user."""
        user = _require_context_user(ctx)
        return [
            _serialize_api_key(item)
            for item in await services.admin_store.list_api_keys_for_user(user.id)
        ]

    async def api_key_create_me(
        label: str,
        ctx: InvocationContext = inject_context(),
    ) -> dict[str, object]:
        """Create an API key for the authenticated MCP user."""
        user = _require_context_user(ctx)
        return await _create_api_key_for_user(services, user_id=user.id, label=label)

    async def api_key_reveal_me(
        key_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> dict[str, object]:
        """Reveal one API key owned by the authenticated MCP user."""
        user = _require_context_user(ctx)
        return await _reveal_api_key_for_user(services, user_id=user.id, key_id=key_id)

    async def api_key_revoke_me(
        key_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> dict[str, object]:
        """Revoke one API key owned by the authenticated MCP user."""
        user = _require_context_user(ctx)
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
