from __future__ import annotations

import asyncio
import inspect
import json
import logging
from functools import wraps
from typing import Any

logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)

from fastapi import HTTPException, Request
from fastmcp import FastMCP
from fastmcp.server.auth import AccessToken, TokenVerifier
from mcp.server.auth.middleware.auth_context import get_access_token
from toolaccess import (
    CLIServer as ToolaccessCLIServer,
    InvocationContext,
    OpenAPIServer as ToolaccessOpenAPIServer,
    Principal,
    StreamableHTTPMCPServer as ToolaccessStreamableHTTPMCPServer,
    ToolDefinition,
    get_public_signature,
    get_surface_spec,
)
from toolaccess.definition import get_cli_signature
from toolaccess.pipeline import (
    call_user_func,
    decode_args,
    invoke_tool,
    render_result,
    resolve_principal,
    validate_access,
)

from gpdb.admin.graph_content import (
    GraphContentConflictError,
    GraphContentNotFoundError,
    GraphContentNotReadyError,
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
from gpdb.admin.runtime import AdminServices
from gpdb.admin.auth import verify_api_key_secret


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

        # All tools receive params in the request body only (no query params).
        route_sig = public_sig.replace(
            parameters=[*public_sig.parameters.values(), request_param]
        )
        annotations["request"] = Request

        @wraps(tool.func)
        async def route_handler(*args, request: Request, **kwargs):
            ctx = InvocationContext(
                surface="rest",
                principal=None,
                raw_request=request,
            )
            raw_args = kwargs
            try:
                return await invoke_tool(
                    tool=tool,
                    raw_args=raw_args,
                    ctx=ctx,
                    context_param_name=context_param,
                    surface_resolver=self.principal_resolver,
                )
            except GraphContentNotFoundError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except GraphContentConflictError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except GraphContentNotReadyError as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from exc
            except PermissionError as exc:
                raise HTTPException(status_code=403, detail=str(exc)) from exc
            except (ValueError, KeyError, TypeError) as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except Exception as exc:
                logger.exception("Unexpected REST tool error for %s", tool.name)
                raise HTTPException(status_code=500, detail=str(exc)) from exc

        route_handler.__signature__ = route_sig
        route_handler.__annotations__ = annotations
        router(f"/{tool.name}", name=tool.name, description=tool.description)(
            route_handler
        )


class AuthMCPServer(ToolaccessStreamableHTTPMCPServer):
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
