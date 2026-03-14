from __future__ import annotations

from typing import TYPE_CHECKING

from mcp.server.auth.middleware.auth_context import get_access_token
from toolaccess import (
    InvocationContext,
    Principal,
    SurfaceSpec,
    ToolService,
    inject_context,
)
from toolaccess.pipeline import invoke_tool

from gpdb.admin.graph_content import GraphContentNotFoundError

if TYPE_CHECKING:
    from gpdb.admin.runtime import AdminServices


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


def _build_mcp_principal_resolver(services: "AdminServices"):
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


def _require_graph_content(services: AdminServices):
    """Return the shared graph-content service once startup has completed."""
    graph_content = services.graph_content
    if graph_content is None:
        raise RuntimeError("Graph content service is not ready yet.")
    return graph_content
