"""Shared helpers for server-rendered admin routes."""

from __future__ import annotations

from urllib.parse import urlencode

from fastapi import HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse

from gpdb.admin.auth import SESSION_COOKIE_NAME, SessionSigner
from gpdb.admin.graph_content import GraphContentNotReadyError
from gpdb.admin.instances import ManagedInstanceMonitor
from gpdb.admin.store import AdminStore


def get_public_url(request: Request) -> str | None:
    """Get the public base URL from config or auto-detect from request headers.

    Priority:
    1. Configured public_url (from config file or GPDB_PUBLIC_URL env var)
    2. Auto-detect from X-Forwarded-* headers (when behind proxy)
    3. Auto-detect from Host header

    Returns None if no URL can be determined.
    """
    # Check configured public_url first
    configured_url = request.app.state.config.server.public_url
    if configured_url:
        return configured_url.rstrip("/")

    # Try to detect from proxy headers
    headers = request.headers
    proto = headers.get("x-forwarded-proto", headers.get("x-scheme", "http"))
    host = headers.get("x-forwarded-host", headers.get("host"))

    if host:
        return f"{proto}://{host}"

    return None


SERVICE_UNAVAILABLE = "Service temporarily unavailable."


def get_admin_store(request: Request) -> AdminStore:
    """Return the admin store; raise 503 if not ready."""
    store = request.app.state.services.admin_store
    if store is None:
        raise HTTPException(status_code=503, detail=SERVICE_UNAVAILABLE)
    return store


def get_session_signer(request: Request) -> SessionSigner:
    """Return the session signer; raise 503 if not ready."""
    signer = request.app.state.services.session_signer
    if signer is None:
        raise HTTPException(status_code=503, detail=SERVICE_UNAVAILABLE)
    return signer


def get_instance_monitor(request: Request) -> ManagedInstanceMonitor:
    """Return the instance monitor; raise 503 if not ready."""
    monitor = request.app.state.services.instance_monitor
    if monitor is None:
        raise HTTPException(status_code=503, detail=SERVICE_UNAVAILABLE)
    return monitor


def _prefixed_url(request: Request, route_name: str, **route_params) -> str:
    """Return URL for the route, with mount prefix (http_root) when the app is mounted."""
    path = request.app.url_path_for(route_name, **route_params)
    root = getattr(request.app.state, "http_root", "") or ""
    if not root:
        return path
    return root.rstrip("/") + path


def render(request: Request, template_name: str, **context) -> HTMLResponse:
    """Render a template with the shared template environment."""
    return request.app.state.templates.TemplateResponse(
        request=request,
        name=template_name,
        context=context,
    )


async def require_owner_user(
    request: Request,
    *,
    error_message: str = "Only the server owner can manage instances and graphs.",
):
    """Return the signed-in owner or redirect away if unavailable."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user
    if not current_user.is_owner:
        return redirect_with_message(
            request,
            "home",
            error=error_message,
        )
    return current_user


async def require_authenticated_user(request: Request):
    """Return the signed-in user or redirect to login."""
    current_user = await current_user_from_request(request)
    if current_user is None:
        return RedirectResponse(
            url=_prefixed_url(request, "login"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return current_user


async def current_user_from_request(request: Request):
    """Resolve the signed session cookie into the current user, if any."""
    services = request.app.state.services
    if services.admin_store is None or services.session_signer is None:
        raise HTTPException(status_code=503, detail=SERVICE_UNAVAILABLE)

    cookie_value = request.cookies.get(SESSION_COOKIE_NAME)
    if not cookie_value:
        return None

    session = services.session_signer.loads(cookie_value)
    if session is None:
        return None

    user = await services.admin_store.get_user_by_id(session.user_id)
    if user is None or not user.is_active:
        return None
    if user.auth_version != session.auth_version:
        return None
    return user


def redirect_with_message(
    request: Request,
    route_name: str,
    *,
    error: str | None = None,
    success: str | None = None,
    **route_params,
) -> RedirectResponse:
    """Redirect to a route and carry a simple status message."""
    url = _prefixed_url(request, route_name, **route_params)
    params = {}
    if error:
        params["error"] = error
    if success:
        params["success"] = success
    if params:
        url = f"{url}?{urlencode(params)}"
    return RedirectResponse(url=url, status_code=status.HTTP_303_SEE_OTHER)


def require_graph_content_service(request: Request):
    """Return the shared graph-content service once startup has completed."""
    graph_content = request.app.state.services.graph_content
    if graph_content is None:
        raise GraphContentNotReadyError("Graph content service is not ready yet.")
    return graph_content
