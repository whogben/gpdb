"""Shared helpers for server-rendered admin routes."""

from __future__ import annotations

from urllib.parse import urlencode

from fastapi import Request, status
from fastapi.responses import HTMLResponse, RedirectResponse

from gpdb.admin.auth import SESSION_COOKIE_NAME


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
            url=request.app.url_path_for("login"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return current_user


async def current_user_from_request(request: Request):
    """Resolve the signed session cookie into the current user, if any."""
    services = request.app.state.services
    assert services.admin_store is not None
    assert services.session_signer is not None

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
    url = request.app.url_path_for(route_name, **route_params)
    params = {}
    if error:
        params["error"] = error
    if success:
        params["success"] = success
    if params:
        url = f"{url}?{urlencode(params)}"
    return RedirectResponse(url=url, status_code=status.HTTP_303_SEE_OTHER)
