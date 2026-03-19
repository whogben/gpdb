"""API key management page routes."""

from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

from gpdb.admin.auth import generate_api_key, hash_api_key_secret
from gpdb.admin.web.routes.common import (
    get_admin_store,
    redirect_with_message,
    render,
    require_authenticated_user,
)

router = APIRouter()


@router.get("/apikeys", response_class=HTMLResponse, name="api_keys_page")
async def api_keys_page(request: Request) -> HTMLResponse:
    """Render the current user's API key management page."""
    admin_store = get_admin_store(request)

    current_user = await require_authenticated_user(request)
    if isinstance(current_user, HTMLResponse):
        return current_user

    return render(
        request,
        "pages/api_keys.html",
        page_title="API Keys",
        current_user=current_user,
        api_keys=await admin_store.list_api_keys_for_user(current_user.id),
        revealed_api_key=None,
        selected_api_key_id=None,
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


@router.get(
    "/apikeys/{api_key_id}", response_class=HTMLResponse, name="api_key_detail_page"
)
async def api_key_detail_page(request: Request, api_key_id: str) -> HTMLResponse:
    """Render one API key detail view with the full revealable value."""
    admin_store = get_admin_store(request)

    current_user = await require_authenticated_user(request)
    if isinstance(current_user, HTMLResponse):
        return current_user

    api_key = await admin_store.get_api_key_by_id(api_key_id)
    if api_key is None or api_key.user_id != current_user.id:
        return redirect_with_message(
            request, "api_keys_page", error="API key not found."
        )

    return render(
        request,
        "pages/api_keys.html",
        page_title="API Keys",
        current_user=current_user,
        api_keys=await admin_store.list_api_keys_for_user(current_user.id),
        revealed_api_key=await admin_store.reveal_api_key(api_key_id),
        selected_api_key_id=api_key_id,
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


@router.post("/apikeys", name="api_key_create")
async def api_key_create(
    request: Request,
    label: str = Form(...),
):
    """Create one API key for the current user."""
    admin_store = get_admin_store(request)

    current_user = await require_authenticated_user(request)
    if isinstance(current_user, HTMLResponse):
        return current_user

    label = label.strip()
    if not label:
        return render(
            request,
            "pages/api_keys.html",
            page_title="API Keys",
            current_user=current_user,
            api_keys=await admin_store.list_api_keys_for_user(current_user.id),
            revealed_api_key=None,
            selected_api_key_id=None,
            error_message="Label is required.",
            success_message=None,
        )

    generated = generate_api_key()
    api_key = await admin_store.create_api_key(
        user_id=current_user.id,
        label=label,
        key_id=generated.key_id,
        preview=generated.preview,
        secret_hash=hash_api_key_secret(generated.secret),
        key_value=generated.token,
    )
    return redirect_with_message(
        request,
        "api_key_detail_page",
        api_key_id=api_key.id,
        success="API key created.",
    )


@router.post("/apikeys/{api_key_id}/revoke", name="api_key_revoke")
async def api_key_revoke(request: Request, api_key_id: str):
    """Revoke one API key owned by the current user."""
    admin_store = get_admin_store(request)

    current_user = await require_authenticated_user(request)
    if isinstance(current_user, HTMLResponse):
        return current_user

    api_key = await admin_store.get_api_key_by_id(api_key_id)
    if api_key is None or api_key.user_id != current_user.id:
        return redirect_with_message(
            request, "api_keys_page", error="API key not found."
        )
    await admin_store.revoke_api_key(api_key_id)
    return redirect_with_message(request, "api_keys_page", success="API key revoked.")
