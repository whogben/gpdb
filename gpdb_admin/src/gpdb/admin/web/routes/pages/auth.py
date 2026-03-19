"""Authentication and setup page routes."""

from __future__ import annotations

from fastapi import APIRouter, Form, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from urllib.parse import urlencode

from gpdb.admin.auth import (
    SESSION_COOKIE_NAME,
    SessionData,
    hash_password,
    verify_password,
)
from gpdb.admin.store import (
    OwnerAlreadyExistsError,
    UserAlreadyExistsError,
)
from gpdb.admin.web.routes.common import (
    _prefixed_url,
    current_user_from_request,
    get_admin_store,
    get_session_signer,
    render,
)

router = APIRouter()


@router.get("/", response_class=HTMLResponse, name="home")
async def home(request: Request) -> HTMLResponse:
    """Render setup, login, or the authenticated admin dashboard."""
    admin_store = get_admin_store(request)
    services = request.app.state.services

    if not await admin_store.owner_exists():
        return render(
            request,
            "pages/setup.html",
            page_title="Create Initial Owner",
        )

    current_user = await current_user_from_request(request)
    if current_user is None:
        return RedirectResponse(
            url=_prefixed_url(request, "login"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    if services.instance_monitor is not None:
        await services.instance_monitor.refresh_all()

    return render(
        request,
        "pages/home.html",
        page_title="GPDB Admin",
        current_user=current_user,
        instances=await admin_store.list_instances(),
        graphs=await admin_store.list_graphs(),
        api_keys=await admin_store.list_api_keys_for_user(current_user.id),
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


@router.get("/login", response_class=HTMLResponse, name="login")
async def login_page(request: Request) -> HTMLResponse:
    """Render the username/password login form."""
    admin_store = get_admin_store(request)

    if not await admin_store.owner_exists():
        return RedirectResponse(
            url=_prefixed_url(request, "home"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    current_user = await current_user_from_request(request)
    if current_user is not None:
        return RedirectResponse(
            url=_prefixed_url(request, "home"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    prefill_username = request.query_params.get("username") or ""

    return render(
        request,
        "pages/login.html",
        page_title="Sign In",
        form_data={"username": prefill_username} if prefill_username else None,
    )


@router.post("/setup", response_class=HTMLResponse, name="setup")
async def setup_owner(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
    display_name: str = Form(""),
):
    """Create the initial owner user on a fresh install."""
    admin_store = get_admin_store(request)

    if await admin_store.owner_exists():
        return RedirectResponse(
            url=_prefixed_url(request, "login"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    username = username.strip()
    display_name = display_name.strip()

    if not username or not password:
        return render(
            request,
            "pages/setup.html",
            page_title="Create Initial Owner",
            error_message="Username and password are required.",
            form_data={"username": username, "display_name": display_name},
        )
    if password != confirm_password:
        return render(
            request,
            "pages/setup.html",
            page_title="Create Initial Owner",
            error_message="Passwords did not match.",
            form_data={"username": username, "display_name": display_name},
        )

    try:
        await admin_store.create_initial_owner(
            username=username,
            password_hash=hash_password(password),
            display_name=display_name or username,
        )
    except (OwnerAlreadyExistsError, UserAlreadyExistsError) as exc:
        return render(
            request,
            "pages/setup.html",
            page_title="Create Initial Owner",
            error_message=str(exc),
            form_data={"username": username, "display_name": display_name},
        )

    login_url = _prefixed_url(request, "login")
    login_with_username = f"{login_url}?{urlencode({'username': username})}"
    response = RedirectResponse(
        url=login_with_username,
        status_code=status.HTTP_303_SEE_OTHER,
    )
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


@router.post("/login", response_class=HTMLResponse, name="login_submit")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    """Authenticate a user and issue the signed session cookie."""
    admin_store = get_admin_store(request)
    session_signer = get_session_signer(request)

    if not await admin_store.owner_exists():
        return RedirectResponse(
            url=_prefixed_url(request, "home"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    user = await admin_store.verify_user_credentials(
        username=username.strip(),
        password=password,
        verify_password=verify_password,
    )
    if user is None:
        return render(
            request,
            "pages/login.html",
            page_title="Sign In",
            error_message="Invalid username or password.",
            form_data={"username": username.strip()},
        )

    response = RedirectResponse(
        url=_prefixed_url(request, "home"),
        status_code=status.HTTP_303_SEE_OTHER,
    )
    response.set_cookie(
        SESSION_COOKIE_NAME,
        session_signer.dumps(
            SessionData(user_id=user.id, auth_version=user.auth_version)
        ),
        httponly=True,
        samesite="lax",
    )
    return response


@router.post("/logout", name="logout")
async def logout(request: Request) -> RedirectResponse:
    """Clear the current browser session."""
    response = RedirectResponse(
        url=_prefixed_url(request, "login"),
        status_code=status.HTTP_303_SEE_OTHER,
    )
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response
