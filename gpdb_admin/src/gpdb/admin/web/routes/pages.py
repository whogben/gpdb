"""Server-rendered page routes for the admin UI."""

from __future__ import annotations

from fastapi import APIRouter, Form, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse

from gpdb.admin.auth import SESSION_COOKIE_NAME, SessionData, hash_password, verify_password
from gpdb.admin.store import OwnerAlreadyExistsError, UserAlreadyExistsError


router = APIRouter()


@router.get("/", response_class=HTMLResponse, name="home")
async def home(request: Request) -> HTMLResponse:
    """Render setup, login, or the authenticated home page."""
    services = request.app.state.services
    assert services.admin_store is not None

    if not await services.admin_store.owner_exists():
        return _render(
            request,
            "pages/setup.html",
            page_title="Create Initial Owner",
        )

    current_user = await _current_user(request)
    if current_user is None:
        return RedirectResponse(
            url=request.app.url_path_for("login"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    return _render(
        request,
        "pages/home.html",
        page_title="GPDB Admin",
        current_user=current_user,
    )


@router.get("/login", response_class=HTMLResponse, name="login")
async def login_page(request: Request) -> HTMLResponse:
    """Render the username/password login form."""
    services = request.app.state.services
    assert services.admin_store is not None

    if not await services.admin_store.owner_exists():
        return RedirectResponse(
            url=request.app.url_path_for("home"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    current_user = await _current_user(request)
    if current_user is not None:
        return RedirectResponse(
            url=request.app.url_path_for("home"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    return _render(
        request,
        "pages/login.html",
        page_title="Sign In",
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
    services = request.app.state.services
    assert services.admin_store is not None

    if await services.admin_store.owner_exists():
        return RedirectResponse(
            url=request.app.url_path_for("login"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    username = username.strip()
    display_name = display_name.strip()

    if not username or not password:
        return _render(
            request,
            "pages/setup.html",
            page_title="Create Initial Owner",
            error_message="Username and password are required.",
            form_data={"username": username, "display_name": display_name},
        )
    if password != confirm_password:
        return _render(
            request,
            "pages/setup.html",
            page_title="Create Initial Owner",
            error_message="Passwords did not match.",
            form_data={"username": username, "display_name": display_name},
        )

    try:
        await services.admin_store.create_initial_owner(
            username=username,
            password_hash=hash_password(password),
            display_name=display_name or username,
        )
    except (OwnerAlreadyExistsError, UserAlreadyExistsError) as exc:
        return _render(
            request,
            "pages/setup.html",
            page_title="Create Initial Owner",
            error_message=str(exc),
            form_data={"username": username, "display_name": display_name},
        )

    response = RedirectResponse(
        url=request.app.url_path_for("login"),
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
    services = request.app.state.services
    assert services.admin_store is not None
    assert services.session_signer is not None

    if not await services.admin_store.owner_exists():
        return RedirectResponse(
            url=request.app.url_path_for("home"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    user = await services.admin_store.verify_user_credentials(
        username=username.strip(),
        password=password,
        verify_password=verify_password,
    )
    if user is None:
        return _render(
            request,
            "pages/login.html",
            page_title="Sign In",
            error_message="Invalid username or password.",
            form_data={"username": username.strip()},
        )

    response = RedirectResponse(
        url=request.app.url_path_for("home"),
        status_code=status.HTTP_303_SEE_OTHER,
    )
    response.set_cookie(
        SESSION_COOKIE_NAME,
        services.session_signer.dumps(
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
        url=request.app.url_path_for("login"),
        status_code=status.HTTP_303_SEE_OTHER,
    )
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


def _render(request: Request, template_name: str, **context) -> HTMLResponse:
    """Render a template with the shared template environment."""
    return request.app.state.templates.TemplateResponse(
        request=request,
        name=template_name,
        context=context,
    )


async def _current_user(request: Request):
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
