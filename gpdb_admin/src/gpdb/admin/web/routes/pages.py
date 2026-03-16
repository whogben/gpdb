"""Server-rendered page routes for the admin UI."""

from __future__ import annotations

import re

from fastapi import APIRouter, Form, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from urllib.parse import urlencode

from gpdb.admin.auth import (
    SESSION_COOKIE_NAME,
    SessionData,
    generate_api_key,
    hash_api_key_secret,
    hash_password,
    verify_password,
)
from gpdb.admin.store import (
    GraphAlreadyExistsError,
    InstanceAlreadyExistsError,
    OwnerAlreadyExistsError,
    UserAlreadyExistsError,
)
from gpdb.admin.web.routes.common import (
    _prefixed_url,
    current_user_from_request,
    get_admin_store,
    get_instance_monitor,
    get_session_signer,
    redirect_with_message,
    render,
    require_authenticated_user,
    require_owner_user,
)


router = APIRouter()
TABLE_PREFIX_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


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


@router.get("/apikeys", response_class=HTMLResponse, name="api_keys_page")
async def api_keys_page(request: Request) -> HTMLResponse:
    """Render the current user's API key management page."""
    admin_store = get_admin_store(request)

    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
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
    if isinstance(current_user, RedirectResponse):
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
    if isinstance(current_user, RedirectResponse):
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
async def api_key_revoke(request: Request, api_key_id: str) -> RedirectResponse:
    """Revoke one API key owned by the current user."""
    admin_store = get_admin_store(request)

    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    api_key = await admin_store.get_api_key_by_id(api_key_id)
    if api_key is None or api_key.user_id != current_user.id:
        return redirect_with_message(
            request, "api_keys_page", error="API key not found."
        )
    await admin_store.revoke_api_key(api_key_id)
    return redirect_with_message(request, "api_keys_page", success="API key revoked.")


@router.get("/instances/new", response_class=HTMLResponse, name="instance_create_page")
async def instance_create_page(request: Request) -> HTMLResponse:
    """Render the add-instance form."""
    current_user = await require_owner_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    return render(
        request,
        "pages/instance_form.html",
        page_title="Add Instance",
        current_user=current_user,
        mode="create",
        submit_url=request.app.url_path_for("instance_create"),
        instance=None,
        graphs=await admin_store.list_graphs(),
        form_data={"port": "5432"},
    )


@router.post("/instances", name="instance_create")
async def instance_create(
    request: Request,
    slug: str = Form(...),
    display_name: str = Form(...),
    description: str = Form(""),
    host: str = Form(...),
    port: str = Form("5432"),
    database: str = Form(...),
    username: str = Form(...),
    password: str = Form(""),
):
    """Create a new external managed instance."""
    admin_store = get_admin_store(request)
    instance_monitor = get_instance_monitor(request)

    current_user = await require_owner_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    form_data = {
        "slug": slug.strip(),
        "display_name": display_name.strip(),
        "description": description.strip(),
        "host": host.strip(),
        "port": port.strip(),
        "database": database.strip(),
        "username": username.strip(),
        "password": password,
    }
    error_message = _validate_instance_form(form_data)
    if error_message:
        return render(
            request,
            "pages/instance_form.html",
            page_title="Add Instance",
            current_user=current_user,
            mode="create",
            submit_url=request.app.url_path_for("instance_create"),
            instance=None,
            form_data=form_data,
            error_message=error_message,
        )

    try:
        instance = await admin_store.create_instance(
            slug=form_data["slug"],
            display_name=form_data["display_name"],
            description=form_data["description"],
            host=form_data["host"],
            port=int(form_data["port"]) if form_data["port"] else None,
            database=form_data["database"],
            username=form_data["username"],
            password=form_data["password"],
        )
        await instance_monitor.refresh_instance(instance.id)
    except InstanceAlreadyExistsError as exc:
        return render(
            request,
            "pages/instance_form.html",
            page_title="Add Instance",
            current_user=current_user,
            mode="create",
            submit_url=request.app.url_path_for("instance_create"),
            instance=None,
            form_data=form_data,
            error_message=str(exc),
        )
    except Exception as exc:
        return render(
            request,
            "pages/instance_form.html",
            page_title="Add Instance",
            current_user=current_user,
            mode="create",
            submit_url=request.app.url_path_for("instance_create"),
            instance=None,
            form_data=form_data,
            error_message=str(exc),
        )

    return redirect_with_message(
        request,
        "home",
        success="Instance added.",
    )


@router.get(
    "/instances/{instance_id}/edit",
    response_class=HTMLResponse,
    name="instance_edit_page",
)
async def instance_edit_page(request: Request, instance_id: str) -> HTMLResponse:
    """Render the edit-instance form."""
    admin_store = get_admin_store(request)

    current_user = await require_owner_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    instance = await admin_store.get_instance_by_id(instance_id)
    if instance is None:
        return redirect_with_message(request, "home", error="Instance not found.")

    return render(
        request,
        "pages/instance_form.html",
        page_title="Edit Instance",
        current_user=current_user,
        mode="edit",
        submit_url=request.app.url_path_for("instance_edit", instance_id=instance.id),
        instance=instance,
        graphs=await admin_store.list_graphs(),
        form_data={
            "display_name": instance.display_name,
            "description": instance.description,
            "host": instance.host or "",
            "port": str(instance.port or ""),
            "database": instance.database or "",
            "username": instance.username or "",
            "is_active": "true" if instance.is_active else "",
        },
    )


@router.post("/instances/{instance_id}/edit", name="instance_edit")
async def instance_edit(
    request: Request,
    instance_id: str,
    display_name: str = Form(...),
    description: str = Form(""),
    host: str = Form(""),
    port: str = Form(""),
    database: str = Form(""),
    username: str = Form(""),
    password: str = Form(""),
    is_active: str | None = Form(None),
):
    """Update an existing managed instance."""
    admin_store = get_admin_store(request)
    instance_monitor = get_instance_monitor(request)

    current_user = await require_owner_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    instance = await admin_store.get_instance_by_id(instance_id)
    if instance is None:
        return redirect_with_message(request, "home", error="Instance not found.")

    form_data = {
        "display_name": display_name.strip(),
        "description": description.strip(),
        "host": host.strip(),
        "port": port.strip(),
        "database": database.strip(),
        "username": username.strip(),
        "password": password,
        "is_active": "true" if is_active else "",
    }
    error_message = _validate_instance_form(
        {
            **form_data,
            "slug": instance.slug,
        },
        require_connection_fields=instance.mode == "external",
    )
    if error_message:
        return render(
            request,
            "pages/instance_form.html",
            page_title="Edit Instance",
            current_user=current_user,
            mode="edit",
            submit_url=request.app.url_path_for(
                "instance_edit", instance_id=instance.id
            ),
            instance=instance,
            form_data=form_data,
            error_message=error_message,
        )

    updated = await admin_store.update_instance(
        instance_id=instance_id,
        display_name=form_data["display_name"],
        description=form_data["description"],
        is_active=bool(is_active),
        host=form_data["host"] or None,
        port=int(form_data["port"]) if form_data["port"] else None,
        database=form_data["database"] or None,
        username=form_data["username"] or None,
        password=form_data["password"] if form_data["password"] else None,
    )
    if updated is None:
        return redirect_with_message(request, "home", error="Instance not found.")

    await instance_monitor.refresh_instance(updated.id)
    return redirect_with_message(
        request,
        "home",
        success="Instance updated.",
    )


@router.post("/instances/{instance_id}/delete", name="instance_delete")
async def instance_delete(request: Request, instance_id: str) -> RedirectResponse:
    """Delete an external managed instance."""
    admin_store = get_admin_store(request)

    current_user = await require_owner_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        await admin_store.delete_instance(instance_id)
    except ValueError as exc:
        return redirect_with_message(request, "home", error=str(exc))
    return redirect_with_message(request, "home", success="Instance removed.")


@router.get("/graphs/new", response_class=HTMLResponse, name="graph_create_page")
async def graph_create_page(request: Request) -> HTMLResponse:
    """Render the add-graph form."""
    admin_store = get_admin_store(request)

    current_user = await require_owner_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    instances = await admin_store.list_instances()
    return render(
        request,
        "pages/graph_form.html",
        page_title="Add Graph",
        current_user=current_user,
        mode="create",
        submit_url=request.app.url_path_for("graph_create"),
        graph=None,
        instances=instances,
        graphs=await admin_store.list_graphs(),
        form_data={},
    )


@router.post("/graphs", name="graph_create")
async def graph_create(
    request: Request,
    instance_id: str = Form(...),
    table_prefix: str = Form(...),
    display_name: str = Form(""),
):
    """Create a new prefixed graph on one managed instance."""
    admin_store = get_admin_store(request)
    instance_monitor = get_instance_monitor(request)

    current_user = await require_owner_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    instances = await admin_store.list_instances()
    form_data = {
        "instance_id": instance_id,
        "table_prefix": table_prefix.strip(),
        "display_name": display_name.strip(),
    }
    error_message = _validate_graph_form(form_data, instances)
    if error_message:
        return render(
            request,
            "pages/graph_form.html",
            page_title="Add Graph",
            current_user=current_user,
            mode="create",
            submit_url=request.app.url_path_for("graph_create"),
            graph=None,
            instances=instances,
            form_data=form_data,
            error_message=error_message,
        )

    try:
        await instance_monitor.create_graph(
            instance_id=instance_id,
            table_prefix=form_data["table_prefix"],
            display_name=form_data["display_name"] or None,
        )
    except (GraphAlreadyExistsError, ValueError) as exc:
        return render(
            request,
            "pages/graph_form.html",
            page_title="Add Graph",
            current_user=current_user,
            mode="create",
            submit_url=request.app.url_path_for("graph_create"),
            graph=None,
            instances=instances,
            form_data=form_data,
            error_message=str(exc),
        )

    return redirect_with_message(request, "home", success="Graph created.")


@router.get(
    "/graphs/{graph_id}/edit", response_class=HTMLResponse, name="graph_edit_page"
)
async def graph_edit_page(request: Request, graph_id: str) -> HTMLResponse:
    """Render the edit-graph form."""
    admin_store = get_admin_store(request)

    current_user = await require_owner_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    graph = await admin_store.get_graph_by_id(graph_id)
    if graph is None:
        return redirect_with_message(request, "home", error="Graph not found.")

    return render(
        request,
        "pages/graph_form.html",
        page_title="Edit Graph",
        current_user=current_user,
        mode="edit",
        submit_url=request.app.url_path_for("graph_edit", graph_id=graph.id),
        graph=graph,
        instances=await admin_store.list_instances(),
        graphs=await admin_store.list_graphs(),
        form_data={
            "display_name": graph.display_name,
        },
    )


@router.post("/graphs/{graph_id}/edit", name="graph_edit")
async def graph_edit(
    request: Request,
    graph_id: str,
    display_name: str = Form(...),
):
    """Update one graph's display name."""
    admin_store = get_admin_store(request)
    instance_monitor = get_instance_monitor(request)

    current_user = await require_owner_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    graph = await admin_store.get_graph_by_id(graph_id)
    if graph is None:
        return redirect_with_message(request, "home", error="Graph not found.")

    updated = await admin_store.update_graph(
        graph_id=graph_id,
        display_name=display_name.strip() or graph.display_name,
    )
    if updated is None:
        return redirect_with_message(request, "home", error="Graph not found.")

    await instance_monitor.refresh_instance(updated.instance_id)
    return redirect_with_message(request, "home", success="Graph updated.")


@router.post("/graphs/{graph_id}/delete", name="graph_delete")
async def graph_delete(request: Request, graph_id: str) -> RedirectResponse:
    """Delete one prefixed graph."""
    instance_monitor = get_instance_monitor(request)

    current_user = await require_owner_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        await instance_monitor.delete_graph(graph_id)
    except ValueError as exc:
        return redirect_with_message(request, "home", error=str(exc))
    return redirect_with_message(request, "home", success="Graph deleted.")


def _validate_instance_form(
    form_data: dict[str, str],
    *,
    require_connection_fields: bool = True,
) -> str | None:
    """Return a validation error message for the instance form, if any."""
    if not form_data["display_name"]:
        return "Display name is required."
    if not form_data["slug"] or not TABLE_PREFIX_PATTERN.match(form_data["slug"]):
        return "Slug must contain only letters, numbers, underscores, or hyphens."
    if require_connection_fields:
        for field_name in ("host", "database", "username"):
            if not form_data[field_name]:
                return "Host, database, and username are required."
    if form_data["port"]:
        try:
            port = int(form_data["port"])
        except ValueError:
            return "Port must be a number."
        if port <= 0:
            return "Port must be a positive number."
    return None


def _validate_graph_form(
    form_data: dict[str, str],
    instances,
) -> str | None:
    """Return a validation error message for the graph form, if any."""
    if not form_data["instance_id"] or not any(
        item.id == form_data["instance_id"] for item in instances
    ):
        return "Choose an instance."
    if not form_data["table_prefix"]:
        return "Table prefix is required."
    if not TABLE_PREFIX_PATTERN.match(form_data["table_prefix"]):
        return (
            "Table prefix must contain only letters, numbers, underscores, or hyphens."
        )
    return None
