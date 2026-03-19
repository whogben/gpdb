"""Instance management page routes."""

from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

from gpdb.admin.store import InstanceAlreadyExistsError
from gpdb.admin.web.routes.common import (
    get_admin_store,
    get_instance_monitor,
    redirect_with_message,
    render,
    require_owner_user,
)
from gpdb.admin.web.routes.pages.validators import _validate_instance_form

router = APIRouter()


@router.get("/instances/new", response_class=HTMLResponse, name="instance_create_page")
async def instance_create_page(request: Request) -> HTMLResponse:
    """Render the add-instance form."""
    admin_store = get_admin_store(request)

    current_user = await require_owner_user(request)
    if isinstance(current_user, HTMLResponse):
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
    if isinstance(current_user, HTMLResponse):
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
    if isinstance(current_user, HTMLResponse):
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
    if isinstance(current_user, HTMLResponse):
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
async def instance_delete(request: Request, instance_id: str):
    """Delete an external managed instance."""
    admin_store = get_admin_store(request)

    current_user = await require_owner_user(request)
    if isinstance(current_user, HTMLResponse):
        return current_user

    try:
        await admin_store.delete_instance(instance_id)
    except ValueError as exc:
        return redirect_with_message(request, "home", error=str(exc))
    return redirect_with_message(request, "home", success="Instance removed.")
