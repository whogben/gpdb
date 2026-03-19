"""Graph management page routes."""

from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

from gpdb.admin.store import GraphAlreadyExistsError
from gpdb.admin.web.routes.common import (
    get_admin_store,
    get_instance_monitor,
    redirect_with_message,
    render,
    require_owner_user,
)
from gpdb.admin.web.routes.pages.validators import _validate_graph_form

router = APIRouter()


@router.get("/graphs/new", response_class=HTMLResponse, name="graph_create_page")
async def graph_create_page(request: Request) -> HTMLResponse:
    """Render the add-graph form."""
    admin_store = get_admin_store(request)

    current_user = await require_owner_user(request)
    if isinstance(current_user, HTMLResponse):
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
    if isinstance(current_user, HTMLResponse):
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
    if isinstance(current_user, HTMLResponse):
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
    if isinstance(current_user, HTMLResponse):
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
async def graph_delete(request: Request, graph_id: str):
    """Delete one prefixed graph."""
    instance_monitor = get_instance_monitor(request)

    current_user = await require_owner_user(request)
    if isinstance(current_user, HTMLResponse):
        return current_user

    try:
        await instance_monitor.delete_graph(graph_id)
    except ValueError as exc:
        return redirect_with_message(request, "home", error=str(exc))
    return redirect_with_message(request, "home", success="Graph deleted.")
