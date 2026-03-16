"""Server-rendered graph schema pages."""

from __future__ import annotations

import json

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from gpdb.admin.graph_content import GraphContentError
from gpdb.admin.web.routes.common import (
    get_admin_store,
    redirect_with_message,
    render,
    require_authenticated_user,
    require_graph_content_service,
)


router = APIRouter()

DEFAULT_SCHEMA_JSON = json.dumps(
    {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
        },
        "required": ["name"],
    },
    indent=2,
    sort_keys=True,
)


@router.get(
    "/graphs/{graph_id}/schemas",
    response_class=HTMLResponse,
    name="graph_schema_list_page",
)
async def graph_schema_list_page(request: Request, graph_id: str) -> HTMLResponse:
    """Render the schema registry page for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        schema_list = await require_graph_content_service(request).list_graph_schemas(
            graph_id=graph_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    return render(
        request,
        "pages/graph_schemas.html",
        page_title=f"{schema_list.graph['display_name']} Schemas",
        current_user=current_user,
        schema_list=schema_list.model_dump(mode="json"),
        current_graph=schema_list.graph,
        graphs=await get_admin_store(request).list_graphs(),
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


@router.get(
    "/graphs/{graph_id}/schemas/new",
    response_class=HTMLResponse,
    name="graph_schema_create_page",
)
async def graph_schema_create_page(request: Request, graph_id: str) -> HTMLResponse:
    """Render the create-schema form for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    return await _render_graph_schema_form(
        request,
        graph_id=graph_id,
        current_user=current_user,
        form_data={"name": "", "kind": "node", "json_schema": DEFAULT_SCHEMA_JSON},
    )


@router.post("/graphs/{graph_id}/schemas", name="graph_schema_create")
async def graph_schema_create(
    request: Request,
    graph_id: str,
    name: str = Form(...),
    kind: str = Form("node"),
    json_schema: str = Form(...),
):
    """Create one schema in a managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    form_data = {
        "name": name.strip(),
        "kind": kind.strip().lower(),
        "json_schema": json_schema.strip(),
    }
    try:
        parsed_schema = _parse_schema_json_text(form_data["json_schema"])
    except ValueError as exc:
        return await _render_graph_schema_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            error_message=str(exc),
        )

    try:
        created = await require_graph_content_service(request).create_graph_schema(
            graph_id=graph_id,
            name=form_data["name"],
            kind=form_data["kind"],
            json_schema=parsed_schema,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return await _render_graph_schema_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            error_message=str(exc),
        )

    return redirect_with_message(
        request,
        "graph_schema_detail_page",
        graph_id=graph_id,
        schema_name=created.schema.name,
        success=f"Schema '{created.schema.name}' created.",
    )


@router.get(
    "/graphs/{graph_id}/schemas/{schema_name}/edit",
    response_class=HTMLResponse,
    name="graph_schema_edit_page",
)
async def graph_schema_edit_page(
    request: Request,
    graph_id: str,
    schema_name: str,
) -> HTMLResponse:
    """Render the edit-schema form for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        detail = await require_graph_content_service(request).get_graph_schema(
            graph_id=graph_id,
            name=schema_name,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_schema_list_page",
            graph_id=graph_id,
            error=str(exc),
        )

    return await _render_graph_schema_form(
        request,
        graph_id=graph_id,
        current_user=current_user,
        form_data={
            "name": detail.schema.name,
            "kind": detail.schema.kind,
            "json_schema": json.dumps(
                detail.schema.json_schema,
                indent=2,
                sort_keys=True,
            ),
        },
        schema_name=detail.schema.name,
        schema_detail=detail.model_dump(mode="json", by_alias=True),
    )


@router.post("/graphs/{graph_id}/schemas/{schema_name}", name="graph_schema_update")
async def graph_schema_update(
    request: Request,
    graph_id: str,
    schema_name: str,
    kind: str = Form("node"),
    json_schema: str = Form(...),
):
    """Update one schema in a managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    form_data = {
        "name": schema_name,
        "kind": kind.strip().lower(),
        "json_schema": json_schema.strip(),
    }
    try:
        parsed_schema = _parse_schema_json_text(form_data["json_schema"])
    except ValueError as exc:
        return await _render_graph_schema_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            schema_name=schema_name,
            error_message=str(exc),
        )

    try:
        updated = await require_graph_content_service(request).update_graph_schema(
            graph_id=graph_id,
            name=schema_name,
            kind=form_data["kind"],
            json_schema=parsed_schema,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return await _render_graph_schema_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            schema_name=schema_name,
            error_message=str(exc),
        )

    return redirect_with_message(
        request,
        "graph_schema_detail_page",
        graph_id=graph_id,
        schema_name=updated.schema.name,
        success=f"Schema '{updated.schema.name}' updated to version {updated.schema.version}.",
    )


@router.get(
    "/graphs/{graph_id}/schemas/{schema_name}",
    response_class=HTMLResponse,
    name="graph_schema_detail_page",
)
async def graph_schema_detail_page(
    request: Request,
    graph_id: str,
    schema_name: str,
) -> HTMLResponse:
    """Render one schema detail page."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        detail = await require_graph_content_service(request).get_graph_schema(
            graph_id=graph_id,
            name=schema_name,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_schema_list_page",
            graph_id=graph_id,
            error=str(exc),
        )

    payload = detail.model_dump(mode="json", by_alias=True)
    return render(
        request,
        "pages/schema_detail.html",
        page_title=f"{payload['schema']['name']} Schema",
        current_user=current_user,
        schema_detail=payload,
        current_graph=payload["graph"],
        schema_json=json.dumps(
            payload["schema"]["json_schema"], indent=2, sort_keys=True
        ),
        graphs=await get_admin_store(request).list_graphs(),
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


@router.post(
    "/graphs/{graph_id}/schemas/{schema_name}/delete",
    name="graph_schema_delete",
)
async def graph_schema_delete(
    request: Request,
    graph_id: str,
    schema_name: str,
):
    """Delete one schema when it is no longer referenced."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        deleted = await require_graph_content_service(request).delete_graph_schema(
            graph_id=graph_id,
            name=schema_name,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_schema_detail_page",
            graph_id=graph_id,
            schema_name=schema_name,
            error=str(exc),
        )

    return redirect_with_message(
        request,
        "graph_schema_list_page",
        graph_id=graph_id,
        success=f"Schema '{deleted.schema.name}' deleted.",
    )


async def _render_graph_schema_form(
    request: Request,
    *,
    graph_id: str,
    current_user,
    form_data: dict[str, str],
    schema_name: str | None = None,
    schema_detail: dict[str, object] | None = None,
    error_message: str | None = None,
) -> HTMLResponse | RedirectResponse:
    """Render the create or edit schema form with live graph context."""
    try:
        if schema_name is None:
            overview = await require_graph_content_service(request).get_graph_overview(
                graph_id=graph_id,
                current_user=current_user,
            )
            overview_payload = overview.model_dump(mode="json")
        else:
            detail = await require_graph_content_service(request).get_graph_schema(
                graph_id=graph_id,
                name=schema_name,
                current_user=current_user,
            )
            overview_payload = {
                "graph": detail.graph,
                "instance": detail.instance,
            }
            schema_detail = detail.model_dump(mode="json", by_alias=True)
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    is_edit = schema_name is not None
    return render(
        request,
        "pages/schema_form.html",
        page_title="Edit Schema" if is_edit else "Create Schema",
        current_user=current_user,
        overview=overview_payload,
        current_graph=overview_payload["graph"],
        schema_detail=schema_detail,
        form_data=form_data,
        is_edit=is_edit,
        submit_url=(
            request.app.url_path_for(
                "graph_schema_update",
                graph_id=graph_id,
                schema_name=schema_name,
            )
            if is_edit
            else request.app.url_path_for("graph_schema_create", graph_id=graph_id)
        ),
        graphs=await get_admin_store(request).list_graphs(),
        error_message=error_message,
    )


def _parse_schema_json_text(json_text: str) -> dict[str, object]:
    text = json_text.strip()
    if not text:
        raise ValueError("Schema JSON is required.")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Schema JSON must be valid JSON: {exc.msg}.") from exc
    if not isinstance(parsed, dict):
        raise ValueError("Schema JSON must be a JSON object.")
    return parsed
