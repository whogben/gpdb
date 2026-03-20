"""Server-rendered graph schema pages."""

from __future__ import annotations

import json
from urllib.parse import urlencode

from fastapi import APIRouter, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from gpdb.admin.graph_content import (
    GraphContentError,
    GraphSchemaCreateParam,
    GraphSchemaList,
    GraphSchemaUpdateParam,
)
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
async def graph_schema_list_page(
    request: Request,
    graph_id: str,
    kind: str = Query("node", description="List node schemas or edge schemas."),
    q: str = Query("", description="Optional substring filter on schema name."),
) -> HTMLResponse:
    """Render the schema registry page for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    list_kind = kind.strip().lower()
    if list_kind not in ("node", "edge"):
        list_kind = "node"
    name_query = q.strip()

    try:
        overview = await require_graph_content_service(request).get_graph_overview(
            graph_id=graph_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    try:
        schema_list = await require_graph_content_service(request).list_graph_schemas(
            graph_id=graph_id,
            current_user=current_user,
            kind=list_kind,
        )
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    if name_query:
        needle = name_query.lower()
        kept = [it for it in schema_list.items if needle in it.name.lower()]
        schema_list = GraphSchemaList(items=kept, total=len(kept))

    def _listing_qs(k: str) -> str:
        params: dict[str, str] = {"kind": k}
        if name_query:
            params["q"] = name_query
        return urlencode(params)

    overview_payload = overview.model_dump(mode="json")
    return render(
        request,
        "pages/graph_schemas.html",
        page_title=f"{overview_payload['graph']['display_name']} Schemas",
        current_user=current_user,
        schema_list=schema_list.model_dump(mode="json"),
        current_graph=overview_payload["graph"],
        graphs=await get_admin_store(request).list_graphs(),
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
        schema_kind=list_kind,
        schema_q=name_query,
        schema_qs_node=_listing_qs("node"),
        schema_qs_edge=_listing_qs("edge"),
        schema_list_clear_qs=urlencode({"kind": list_kind}),
        filter_summary=(
            f'Name contains "{name_query}".' if name_query else ""
        ),
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


@router.post("/graphs/{graph_id}/schemas", name="graph_schemas_create")
async def graph_schemas_create(
    request: Request,
    graph_id: str,
    name: str = Form(...),
    kind: str = Form("node"),
    json_schema: str = Form(...),
    alias: str = Form(""),
    svg_icon: str = Form(""),
    extends: str = Form(""),
):
    """Create one schema in a managed graph (wraps into a one-item batch)."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    form_data = {
        "name": name.strip(),
        "kind": kind.strip().lower(),
        "json_schema": json_schema.strip(),
        "alias": alias.strip() or None,
        "svg_icon": svg_icon.strip() or None,
        "extends": extends.strip(),
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
        created_list = await require_graph_content_service(request).create_graph_schemas(
            graph_id=graph_id,
            schemas=[
                GraphSchemaCreateParam(
                    name=form_data["name"],
                    kind=form_data["kind"],
                    json_schema=parsed_schema,
                    alias=form_data["alias"],
                    svg_icon=form_data["svg_icon"],
                    extends=_parse_extends_list(form_data["extends"]),
                )
            ],
            current_user=current_user,
        )
        created = created_list[0]
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
        kind=created.schema.kind,
        success=f"Schema '{created.schema.name}' created.",
    )


@router.get(
    "/graphs/{graph_id}/schemas/{schema_name}/{kind}/edit",
    response_class=HTMLResponse,
    name="graph_schema_edit_page",
)
async def graph_schema_edit_page(
    request: Request,
    graph_id: str,
    schema_name: str,
    kind: str,
) -> HTMLResponse:
    """Render the edit-schema form for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    # Prevent editing protected default schemas
    if schema_name == "__default__":
        return redirect_with_message(
            request,
            "graph_schema_detail_page",
            graph_id=graph_id,
            schema_name=schema_name,
            kind=kind,
            error="Cannot edit the protected default schema.",
        )

    try:
        details = await require_graph_content_service(request).get_graph_schemas(
            graph_id=graph_id,
            names=[schema_name],
            kind=kind,
            current_user=current_user,
        )
        detail = details[0]
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
            "alias": detail.schema.alias or "",
            "svg_icon": detail.schema.svg_icon or "",
            "extends": ", ".join(detail.schema.extends) if detail.schema.extends else "",
        },
        schema_name=detail.schema.name,
        schema_kind=detail.schema.kind,
        schema_detail=detail.model_dump(mode="json", by_alias=True),
    )


@router.post("/graphs/{graph_id}/schemas/{schema_name}/{kind}", name="graph_schemas_update")
async def graph_schemas_update(
    request: Request,
    graph_id: str,
    schema_name: str,
    kind: str,
    json_schema: str = Form(...),
    alias: str = Form(""),
    svg_icon: str = Form(""),
    extends: str = Form(""),
):
    """Update one schema in a managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    # Prevent updating protected default schemas
    if schema_name == "__default__":
        return redirect_with_message(
            request,
            "graph_schema_detail_page",
            graph_id=graph_id,
            schema_name=schema_name,
            kind=kind,
            error="Cannot update the protected default schema.",
        )

    form_data = {
        "name": schema_name,
        "kind": kind.strip().lower(),
        "json_schema": json_schema.strip(),
        "alias": alias.strip() or None,
        "svg_icon": svg_icon.strip() or None,
        "extends": extends.strip(),
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
            schema_kind=form_data["kind"],
            error_message=str(exc),
        )

    try:
        updated = await require_graph_content_service(request).update_graph_schemas(
            graph_id=graph_id,
            schemas=[
                GraphSchemaUpdateParam(
                    name=schema_name,
                    kind=form_data["kind"],
                    json_schema=parsed_schema,
                    alias=form_data["alias"],
                    svg_icon=form_data["svg_icon"],
                    extends=_parse_extends_list(form_data["extends"]),
                )
            ],
            current_user=current_user,
        )
        updated = updated[0]  # Unwrap single result
    except GraphContentError as exc:
        return await _render_graph_schema_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            schema_name=schema_name,
            schema_kind=form_data["kind"],
            error_message=str(exc),
        )

    return redirect_with_message(
        request,
        "graph_schema_detail_page",
        graph_id=graph_id,
        schema_name=updated.schema.name,
        kind=updated.schema.kind,
        success=f"Schema '{updated.schema.name}' updated to version {updated.schema.version}.",
    )


@router.get(
    "/graphs/{graph_id}/schemas/{schema_name}/{kind}",
    response_class=HTMLResponse,
    name="graph_schema_detail_page",
)
async def graph_schema_detail_page(
    request: Request,
    graph_id: str,
    schema_name: str,
    kind: str,
) -> HTMLResponse:
    """Render one schema detail page."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        overview = await require_graph_content_service(request).get_graph_overview(
            graph_id=graph_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_schema_list_page",
            graph_id=graph_id,
            error=str(exc),
        )

    try:
        details = await require_graph_content_service(request).get_graph_schemas(
            graph_id=graph_id,
            names=[schema_name],
            kind=kind,
            current_user=current_user,
        )
        detail = details[0]
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_schema_list_page",
            graph_id=graph_id,
            error=str(exc),
        )

    overview_payload = overview.model_dump(mode="json")
    payload = detail.model_dump(mode="json", by_alias=True)
    return render(
        request,
        "pages/schema_detail.html",
        page_title=f"{payload['schema']['name']} Schema",
        current_user=current_user,
        schema_detail=payload,
        current_graph=overview_payload["graph"],
        current_instance=overview_payload["instance"],
        schema_json=json.dumps(
            payload["schema"]["json_schema"], indent=2, sort_keys=True
        ),
        graphs=await get_admin_store(request).list_graphs(),
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


@router.post(
    "/graphs/{graph_id}/schemas/{schema_name}/{kind}/delete",
    name="graph_schemas_delete",
)
async def graph_schemas_delete(
    request: Request,
    graph_id: str,
    schema_name: str,
    kind: str,
):
    """Delete one schema when it is no longer referenced."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    # Prevent deleting protected default schemas
    if schema_name == "__default__":
        return redirect_with_message(
            request,
            "graph_schema_detail_page",
            graph_id=graph_id,
            schema_name=schema_name,
            kind=kind,
            error="Cannot delete the protected default schema.",
        )

    try:
        deleted = await require_graph_content_service(request).delete_graph_schemas(
            graph_id=graph_id,
            names=[schema_name],
            kind=kind,
            current_user=current_user,
        )
        # Unwrap the single result from the batch response
        if len(deleted) != 1:
            raise GraphContentError("Expected exactly one schema to be deleted")
        deleted_schema = deleted[0]
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_schema_detail_page",
            graph_id=graph_id,
            schema_name=schema_name,
            kind=kind,
            error=str(exc),
        )

    return redirect_with_message(
        request,
        "graph_schema_list_page",
        graph_id=graph_id,
        success=f"Schema '{deleted_schema.name}' deleted.",
    )


async def _render_graph_schema_form(
    request: Request,
    *,
    graph_id: str,
    current_user,
    form_data: dict[str, str],
    schema_name: str | None = None,
    schema_kind: str | None = None,
    schema_detail: dict[str, object] | None = None,
    error_message: str | None = None,
) -> HTMLResponse | RedirectResponse:
    """Render the create or edit schema form with live graph context."""
    try:
        overview = await require_graph_content_service(request).get_graph_overview(
            graph_id=graph_id,
            current_user=current_user,
        )
        overview_payload = overview.model_dump(mode="json")
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    if schema_name is not None and schema_kind is not None:
        try:
            details = await require_graph_content_service(request).get_graph_schemas(
                graph_id=graph_id,
                names=[schema_name],
                kind=schema_kind,
                current_user=current_user,
            )
            detail = details[0]
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
                "graph_schemas_update",
                graph_id=graph_id,
                schema_name=schema_name,
                kind=schema_kind,
            )
            if is_edit
            else request.app.url_path_for("graph_schemas_create", graph_id=graph_id)
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


def _parse_extends_list(extends_text: str) -> list[str]:
    """Parse comma-separated parent schema names from the web form.

    Empty or whitespace-only input yields ``[]`` (no parents on create; clear
    parents on update). This matches full-form POST semantics: the field is
    always explicit, unlike JSON APIs where ``null`` means leave unchanged.
    """
    return [item.strip() for item in extends_text.split(",") if item.strip()]
