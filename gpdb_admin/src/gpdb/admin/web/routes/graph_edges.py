"""Server-rendered graph edge pages."""

from __future__ import annotations

import json

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from gpdb.admin.graph_content import (
    GraphContentError,
    GraphEdgeCreateParam,
    GraphEdgeUpdateParam,
)
from gpdb.admin.web.routes.common import (
    get_admin_store,
    redirect_with_message,
    render,
    require_authenticated_user,
    require_graph_content_service,
)
from gpdb.admin.web.routes.list_filters import (
    build_edge_list_url,
    edge_filter_form_from_request,
)


router = APIRouter()
DEFAULT_EDGE_DATA = json.dumps({}, indent=2, sort_keys=True)
DEFAULT_EDGE_LIMIT = 20
DEFAULT_EDGE_SORT = "created_at_desc"
EDGE_SORT_OPTIONS = (
    ("created_at_desc", "Created newest first"),
    ("created_at_asc", "Created oldest first"),
    ("updated_at_desc", "Updated newest first"),
    ("updated_at_asc", "Updated oldest first"),
    ("type_asc", "Type A-Z"),
    ("type_desc", "Type Z-A"),
)


@router.get(
    "/graphs/{graph_id}/edges", response_class=HTMLResponse, name="graph_edge_list_page"
)
async def graph_edge_list_page(request: Request, graph_id: str) -> HTMLResponse:
    """Render the edge list page for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    filter_form = edge_filter_form_from_request(
        request,
        default_limit=DEFAULT_EDGE_LIMIT,
        default_sort=DEFAULT_EDGE_SORT,
    )

    try:
        graph_content = require_graph_content_service(request)
        edge_list = await graph_content.list_graph_edges(
            graph_id=graph_id,
            current_user=current_user,
            type=filter_form["type"],
            source_id=filter_form["source_id"],
            target_id=filter_form["target_id"],
            filter_dsl=filter_form["filter"] or None,
            limit=filter_form["limit"],
            offset=filter_form["offset"],
            sort=filter_form["sort"],
        )
        overview = await graph_content.get_graph_overview(
            graph_id=graph_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    payload = edge_list.model_dump(mode="json")
    overview_payload = overview.model_dump(mode="json")
    previous_url = None
    if payload["offset"] > 0:
        previous_url = build_edge_list_url(
            request,
            graph_id=graph_id,
            type=filter_form["type"],
            source_id=filter_form["source_id"],
            target_id=filter_form["target_id"],
            filter=filter_form["filter"],
            sort=filter_form["sort"],
            limit=filter_form["limit"],
            offset=max(0, payload["offset"] - payload["limit"]),
        )
    next_url = None
    if payload["offset"] + payload["limit"] < payload["total"]:
        next_url = build_edge_list_url(
            request,
            graph_id=graph_id,
            type=filter_form["type"],
            source_id=filter_form["source_id"],
            target_id=filter_form["target_id"],
            filter=filter_form["filter"],
            sort=filter_form["sort"],
            limit=filter_form["limit"],
            offset=payload["offset"] + payload["limit"],
        )

    return render(
        request,
        "pages/graph_edges.html",
        page_title=f"{overview_payload['graph']['display_name']} Edges",
        current_user=current_user,
        edge_list=payload,
        current_graph=overview_payload["graph"],
        filter_form=filter_form,
        sort_options=EDGE_SORT_OPTIONS,
        previous_url=previous_url,
        next_url=next_url,
        clear_filters_url=request.app.url_path_for(
            "graph_edge_list_page", graph_id=graph_id
        ),
        graphs=await get_admin_store(request).list_graphs(),
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


@router.get(
    "/graphs/{graph_id}/edges/new",
    response_class=HTMLResponse,
    name="graph_edge_create_page",
)
async def graph_edge_create_page(request: Request, graph_id: str) -> HTMLResponse:
    """Render the create-edge form for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    return await _render_graph_edge_form(
        request,
        graph_id=graph_id,
        current_user=current_user,
        form_data={
            "type": "__default__",
            "source_id": "",
            "target_id": "",
            "tags": "",
            "data": DEFAULT_EDGE_DATA,
        },
    )


@router.post("/graphs/{graph_id}/edges", name="graph_edges_create")
async def graph_edges_create(
    request: Request,
    graph_id: str,
    type: str = Form(...),
    source_id: str = Form(...),
    target_id: str = Form(...),
    tags: str = Form(""),
    data: str = Form(...),
):
    """Create one edge in a managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    form_data = {
        "type": type.strip() or "__default__",
        "source_id": source_id.strip(),
        "target_id": target_id.strip(),
        "tags": tags.strip(),
        "data": data.strip(),
    }
    try:
        parsed_data = _parse_edge_data_text(form_data["data"])
    except ValueError as exc:
        return await _render_graph_edge_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            error_message=str(exc),
        )

    try:
        created_list = await require_graph_content_service(request).create_graph_edges(
            graph_id=graph_id,
            edges=[
                GraphEdgeCreateParam(
                    type=form_data["type"],
                    source_id=form_data["source_id"],
                    target_id=form_data["target_id"],
                    tags=_parse_tags_text(form_data["tags"]),
                    data=parsed_data,
                )
            ],
            current_user=current_user,
        )
        created = created_list[0]
    except GraphContentError as exc:
        return await _render_graph_edge_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            error_message=str(exc),
        )

    return redirect_with_message(
        request,
        "graph_edge_detail_page",
        graph_id=graph_id,
        edge_id=created.edge.id,
        success=f"Edge '{created.edge.id}' created.",
    )


@router.get(
    "/graphs/{graph_id}/edges/{edge_id}/edit",
    response_class=HTMLResponse,
    name="graph_edge_edit_page",
)
async def graph_edge_edit_page(
    request: Request,
    graph_id: str,
    edge_id: str,
) -> HTMLResponse:
    """Render the edit-edge form for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        graph_content = require_graph_content_service(request)
        detail_list = await graph_content.get_graph_edges(
            graph_id=graph_id,
            edge_ids=[edge_id],
            current_user=current_user,
        )
        detail = detail_list[0]
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_edge_list_page",
            graph_id=graph_id,
            error=str(exc),
        )

    return await _render_graph_edge_form(
        request,
        graph_id=graph_id,
        current_user=current_user,
        form_data={
            "type": detail.edge.type,
            "source_id": detail.edge.source_id,
            "target_id": detail.edge.target_id,
            "tags": ", ".join(detail.edge.tags),
            "data": json.dumps(detail.edge.data, indent=2, sort_keys=True),
        },
        edge_id=detail.edge.id,
        edge_detail=detail.model_dump(mode="json", by_alias=True),
    )


@router.post("/graphs/{graph_id}/edges/{edge_id}", name="graph_edges_update")
async def graph_edges_update(
    request: Request,
    graph_id: str,
    edge_id: str,
    type: str = Form(...),
    source_id: str = Form(...),
    target_id: str = Form(...),
    tags: str = Form(""),
    data: str = Form(...),
):
    """Update one edge in a managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    form_data = {
        "type": type.strip() or "__default__",
        "source_id": source_id.strip(),
        "target_id": target_id.strip(),
        "tags": tags.strip(),
        "data": data.strip(),
    }
    try:
        parsed_data = _parse_edge_data_text(form_data["data"])
    except ValueError as exc:
        return await _render_graph_edge_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            edge_id=edge_id,
            error_message=str(exc),
        )

    try:
        updated_list = await require_graph_content_service(request).update_graph_edges(
            graph_id=graph_id,
            updates=[
                GraphEdgeUpdateParam(
                    edge_id=edge_id,
                    type=form_data["type"],
                    source_id=form_data["source_id"],
                    target_id=form_data["target_id"],
                    tags=_parse_tags_text(form_data["tags"]),
                    data=parsed_data,
                )
            ],
            current_user=current_user,
        )
        updated = updated_list[0]
    except GraphContentError as exc:
        return await _render_graph_edge_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            edge_id=edge_id,
            error_message=str(exc),
        )

    return redirect_with_message(
        request,
        "graph_edge_detail_page",
        graph_id=graph_id,
        edge_id=updated.edge.id,
        success=f"Edge '{updated.edge.id}' updated.",
    )


@router.get(
    "/graphs/{graph_id}/edges/{edge_id}",
    response_class=HTMLResponse,
    name="graph_edge_detail_page",
)
async def graph_edge_detail_page(
    request: Request,
    graph_id: str,
    edge_id: str,
) -> HTMLResponse:
    """Render one edge detail page."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        graph_content = require_graph_content_service(request)
        detail_list = await graph_content.get_graph_edges(
            graph_id=graph_id,
            edge_ids=[edge_id],
            current_user=current_user,
        )
        detail = detail_list[0]
        overview = await graph_content.get_graph_overview(
            graph_id=graph_id,
            current_user=current_user,
        )
        schema_json = None
        if detail.edge.type:
            schema_details = await graph_content.get_graph_schemas(
                graph_id=graph_id,
                names=[detail.edge.type],
                kind="edge",
                current_user=current_user,
            )
            schema_detail = schema_details[0]
            schema_json = schema_detail.schema.json_schema
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_edge_list_page",
            graph_id=graph_id,
            error=str(exc),
        )

    payload = detail.model_dump(mode="json", by_alias=True)
    overview_payload = overview.model_dump(mode="json")
    return render(
        request,
        "pages/edge_detail.html",
        page_title=f"{payload['edge']['type']} Edge",
        current_user=current_user,
        edge_detail=payload,
        current_graph=overview_payload["graph"],
        schema_json=schema_json,
        edge_json=json.dumps(payload["edge"]["data"], indent=2, sort_keys=True),
        graphs=await get_admin_store(request).list_graphs(),
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


@router.post("/graphs/{graph_id}/edges/{edge_id}/delete", name="graph_edges_delete")
async def graph_edges_delete(
    request: Request,
    graph_id: str,
    edge_id: str,
):
    """Delete one edge from a managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        deleted_list = await require_graph_content_service(request).delete_graph_edges(
            graph_id=graph_id,
            edge_ids=[edge_id],
            current_user=current_user,
        )
        deleted = deleted_list[0]
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_edge_detail_page",
            graph_id=graph_id,
            edge_id=edge_id,
            error=str(exc),
        )

    return redirect_with_message(
        request,
        "graph_edge_list_page",
        graph_id=graph_id,
        success=f"Edge '{deleted.id}' deleted.",
    )


async def _render_graph_edge_form(
    request: Request,
    *,
    graph_id: str,
    current_user,
    form_data: dict[str, str],
    edge_id: str | None = None,
    edge_detail: dict[str, object] | None = None,
    error_message: str | None = None,
) -> HTMLResponse | RedirectResponse:
    """Render the create or edit edge form with live graph context."""
    try:
        graph_content = require_graph_content_service(request)
        overview = await graph_content.get_graph_overview(
            graph_id=graph_id,
            current_user=current_user,
        )
        overview_payload = overview.model_dump(mode="json")
        if edge_id is not None:
            detail_list = await graph_content.get_graph_edges(
                graph_id=graph_id,
                edge_ids=[edge_id],
                current_user=current_user,
            )
            detail = detail_list[0]
            edge_detail = detail.model_dump(mode="json", by_alias=True)
        schema_list = await graph_content.list_graph_schemas(
            graph_id=graph_id,
            current_user=current_user,
            kind="edge",
            include_json_schema=True,
        )
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    is_edit = edge_id is not None
    return render(
        request,
        "pages/edge_form.html",
        page_title="Edit Edge" if is_edit else "Create Edge",
        current_user=current_user,
        overview=overview_payload,
        current_graph=overview_payload["graph"],
        edge_detail=edge_detail,
        form_data=form_data,
        schema_names=[item.name for item in schema_list.items],
        schema_json_map={
            item.name: item.json_schema
            for item in schema_list.items
            if item.json_schema is not None
        },
        is_edit=is_edit,
        submit_url=(
            request.app.url_path_for(
                "graph_edges_update",
                graph_id=graph_id,
                edge_id=edge_id,
            )
            if is_edit
            else request.app.url_path_for("graph_edges_create", graph_id=graph_id)
        ),
        graphs=await get_admin_store(request).list_graphs(),
        error_message=error_message,
    )


def _parse_edge_data_text(json_text: str) -> dict[str, object]:
    text = json_text.strip()
    if not text:
        raise ValueError("Edge data JSON is required.")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Edge data JSON must be valid JSON: {exc.msg}.") from exc
    if not isinstance(parsed, dict):
        raise ValueError("Edge data JSON must be a JSON object.")
    return parsed


def _parse_tags_text(tags_text: str) -> list[str]:
    return [item.strip() for item in tags_text.split(",") if item.strip()]
