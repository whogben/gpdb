"""Server-rendered graph node pages."""

from __future__ import annotations

import json
from urllib.parse import urlencode

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from gpdb.admin.graph_content import GraphContentError
from gpdb.admin.web.routes.common import (
    redirect_with_message,
    render,
    require_authenticated_user,
    require_graph_content_service,
)


router = APIRouter()
DEFAULT_NODE_DATA = json.dumps({}, indent=2, sort_keys=True)
DEFAULT_NODE_LIMIT = 20
DEFAULT_NODE_SORT = "created_at_desc"
NODE_SORT_OPTIONS = (
    ("created_at_desc", "Created newest first"),
    ("created_at_asc", "Created oldest first"),
    ("updated_at_desc", "Updated newest first"),
    ("updated_at_asc", "Updated oldest first"),
    ("name_asc", "Name A-Z"),
    ("name_desc", "Name Z-A"),
)


@router.get("/graphs/{graph_id}/nodes", response_class=HTMLResponse, name="graph_node_list_page")
async def graph_node_list_page(request: Request, graph_id: str) -> HTMLResponse:
    """Render the node list page for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    filter_form = {
        "type": request.query_params.get("type", "").strip(),
        "schema_name": request.query_params.get("schema_name", "").strip(),
        "parent_id": request.query_params.get("parent_id", "").strip(),
        "sort": request.query_params.get("sort", DEFAULT_NODE_SORT).strip()
        or DEFAULT_NODE_SORT,
        "limit": _parse_int_query_param(
            request.query_params.get("limit"),
            default=DEFAULT_NODE_LIMIT,
            minimum=1,
        ),
        "offset": _parse_int_query_param(
            request.query_params.get("offset"),
            default=0,
            minimum=0,
        ),
    }

    try:
        node_list = await require_graph_content_service(request).list_graph_nodes(
            graph_id=graph_id,
            current_user=current_user,
            type=filter_form["type"],
            schema_name=filter_form["schema_name"],
            parent_id=filter_form["parent_id"],
            limit=filter_form["limit"],
            offset=filter_form["offset"],
            sort=filter_form["sort"],
        )
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    payload = node_list.model_dump(mode="json")
    previous_url = None
    if payload["offset"] > 0:
        previous_url = _build_node_list_url(
            request,
            graph_id=graph_id,
            type=filter_form["type"],
            schema_name=filter_form["schema_name"],
            parent_id=filter_form["parent_id"],
            sort=filter_form["sort"],
            limit=filter_form["limit"],
            offset=max(0, payload["offset"] - payload["limit"]),
        )
    next_url = None
    if payload["offset"] + payload["limit"] < payload["total"]:
        next_url = _build_node_list_url(
            request,
            graph_id=graph_id,
            type=filter_form["type"],
            schema_name=filter_form["schema_name"],
            parent_id=filter_form["parent_id"],
            sort=filter_form["sort"],
            limit=filter_form["limit"],
            offset=payload["offset"] + payload["limit"],
        )

    return render(
        request,
        "pages/graph_nodes.html",
        page_title=f"{payload['graph']['display_name']} Nodes",
        current_user=current_user,
        node_list=payload,
        filter_form=filter_form,
        sort_options=NODE_SORT_OPTIONS,
        previous_url=previous_url,
        next_url=next_url,
        clear_filters_url=request.app.url_path_for("graph_node_list_page", graph_id=graph_id),
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


@router.get(
    "/graphs/{graph_id}/nodes/new",
    response_class=HTMLResponse,
    name="graph_node_create_page",
)
async def graph_node_create_page(request: Request, graph_id: str) -> HTMLResponse:
    """Render the create-node form for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    return await _render_graph_node_form(
        request,
        graph_id=graph_id,
        current_user=current_user,
        form_data={
            "type": "",
            "name": "",
            "schema_name": "",
            "owner_id": "",
            "parent_id": "",
            "tags": "",
            "data": DEFAULT_NODE_DATA,
        },
    )


@router.post("/graphs/{graph_id}/nodes", name="graph_node_create")
async def graph_node_create(
    request: Request,
    graph_id: str,
    type: str = Form(...),
    name: str = Form(""),
    schema_name: str = Form(""),
    owner_id: str = Form(""),
    parent_id: str = Form(""),
    tags: str = Form(""),
    data: str = Form(...),
):
    """Create one node in a managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    form_data = {
        "type": type.strip(),
        "name": name.strip(),
        "schema_name": schema_name.strip(),
        "owner_id": owner_id.strip(),
        "parent_id": parent_id.strip(),
        "tags": tags.strip(),
        "data": data.strip(),
    }
    try:
        parsed_data = _parse_node_data_text(form_data["data"])
    except ValueError as exc:
        return await _render_graph_node_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            error_message=str(exc),
        )

    try:
        created = await require_graph_content_service(request).create_graph_node(
            graph_id=graph_id,
            type=form_data["type"],
            name=form_data["name"],
            schema_name=form_data["schema_name"],
            owner_id=form_data["owner_id"],
            parent_id=form_data["parent_id"],
            tags=_parse_tags_text(form_data["tags"]),
            data=parsed_data,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return await _render_graph_node_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            error_message=str(exc),
        )

    return redirect_with_message(
        request,
        "graph_node_detail_page",
        graph_id=graph_id,
        node_id=created.node.id,
        success=f"Node '{created.node.name or created.node.id}' created.",
    )


@router.get(
    "/graphs/{graph_id}/nodes/{node_id}",
    response_class=HTMLResponse,
    name="graph_node_detail_page",
)
async def graph_node_detail_page(
    request: Request,
    graph_id: str,
    node_id: str,
) -> HTMLResponse:
    """Render one node detail page."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        detail = await require_graph_content_service(request).get_graph_node(
            graph_id=graph_id,
            node_id=node_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_node_list_page",
            graph_id=graph_id,
            error=str(exc),
        )

    payload = detail.model_dump(mode="json", by_alias=True)
    return render(
        request,
        "pages/node_detail.html",
        page_title=f"{payload['node']['name'] or payload['node']['id']} Node",
        current_user=current_user,
        node_detail=payload,
        node_json=json.dumps(payload["node"]["data"], indent=2, sort_keys=True),
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


async def _render_graph_node_form(
    request: Request,
    *,
    graph_id: str,
    current_user,
    form_data: dict[str, str],
    error_message: str | None = None,
) -> HTMLResponse | RedirectResponse:
    """Render the create-node form with live graph and schema context."""
    try:
        graph_content = require_graph_content_service(request)
        overview = await graph_content.get_graph_overview(
            graph_id=graph_id,
            current_user=current_user,
        )
        schema_list = await graph_content.list_graph_schemas(
            graph_id=graph_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    return render(
        request,
        "pages/node_form.html",
        page_title="Create Node",
        current_user=current_user,
        overview=overview.model_dump(mode="json"),
        form_data=form_data,
        schema_names=[item.name for item in schema_list.items],
        submit_url=request.app.url_path_for("graph_node_create", graph_id=graph_id),
        error_message=error_message,
    )


def _parse_node_data_text(json_text: str) -> dict[str, object]:
    text = json_text.strip()
    if not text:
        raise ValueError("Node data JSON is required.")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Node data JSON must be valid JSON: {exc.msg}.") from exc
    if not isinstance(parsed, dict):
        raise ValueError("Node data JSON must be a JSON object.")
    return parsed


def _parse_tags_text(tags_text: str) -> list[str]:
    return [item.strip() for item in tags_text.split(",") if item.strip()]


def _parse_int_query_param(
    raw_value: str | None,
    *,
    default: int,
    minimum: int,
) -> int:
    if raw_value is None or not raw_value.strip():
        return default
    try:
        parsed = int(raw_value)
    except ValueError:
        return default
    return parsed if parsed >= minimum else default


def _build_node_list_url(
    request: Request,
    *,
    graph_id: str,
    type: str,
    schema_name: str,
    parent_id: str,
    sort: str,
    limit: int,
    offset: int,
) -> str:
    params: dict[str, object] = {
        "sort": sort,
        "limit": limit,
        "offset": offset,
    }
    if type:
        params["type"] = type
    if schema_name:
        params["schema_name"] = schema_name
    if parent_id:
        params["parent_id"] = parent_id
    return (
        f"{request.app.url_path_for('graph_node_list_page', graph_id=graph_id)}"
        f"?{urlencode(params)}"
    )
