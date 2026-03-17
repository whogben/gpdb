"""Server-rendered graph viewer page and JSON data endpoint."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from gpdb.admin.graph_content import GraphContentError
from gpdb.admin.web.routes.common import (
    get_admin_store,
    redirect_with_message,
    render,
    require_authenticated_user,
    require_graph_content_service,
)
from gpdb.admin.web.routes.list_filters import (
    parse_int_query_param,
)

router = APIRouter()

DEFAULT_VIEWER_NODE_LIMIT = 200
DEFAULT_VIEWER_EDGE_LIMIT = 200

FILTER_SUMMARY_MAX_LEN = 72


def _viewer_filter_summary(params: dict[str, str | int]) -> str:
    """Build a short one-line summary of active filters for display when panel is closed."""
    parts: list[str] = []
    if params.get("node_type"):
        parts.append(f"node type={params['node_type']}")
    if params.get("node_schema_name"):
        parts.append(f"node schema={params['node_schema_name']}")
    if params.get("node_parent_id"):
        parts.append("node parent=…")
    if params.get("node_filter"):
        parts.append("node DSL")
    if params.get("edge_type"):
        parts.append(f"edge type={params['edge_type']}")
    if params.get("edge_schema_name"):
        parts.append(f"edge schema={params['edge_schema_name']}")
    if params.get("edge_source_id"):
        parts.append("edge source=…")
    if params.get("edge_target_id"):
        parts.append("edge target=…")
    if params.get("edge_filter"):
        parts.append("edge DSL")
    if not parts:
        return ""
    summary = "; ".join(parts)
    if len(summary) > FILTER_SUMMARY_MAX_LEN:
        summary = summary[: FILTER_SUMMARY_MAX_LEN - 1].rstrip() + "…"
    return summary


def _viewer_filter_params_from_request(request: Request) -> dict[str, str | int]:
    """Extract node and edge filter query params for the viewer (prefixed)."""
    return {
        "node_type": request.query_params.get("node_type", "").strip(),
        "node_schema_name": request.query_params.get("node_schema_name", "").strip(),
        "node_parent_id": request.query_params.get("node_parent_id", "").strip(),
        "node_filter": request.query_params.get("node_filter", "").strip(),
        "node_limit": parse_int_query_param(
            request.query_params.get("node_limit"),
            default=DEFAULT_VIEWER_NODE_LIMIT,
            minimum=1,
        ),
        "edge_type": request.query_params.get("edge_type", "").strip(),
        "edge_schema_name": request.query_params.get("edge_schema_name", "").strip(),
        "edge_source_id": request.query_params.get("edge_source_id", "").strip(),
        "edge_target_id": request.query_params.get("edge_target_id", "").strip(),
        "edge_filter": request.query_params.get("edge_filter", "").strip(),
        "edge_limit": parse_int_query_param(
            request.query_params.get("edge_limit"),
            default=DEFAULT_VIEWER_EDGE_LIMIT,
            minimum=1,
        ),
    }


@router.get(
    "/graphs/{graph_id}/viewer", response_class=HTMLResponse, name="graph_viewer_page"
)
async def graph_viewer_page(request: Request, graph_id: str) -> HTMLResponse:
    """Render the graph viewer page for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        overview = await require_graph_content_service(request).get_graph_overview(
            graph_id=graph_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    filter_params = _viewer_filter_params_from_request(request)
    filter_summary = _viewer_filter_summary(filter_params)
    overview_payload = overview.model_dump(mode="json")
    return render(
        request,
        "pages/graph_viewer.html",
        page_title=f"{overview_payload['graph']['display_name']} Viewer",
        current_user=current_user,
        overview=overview_payload,
        current_graph=overview_payload["graph"],
        graph_id=graph_id,
        filter_params=filter_params,
        filter_summary=filter_summary,
        graphs=await get_admin_store(request).list_graphs(),
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


@router.get("/graphs/{graph_id}/viewer/data", name="graph_viewer_data")
async def graph_viewer_data(request: Request, graph_id: str) -> JSONResponse:
    """Return filtered nodes and edges as JSON for the graph viewer (Cytoscape elements)."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return JSONResponse(
            status_code=401,
            content={"error": "Authentication required"},
        )

    params = _viewer_filter_params_from_request(request)
    try:
        graph_content = require_graph_content_service(request)
        data = await graph_content.get_graph_viewer_data(
            graph_id=graph_id,
            current_user=current_user,
            node_type=params["node_type"] or None,
            node_schema_name=params["node_schema_name"] or None,
            node_parent_id=params["node_parent_id"] or None,
            node_filter_dsl=params["node_filter"] or None,
            node_limit=params["node_limit"],
            edge_type=params["edge_type"] or None,
            edge_schema_name=params["edge_schema_name"] or None,
            edge_source_id=params["edge_source_id"] or None,
            edge_target_id=params["edge_target_id"] or None,
            edge_filter_dsl=params["edge_filter"] or None,
            edge_limit=params["edge_limit"],
        )
        overview = await graph_content.get_graph_overview(
            graph_id=graph_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return JSONResponse(
            status_code=400,
            content={
                "error": str(exc),
                "elements": [],
                "node_count": 0,
                "edge_count": 0,
            },
        )

    payload = data.model_dump(mode="json")
    payload["graph"] = overview.model_dump(mode="json")["graph"]
    if data.error:
        return JSONResponse(status_code=400, content=payload)
    return JSONResponse(content=payload)
