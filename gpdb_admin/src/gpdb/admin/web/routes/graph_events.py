"""Server-rendered graph events pages."""

from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from gpdb.admin.graph_content import (
    GraphChangeEventFilter,
    GraphContentError,
)
from gpdb.admin.web.routes.common import (
    get_admin_store,
    redirect_with_message,
    render,
    require_authenticated_user,
    require_graph_content_service,
)
from gpdb.admin.web.routes.list_filters import parse_int_query_param


router = APIRouter()
DEFAULT_EVENT_LIMIT = 50


def event_filter_form_from_request(
    request: Request,
    *,
    default_limit: int = DEFAULT_EVENT_LIMIT,
) -> dict[str, str | int | bool | list[str]]:
    """Build events list filter form state from request query params."""
    def _split_comma_separated(values: list[str]) -> list[str]:
        """Split comma-separated values from form input."""
        return [t.strip() for t in ",".join(values).split(",") if t.strip()]
    
    return {
        "since_time": request.query_params.get("since_time", "").strip(),
        "node_created": request.query_params.get("node_created") in ("true", "on"),
        "node_updated": request.query_params.get("node_updated") in ("true", "on"),
        "node_deleted": request.query_params.get("node_deleted") in ("true", "on"),
        "node_origin_edge_created": request.query_params.get("node_origin_edge_created") in ("true", "on"),
        "node_origin_edge_updated": request.query_params.get("node_origin_edge_updated") in ("true", "on"),
        "node_origin_edge_deleted": request.query_params.get("node_origin_edge_deleted") in ("true", "on"),
        "node_destination_edge_created": request.query_params.get("node_destination_edge_created") in ("true", "on"),
        "node_destination_edge_updated": request.query_params.get("node_destination_edge_updated") in ("true", "on"),
        "node_destination_edge_deleted": request.query_params.get("node_destination_edge_deleted") in ("true", "on"),
        "node_types": _split_comma_separated(request.query_params.getlist("node_types")),
        "edge_types": _split_comma_separated(request.query_params.getlist("edge_types")),
        "origin_types": _split_comma_separated(request.query_params.getlist("origin_types")),
        "destination_types": _split_comma_separated(request.query_params.getlist("destination_types")),
        "limit": parse_int_query_param(
            request.query_params.get("limit"),
            default=default_limit,
            minimum=1,
        ),
        "offset": parse_int_query_param(
            request.query_params.get("offset"),
            default=0,
            minimum=0,
        ),
    }


def build_event_filter_from_form(filter_form: dict) -> GraphChangeEventFilter:
    """Build GraphChangeEventFilter from form state."""
    return GraphChangeEventFilter(
        node_created=filter_form["node_created"],
        node_updated=filter_form["node_updated"],
        node_deleted=filter_form["node_deleted"],
        node_origin_edge_created=filter_form["node_origin_edge_created"],
        node_origin_edge_updated=filter_form["node_origin_edge_updated"],
        node_origin_edge_deleted=filter_form["node_origin_edge_deleted"],
        node_destination_edge_created=filter_form["node_destination_edge_created"],
        node_destination_edge_updated=filter_form["node_destination_edge_updated"],
        node_destination_edge_deleted=filter_form["node_destination_edge_deleted"],
        node_types=filter_form["node_types"] or None,
        edge_types=filter_form["edge_types"] or None,
        origin_types=filter_form["origin_types"] or None,
        destination_types=filter_form["destination_types"] or None,
    )


def build_events_list_url(
    request: Request,
    *,
    graph_id: str,
    since_time: str,
    event_filter: GraphChangeEventFilter | None,
    limit: int,
    offset: int,
    route_name: str = "graph_events_page",
) -> str:
    """Build events list URL preserving filter state and pagination."""
    params: dict[str, object] = {
        "limit": limit,
        "offset": offset,
    }
    if since_time:
        params["since_time"] = since_time
    if event_filter:
        if event_filter.node_created is not None:
            params["node_created"] = "true" if event_filter.node_created else "false"
        if event_filter.node_updated is not None:
            params["node_updated"] = "true" if event_filter.node_updated else "false"
        if event_filter.node_deleted is not None:
            params["node_deleted"] = "true" if event_filter.node_deleted else "false"
        if event_filter.node_origin_edge_created is not None:
            params["node_origin_edge_created"] = "true" if event_filter.node_origin_edge_created else "false"
        if event_filter.node_origin_edge_updated is not None:
            params["node_origin_edge_updated"] = "true" if event_filter.node_origin_edge_updated else "false"
        if event_filter.node_origin_edge_deleted is not None:
            params["node_origin_edge_deleted"] = "true" if event_filter.node_origin_edge_deleted else "false"
        if event_filter.node_destination_edge_created is not None:
            params["node_destination_edge_created"] = "true" if event_filter.node_destination_edge_created else "false"
        if event_filter.node_destination_edge_updated is not None:
            params["node_destination_edge_updated"] = "true" if event_filter.node_destination_edge_updated else "false"
        if event_filter.node_destination_edge_deleted is not None:
            params["node_destination_edge_deleted"] = "true" if event_filter.node_destination_edge_deleted else "false"
        if event_filter.node_types:
            params["node_types"] = event_filter.node_types
        if event_filter.edge_types:
            params["edge_types"] = event_filter.edge_types
        if event_filter.origin_types:
            params["origin_types"] = event_filter.origin_types
        if event_filter.destination_types:
            params["destination_types"] = event_filter.destination_types
    return (
        f"{request.app.url_path_for(route_name, graph_id=graph_id)}"
        f"?{urlencode(params, doseq=True)}"
    )


@router.get(
    "/graphs/{graph_id}/events", response_class=HTMLResponse, name="graph_events_page"
)
async def graph_events_page(request: Request, graph_id: str) -> HTMLResponse:
    """Render the events list page for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    filter_form = event_filter_form_from_request(request)

    # Parse since_time - default to all-time (epoch) if not provided
    since_time_str = filter_form["since_time"]
    if since_time_str:
        try:
            since_time = datetime.fromisoformat(since_time_str)
        except ValueError:
            since_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
    else:
        since_time = datetime(1970, 1, 1, tzinfo=timezone.utc)

    event_filter = build_event_filter_from_form(filter_form)

    try:
        graph_content = require_graph_content_service(request)
        events_list = await graph_content.list_graph_change_events(
            graph_id=graph_id,
            since_time=since_time,
            event_filter=event_filter,
            limit=filter_form["limit"],
            offset=filter_form["offset"],
            current_user=current_user,
        )
        overview = await graph_content.get_graph_overview(
            graph_id=graph_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    payload = events_list.model_dump(mode="json")
    current_graph = overview.model_dump(mode="json")["graph"]

    previous_url = None
    if payload["offset"] > 0:
        previous_url = build_events_list_url(
            request,
            graph_id=graph_id,
            since_time=since_time.isoformat(),
            event_filter=event_filter,
            limit=filter_form["limit"],
            offset=max(0, payload["offset"] - payload["limit"]),
        )
    next_url = None
    if payload["offset"] + payload["limit"] < payload["total"]:
        next_url = build_events_list_url(
            request,
            graph_id=graph_id,
            since_time=since_time.isoformat(),
            event_filter=event_filter,
            limit=filter_form["limit"],
            offset=payload["offset"] + payload["limit"],
        )

    clear_filters_url = request.app.url_path_for("graph_events_page", graph_id=graph_id)

    return render(
        request,
        "pages/graph_events.html",
        page_title=f"Events - {current_graph['display_name']}",
        current_graph=current_graph,
        current_user=current_user,
        events_list=payload,
        filter_form=filter_form,
        previous_url=previous_url,
        next_url=next_url,
        clear_filters_url=clear_filters_url,
        graphs=await get_admin_store(request).list_graphs(),
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )
