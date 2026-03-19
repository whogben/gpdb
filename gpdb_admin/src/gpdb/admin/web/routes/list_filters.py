"""Shared helpers for list-page filter parsing and URL building."""

from __future__ import annotations

from urllib.parse import urlencode

from fastapi import Request


def parse_int_query_param(
    raw_value: str | None,
    *,
    default: int,
    minimum: int,
) -> int:
    """Parse an integer query parameter with default and minimum."""
    if raw_value is None or not raw_value.strip():
        return default
    try:
        parsed = int(raw_value)
    except ValueError:
        return default
    return parsed if parsed >= minimum else default


def node_filter_form_from_request(
    request: Request,
    *,
    default_limit: int = 20,
    default_sort: str = "created_at_desc",
) -> dict[str, str | int]:
    """Build node list filter form state from request query params."""
    sort_raw = (request.query_params.get("sort") or "").strip() or default_sort
    return {
        "type": request.query_params.get("type", "").strip(),
        "parent_id": request.query_params.get("parent_id", "").strip(),
        "filter": request.query_params.get("filter", "").strip(),
        "sort": sort_raw,
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


def edge_filter_form_from_request(
    request: Request,
    *,
    default_limit: int = 20,
    default_sort: str = "created_at_desc",
) -> dict[str, str | int]:
    """Build edge list filter form state from request query params."""
    sort_raw = (request.query_params.get("sort") or "").strip() or default_sort
    return {
        "type": request.query_params.get("type", "").strip(),
        "source_id": request.query_params.get("source_id", "").strip(),
        "target_id": request.query_params.get("target_id", "").strip(),
        "filter": request.query_params.get("filter", "").strip(),
        "sort": sort_raw,
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


def build_node_list_url(
    request: Request,
    *,
    graph_id: str,
    type: str,
    parent_id: str,
    filter: str,
    sort: str,
    limit: int,
    offset: int,
    route_name: str = "graph_node_list_page",
) -> str:
    """Build node list URL preserving filter state and pagination."""
    params: dict[str, object] = {
        "sort": sort,
        "limit": limit,
        "offset": offset,
    }
    if type:
        params["type"] = type
    if parent_id:
        params["parent_id"] = parent_id
    if filter:
        params["filter"] = filter
    return (
        f"{request.app.url_path_for(route_name, graph_id=graph_id)}"
        f"?{urlencode(params)}"
    )


def build_edge_list_url(
    request: Request,
    *,
    graph_id: str,
    type: str,
    source_id: str,
    target_id: str,
    filter: str,
    sort: str,
    limit: int,
    offset: int,
    route_name: str = "graph_edge_list_page",
) -> str:
    """Build edge list URL preserving filter state and pagination."""
    params: dict[str, object] = {
        "sort": sort,
        "limit": limit,
        "offset": offset,
    }
    if type:
        params["type"] = type
    if source_id:
        params["source_id"] = source_id
    if target_id:
        params["target_id"] = target_id
    if filter:
        params["filter"] = filter
    return (
        f"{request.app.url_path_for(route_name, graph_id=graph_id)}"
        f"?{urlencode(params)}"
    )
