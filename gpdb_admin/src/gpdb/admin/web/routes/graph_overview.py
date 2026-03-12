"""Server-rendered graph overview pages."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from gpdb.admin.graph_content import GraphContentError
from gpdb.admin.web.routes.common import (
    redirect_with_message,
    render,
    require_authenticated_user,
)


router = APIRouter()


@router.get("/graphs/{graph_id}", response_class=HTMLResponse, name="graph_overview_page")
async def graph_overview_page(request: Request, graph_id: str) -> HTMLResponse:
    """Render the first graph-content overview page for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    services = request.app.state.services
    graph_content = services.graph_content
    if graph_content is None:
        return redirect_with_message(
            request,
            "home",
            error="Graph content service is not ready yet.",
        )

    try:
        overview = await graph_content.get_graph_overview(
            graph_id=graph_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "home",
            error=str(exc),
        )

    payload = overview.model_dump(mode="json")
    return render(
        request,
        "pages/graph_overview.html",
        page_title=payload["graph"]["display_name"],
        current_user=current_user,
        overview=payload,
    )
