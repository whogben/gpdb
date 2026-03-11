"""Server-rendered page routes for the admin UI."""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse


router = APIRouter()


@router.get("/", response_class=HTMLResponse, name="home")
async def home(request: Request) -> HTMLResponse:
    """Render the landing page for the admin UI."""
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="pages/home.html",
        context={"page_title": "GPDB Admin"},
    )
