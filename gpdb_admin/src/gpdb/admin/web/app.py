"""FastAPI app for the server-rendered admin web UI."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .routes.pages import router as pages_router


WEB_ROOT = Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=str(WEB_ROOT / "templates"))


def create_web_app() -> FastAPI:
    """Create the admin web application."""
    app = FastAPI(
        title="GPDB Admin Web",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.templates = TEMPLATES

    def _inject_web_app(request):
        return {"web_app": app}

    TEMPLATES.context_processors.append(_inject_web_app)

    app.include_router(pages_router)
    app.mount("/static", StaticFiles(directory=str(WEB_ROOT / "static")), name="static")
    return app
