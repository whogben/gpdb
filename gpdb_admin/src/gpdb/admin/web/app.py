"""FastAPI app for the server-rendered admin web UI."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from gpdb.admin.config import ConfigStore, ResolvedConfig
from gpdb.admin.runtime import AdminServices

from .routes.graph_overview import router as graph_overview_router
from .routes.graph_nodes import router as graph_nodes_router
from .routes.graph_schemas import router as graph_schemas_router
from .routes.pages import router as pages_router


WEB_ROOT = Path(__file__).resolve().parent


def create_web_app(
    resolved_config: ResolvedConfig,
    config_store: ConfigStore,
    services: AdminServices,
) -> FastAPI:
    """Create the admin web application."""
    app = FastAPI(
        title="GPDB Admin Web",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    templates = Jinja2Templates(directory=str(WEB_ROOT / "templates"))
    app.state.templates = templates
    app.state.config = resolved_config
    app.state.config_store = config_store
    app.state.services = services

    def _inject_web_app(request):
        return {"web_app": app, "resolved_config": resolved_config}

    templates.context_processors.append(_inject_web_app)

    app.include_router(pages_router)
    app.include_router(graph_overview_router)
    app.include_router(graph_schemas_router)
    app.include_router(graph_nodes_router)
    app.mount("/static", StaticFiles(directory=str(WEB_ROOT / "static")), name="static")
    return app
