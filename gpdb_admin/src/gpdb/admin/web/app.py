"""FastAPI app for the server-rendered admin web UI."""

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates

from gpdb.admin.config import ConfigStore, ResolvedConfig
from gpdb.admin.runtime import AdminServices

from .routes.graph_edges import router as graph_edges_router
from .routes.graph_overview import router as graph_overview_router
from .routes.graph_nodes import router as graph_nodes_router
from .routes.graph_schemas import router as graph_schemas_router
from .routes.graph_viewer import router as graph_viewer_router
from .routes.pages import router as pages_router


WEB_ROOT = Path(__file__).resolve().parent
STATIC_DIR = WEB_ROOT / "static"


def create_web_app(
    resolved_config: ResolvedConfig,
    config_store: ConfigStore,
    services: AdminServices,
    *,
    http_root: str = "",  # For template context, not path prefixing
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
    app.state.http_root = http_root  # Store for potential route use

    # Compute API docs URL based on mount point
    api_docs_url = f"{http_root}/api/docs" if http_root else "/api/docs"

    def _inject_web_app(request):
        return {
            "web_app": app,
            "resolved_config": resolved_config,
            "mount_prefix": http_root,
            "api_docs_url": api_docs_url,
        }

    templates.context_processors.append(_inject_web_app)

    # Include routers (routes are relative, MountableApp handles prefix)
    app.include_router(pages_router)
    app.include_router(graph_overview_router)
    app.include_router(graph_schemas_router)
    app.include_router(graph_nodes_router)
    app.include_router(graph_edges_router)
    app.include_router(graph_viewer_router)

    # Static files: use a route instead of Mount so that when this app is itself
    # mounted (e.g. at /gpdb), scope["path"] is still /static/... and we strip
    # the /static prefix ourselves. Starlette's Mount doesn't set the child's
    # path to the remainder, so StaticFiles would see path=/static/css/... and
    # look for static/static/css/... (404).
    @app.get("/static/{path:path}", name="static")
    def _serve_static(path: str):
        root = STATIC_DIR.resolve()
        full = (STATIC_DIR / path).resolve()
        if not full.is_file():
            raise HTTPException(status_code=404)
        if full != root and not str(full).startswith(str(root) + os.sep):
            raise HTTPException(status_code=404)
        return FileResponse(full)

    return app
