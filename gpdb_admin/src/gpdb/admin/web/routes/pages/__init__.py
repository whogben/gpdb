"""Server-rendered page routes for the admin UI."""

from __future__ import annotations

from fastapi import APIRouter

from gpdb.admin.web.routes.pages.api_keys import router as api_keys_router
from gpdb.admin.web.routes.pages.auth import router as auth_router
from gpdb.admin.web.routes.pages.graphs import router as graphs_router
from gpdb.admin.web.routes.pages.instances import router as instances_router

router = APIRouter()
router.include_router(auth_router, tags=["auth"])
router.include_router(api_keys_router, tags=["api_keys"])
router.include_router(instances_router, tags=["instances"])
router.include_router(graphs_router, tags=["graphs"])

__all__ = ["router"]
