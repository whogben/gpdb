"""Admin package for the optional GPDB web/API/CLI surface."""

from .config import AdminConfig, ConfigStore, ResolvedConfig
from .entry import (
    AdminRuntime,
    attach_admin_to_manager,
    bootstrap_runtime,
    create_admin_runtime,
    create_manager,
    main,
    status,
)
from .runtime import AdminServices, create_admin_lifespan

__all__ = [
    "AdminConfig",
    "AdminRuntime",
    "AdminServices",
    "ConfigStore",
    "ResolvedConfig",
    "attach_admin_to_manager",
    "bootstrap_runtime",
    "create_admin_lifespan",
    "create_admin_runtime",
    "create_manager",
    "main",
    "status",
]
