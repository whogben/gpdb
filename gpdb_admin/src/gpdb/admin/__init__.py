"""Admin package for the optional GPDB web/API/CLI surface."""

from .config import AdminConfig, ConfigStore, ResolvedConfig
from .entry import bootstrap_runtime, create_manager, main, status

__all__ = [
    "AdminConfig",
    "ConfigStore",
    "ResolvedConfig",
    "bootstrap_runtime",
    "create_manager",
    "main",
    "status",
]
