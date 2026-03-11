"""Admin package for the optional GPDB web/API/CLI surface."""

from .entry import create_manager, main, status

__all__ = ["create_manager", "main", "status"]
