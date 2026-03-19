"""Exception classes for graph-content operations."""

from __future__ import annotations


class GraphContentError(RuntimeError):
    """Base class for graph-content errors surfaced by admin code."""


class GraphContentNotReadyError(GraphContentError):
    """Raised when graph-content services are requested before startup completes."""


class GraphContentPermissionError(GraphContentError):
    """Raised when the current actor cannot access graph content."""


class GraphContentNotFoundError(GraphContentError):
    """Raised when a managed graph or instance cannot be resolved."""


class GraphContentConflictError(GraphContentError):
    """Raised when a requested create operation conflicts with existing content."""


class GraphContentValidationError(GraphContentError):
    """Raised when graph-content input is invalid for admin use."""
