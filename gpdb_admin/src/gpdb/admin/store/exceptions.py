"""Exceptions for admin store operations."""


class OwnerAlreadyExistsError(RuntimeError):
    """Raised when bootstrap is attempted after an owner already exists."""


class UserAlreadyExistsError(RuntimeError):
    """Raised when creating a user with a duplicate username."""


class InstanceAlreadyExistsError(RuntimeError):
    """Raised when a managed instance slug already exists."""


class GraphAlreadyExistsError(RuntimeError):
    """Raised when a managed graph prefix already exists for an instance."""
