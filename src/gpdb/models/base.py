"""
Base classes and exceptions for the graph database.
"""

from __future__ import annotations

import secrets
from typing import Any, Dict, Literal, Union, cast

from pydantic import BaseModel
from sqlalchemy.orm import DeclarativeBase


class SchemaNotFoundError(Exception):
    """Raised when a schema is not found."""

    pass


class SchemaValidationError(Exception):
    """Raised when data validation against a schema fails."""

    pass


class SchemaKindMismatchError(SchemaValidationError):
    """Raised when a schema is attached to the wrong graph record kind."""

    pass


class SchemaBreakingChangeError(Exception):
    """Raised when a schema update contains breaking changes."""

    pass


class SchemaInUseError(Exception):
    """Raised when attempting to delete a schema that is still referenced by nodes or edges."""

    pass


class SchemaProtectedError(Exception):
    """Raised when attempting to modify or delete a protected schema."""

    pass


class SchemaInheritanceError(Exception):
    """Raised when schema inheritance validation fails."""

    pass


SchemaKind = Literal["node", "edge"]
_SCHEMA_KIND_FIELD = "x-gpdb-kind"
_SCHEMA_KIND_VALUES = {"node", "edge"}

_ID_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"
_ID_MAX_COLLISION_ATTEMPTS = 10

# PostgreSQL SQLSTATE for unique_violation (covers primary key and unique constraints).
_PG_UNIQUE_VIOLATION = "23505"


def _normalize_schema_kind(kind: str) -> SchemaKind:
    clean_kind = kind.strip().lower()
    if clean_kind not in _SCHEMA_KIND_VALUES:
        raise ValueError("Schema kind must be either 'node' or 'edge'.")
    return cast(SchemaKind, clean_kind)


def _extract_schema_kind(
    json_schema: Dict[str, Any], *, required: bool = True
) -> SchemaKind | None:
    raw_kind = json_schema.get(_SCHEMA_KIND_FIELD)
    if raw_kind is None:
        if required:
            raise ValueError(
                f"Schema JSON must include '{_SCHEMA_KIND_FIELD}' with value 'node' or 'edge'."
            )
        return None
    if not isinstance(raw_kind, str):
        raise ValueError(
            f"Schema field '{_SCHEMA_KIND_FIELD}' must be a string with value 'node' or 'edge'."
        )
    return _normalize_schema_kind(raw_kind)


class _Base(DeclarativeBase):
    pass


def _default_generate_id() -> str:
    """
    Default ID generator.

    Generates a short, human-friendly ID in SSN-like form: 3-2-4 lowercase
    alphanumeric segments (e.g. abc-de-fghi) for readability.
    """
    part1 = "".join(secrets.choice(_ID_ALPHABET) for _ in range(3))
    part2 = "".join(secrets.choice(_ID_ALPHABET) for _ in range(2))
    part3 = "".join(secrets.choice(_ID_ALPHABET) for _ in range(4))
    return f"{part1}-{part2}-{part3}"


ID_GENERATOR = _default_generate_id


def generate_id() -> str:
    """
    Public ID generator hook.

    Downstream projects can override `ID_GENERATOR` to customize how IDs
    are generated without changing call sites or ORM defaults.
    """
    return ID_GENERATOR()


def _is_primary_key_violation(exc: BaseException) -> bool:
    """Return True if the exception is a primary key (id) duplicate.

    Only treats PK duplicates as retryable; other unique constraint
    violations (e.g. parent_id+name) are not. Uses pgcode/sqlstate 23505
    plus message containing "pkey" or "primary key".
    """
    msg = str(getattr(exc, "orig", exc)).lower()
    if "pkey" not in msg and "primary key" not in msg:
        return False
    code = getattr(exc, "pgcode", None) or getattr(exc, "sqlstate", None)
    if code == _PG_UNIQUE_VIOLATION:
        return True
    orig = getattr(exc, "orig", None)
    if orig is not None:
        code = getattr(orig, "pgcode", None) or getattr(orig, "sqlstate", None)
        if code == _PG_UNIQUE_VIOLATION:
            return True
    return False
