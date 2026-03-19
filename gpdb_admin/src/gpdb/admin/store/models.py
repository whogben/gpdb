"""Data models for admin identity and managed graph data."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AdminUser:
    """Minimal user view used by the web/auth layers."""

    id: str
    username: str
    display_name: str
    is_owner: bool
    is_active: bool
    auth_version: int


@dataclass(frozen=True)
class ManagedInstance:
    """Managed GPDB connection metadata stored in the captive admin DB."""

    id: str
    slug: str
    display_name: str
    description: str
    mode: str
    is_builtin: bool
    is_default: bool
    is_active: bool
    connection_kind: str
    host: str | None
    port: int | None
    database: str | None
    username: str | None
    password: str | None
    status: str
    status_message: str | None
    last_checked_at: str | None


@dataclass(frozen=True)
class ManagedGraph:
    """Managed graph metadata scoped to one instance and table prefix."""

    id: str
    instance_id: str
    instance_slug: str
    instance_display_name: str
    display_name: str
    table_prefix: str
    status: str
    status_message: str | None
    last_checked_at: str | None
    exists_in_instance: bool
    source: str
    is_default: bool


@dataclass(frozen=True)
class AdminAPIKey:
    """One revealable API key owned by an admin user."""

    id: str
    user_id: str
    label: str
    key_id: str
    preview: str
    created_at: str
    last_used_at: str | None
    revoked_at: str | None
    is_active: bool
