from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING

from toolaccess import (
    InvocationContext,
    ToolDefinition,
    ToolService,
    inject_context,
)

from gpdb.admin.auth import generate_api_key, hash_api_key_secret, verify_api_key_secret
from gpdb.admin.context import _require_context_user

if TYPE_CHECKING:
    from gpdb.admin.runtime import AdminServices


def _build_cli_api_key_tools(services: AdminServices) -> list[ToolDefinition]:
    """Build trusted local CLI commands for API key management."""

    async def api_key_list(username: str) -> list[dict[str, object]]:
        """List API keys for one local admin user."""
        user = await _require_user_by_username(services, username)
        return [
            _serialize_api_key(item)
            for item in await services.admin_store.list_api_keys_for_user(user.id)
        ]

    async def api_key_create(username: str, label: str) -> dict[str, object]:
        """Create an API key for one local admin user."""
        user = await _require_user_by_username(services, username)
        return await _create_api_key_for_user(services, user_id=user.id, label=label)

    async def api_key_reveal(username: str, key_id: str) -> dict[str, object]:
        """Reveal one API key owned by the named local user."""
        user = await _require_user_by_username(services, username)
        return await _reveal_api_key_for_user(services, user_id=user.id, key_id=key_id)

    async def api_key_revoke(username: str, key_id: str) -> dict[str, object]:
        """Revoke one API key owned by the named local user."""
        user = await _require_user_by_username(services, username)
        return await _revoke_api_key_for_user(services, user_id=user.id, key_id=key_id)

    return [
        ToolDefinition(api_key_list, "api_key_list"),
        ToolDefinition(api_key_create, "api_key_create"),
        ToolDefinition(api_key_reveal, "api_key_revoke"),
        ToolDefinition(api_key_revoke, "api_key_revoke"),
    ]


def _build_mcp_api_key_tools(services: AdminServices) -> list[ToolDefinition]:
    """Build authenticated MCP tools for current-user API key management."""

    async def api_key_list_me(
        ctx: InvocationContext = inject_context(),
    ) -> list[dict[str, object]]:
        """List API keys for the authenticated MCP user."""
        user = _require_context_user(ctx)
        return [
            _serialize_api_key(item)
            for item in await services.admin_store.list_api_keys_for_user(user.id)
        ]

    async def api_key_create_me(
        label: str,
        ctx: InvocationContext = inject_context(),
    ) -> dict[str, object]:
        """Create an API key for the authenticated MCP user."""
        user = _require_context_user(ctx)
        return await _create_api_key_for_user(services, user_id=user.id, label=label)

    async def api_key_reveal_me(
        key_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> dict[str, object]:
        """Reveal one API key owned by the authenticated MCP user."""
        user = _require_context_user(ctx)
        return await _reveal_api_key_for_user(services, user_id=user.id, key_id=key_id)

    async def api_key_revoke_me(
        key_id: str,
        ctx: InvocationContext = inject_context(),
    ) -> dict[str, object]:
        """Revoke one API key owned by the authenticated MCP user."""
        user = _require_context_user(ctx)
        return await _revoke_api_key_for_user(services, user_id=user.id, key_id=key_id)

    return [
        ToolDefinition(api_key_list_me, "api_key_list"),
        ToolDefinition(api_key_create_me, "api_key_create"),
        ToolDefinition(api_key_reveal_me, "api_key_reveal"),
        ToolDefinition(api_key_revoke_me, "api_key_revoke"),
    ]


async def _require_user_by_username(services: AdminServices, username: str):
    """Return an active admin user by username or raise a friendly error."""
    admin_store = services.admin_store
    if admin_store is None:
        raise RuntimeError("Admin store is not ready yet.")
    user = await admin_store.get_user_by_username(username.strip())
    if user is None or not user.is_active:
        raise ValueError(f"User '{username}' was not found.")
    return user


async def _create_api_key_for_user(
    services: AdminServices,
    *,
    user_id: str,
    label: str,
) -> dict[str, object]:
    """Create one API key for a target user and return revealable metadata."""
    admin_store = services.admin_store
    if admin_store is None:
        raise RuntimeError("Admin store is not ready yet.")
    clean_label = label.strip()
    if not clean_label:
        raise ValueError("API key label is required.")
    generated = generate_api_key()
    api_key = await admin_store.create_api_key(
        user_id=user_id,
        label=clean_label,
        key_id=generated.key_id,
        preview=generated.preview,
        secret_hash=hash_api_key_secret(generated.secret),
        key_value=generated.token,
    )
    result = _serialize_api_key(api_key)
    result["api_key"] = generated.token
    return result


async def _reveal_api_key_for_user(
    services: AdminServices,
    *,
    user_id: str,
    key_id: str,
) -> dict[str, object]:
    """Reveal one API key if it belongs to the target user."""
    api_key = await _require_owned_api_key(services, user_id=user_id, key_id=key_id)
    revealed = await services.admin_store.reveal_api_key(api_key.id)
    if revealed is None:
        raise ValueError(f"API key '{key_id}' was not found.")
    result = _serialize_api_key(api_key)
    result["api_key"] = revealed
    return result


async def _revoke_api_key_for_user(
    services: AdminServices,
    *,
    user_id: str,
    key_id: str,
) -> dict[str, object]:
    """Revoke one API key if it belongs to the target user."""
    admin_store = services.admin_store
    if admin_store is None:
        raise RuntimeError("Admin store is not ready yet.")
    api_key = await _require_owned_api_key(services, user_id=user_id, key_id=key_id)
    updated = await admin_store.revoke_api_key(api_key.id)
    if updated is None:
        raise ValueError(f"API key '{key_id}' was not found.")
    return _serialize_api_key(updated)


async def _require_owned_api_key(
    services: AdminServices,
    *,
    user_id: str,
    key_id: str,
):
    """Return one API key when it belongs to the requested user."""
    admin_store = services.admin_store
    if admin_store is None:
        raise RuntimeError("Admin store is not ready yet.")
    api_key = await admin_store.get_api_key_by_key_id(key_id.strip())
    if api_key is None or api_key.user_id != user_id:
        raise ValueError(f"API key '{key_id}' was not found.")
    return api_key


def _serialize_api_key(api_key) -> dict[str, object]:
    """Project one API key dataclass into a tool-friendly dict."""
    return asdict(api_key)
