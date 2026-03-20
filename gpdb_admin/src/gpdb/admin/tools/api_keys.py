from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field
from toolaccess import (
    InvocationContext,
    ToolService,
    inject_context,
)

from gpdb.admin.auth import (
    generate_api_key,
    hash_api_key_secret,
    parse_provided_api_key,
)
from gpdb.admin.context import _require_context_user
from gpdb.admin.tools.base import (
    API_KEY_TOOL_ACCESS,
    CLI_JSON_RENDERER,
    _api_key_surface_specs,
)

if TYPE_CHECKING:
    from gpdb.admin.runtime import AdminServices


class ApiKeyListParams(BaseModel):
    """Parameters for listing API keys. If username omitted, uses authenticated user."""

    username: str | None = Field(
        None, description="Username of the admin user (omit for self)."
    )


class ApiKeyCreateParams(BaseModel):
    """Parameters for creating an API key. If username omitted, uses authenticated user."""

    username: str | None = Field(
        None, description="Username of the admin user (omit for self)."
    )
    label: str = Field(..., description="Label for the API key.")
    api_key: str | None = Field(
        None, description="API key value (omit to auto-generate)."
    )


class ApiKeyRevealParams(BaseModel):
    """Parameters for revealing an API key. If username omitted, uses authenticated user."""

    username: str | None = Field(
        None, description="Username of the admin user (omit for self)."
    )
    key_id: str = Field(..., description="API key ID.")


class ApiKeyRevokeParams(BaseModel):
    """Parameters for revoking an API key. If username omitted, uses authenticated user."""

    username: str | None = Field(
        None, description="Username of the admin user (omit for self)."
    )
    key_id: str = Field(..., description="API key ID.")


def _build_api_key_service(services: AdminServices) -> ToolService:
    """Build API key management tools for all surfaces."""
    service = ToolService("admin-apikeys")

    @service.tool(
        name="api_key_list",
        surfaces=_api_key_surface_specs(cli_renderer=CLI_JSON_RENDERER),
        access=API_KEY_TOOL_ACCESS,
    )
    async def api_key_list(
        params: ApiKeyListParams,
        ctx: InvocationContext = inject_context(),
    ) -> list[dict[str, object]]:
        """List API keys for a user."""
        user = await _require_target_user_for_api_key_operation(
            services, ctx, username=params.username
        )
        return [
            _serialize_api_key(item)
            for item in await services.admin_store.list_api_keys_for_user(user.id)
        ]

    @service.tool(
        name="api_key_create",
        surfaces=_api_key_surface_specs(cli_renderer=CLI_JSON_RENDERER),
        access=API_KEY_TOOL_ACCESS,
    )
    async def api_key_create(
        params: ApiKeyCreateParams,
        ctx: InvocationContext = inject_context(),
    ) -> dict[str, object]:
        """Create an API key for a user."""
        user = await _require_target_user_for_api_key_operation(
            services, ctx, username=params.username
        )
        return await _create_api_key_for_user(
            services, user_id=user.id, label=params.label, api_key=params.api_key
        )

    @service.tool(
        name="api_key_reveal",
        surfaces=_api_key_surface_specs(cli_renderer=CLI_JSON_RENDERER),
        access=API_KEY_TOOL_ACCESS,
    )
    async def api_key_reveal(
        params: ApiKeyRevealParams,
        ctx: InvocationContext = inject_context(),
    ) -> dict[str, object]:
        """Reveal an API key for a user."""
        user = await _require_target_user_for_api_key_operation(
            services, ctx, username=params.username
        )
        return await _reveal_api_key_for_user(
            services, user_id=user.id, key_id=params.key_id
        )

    @service.tool(
        name="api_key_revoke",
        surfaces=_api_key_surface_specs(cli_renderer=CLI_JSON_RENDERER),
        access=API_KEY_TOOL_ACCESS,
    )
    async def api_key_revoke(
        params: ApiKeyRevokeParams,
        ctx: InvocationContext = inject_context(),
    ) -> dict[str, object]:
        """Revoke an API key for a user."""
        user = await _require_target_user_for_api_key_operation(
            services, ctx, username=params.username
        )
        return await _revoke_api_key_for_user(
            services, user_id=user.id, key_id=params.key_id
        )

    return service


async def _require_user_by_username(services: AdminServices, username: str):
    """Return an active admin user by username or raise a friendly error."""
    admin_store = services.admin_store
    if admin_store is None:
        raise RuntimeError("Admin store is not ready yet.")
    user = await admin_store.get_user_by_username(username.strip())
    if user is None or not user.is_active:
        raise ValueError(f"User '{username}' was not found.")
    return user


async def _require_target_user_for_api_key_operation(
    services: "AdminServices",
    ctx: InvocationContext,
    *,
    username: str | None,
):
    """Resolve the target admin user for API key operations.

    - REST/MCP: when `username` is omitted, use the authenticated user from ctx.state.
    - CLI: when `username` is omitted, use the active owner user (trusted local tooling).
    """
    if username:
        return await _require_user_by_username(services, username)

    if getattr(ctx, "surface", None) == "cli":
        admin_store = services.admin_store
        if admin_store is None:
            raise RuntimeError("Admin store is not ready yet.")
        owner = await admin_store.get_active_owner_user()
        if owner is None:
            raise RuntimeError(
                "Owner user required. Run setup first, or pass --username."
            )
        return owner

    return _require_context_user(ctx)


async def _create_api_key_for_user(
    services: AdminServices,
    *,
    user_id: str,
    label: str,
    api_key: str | None = None,
) -> dict[str, object]:
    """Create one API key for a target user and return revealable metadata."""
    admin_store = services.admin_store
    if admin_store is None:
        raise RuntimeError("Admin store is not ready yet.")
    clean_label = label.strip()
    if not clean_label:
        raise ValueError("API key label is required.")
    
    if api_key:
        clean_api_key = api_key.strip()
        if not clean_api_key:
            raise ValueError("API key value cannot be empty if provided.")
        parsed = parse_provided_api_key(clean_api_key)
        if parsed is None:
            raise ValueError("Invalid API key format. Must be in format: gpdb_<key_id>_<secret>")
        generated = parsed
    else:
        generated = generate_api_key()
    
    api_key_record = await admin_store.create_api_key(
        user_id=user_id,
        label=clean_label,
        key_id=generated.key_id,
        preview=generated.preview,
        secret_hash=hash_api_key_secret(generated.secret),
        key_value=generated.token,
    )
    result = _serialize_api_key(api_key_record)
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
