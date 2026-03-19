"""API key operations for the admin store."""

from __future__ import annotations

from datetime import UTC, datetime

from gpdb import Filter, FilterGroup, Logic, NodeRead, NodeUpsert, SearchQuery
from gpdb.admin.auth import parse_api_key
from gpdb.admin.store.models import AdminAPIKey, AdminUser
from gpdb.admin.store.users import get_user_by_id

API_KEY_NODE_TYPE = "api_key"


async def list_api_keys_for_user(store, user_id: str) -> list[AdminAPIKey]:
    """Return all API keys owned by one user."""
    nodes = await store._search_nodes(
        filters=[
            Filter(field="type", value=API_KEY_NODE_TYPE),
            Filter(field="parent_id", value=user_id),
        ],
        limit=500,
    )
    api_keys = [_admin_api_key_from_node(node) for node in nodes]
    return sorted(
        api_keys,
        key=lambda item: (item.created_at, item.label.lower(), item.key_id),
        reverse=True,
    )


async def get_api_key_by_id(store, api_key_id: str) -> AdminAPIKey | None:
    """Return one API key by its admin node id."""
    try:
        nodes = await store.db.get_nodes([api_key_id])
        node = nodes[0]
        if node.type != API_KEY_NODE_TYPE or not node.parent_id:
            return None
        return _admin_api_key_from_node(node)
    except ValueError:
        return None


async def get_api_key_by_key_id(store, key_id: str) -> AdminAPIKey | None:
    """Return one API key by its public identifier."""
    node = await store._get_node_by_filters(
        [
            Filter(field="type", value=API_KEY_NODE_TYPE),
            Filter(field="data.key_id", value=key_id),
        ]
    )
    if node is None or not node.parent_id:
        return None
    return _admin_api_key_from_node(node)


async def create_api_key(
    store,
    *,
    user_id: str,
    label: str,
    key_id: str,
    preview: str,
    secret_hash: str,
    key_value: str,
) -> AdminAPIKey:
    """Create and persist one API key for a user."""
    node_list = await store.db.set_nodes(
        [
            NodeUpsert(
                type=API_KEY_NODE_TYPE,
                name=key_id,
                parent_id=user_id,
                data={
                    "label": label,
                    "key_id": key_id,
                    "preview": preview,
                    "secret_hash": secret_hash,
                    "key_value": store._encrypt_instance_secret(key_value),
                    "created_at": _timestamp_now(),
                    "last_used_at": None,
                    "revoked_at": None,
                    "is_active": True,
                },
            )
        ]
    )
    node = node_list[0]
    return _admin_api_key_from_node(node)


async def reveal_api_key(store, api_key_id: str) -> str | None:
    """Return the stored plaintext API key for one key record."""
    try:
        nodes = await store.db.get_nodes([api_key_id])
        node = nodes[0]
        if node.type != API_KEY_NODE_TYPE:
            return None
        return store._decrypt_instance_secret(
            _optional_string(node.data.get("key_value"))
        )
    except ValueError:
        return None


async def revoke_api_key(store, api_key_id: str) -> AdminAPIKey | None:
    """Revoke one API key and prevent future authentication."""
    try:
        nodes = await store.db.get_nodes([api_key_id])
        node = nodes[0]
        if node.type != API_KEY_NODE_TYPE or not node.parent_id:
            return None
        updated_data = dict(node.data)
        updated_data["is_active"] = False
        updated_data["revoked_at"] = updated_data.get("revoked_at") or _timestamp_now()
        updated_list = await store.db.set_nodes(
            [
                NodeUpsert(
                    id=node.id,
                    type=node.type,
                    name=node.name,
                    parent_id=node.parent_id,
                    data=updated_data,
                )
            ]
        )
        updated = updated_list[0]
        return _admin_api_key_from_node(updated)
    except ValueError:
        return None


async def authenticate_api_key(
    store,
    *,
    api_key_token: str,
    verify_secret: callable,
) -> tuple[AdminUser, AdminAPIKey] | None:
    """Return the owning user and API key when a token is valid."""
    parsed = parse_api_key(api_key_token)
    if parsed is None:
        return None
    node = await store._get_node_by_filters(
        [
            Filter(field="type", value=API_KEY_NODE_TYPE),
            Filter(field="data.key_id", value=parsed.key_id),
        ]
    )
    if node is None or not node.parent_id:
        return None
    secret_hash = node.data.get("secret_hash")
    if not isinstance(secret_hash, str):
        return None
    if not bool(node.data.get("is_active", False)):
        return None
    if node.data.get("revoked_at"):
        return None
    if not verify_secret(parsed.secret, secret_hash):
        return None

    user = await get_user_by_id(store, node.parent_id)
    if user is None or not user.is_active:
        return None

    updated_data = dict(node.data)
    updated_data["last_used_at"] = _timestamp_now()
    try:
        updated_list = await store.db.set_nodes(
            [
                NodeUpsert(
                    id=node.id,
                    type=node.type,
                    name=node.name,
                    parent_id=node.parent_id,
                    data=updated_data,
                )
            ]
        )
        updated = updated_list[0]
        return user, _admin_api_key_from_node(updated)
    except Exception:
        # Concurrent update on last_used_at - non-critical, return success
        return user, _admin_api_key_from_node(node)


def _admin_api_key_from_node(node: NodeRead) -> AdminAPIKey:
    """Project a GPDB node into one API key management view."""
    return AdminAPIKey(
        id=node.id,
        user_id=str(node.parent_id or ""),
        label=str(node.data.get("label") or node.data.get("key_id") or node.name or ""),
        key_id=str(node.data["key_id"]),
        preview=str(node.data.get("preview") or node.data["key_id"]),
        created_at=str(node.data.get("created_at") or ""),
        last_used_at=_optional_string(node.data.get("last_used_at")),
        revoked_at=_optional_string(node.data.get("revoked_at")),
        is_active=bool(node.data.get("is_active", True)),
    )


def _optional_string(value: object) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _timestamp_now() -> str:
    """Return the current UTC timestamp for admin metadata writes."""
    return datetime.now(tz=UTC).isoformat()
