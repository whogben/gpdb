"""User operations for the admin store."""

from __future__ import annotations

from gpdb import Filter, FilterGroup, Logic, NodeRead, NodeUpsert, SearchQuery
from gpdb.admin.store.models import AdminUser
from gpdb.admin.store.exceptions import OwnerAlreadyExistsError, UserAlreadyExistsError


async def owner_exists(store) -> bool:
    """Return whether an owner user has already been created."""
    page = await store.db.search_nodes(
        SearchQuery(
            filter=FilterGroup(
                logic=Logic.AND,
                filters=[
                    Filter(field="type", value="user"),
                    Filter(field="data.is_owner", value=True),
                    Filter(field="data.is_active", value=True),
                ],
            ),
            limit=1,
        )
    )
    return bool(page.items)


async def get_active_owner_user(store) -> AdminUser | None:
    """Return the single active owner user, or None if uninitialized.

    Owner resolution is intended for trusted local tooling (e.g. CLI) where
    the instance is assumed to be controlled by the owner/admin process.
    """
    page = await store.db.search_nodes(
        SearchQuery(
            filter=FilterGroup(
                logic=Logic.AND,
                filters=[
                    Filter(field="type", value="user"),
                    Filter(field="data.is_owner", value=True),
                    Filter(field="data.is_active", value=True),
                ],
            ),
            limit=2,
        )
    )
    if not page.items:
        return None
    # If multiple owners are present, treat it as a data integrity issue
    # and fail loudly for safety (best-effort owner selection would hide it).
    if len(page.items) > 1:
        raise RuntimeError("Multiple active owner users exist.")
    return _admin_user_from_node(page.items[0])


async def get_user_by_username(store, username: str) -> AdminUser | None:
    """Return a user by username if present."""
    page = await store.db.search_nodes(
        SearchQuery(
            filter=FilterGroup(
                logic=Logic.AND,
                filters=[
                    Filter(field="type", value="user"),
                    Filter(field="data.username", value=username),
                ],
            ),
            limit=1,
        )
    )
    if not page.items:
        return None
    return _admin_user_from_node(page.items[0])


async def get_user_node_by_username(store, username: str) -> NodeRead | None:
    """Return the raw node for a username when update access is needed."""
    page = await store.db.search_nodes(
        SearchQuery(
            filter=FilterGroup(
                logic=Logic.AND,
                filters=[
                    Filter(field="type", value="user"),
                    Filter(field="data.username", value=username),
                ],
            ),
            limit=1,
        )
    )
    if not page.items:
        return None
    return page.items[0]


async def get_user_by_id(store, user_id: str) -> AdminUser | None:
    """Return a user by id if present."""
    try:
        nodes = await store.db.get_nodes([user_id])
        node = nodes[0]
        if node.type != "user":
            return None
        return _admin_user_from_node(node)
    except ValueError:
        return None


async def create_initial_owner(
    store,
    *,
    username: str,
    password_hash: str,
    display_name: str | None = None,
) -> AdminUser:
    """Create the first owner account for a fresh admin install."""
    if await owner_exists(store):
        raise OwnerAlreadyExistsError("An owner user already exists")
    if await get_user_by_username(store, username):
        raise UserAlreadyExistsError(f"User '{username}' already exists")

    node_list = await store.db.set_nodes(
        [
            NodeUpsert(
                type="user",
                name=username,
                data={
                    "username": username,
                    "display_name": display_name or username,
                    "password_hash": password_hash,
                    "is_owner": True,
                    "is_active": True,
                    "auth_version": 1,
                },
            )
        ]
    )
    node = node_list[0]
    return _admin_user_from_node(node)


async def verify_user_credentials(
    store,
    *,
    username: str,
    password: str,
    verify_password: callable,
) -> AdminUser | None:
    """Return the authenticated user if the credentials are valid."""
    node = await get_user_node_by_username(store, username)
    if node is None:
        return None

    password_hash = node.data.get("password_hash")
    if not isinstance(password_hash, str):
        return None
    if not bool(node.data.get("is_active", False)):
        return None
    if not verify_password(password, password_hash):
        return None

    return _admin_user_from_node(node)


def _admin_user_from_node(node: NodeRead) -> AdminUser:
    """Project a GPDB node into the minimal auth-facing user model."""
    return AdminUser(
        id=node.id,
        username=str(node.data["username"]),
        display_name=str(node.data.get("display_name") or node.data["username"]),
        is_owner=bool(node.data.get("is_owner", False)),
        is_active=bool(node.data.get("is_active", False)),
        auth_version=int(node.data.get("auth_version", 1)),
    )
