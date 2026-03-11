"""Persistence helpers for admin identity data."""

from __future__ import annotations

from dataclasses import dataclass

from gpdb import Filter, FilterGroup, GPGraph, Logic, NodeRead, NodeUpsert, SearchQuery


ADMIN_TABLE_PREFIX = "admin"


class OwnerAlreadyExistsError(RuntimeError):
    """Raised when bootstrap is attempted after an owner already exists."""


class UserAlreadyExistsError(RuntimeError):
    """Raised when creating a user with a duplicate username."""


@dataclass(frozen=True)
class AdminUser:
    """Minimal user view used by the web/auth layers."""

    id: str
    username: str
    display_name: str
    is_owner: bool
    is_active: bool
    auth_version: int


class AdminStore:
    """Access admin users stored in the captive admin GPDB instance."""

    def __init__(self, url: str):
        self.db = GPGraph(url, table_prefix=ADMIN_TABLE_PREFIX)

    async def initialize(self) -> None:
        """Create required admin tables if they do not exist."""
        await self.db.create_tables()

    async def close(self) -> None:
        """Dispose the underlying SQLAlchemy engine."""
        await self.db.sqla_engine.dispose()

    async def owner_exists(self) -> bool:
        """Return whether an owner user has already been created."""
        page = await self.db.search_nodes(
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

    async def get_user_by_username(self, username: str) -> AdminUser | None:
        """Return a user by username if present."""
        page = await self.db.search_nodes(
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

    async def get_user_node_by_username(self, username: str) -> NodeRead | None:
        """Return the raw node for a username when update access is needed."""
        page = await self.db.search_nodes(
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

    async def get_user_by_id(self, user_id: str) -> AdminUser | None:
        """Return a user by id if present."""
        node = await self.db.get_node(user_id)
        if node is None or node.type != "user":
            return None
        return _admin_user_from_node(node)

    async def create_initial_owner(
        self,
        *,
        username: str,
        password_hash: str,
        display_name: str | None = None,
    ) -> AdminUser:
        """Create the first owner account for a fresh admin install."""
        if await self.owner_exists():
            raise OwnerAlreadyExistsError("An owner user already exists")
        if await self.get_user_by_username(username):
            raise UserAlreadyExistsError(f"User '{username}' already exists")

        node = await self.db.set_node(
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
        )
        return _admin_user_from_node(node)

    async def verify_user_credentials(
        self,
        *,
        username: str,
        password: str,
        verify_password: callable,
    ) -> AdminUser | None:
        """Return the authenticated user if the credentials are valid."""
        node = await self.get_user_node_by_username(username)
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
