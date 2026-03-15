"""Persistence helpers for admin identity and managed graph data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from gpdb import Filter, FilterGroup, GPGraph, Logic, NodeRead, NodeUpsert, SearchQuery
from gpdb.admin.auth import parse_api_key
from gpdb.admin.secrets import SecretCipher
from sqlalchemy.orm.exc import StaleDataError


ADMIN_TABLE_PREFIX = "admin"
INSTANCE_NODE_TYPE = "instance"
GRAPH_NODE_TYPE = "graph"
API_KEY_NODE_TYPE = "api_key"
DEFAULT_INSTANCE_SLUG = "default"
DEFAULT_GRAPH_NODE_NAME = "__default__"


class OwnerAlreadyExistsError(RuntimeError):
    """Raised when bootstrap is attempted after an owner already exists."""


class UserAlreadyExistsError(RuntimeError):
    """Raised when creating a user with a duplicate username."""


class InstanceAlreadyExistsError(RuntimeError):
    """Raised when a managed instance slug already exists."""


class GraphAlreadyExistsError(RuntimeError):
    """Raised when a managed graph prefix already exists for an instance."""


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


class AdminStore:
    """Access admin users and managed graph metadata in the captive admin DB."""

    def __init__(self, url: str, *, instance_secret: str | None = None):
        self.db = GPGraph(url, table_prefix=ADMIN_TABLE_PREFIX)
        self._instance_secret_cipher = (
            SecretCipher(instance_secret) if instance_secret else None
        )

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

    async def list_api_keys_for_user(self, user_id: str) -> list[AdminAPIKey]:
        """Return all API keys owned by one user."""
        nodes = await self._search_nodes(
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

    async def get_api_key_by_id(self, api_key_id: str) -> AdminAPIKey | None:
        """Return one API key by its admin node id."""
        node = await self.db.get_node(api_key_id)
        if node is None or node.type != API_KEY_NODE_TYPE or not node.parent_id:
            return None
        return _admin_api_key_from_node(node)

    async def get_api_key_by_key_id(self, key_id: str) -> AdminAPIKey | None:
        """Return one API key by its public identifier."""
        node = await self._get_node_by_filters(
            [
                Filter(field="type", value=API_KEY_NODE_TYPE),
                Filter(field="data.key_id", value=key_id),
            ]
        )
        if node is None or not node.parent_id:
            return None
        return _admin_api_key_from_node(node)

    async def create_api_key(
        self,
        *,
        user_id: str,
        label: str,
        key_id: str,
        preview: str,
        secret_hash: str,
        key_value: str,
    ) -> AdminAPIKey:
        """Create and persist one API key for a user."""
        node = await self.db.set_node(
            NodeUpsert(
                type=API_KEY_NODE_TYPE,
                name=key_id,
                parent_id=user_id,
                data={
                    "label": label,
                    "key_id": key_id,
                    "preview": preview,
                    "secret_hash": secret_hash,
                    "key_value": self._encrypt_instance_secret(key_value),
                    "created_at": _timestamp_now(),
                    "last_used_at": None,
                    "revoked_at": None,
                    "is_active": True,
                },
            )
        )
        return _admin_api_key_from_node(node)

    async def reveal_api_key(self, api_key_id: str) -> str | None:
        """Return the stored plaintext API key for one key record."""
        node = await self.db.get_node(api_key_id)
        if node is None or node.type != API_KEY_NODE_TYPE:
            return None
        return self._decrypt_instance_secret(
            _optional_string(node.data.get("key_value"))
        )

    async def revoke_api_key(self, api_key_id: str) -> AdminAPIKey | None:
        """Revoke one API key and prevent future authentication."""
        node = await self.db.get_node(api_key_id)
        if node is None or node.type != API_KEY_NODE_TYPE or not node.parent_id:
            return None
        updated_data = dict(node.data)
        updated_data["is_active"] = False
        updated_data["revoked_at"] = updated_data.get("revoked_at") or _timestamp_now()
        updated = await self.db.set_node(
            NodeUpsert(
                id=node.id,
                type=node.type,
                name=node.name,
                parent_id=node.parent_id,
                data=updated_data,
            )
        )
        return _admin_api_key_from_node(updated)

    async def authenticate_api_key(
        self,
        *,
        api_key_token: str,
        verify_secret: callable,
    ) -> tuple[AdminUser, AdminAPIKey] | None:
        """Return the owning user and API key when a token is valid."""
        parsed = parse_api_key(api_key_token)
        if parsed is None:
            return None
        node = await self._get_node_by_filters(
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

        user = await self.get_user_by_id(node.parent_id)
        if user is None or not user.is_active:
            return None

        updated_data = dict(node.data)
        updated_data["last_used_at"] = _timestamp_now()
        try:
            updated = await self.db.set_node(
                NodeUpsert(
                    id=node.id,
                    type=node.type,
                    name=node.name,
                    parent_id=node.parent_id,
                    data=updated_data,
                )
            )
            return user, _admin_api_key_from_node(updated)
        except StaleDataError:
            # Concurrent update on last_used_at - non-critical, return success
            return user, _admin_api_key_from_node(node)

    async def list_instances(self) -> list[ManagedInstance]:
        """Return all managed instance records."""
        nodes = await self._search_nodes(
            filters=[Filter(field="type", value=INSTANCE_NODE_TYPE)],
            limit=500,
        )
        instances = [self._managed_instance_from_node(node) for node in nodes]
        return sorted(
            instances,
            key=lambda item: (
                not item.is_builtin,
                item.display_name.lower(),
                item.slug.lower(),
            ),
        )

    async def get_instance_by_id(self, instance_id: str) -> ManagedInstance | None:
        """Return one managed instance by node id."""
        node = await self.db.get_node(instance_id)
        if node is None or node.type != INSTANCE_NODE_TYPE:
            return None
        return self._managed_instance_from_node(node)

    async def get_instance_by_slug(self, slug: str) -> ManagedInstance | None:
        """Return one managed instance by slug."""
        node = await self._get_node_by_filters(
            [
                Filter(field="type", value=INSTANCE_NODE_TYPE),
                Filter(field="data.slug", value=slug),
            ]
        )
        if node is None:
            return None
        return self._managed_instance_from_node(node)

    async def ensure_builtin_instance(
        self,
        *,
        display_name: str = "Default instance",
        description: str = "Built-in captive GPDB instance managed by gpdb-admin.",
    ) -> ManagedInstance:
        """Create or refresh the built-in captive instance metadata."""
        existing = await self._get_node_by_filters(
            [
                Filter(field="type", value=INSTANCE_NODE_TYPE),
                Filter(field="data.is_builtin", value=True),
            ]
        )
        payload = {
            "slug": DEFAULT_INSTANCE_SLUG,
            "display_name": display_name,
            "description": description,
            "mode": "captive",
            "is_builtin": True,
            "is_default": True,
            "is_active": True,
            "connection_kind": "postgres",
            "host": None,
            "port": None,
            "database": None,
            "username": None,
            "password": None,
            "status": (
                existing.data.get("status", "checking") if existing else "checking"
            ),
            "status_message": existing.data.get("status_message") if existing else None,
            "last_checked_at": (
                existing.data.get("last_checked_at") if existing else None
            ),
        }
        node = await self.db.set_node(
            NodeUpsert(
                id=existing.id if existing else None,
                type=INSTANCE_NODE_TYPE,
                name=DEFAULT_INSTANCE_SLUG,
                data=payload,
            )
        )
        return self._managed_instance_from_node(node)

    async def create_instance(
        self,
        *,
        slug: str,
        display_name: str,
        description: str,
        host: str,
        port: int | None,
        database: str,
        username: str,
        password: str | None,
    ) -> ManagedInstance:
        """Create a new external managed instance."""
        if await self.get_instance_by_slug(slug):
            raise InstanceAlreadyExistsError(f"Instance '{slug}' already exists")

        node = await self.db.set_node(
            NodeUpsert(
                type=INSTANCE_NODE_TYPE,
                name=slug,
                data={
                    "slug": slug,
                    "display_name": display_name,
                    "description": description,
                    "mode": "external",
                    "is_builtin": False,
                    "is_default": False,
                    "is_active": True,
                    "connection_kind": "postgres",
                    "host": host,
                    "port": port,
                    "database": database,
                    "username": username,
                    "password": self._encrypt_instance_secret(password),
                    "status": "checking",
                    "status_message": None,
                    "last_checked_at": None,
                },
            )
        )
        return self._managed_instance_from_node(node)

    async def update_instance(
        self,
        *,
        instance_id: str,
        display_name: str | None = None,
        description: str | None = None,
        is_active: bool | None = None,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> ManagedInstance | None:
        """Update one managed instance's metadata and connection fields. Omitted fields are left unchanged."""
        node = await self.db.get_node(instance_id)
        if node is None or node.type != INSTANCE_NODE_TYPE:
            return None

        updated_data = dict(node.data)
        if display_name is not None:
            updated_data["display_name"] = display_name
        if description is not None:
            updated_data["description"] = description
        if is_active is not None:
            updated_data["is_active"] = is_active

        if updated_data.get("mode") == "external":
            if host is not None:
                updated_data["host"] = host
            if port is not None:
                updated_data["port"] = port
            if database is not None:
                updated_data["database"] = database
            if username is not None:
                updated_data["username"] = username
            if password is not None:
                updated_data["password"] = self._encrypt_instance_secret(password)
            # When password is not in the update set, updated_data already has the
            # existing encrypted password from dict(node.data); no decrypt/re-encrypt.

        updated = await self.db.set_node(
            NodeUpsert(
                id=node.id,
                type=node.type,
                name=node.name,
                parent_id=node.parent_id,
                data=updated_data,
            )
        )
        return self._managed_instance_from_node(updated)

    async def delete_instance(self, instance_id: str) -> None:
        """Delete an external managed instance and its graph metadata."""
        instance = await self.get_instance_by_id(instance_id)
        if instance is None:
            return
        if instance.is_builtin:
            raise ValueError("The built-in instance cannot be deleted")

        graphs = await self.list_graphs_for_instance(instance_id)
        for graph in graphs:
            await self.db.delete_node(graph.id)
        await self.db.delete_node(instance_id)

    async def update_instance_status(
        self,
        instance_id: str,
        *,
        status: str,
        status_message: str | None,
    ) -> ManagedInstance | None:
        """Persist the latest instance health status."""
        node = await self.db.get_node(instance_id)
        if node is None or node.type != INSTANCE_NODE_TYPE:
            return None

        updated_data = dict(node.data)
        updated_data["status"] = status
        updated_data["status_message"] = status_message
        updated_data["last_checked_at"] = _timestamp_now()
        updated = await self.db.set_node(
            NodeUpsert(
                id=node.id,
                type=node.type,
                name=node.name,
                parent_id=node.parent_id,
                data=updated_data,
            )
        )
        return self._managed_instance_from_node(updated)

    async def list_graphs(self) -> list[ManagedGraph]:
        """Return all managed graph records across all instances."""
        instances = {item.id: item for item in await self.list_instances()}
        nodes = await self._search_nodes(
            filters=[Filter(field="type", value=GRAPH_NODE_TYPE)],
            limit=2000,
        )
        graphs: list[ManagedGraph] = []
        for node in nodes:
            instance = instances.get(node.parent_id or "")
            if instance is None:
                continue
            graphs.append(_managed_graph_from_node(node, instance))
        return sorted(
            graphs,
            key=lambda item: (
                item.display_name.lower(),
                item.instance_display_name.lower(),
                item.table_prefix.lower(),
            ),
        )

    async def list_graphs_for_instance(self, instance_id: str) -> list[ManagedGraph]:
        """Return all managed graph records for one instance."""
        instance = await self.get_instance_by_id(instance_id)
        if instance is None:
            return []
        nodes = await self._search_nodes(
            filters=[
                Filter(field="type", value=GRAPH_NODE_TYPE),
                Filter(field="parent_id", value=instance_id),
            ],
            limit=1000,
        )
        graphs = [_managed_graph_from_node(node, instance) for node in nodes]
        return sorted(
            graphs,
            key=lambda item: (
                item.display_name.lower(),
                item.table_prefix.lower(),
            ),
        )

    async def get_graph_by_id(self, graph_id: str) -> ManagedGraph | None:
        """Return one managed graph by node id."""
        node = await self.db.get_node(graph_id)
        if node is None or node.type != GRAPH_NODE_TYPE or not node.parent_id:
            return None
        instance = await self.get_instance_by_id(node.parent_id)
        if instance is None:
            return None
        return _managed_graph_from_node(node, instance)

    async def get_graph_by_scope(
        self,
        instance_id: str,
        table_prefix: str,
    ) -> ManagedGraph | None:
        """Return one managed graph by its `(instance, table_prefix)` scope."""
        instance = await self.get_instance_by_id(instance_id)
        if instance is None:
            return None
        node = await self._get_node_by_filters(
            [
                Filter(field="type", value=GRAPH_NODE_TYPE),
                Filter(field="parent_id", value=instance_id),
                Filter(field="data.table_prefix", value=table_prefix),
            ]
        )
        if node is None:
            return None
        return _managed_graph_from_node(node, instance)

    async def update_graph(
        self,
        *,
        graph_id: str,
        display_name: str | None = None,
    ) -> ManagedGraph | None:
        """Update one managed graph's display name. Omitted fields are left unchanged."""
        node = await self.db.get_node(graph_id)
        if node is None or node.type != GRAPH_NODE_TYPE or not node.parent_id:
            return None
        instance = await self.get_instance_by_id(node.parent_id)
        if instance is None:
            return None

        updated_data = dict(node.data)
        if display_name is not None:
            updated_data["display_name"] = display_name
        updated = await self.db.set_node(
            NodeUpsert(
                id=node.id,
                type=node.type,
                name=node.name,
                parent_id=node.parent_id,
                data=updated_data,
            )
        )
        return _managed_graph_from_node(updated, instance)

    async def delete_graph(self, graph_id: str) -> None:
        """Delete one managed graph metadata node."""
        await self.db.delete_node(graph_id)

    async def upsert_graph_metadata(
        self,
        *,
        instance_id: str,
        table_prefix: str,
        display_name: str | None = None,
        status: str | None = None,
        status_message: str | None = None,
        exists_in_instance: bool | None = None,
        source: str | None = None,
    ) -> ManagedGraph:
        """Create or update graph metadata for one `(instance, table_prefix)` scope."""
        instance = await self.get_instance_by_id(instance_id)
        if instance is None:
            raise ValueError("Managed instance was not found")

        existing_node = await self._get_node_by_filters(
            [
                Filter(field="type", value=GRAPH_NODE_TYPE),
                Filter(field="parent_id", value=instance_id),
                Filter(field="data.table_prefix", value=table_prefix),
            ]
        )
        graph_name = _graph_node_name(table_prefix)
        if existing_node is None:
            sibling = await self._get_node_by_filters(
                [
                    Filter(field="type", value=GRAPH_NODE_TYPE),
                    Filter(field="parent_id", value=instance_id),
                    Filter(field="name", value=graph_name),
                ]
            )
            if sibling is not None:
                raise GraphAlreadyExistsError(
                    f"Graph '{table_prefix or 'default'}' already exists"
                )

        current_data = dict(existing_node.data) if existing_node else {}
        node = await self.db.set_node(
            NodeUpsert(
                id=existing_node.id if existing_node else None,
                type=GRAPH_NODE_TYPE,
                name=graph_name,
                parent_id=instance_id,
                data={
                    "table_prefix": table_prefix,
                    "display_name": display_name
                    or current_data.get("display_name")
                    or _default_graph_display_name(table_prefix, instance.display_name),
                    "status": status or current_data.get("status", "checking"),
                    "status_message": (
                        status_message
                        if status_message is not None
                        else current_data.get("status_message")
                    ),
                    "last_checked_at": _timestamp_now(),
                    "exists_in_instance": (
                        exists_in_instance
                        if exists_in_instance is not None
                        else current_data.get("exists_in_instance", False)
                    ),
                    "source": source or current_data.get("source", "discovered"),
                },
            )
        )
        return _managed_graph_from_node(node, instance)

    async def sync_graph_snapshot(
        self,
        instance_id: str,
        *,
        discovered_prefixes: set[str] | None,
        instance_status: str,
        instance_status_message: str | None,
    ) -> None:
        """Sync graph metadata against the latest discovery snapshot."""
        instance = await self.get_instance_by_id(instance_id)
        if instance is None:
            return

        existing_graphs = {
            graph.table_prefix: graph
            for graph in await self.list_graphs_for_instance(instance_id)
        }
        if discovered_prefixes is not None:
            for table_prefix in discovered_prefixes:
                if table_prefix in existing_graphs:
                    continue
                graph = await self.upsert_graph_metadata(
                    instance_id=instance_id,
                    table_prefix=table_prefix,
                    exists_in_instance=True,
                    source="discovered",
                )
                existing_graphs[table_prefix] = graph

        for table_prefix, graph in existing_graphs.items():
            exists_in_instance = (
                graph.exists_in_instance
                if discovered_prefixes is None
                else table_prefix in discovered_prefixes
            )
            if instance_status == "online":
                status = "ready" if exists_in_instance else "missing_tables"
            else:
                status = instance_status

            await self.upsert_graph_metadata(
                instance_id=instance_id,
                table_prefix=table_prefix,
                display_name=graph.display_name,
                status=status,
                status_message=instance_status_message,
                exists_in_instance=exists_in_instance,
                source=graph.source,
            )

    async def _search_nodes(
        self,
        *,
        filters: list[Filter],
        limit: int,
    ) -> list[NodeRead]:
        page = await self.db.search_nodes(
            SearchQuery(
                filter=FilterGroup(logic=Logic.AND, filters=filters),
                limit=limit,
            )
        )
        return list(page.items)

    async def _get_node_by_filters(self, filters: list[Filter]) -> NodeRead | None:
        nodes = await self._search_nodes(filters=filters, limit=1)
        if not nodes:
            return None
        return nodes[0]

    def _managed_instance_from_node(self, node: NodeRead) -> ManagedInstance:
        """Project a GPDB node into a managed instance view."""
        instance = _managed_instance_from_node(node)
        return ManagedInstance(
            id=instance.id,
            slug=instance.slug,
            display_name=instance.display_name,
            description=instance.description,
            mode=instance.mode,
            is_builtin=instance.is_builtin,
            is_default=instance.is_default,
            is_active=instance.is_active,
            connection_kind=instance.connection_kind,
            host=instance.host,
            port=instance.port,
            database=instance.database,
            username=instance.username,
            password=self._decrypt_instance_secret(instance.password),
            status=instance.status,
            status_message=instance.status_message,
            last_checked_at=instance.last_checked_at,
        )

    def _encrypt_instance_secret(self, value: str | None) -> str | None:
        """Encrypt one stored connection secret when a cipher is configured."""
        if self._instance_secret_cipher is None:
            return value
        return self._instance_secret_cipher.encrypt(value)

    def _decrypt_instance_secret(self, value: str | None) -> str | None:
        """Decrypt one stored connection secret when a cipher is configured."""
        if self._instance_secret_cipher is None:
            return value
        return self._instance_secret_cipher.decrypt(value)


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


def _managed_instance_from_node(node: NodeRead) -> ManagedInstance:
    """Project a GPDB node into a managed instance view."""
    return ManagedInstance(
        id=node.id,
        slug=str(node.data["slug"]),
        display_name=str(node.data.get("display_name") or node.data["slug"]),
        description=str(node.data.get("description") or ""),
        mode=str(node.data.get("mode") or "external"),
        is_builtin=bool(node.data.get("is_builtin", False)),
        is_default=bool(node.data.get("is_default", False)),
        is_active=bool(node.data.get("is_active", True)),
        connection_kind=str(node.data.get("connection_kind") or "postgres"),
        host=_optional_string(node.data.get("host")),
        port=_optional_int(node.data.get("port")),
        database=_optional_string(node.data.get("database")),
        username=_optional_string(node.data.get("username")),
        password=_optional_string(node.data.get("password")),
        status=str(node.data.get("status") or "checking"),
        status_message=_optional_string(node.data.get("status_message")),
        last_checked_at=_optional_string(node.data.get("last_checked_at")),
    )


def _managed_graph_from_node(node: NodeRead, instance: ManagedInstance) -> ManagedGraph:
    """Project a GPDB node into a managed graph view."""
    table_prefix = str(node.data.get("table_prefix") or "")
    return ManagedGraph(
        id=node.id,
        instance_id=instance.id,
        instance_slug=instance.slug,
        instance_display_name=instance.display_name,
        display_name=str(
            node.data.get("display_name")
            or _default_graph_display_name(table_prefix, instance.display_name)
        ),
        table_prefix=table_prefix,
        status=str(node.data.get("status") or "checking"),
        status_message=_optional_string(node.data.get("status_message")),
        last_checked_at=_optional_string(node.data.get("last_checked_at")),
        exists_in_instance=bool(node.data.get("exists_in_instance", False)),
        source=str(node.data.get("source") or "discovered"),
        is_default=table_prefix == "",
    )


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


def _default_graph_display_name(table_prefix: str, instance_display_name: str) -> str:
    """Return a default display name for one graph scope."""
    if not table_prefix:
        return f"{instance_display_name} default graph"
    return table_prefix


def _graph_node_name(table_prefix: str) -> str:
    """Return the node name used for one graph metadata record."""
    if not table_prefix:
        return DEFAULT_GRAPH_NODE_NAME
    return table_prefix


def _optional_string(value: object) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _timestamp_now() -> str:
    """Return the current UTC timestamp for admin metadata writes."""
    return datetime.now(tz=UTC).isoformat()
