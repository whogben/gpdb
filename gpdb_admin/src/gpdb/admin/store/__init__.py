"""Persistence helpers for admin identity and managed graph data."""

from __future__ import annotations

from gpdb import Filter, FilterGroup, GPGraph, Logic, NodeRead, SearchQuery
from gpdb.admin.secrets import SecretCipher

# Re-export models and exceptions
from gpdb.admin.store.models import (
    AdminAPIKey,
    AdminUser,
    ManagedGraph,
    ManagedInstance,
)
from gpdb.admin.store.exceptions import (
    GraphAlreadyExistsError,
    InstanceAlreadyExistsError,
    OwnerAlreadyExistsError,
    UserAlreadyExistsError,
)

# Import operation modules
from gpdb.admin.store import api_keys, graphs, instances, users

ADMIN_TABLE_PREFIX = "admin"


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

    # User operations
    owner_exists = users.owner_exists
    get_active_owner_user = users.get_active_owner_user
    get_user_by_username = users.get_user_by_username
    get_user_node_by_username = users.get_user_node_by_username
    get_user_by_id = users.get_user_by_id
    create_initial_owner = users.create_initial_owner
    verify_user_credentials = users.verify_user_credentials

    # API key operations
    list_api_keys_for_user = api_keys.list_api_keys_for_user
    get_api_key_by_id = api_keys.get_api_key_by_id
    get_api_key_by_key_id = api_keys.get_api_key_by_key_id
    create_api_key = api_keys.create_api_key
    reveal_api_key = api_keys.reveal_api_key
    revoke_api_key = api_keys.revoke_api_key
    authenticate_api_key = api_keys.authenticate_api_key

    # Instance operations
    list_instances = instances.list_instances
    get_instance_by_id = instances.get_instance_by_id
    get_instance_by_slug = instances.get_instance_by_slug
    ensure_builtin_instance = instances.ensure_builtin_instance
    create_instance = instances.create_instance
    update_instance = instances.update_instance
    delete_instance = instances.delete_instance
    update_instance_status = instances.update_instance_status

    # Graph operations
    list_graphs = graphs.list_graphs
    list_graphs_for_instance = graphs.list_graphs_for_instance
    get_graph_by_id = graphs.get_graph_by_id
    get_graph_by_scope = graphs.get_graph_by_scope
    update_graph = graphs.update_graph
    delete_graph = graphs.delete_graph
    upsert_graph_metadata = graphs.upsert_graph_metadata
    sync_graph_snapshot = graphs.sync_graph_snapshot

    # Private helper methods
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
        instance = instances._managed_instance_from_node(node)
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


__all__ = [
    "AdminStore",
    "AdminUser",
    "ManagedInstance",
    "ManagedGraph",
    "AdminAPIKey",
    "OwnerAlreadyExistsError",
    "UserAlreadyExistsError",
    "InstanceAlreadyExistsError",
    "GraphAlreadyExistsError",
]
