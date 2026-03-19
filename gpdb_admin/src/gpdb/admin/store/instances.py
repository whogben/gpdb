"""Instance operations for the admin store."""

from __future__ import annotations

from datetime import UTC, datetime

from gpdb import Filter, FilterGroup, Logic, NodeRead, NodeUpsert, SearchQuery
from gpdb.admin.store.models import ManagedInstance
from gpdb.admin.store.exceptions import InstanceAlreadyExistsError

INSTANCE_NODE_TYPE = "instance"
DEFAULT_INSTANCE_SLUG = "default"


async def list_instances(store) -> list[ManagedInstance]:
    """Return all managed instance records."""
    nodes = await store._search_nodes(
        filters=[Filter(field="type", value=INSTANCE_NODE_TYPE)],
        limit=500,
    )
    instances = [store._managed_instance_from_node(node) for node in nodes]
    return sorted(
        instances,
        key=lambda item: (
            not item.is_builtin,
            item.display_name.lower(),
            item.slug.lower(),
        ),
    )


async def get_instance_by_id(store, instance_id: str) -> ManagedInstance | None:
    """Return one managed instance by node id."""
    try:
        nodes = await store.db.get_nodes([instance_id])
        node = nodes[0]
        if node.type != INSTANCE_NODE_TYPE:
            return None
        return store._managed_instance_from_node(node)
    except ValueError:
        return None


async def get_instance_by_slug(store, slug: str) -> ManagedInstance | None:
    """Return one managed instance by slug."""
    node = await store._get_node_by_filters(
        [
            Filter(field="type", value=INSTANCE_NODE_TYPE),
            Filter(field="data.slug", value=slug),
        ]
    )
    if node is None:
        return None
    return store._managed_instance_from_node(node)


async def ensure_builtin_instance(
    store,
    *,
    display_name: str = "Default instance",
    description: str = "Built-in captive GPDB instance managed by gpdb-admin.",
) -> ManagedInstance:
    """Create or refresh the built-in captive instance metadata."""
    existing = await store._get_node_by_filters(
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
    node_list = await store.db.set_nodes(
        [
            NodeUpsert(
                id=existing.id if existing else None,
                type=INSTANCE_NODE_TYPE,
                name=DEFAULT_INSTANCE_SLUG,
                data=payload,
            )
        ]
    )
    node = node_list[0]
    return store._managed_instance_from_node(node)


async def create_instance(
    store,
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
    if await get_instance_by_slug(store, slug):
        raise InstanceAlreadyExistsError(f"Instance '{slug}' already exists")

    node_list = await store.db.set_nodes(
        [
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
                    "password": store._encrypt_instance_secret(password),
                    "status": "checking",
                    "status_message": None,
                    "last_checked_at": None,
                },
            )
        ]
    )
    node = node_list[0]
    return store._managed_instance_from_node(node)


async def update_instance(
    store,
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
    try:
        nodes = await store.db.get_nodes([instance_id])
        node = nodes[0]
        if node.type != INSTANCE_NODE_TYPE:
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
                updated_data["password"] = store._encrypt_instance_secret(password)
            # When password is not in the update set, updated_data already has the
            # existing encrypted password from dict(node.data); no decrypt/re-encrypt.

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
        return store._managed_instance_from_node(updated)
    except ValueError:
        return None


async def delete_instance(store, instance_id: str) -> None:
    """Delete an external managed instance and its graph metadata."""
    instance = await get_instance_by_id(store, instance_id)
    if instance is None:
        return
    if instance.is_builtin:
        raise ValueError("The built-in instance cannot be deleted")

    from gpdb.admin.store.graphs import list_graphs_for_instance

    graphs = await list_graphs_for_instance(store, instance_id)
    graph_ids = [graph.id for graph in graphs]
    await store.db.delete_nodes(graph_ids + [instance_id])


async def update_instance_status(
    store,
    instance_id: str,
    *,
    status: str,
    status_message: str | None,
) -> ManagedInstance | None:
    """Persist the latest instance health status."""
    try:
        nodes = await store.db.get_nodes([instance_id])
        node = nodes[0]
        if node.type != INSTANCE_NODE_TYPE:
            return None

        updated_data = dict(node.data)
        updated_data["status"] = status
        updated_data["status_message"] = status_message
        updated_data["last_checked_at"] = _timestamp_now()
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
        return store._managed_instance_from_node(updated)
    except ValueError:
        return None


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
