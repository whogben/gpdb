"""Instance operations for graph-content service."""

from __future__ import annotations

from gpdb.admin.store import AdminStore, AdminUser, InstanceAlreadyExistsError

from gpdb.admin.graph_content.exceptions import (
    GraphContentConflictError,
    GraphContentNotFoundError,
)
from gpdb.admin.graph_content.models import InstanceDetail, InstanceList
from gpdb.admin.graph_content._helpers import (
    serialize_instance,
    serialize_instance_record,
    require_admin_store,
)


async def list_instances(
    self,
    *,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> InstanceList:
    """Return all managed instances for the authenticated caller."""
    _ = (current_user, allow_local_system)
    admin_store = require_admin_store(self._admin_store)
    instances = await admin_store.list_instances()
    items = [serialize_instance_record(instance) for instance in instances]
    return InstanceList(items=items, total=len(items))


async def get_instance(
    self,
    *,
    instance_id: str,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> InstanceDetail:
    """Return one managed instance for the authenticated caller."""
    _ = (current_user, allow_local_system)
    admin_store = require_admin_store(self._admin_store)
    instance = await admin_store.get_instance_by_id(instance_id)
    if instance is None:
        raise GraphContentNotFoundError(f"Instance '{instance_id}' was not found.")
    return InstanceDetail(instance=serialize_instance_record(instance))


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
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> InstanceDetail:
    """Create one external managed instance for the authenticated caller."""
    _ = (current_user, allow_local_system)
    admin_store = require_admin_store(self._admin_store)
    try:
        instance = await admin_store.create_instance(
            slug=slug,
            display_name=display_name,
            description=description,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
        )
    except InstanceAlreadyExistsError as exc:
        raise GraphContentConflictError(str(exc)) from exc
    return InstanceDetail(instance=serialize_instance_record(instance))


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
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> InstanceDetail:
    """Update one managed instance's metadata and connection fields. Omitted fields are left unchanged."""
    _ = (current_user, allow_local_system)
    admin_store = require_admin_store(self._admin_store)
    instance = await admin_store.update_instance(
        instance_id=instance_id,
        display_name=display_name,
        description=description,
        is_active=is_active,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
    )
    if instance is None:
        raise GraphContentNotFoundError(f"Instance '{instance_id}' was not found.")
    return InstanceDetail(instance=serialize_instance_record(instance))


async def delete_instance(
    self,
    *,
    instance_id: str,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> None:
    """Delete one external managed instance for the authenticated caller."""
    _ = (current_user, allow_local_system)
    admin_store = require_admin_store(self._admin_store)
    instance = await admin_store.get_instance_by_id(instance_id)
    if instance is None:
        raise GraphContentNotFoundError(f"Instance '{instance_id}' was not found.")
    await admin_store.delete_instance(instance_id)
