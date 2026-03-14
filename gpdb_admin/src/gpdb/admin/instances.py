"""Managed instance connection and graph discovery helpers."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
import logging

from sqlalchemy import text
from sqlalchemy.engine import URL

from gpdb import GPGraph
from gpdb.admin.store import (
    ADMIN_TABLE_PREFIX,
    AdminStore,
    GraphAlreadyExistsError,
    ManagedGraph,
    ManagedInstance,
)

HEALTH_CHECK_INTERVAL_SECONDS = 15


def build_postgres_url(instance: ManagedInstance) -> str:
    """Build an async PostgreSQL URL from a structured instance record."""
    if instance.mode == "captive":
        raise ValueError("Captive instances use the runtime-managed URL")
    if not instance.database:
        raise ValueError("Managed instance is missing a database name")

    query: dict[str, str] = {}
    host = instance.host or "127.0.0.1"
    url_host: str | None = host

    # SQLAlchemy represents unix socket connections as query params.
    if host.startswith("/"):
        url_host = None
        query["host"] = host
        if instance.port is not None:
            query["port"] = str(instance.port)

    return URL.create(
        "postgresql+asyncpg",
        username=instance.username or None,
        password=instance.password or None,
        host=url_host,
        port=instance.port if url_host is not None else None,
        database=instance.database,
        query=query,
    ).render_as_string(hide_password=False)


class ManagedInstanceMonitor:
    """Refresh instance health and synchronize discovered graphs."""

    def __init__(
        self,
        *,
        admin_store: AdminStore,
        captive_url_factory: Callable[[], str],
    ) -> None:
        self.admin_store = admin_store
        self._captive_url_factory = captive_url_factory
        self._lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the periodic background health monitor."""
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Stop the periodic background health monitor."""
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def refresh_all(self) -> None:
        """Refresh status and discovered graphs for every managed instance."""
        async with self._lock:
            instances = await self.admin_store.list_instances()
            for instance in instances:
                await self._refresh_instance(instance)

    async def refresh_instance(self, instance_id: str) -> None:
        """Refresh status and discovered graphs for one managed instance."""
        async with self._lock:
            instance = await self.admin_store.get_instance_by_id(instance_id)
            if instance is None:
                return
            await self._refresh_instance(instance)

    async def create_graph(
        self,
        *,
        instance_id: str,
        table_prefix: str,
        display_name: str | None = None,
    ) -> ManagedGraph:
        """Create a new graph's tables and metadata."""
        instance = await self.admin_store.get_instance_by_id(instance_id)
        if instance is None:
            raise ValueError("Managed instance was not found")
        if not table_prefix:
            raise ValueError("New graphs must use a non-empty table prefix")

        # Check if graph already exists
        existing_graph = await self.admin_store.get_graph_by_scope(
            instance_id, table_prefix
        )
        if existing_graph is not None:
            raise GraphAlreadyExistsError(
                f"Graph '{table_prefix}' already exists for instance '{instance_id}'"
            )

        db = GPGraph(self._resolve_instance_url(instance), table_prefix=table_prefix)
        try:
            await db.create_tables()
        finally:
            await db.sqla_engine.dispose()

        await self.admin_store.upsert_graph_metadata(
            instance_id=instance_id,
            table_prefix=table_prefix,
            display_name=display_name,
            source="managed",
        )
        await self.refresh_instance(instance_id)
        graph = await self.admin_store.get_graph_by_scope(instance_id, table_prefix)
        if graph is None:
            raise RuntimeError("Managed graph was not found after creation")
        return graph

    async def delete_graph(self, graph_id: str) -> None:
        """Drop a managed graph's tables and remove its metadata."""
        graph = await self.admin_store.get_graph_by_id(graph_id)
        if graph is None:
            raise ValueError("Managed graph was not found")
        if not graph.table_prefix:
            raise ValueError("The default graph cannot be deleted")

        instance = await self.admin_store.get_instance_by_id(graph.instance_id)
        if instance is None:
            raise ValueError("Managed instance was not found")

        db = GPGraph(
            self._resolve_instance_url(instance), table_prefix=graph.table_prefix
        )
        try:
            await db.drop_tables()
        finally:
            await db.sqla_engine.dispose()

        await self.admin_store.delete_graph(graph_id)
        await self.refresh_instance(instance.id)

    def _resolve_instance_url(self, instance: ManagedInstance) -> str:
        if instance.mode == "captive":
            return self._captive_url_factory()
        return build_postgres_url(instance)

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.refresh_all()
            except Exception:
                # Keep monitoring alive even if one refresh cycle fails.
                logging.exception("Managed instance monitor refresh loop failed")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=HEALTH_CHECK_INTERVAL_SECONDS,
                )
            except asyncio.TimeoutError:
                continue

    async def _refresh_instance(self, instance: ManagedInstance) -> None:
        if not instance.is_active:
            await self.admin_store.update_instance_status(
                instance.id,
                status="disabled",
                status_message="Instance is disabled.",
            )
            await self.admin_store.sync_graph_snapshot(
                instance.id,
                discovered_prefixes=None,
                instance_status="disabled",
                instance_status_message="Instance is disabled.",
            )
            return

        try:
            discovered_prefixes = await self._discover_graph_prefixes(
                self._resolve_instance_url(instance)
            )
            if instance.is_builtin and ADMIN_TABLE_PREFIX in discovered_prefixes:
                discovered_prefixes.remove(ADMIN_TABLE_PREFIX)
            await self.admin_store.update_instance_status(
                instance.id,
                status="online",
                status_message=None,
            )
            await self.admin_store.sync_graph_snapshot(
                instance.id,
                discovered_prefixes=discovered_prefixes,
                instance_status="online",
                instance_status_message=None,
            )
        except Exception as exc:
            await self.admin_store.update_instance_status(
                instance.id,
                status="offline",
                status_message=str(exc),
            )
            await self.admin_store.sync_graph_snapshot(
                instance.id,
                discovered_prefixes=None,
                instance_status="offline",
                instance_status_message=str(exc),
            )

    async def _discover_graph_prefixes(self, url: str) -> set[str]:
        db = GPGraph(url)
        try:
            async with db.sqla_engine.connect() as conn:
                result = await conn.execute(
                    text(
                        "select tablename from pg_tables "
                        "where schemaname = current_schema()"
                    )
                )
                table_names = {str(name) for name in result.scalars().all()}
        finally:
            await db.sqla_engine.dispose()

        prefixes: set[str] = set()
        if {"nodes", "edges", "schemas"}.issubset(table_names):
            prefixes.add("")

        for table_name in table_names:
            if not table_name.endswith("_nodes"):
                continue
            prefix = table_name[: -len("_nodes")]
            if not prefix:
                continue
            if {
                f"{prefix}_nodes",
                f"{prefix}_edges",
                f"{prefix}_schemas",
            }.issubset(table_names):
                prefixes.add(prefix)
        return prefixes
