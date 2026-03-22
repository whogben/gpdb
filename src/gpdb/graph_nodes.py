"""
Node-related methods for GPGraph.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import undefer

from gpdb.conversions import (
    _node_upsert_to_orm,
    _node_orm_to_read,
    _node_orm_to_read_with_payload,
)
from gpdb.models import (
    NodeRead,
    NodeReadWithPayload,
    NodeUpsert,
    TombstoneAlreadyDeletedError,
    TombstoneDeleteBlockedError,
    _ID_MAX_COLLISION_ATTEMPTS,
    generate_id,
)


class NodeMixin:
    """Mixin class providing node-related methods for GPGraph."""

    async def set_nodes(self, nodes: list[NodeUpsert]) -> list[NodeRead]:
        """
        Upsert multiple Nodes.
        Creates if new, updates if existing (matched by id).
        Returns list of NodeRead objects (without payload).

        Note: If node.payload is provided, it will be stored.
        For updating only payload, use set_node_payload().

        Rejects duplicate ids in the input before any database writes.
        Performs the entire batch atomically in a single transaction.
        Preserves the existing semantics of omitted fields on update paths.
        """
        if not nodes:
            return []

        # Reject duplicate ids before doing any database writes
        node_ids = [node.id for node in nodes if node.id is not None]
        if len(node_ids) != len(set(node_ids)):
            duplicates = [id for id in node_ids if node_ids.count(id) > 1]
            raise ValueError(f"Duplicate node ids provided: {set(duplicates)}")

        # Perform all operations atomically in a single transaction
        for attempt in range(_ID_MAX_COLLISION_ATTEMPTS):
            try:
                async with self._get_session() as session:
                    # Fetch all explicitly provided ids in one query.
                    # For nodes without an explicit id, we treat them as creates
                    # (even if the generated id collides), so collisions must
                    # raise and trigger retry logic rather than performing a
                    # silent update.
                    existing_ids = [node.id for node in nodes if node.id is not None]
                    existing_map = {}
                    if existing_ids:
                        stmt = select(self._Node).where(self._Node.id.in_(existing_ids))
                        result = await session.execute(stmt)
                        for orm in result.scalars().all():
                            existing_map[orm.id] = orm

                    parent_ids_to_check: set[str] = set()
                    for node in nodes:
                        if node.id is not None and node.id in existing_map:
                            ex = existing_map[node.id]
                            if "parent_id" in node.model_fields_set:
                                pid = node.parent_id
                            else:
                                pid = ex.parent_id
                        else:
                            pid = node.parent_id
                        if pid is not None:
                            parent_ids_to_check.add(pid)

                    parent_map: Dict[str, Any] = {}
                    if parent_ids_to_check:
                        pr = await session.execute(
                            select(self._Node).where(
                                self._Node.id.in_(parent_ids_to_check),
                                self._Node.deleted_at.is_(None),
                            )
                        )
                        for p in pr.scalars().all():
                            parent_map[p.id] = p

                    # Validate schemas and prepare ORM objects
                    orms = []
                    for node in nodes:
                        explicit_id = node.id is not None
                        node_for_attempt = node
                        existing = existing_map.get(node.id) if explicit_id else None

                        if explicit_id and existing is not None and existing.deleted_at is not None:
                            raise ValueError(f"Cannot update deleted node: {node.id}")

                        if not explicit_id:
                            # Generate ids in Python so tests can patch `generate_id()`.
                            # On collision, we will retry the whole batch.
                            node_for_attempt = node.model_copy(update={"id": generate_id()})
                            existing = None  # Treat as create even if it collides.

                        # Use type as the schema name
                        schema_to_validate = node_for_attempt.type
                        if explicit_id and existing is not None and node_for_attempt.type is None and existing.type:
                            # Preserve type on omitted type updates, while still validating
                            # the provided data against the existing schema.
                            schema_to_validate = existing.type
                            node_for_attempt = node_for_attempt.model_copy(
                                update={"type": schema_to_validate}
                            )

                        # Validate schema exists (except for __default__)
                        if schema_to_validate and schema_to_validate != "__default__":
                            from gpdb.models import SchemaRef
                            ref = SchemaRef(name=schema_to_validate, kind="node")
                            try:
                                await self._get_schema_by_ref(ref)
                            except Exception:
                                from gpdb.models import SchemaNotFoundError
                                raise SchemaNotFoundError(
                                    f"Schema '{schema_to_validate}' not found for node type"
                                )

                        if schema_to_validate:
                            await self._validate_data(
                                schema_to_validate,
                                node_for_attempt.data,
                                expected_kind="node",
                            )

                        if explicit_id and existing is not None:
                            eff_parent = (
                                node_for_attempt.parent_id
                                if "parent_id" in node.model_fields_set
                                else existing.parent_id
                            )
                        else:
                            eff_parent = node_for_attempt.parent_id
                        if eff_parent is not None:
                            p_orm = parent_map.get(eff_parent)
                            if p_orm is None or p_orm.deleted_at is not None:
                                raise ValueError(
                                    f"Parent node not found or deleted: {eff_parent}"
                                )

                        orm = _node_upsert_to_orm(node_for_attempt, existing, self._Node)
                        orms.append(orm)

                    # Add all ORM objects to session
                    for orm in orms:
                        session.add(orm)

                    # Flush to generate IDs and validate constraints
                    await session.flush()

                    # Refresh all ORM objects to get generated IDs and timestamps
                    for orm in orms:
                        await session.refresh(orm)

                    # Return results in input order
                    return [_node_orm_to_read(orm) for orm in orms]

            except Exception as e:
                from gpdb.models.base import _is_primary_key_violation

                if _is_primary_key_violation(e):
                    if attempt == _ID_MAX_COLLISION_ATTEMPTS - 1:
                        raise RuntimeError(
                            f"Failed to generate unique node ID after {_ID_MAX_COLLISION_ATTEMPTS} attempts."
                        ) from e
                    # Otherwise, retry with new IDs for creates
                    continue
                # Non-PK errors should never be masked by the retry limit.
                raise

        # This should never be reached, but satisfy the type checker
        raise RuntimeError(
            f"Failed to generate unique node ID after {_ID_MAX_COLLISION_ATTEMPTS} attempts."
        )

    async def get_nodes(
        self, ids: list[str], *, include_deleted: bool = False
    ) -> list[NodeRead]:
        """
        Get multiple Nodes without payload.
        Returns list of NodeRead objects.
        Raises ValueError if duplicate ids are provided.
        Raises ValueError if any requested id is not found (tombstones excluded unless include_deleted).
        Preserves input order in returned results.
        """
        if not ids:
            return []

        # Reject duplicate ids
        if len(ids) != len(set(ids)):
            duplicates = [id for id in ids if ids.count(id) > 1]
            raise ValueError(f"Duplicate node ids provided: {set(duplicates)}")

        async with self._get_session() as session:
            conds = [self._Node.id.in_(ids)]
            if not include_deleted:
                conds.append(self._Node.deleted_at.is_(None))
            stmt = select(self._Node).where(and_(*conds))
            result = await session.execute(stmt)
            orms = result.scalars().all()

            # Create a mapping of id to orm for quick lookup
            orm_map = {orm.id: orm for orm in orms}

            # Check if any requested id is missing
            missing_ids = [id for id in ids if id not in orm_map]
            if missing_ids:
                raise ValueError(f"Node ids not found: {missing_ids}")

            # Return results in input order
            return [_node_orm_to_read(orm_map[id]) for id in ids]

    async def get_node_payloads(
        self, ids: list[str], *, include_deleted: bool = False
    ) -> list[NodeReadWithPayload]:
        """
        Get multiple Nodes with payloads included.
        Returns list of NodeReadWithPayload in the same order as input ids.
        Raises ValueError if any requested id is missing or if duplicate ids are provided.
        Nodes without payload are still returned with id filled and no payload bytes.
        """
        # Reject duplicate ids before doing any work
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate node ids provided")

        async with self._get_session() as session:
            conds = [self._Node.id.in_(ids)]
            if not include_deleted:
                conds.append(self._Node.deleted_at.is_(None))
            stmt = (
                select(self._Node)
                .where(and_(*conds))
                .options(undefer(self._Node.payload))
            )
            result = await session.execute(stmt)
            orms = result.scalars().all()

            # Create a mapping of id to orm for ordered lookup
            orm_map = {orm.id: orm for orm in orms}

            # Verify all requested ids exist
            missing_ids = [id for id in ids if id not in orm_map]
            if missing_ids:
                raise ValueError(f"Node(s) not found: {', '.join(missing_ids)}")

            # Return results in input order
            return [_node_orm_to_read_with_payload(orm_map[id]) for id in ids]

    async def get_node_payload(
        self, id: str, *, include_deleted: bool = False
    ) -> bytes | None:
        """
        Get only the payload bytes for a Node.
        Returns bytes if node exists and has payload, None if missing, no payload, or tombstoned (unless include_deleted).
        """
        async with self._get_session() as session:
            conds = [self._Node.id == id]
            if not include_deleted:
                conds.append(self._Node.deleted_at.is_(None))
            stmt = select(self._Node.payload).where(and_(*conds))
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def set_node_payload(
        self,
        id: str,
        payload: bytes,
        mime: str | None = None,
        filename: str | None = None,
    ) -> NodeRead:
        """
        Set payload for an existing Node.
        Auto-calculates size and hash.
        Returns updated NodeRead.
        """
        async with self._get_session() as session:
            orm = await session.get(self._Node, id)
            if orm is None or orm.deleted_at is not None:
                raise ValueError(f"Node not found: {id}")
            orm.payload = payload
            if mime is not None:  # Only update mime when explicitly provided
                orm.payload_mime = mime
            if filename is not None:  # Only update filename when explicitly provided
                orm.payload_filename = filename
            await session.flush()
            await session.refresh(orm)
            return _node_orm_to_read(orm)

    async def clear_node_payload(self, id: str) -> NodeRead:
        """
        Remove payload bytes and payload metadata from an existing Node.
        Returns updated NodeRead.
        """
        async with self._get_session() as session:
            orm = await session.get(self._Node, id)
            if orm is None or orm.deleted_at is not None:
                raise ValueError(f"Node not found: {id}")
            orm.payload = None
            await session.flush()
            await session.refresh(orm)
            return _node_orm_to_read(orm)

    async def get_node_child(
        self, parent_id: str, name: str, *, include_deleted: bool = False
    ) -> NodeRead | None:
        """
        Get a child node by name under a specific parent.
        Returns NodeRead if found, None if not found.
        """
        async with self._get_session() as session:
            conds = [
                self._Node.parent_id == parent_id,
                self._Node.name == name,
            ]
            if not include_deleted:
                conds.append(self._Node.deleted_at.is_(None))
            stmt = select(self._Node).where(and_(*conds))
            result = await session.execute(stmt)
            orm = result.scalar_one_or_none()
            if orm is None:
                return None
            return _node_orm_to_read(orm)

    async def delete_nodes(self, ids: list[str]) -> None:
        """
        Tombstone multiple nodes: strip content, clear type, set deleted_at.
        Fails if live edges or child nodes reference any id, or if any id is already tombstoned.

        Rejects duplicate ids before doing any work.
        If any deletion would fail, fails the entire batch.
        """
        if not ids:
            return

        # Reject duplicate ids
        if len(ids) != len(set(ids)):
            duplicates = [id for id in ids if ids.count(id) > 1]
            raise ValueError(f"Duplicate node ids: {duplicates}")

        async with self._get_session() as session:
            stmt = select(self._Node).where(self._Node.id.in_(ids))
            result = await session.execute(stmt)
            orms = list(result.scalars().all())
            found_ids = {orm.id for orm in orms}
            missing_ids = [id for id in ids if id not in found_ids]
            if missing_ids:
                raise ValueError(f"Node ids not found: {missing_ids}")

            already = [o.id for o in orms if o.deleted_at is not None]
            if already:
                raise TombstoneAlreadyDeletedError(
                    f"Node id(s) already deleted: {already}"
                )

            edge_stmt = select(self._Edge.id).where(
                self._Edge.deleted_at.is_(None),
                or_(
                    self._Edge.source_id.in_(ids),
                    self._Edge.target_id.in_(ids),
                ),
            )
            edge_result = await session.execute(edge_stmt)
            if edge_result.first() is not None:
                raise TombstoneDeleteBlockedError(
                    "Cannot delete nodes while live edges still reference them"
                )

            child_stmt = select(self._Node.id).where(
                self._Node.deleted_at.is_(None),
                self._Node.parent_id.in_(ids),
            )
            child_result = await session.execute(child_stmt)
            if child_result.first() is not None:
                raise TombstoneDeleteBlockedError(
                    "Cannot delete nodes while live child nodes still reference them as parent"
                )

            now = datetime.now(timezone.utc)
            for orm in orms:
                orm.deleted_at = now
                orm.type = None
                orm.data = {}
                orm.payload = None
                orm.payload_size = 0
                orm.payload_hash = None
                orm.payload_mime = None
                orm.payload_filename = None
            await session.flush()

    async def search_nodes(self, query: Any) -> Any:
        """
        Search for Nodes. Returns NodeRead without payload.

        For nodes with payload, use get_node_payloads() on individual results.
        For column projection, use search_nodes_projection().
        """
        from gpdb.search import search_nodes

        if query.select:
            raise ValueError(
                "query.select is not supported in search_nodes(). "
                "Use search_nodes_projection() instead."
            )
        return await search_nodes(
            query,
            model=self._Node,
            session_getter=self._get_session,
            converter=_node_orm_to_read,
        )

    async def search_nodes_projection(
        self,
        query: Any,
    ) -> Any:
        """
        Search for Nodes with field projection.
        query.select determines returned fields.
        Returns paginated dict results.
        """
        from gpdb.search import search_nodes_projection

        if not query.select:
            raise ValueError("query.select is required for projection search")
        return await search_nodes_projection(
            query,
            model=self._Node,
            session_getter=self._get_session,
        )
