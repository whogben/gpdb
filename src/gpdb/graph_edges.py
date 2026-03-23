"""
Edge-related methods for GPGraph.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import and_, select

from gpdb.conversions import (
    _edge_upsert_to_orm,
    _edge_orm_to_read,
)
from gpdb.events import (
    GraphEvent,
    NodeDestinationEdgeCreatedEvent,
    NodeDestinationEdgeDeletedEvent,
    NodeDestinationEdgeUpdatedEvent,
    NodeOriginEdgeCreatedEvent,
    NodeOriginEdgeDeletedEvent,
    NodeOriginEdgeUpdatedEvent,
)
from gpdb.models import (
    EdgeRead,
    EdgeUpsert,
    TombstoneAlreadyDeletedError,
    _ID_MAX_COLLISION_ATTEMPTS,
    generate_id,
)


class EdgeMixin:
    """Mixin class providing edge-related methods for GPGraph."""

    async def set_edges(self, edges: list[EdgeUpsert]) -> list[EdgeRead]:
        """
        Upsert multiple Edges.
        Creates if new, updates if existing (matched by id).
        All operations are performed atomically in a single transaction.

        On update, ``type`` is immutable: omit it or re-send the same value; a
        different type raises ``RecordTypeImmutableError`` from the conversion
        layer. Update data is always validated against the stored type.
        """
        # Reject duplicate ids in the input before doing any database writes
        edge_ids = [edge.id for edge in edges if edge.id is not None]
        if len(edge_ids) != len(set(edge_ids)):
            raise ValueError("Duplicate edge ids provided")

        update_edge_ids = [e.id for e in edges if e.id is not None]
        existing_by_id: dict[str, Any] = {}
        if update_edge_ids:
            async with self._get_session() as session:
                er = await session.execute(
                    select(self._Edge).where(self._Edge.id.in_(update_edge_ids))
                )
                for row in er.scalars().all():
                    existing_by_id[row.id] = row

        edges_to_process = []
        for edge in edges:
            ex_orm = existing_by_id.get(edge.id) if edge.id else None
            if edge.id and ex_orm is not None:
                schema_to_validate = ex_orm.type
            else:
                schema_to_validate = edge.type

            # Validate schema exists (except for __default__)
            if schema_to_validate and schema_to_validate != "__default__":
                from gpdb.models import SchemaRef
                ref = SchemaRef(name=schema_to_validate, kind="edge")
                try:
                    await self._get_schema_by_ref(ref)
                except Exception:
                    from gpdb.models import SchemaNotFoundError
                    raise SchemaNotFoundError(
                        f"Schema '{schema_to_validate}' not found for edge type"
                    )

            # Validate data against schema if type is provided
            if schema_to_validate:
                await self._validate_data(
                    schema_to_validate,
                    edge.data,
                    expected_kind="edge",
                )
            edges_to_process.append(edge)

        # Perform all operations atomically in a single transaction
        for attempt in range(_ID_MAX_COLLISION_ATTEMPTS):
            try:
                async with self._get_session() as session:
                    all_nt_ids: set[str] = set()
                    for edge in edges_to_process:
                        all_nt_ids.add(edge.source_id)
                        all_nt_ids.add(edge.target_id)
                    if all_nt_ids:
                        nd_res = await session.execute(
                            select(self._Node).where(self._Node.id.in_(all_nt_ids))
                        )
                        live_nodes = {n.id: n for n in nd_res.scalars().all()}
                        for edge in edges_to_process:
                            for label, nid in (
                                ("source", edge.source_id),
                                ("target", edge.target_id),
                            ):
                                nn = live_nodes.get(nid)
                                if nn is None or nn.deleted_at is not None:
                                    raise ValueError(
                                        f"Edge {label}_id refers to missing or deleted node: {nid}"
                                    )

                    results = []
                    edge_was_create: list[bool] = []
                    for edge in edges_to_process:
                        existing = None
                        if edge.id:
                            existing = await session.get(self._Edge, edge.id)

                        if existing is not None and existing.deleted_at is not None:
                            raise ValueError(f"Cannot update deleted edge: {edge.id}")

                        orm = _edge_upsert_to_orm(edge, existing, self._Edge)

                        if existing is not None:
                            # Update path
                            await session.flush()
                            await session.refresh(orm)
                            results.append(_edge_orm_to_read(orm))
                            edge_was_create.append(False)
                        else:
                            # Create path
                            if not orm.id:
                                orm.id = generate_id()
                            session.add(orm)
                            results.append(orm)
                            edge_was_create.append(True)

                    # Flush all creates at once
                    await session.flush()

                    # Refresh and convert all created edges
                    final_results = []
                    for i, result in enumerate(results):
                        if isinstance(result, EdgeRead):
                            # This was an update, already converted
                            final_results.append(result)
                        else:
                            # This was a create, need to refresh and convert
                            await session.refresh(result)
                            final_results.append(_edge_orm_to_read(result))

                    prefix = self.table_prefix
                    evs: list[GraphEvent] = []
                    for er, was_create in zip(final_results, edge_was_create):
                        st = (
                            live_nodes[er.source_id].type
                            if er.source_id and er.source_id in live_nodes
                            else None
                        )
                        tt = (
                            live_nodes[er.target_id].type
                            if er.target_id and er.target_id in live_nodes
                            else None
                        )
                        if was_create:
                            evs.append(
                                NodeOriginEdgeCreatedEvent(
                                    table_prefix=prefix,
                                    occurred_at=er.created_at,
                                    edge_id=er.id,
                                    edge_type=er.type,
                                    source_id=er.source_id or "",
                                    target_id=er.target_id or "",
                                    source_node_type=st,
                                    target_node_type=tt,
                                )
                            )
                            evs.append(
                                NodeDestinationEdgeCreatedEvent(
                                    table_prefix=prefix,
                                    occurred_at=er.created_at,
                                    edge_id=er.id,
                                    edge_type=er.type,
                                    source_id=er.source_id or "",
                                    target_id=er.target_id or "",
                                    source_node_type=st,
                                    target_node_type=tt,
                                )
                            )
                        else:
                            evs.append(
                                NodeOriginEdgeUpdatedEvent(
                                    table_prefix=prefix,
                                    occurred_at=er.updated_at,
                                    edge_id=er.id,
                                    edge_type=er.type,
                                    source_id=er.source_id or "",
                                    target_id=er.target_id or "",
                                    source_node_type=st,
                                    target_node_type=tt,
                                )
                            )
                            evs.append(
                                NodeDestinationEdgeUpdatedEvent(
                                    table_prefix=prefix,
                                    occurred_at=er.updated_at,
                                    edge_id=er.id,
                                    edge_type=er.type,
                                    source_id=er.source_id or "",
                                    target_id=er.target_id or "",
                                    source_node_type=st,
                                    target_node_type=tt,
                                )
                            )
                    self._record_graph_events(session, evs)

                    return final_results
            except Exception as e:
                from gpdb.models.base import _is_primary_key_violation

                if not _is_primary_key_violation(e):
                    raise
                # If this is the last attempt, raise the error
                if attempt == _ID_MAX_COLLISION_ATTEMPTS - 1:
                    raise RuntimeError(
                        "Failed to generate unique edge IDs after "
                        f"{_ID_MAX_COLLISION_ATTEMPTS} attempts."
                    )
                # Otherwise, retry the entire batch
        raise RuntimeError(
            "Failed to generate unique edge IDs after "
            f"{_ID_MAX_COLLISION_ATTEMPTS} attempts."
        )

    async def get_edges(
        self, ids: list[str], *, include_deleted: bool = False
    ) -> list[EdgeRead]:
        """
        Get multiple Edges.
        Returns list of EdgeRead objects.
        Fails if any requested id is missing or if duplicate ids are provided.
        Tombstoned edges are excluded unless include_deleted is True.
        """
        # Reject duplicate ids before doing any work
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate edge ids provided")

        async with self._get_session() as session:
            conds = [self._Edge.id.in_(ids)]
            if not include_deleted:
                conds.append(self._Edge.deleted_at.is_(None))
            result = await session.execute(select(self._Edge).where(and_(*conds)))
            orms = result.scalars().all()

            # Check if all requested ids were found
            found_ids = {orm.id for orm in orms}
            missing_ids = set(ids) - found_ids
            if missing_ids:
                raise ValueError(f"Edge ids not found: {missing_ids}")

            # Preserve input order in returned results
            id_to_orm = {orm.id: orm for orm in orms}
            return [_edge_orm_to_read(id_to_orm[edge_id]) for edge_id in ids]

    async def delete_edges(self, ids: list[str]):
        """
        Tombstone edges in bulk: clear data; set deleted_at.

        ``type``, ``source_id``, and ``target_id`` are preserved for audit, event filters,
        and schema lifecycle (schema delete only counts *live* rows; see ``delete_schemas``).

        Raises:
            ValueError: If duplicate IDs are provided or if any edge ID is not found.
            TombstoneAlreadyDeletedError: If any edge is already tombstoned.
        """
        # Reject duplicate ids before doing any work
        if len(ids) != len(set(ids)):
            duplicates = [id for id in ids if ids.count(id) > 1]
            raise ValueError(f"Duplicate edge ids provided: {set(duplicates)}")

        async with self._get_session() as session:
            result = await session.execute(
                select(self._Edge).where(self._Edge.id.in_(ids))
            )
            orms = list(result.scalars().all())
            found_ids = {orm.id for orm in orms}
            missing_ids = set(ids) - found_ids
            if missing_ids:
                raise ValueError(f"Edge ids not found: {missing_ids}")

            already = [o.id for o in orms if o.deleted_at is not None]
            if already:
                raise TombstoneAlreadyDeletedError(
                    f"Edge id(s) already deleted: {already}"
                )

            nt_ids: set[str] = set()
            for o in orms:
                if o.source_id:
                    nt_ids.add(o.source_id)
                if o.target_id:
                    nt_ids.add(o.target_id)
            nt_types: dict[str, str | None] = {}
            if nt_ids:
                nres = await session.execute(
                    select(self._Node).where(self._Node.id.in_(nt_ids))
                )
                for row in nres.scalars().all():
                    nt_types[row.id] = row.type

            now = datetime.now(timezone.utc)
            for orm in orms:
                orm.deleted_at = now
                orm.data = {}
            await session.flush()
            for orm in orms:
                await session.refresh(orm)

            prefix = self.table_prefix
            evs: list[GraphEvent] = []
            for orm in orms:
                st = nt_types.get(orm.source_id) if orm.source_id else None
                tt = nt_types.get(orm.target_id) if orm.target_id else None
                evs.append(
                    NodeOriginEdgeDeletedEvent(
                        table_prefix=prefix,
                        occurred_at=orm.deleted_at,
                        edge_id=orm.id,
                        edge_type=orm.type,
                        source_id=orm.source_id or "",
                        target_id=orm.target_id or "",
                        source_node_type=st,
                        target_node_type=tt,
                    )
                )
                evs.append(
                    NodeDestinationEdgeDeletedEvent(
                        table_prefix=prefix,
                        occurred_at=orm.deleted_at,
                        edge_id=orm.id,
                        edge_type=orm.type,
                        source_id=orm.source_id or "",
                        target_id=orm.target_id or "",
                        source_node_type=st,
                        target_node_type=tt,
                    )
                )
            self._record_graph_events(session, evs)

    async def search_edges(self, query: Any) -> Any:
        """
        Search for Edges. Returns paginated EdgeRead results.

        For column projection, use search_edges_projection().
        """
        from gpdb.search import search_edges

        if query.select:
            raise ValueError(
                "query.select is not supported in search_edges(). "
                "Use search_edges_projection() instead."
            )
        return await search_edges(
            query,
            model=self._Edge,
            session_getter=self._get_session,
            converter=_edge_orm_to_read,
        )

    async def search_edges_projection(
        self,
        query: Any,
    ) -> Any:
        """
        Search for Edges with field projection.
        query.select determines returned fields.
        Returns paginated dict results.
        """
        from gpdb.search import search_edges_projection

        if not query.select:
            raise ValueError("query.select is required for projection search")
        return await search_edges_projection(
            query,
            model=self._Edge,
            session_getter=self._get_session,
        )
