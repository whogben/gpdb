"""
Edge-related methods for GPGraph.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import delete, select

from gpdb.conversions import (
    _edge_upsert_to_orm,
    _edge_orm_to_read,
)
from gpdb.models import (
    EdgeRead,
    EdgeUpsert,
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
        """
        # Reject duplicate ids in the input before doing any database writes
        edge_ids = [edge.id for edge in edges if edge.id is not None]
        if len(edge_ids) != len(set(edge_ids)):
            raise ValueError("Duplicate edge ids provided")

        # Preserve existing types and validate all edges before any writes
        edges_to_process = []
        for edge in edges:
            schema_to_validate = edge.type
            if edge.id and edge.type is None:
                async with self._get_session() as session:
                    existing = await session.get(self._Edge, edge.id)
                    if existing and existing.type:
                        schema_to_validate = existing.type
                        # Update DTO to ensure type persistence
                        edge.type = schema_to_validate

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
                    results = []
                    for edge in edges_to_process:
                        existing = None
                        if edge.id:
                            existing = await session.get(self._Edge, edge.id)

                        orm = _edge_upsert_to_orm(edge, existing, self._Edge)

                        if existing is not None:
                            # Update path
                            await session.flush()
                            await session.refresh(orm)
                            results.append(_edge_orm_to_read(orm))
                        else:
                            # Create path
                            if not orm.id:
                                orm.id = generate_id()
                            session.add(orm)
                            results.append(orm)

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

    async def get_edges(self, ids: list[str]) -> list[EdgeRead]:
        """
        Get multiple Edges.
        Returns list of EdgeRead objects.
        Fails if any requested id is missing or if duplicate ids are provided.
        """
        # Reject duplicate ids before doing any work
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate edge ids provided")

        async with self._get_session() as session:
            # Fetch all edges in a single query
            result = await session.execute(
                select(self._Edge).where(self._Edge.id.in_(ids))
            )
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
        Hard delete Edges in bulk.

        Args:
            ids: List of edge IDs to delete.

        Raises:
            ValueError: If duplicate IDs are provided or if any edge ID is not found.
        """
        # Reject duplicate ids before doing any work
        if len(ids) != len(set(ids)):
            duplicates = [id for id in ids if ids.count(id) > 1]
            raise ValueError(f"Duplicate edge ids provided: {set(duplicates)}")

        async with self._get_session() as session:
            # Check if all requested ids exist before deleting (all-or-nothing)
            result = await session.execute(
                select(self._Edge.id).where(self._Edge.id.in_(ids))
            )
            found_ids = {row[0] for row in result.all()}
            missing_ids = set(ids) - found_ids
            if missing_ids:
                raise ValueError(f"Edge ids not found: {missing_ids}")

            # Delete all edges in one operation (atomic)
            await session.execute(delete(self._Edge).where(self._Edge.id.in_(ids)))

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
