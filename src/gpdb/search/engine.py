"""
Search engine implementation for nodes and edges.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Generic, List, TypeVar

from sqlalchemy import Integer, and_, func, or_, select
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.orm import undefer

from gpdb.search.query import Filter, FilterGroup, Logic, Page, SearchQuery, Sort


T = TypeVar("T")


def _build_condition(
    model: Any,
    item: Filter | FilterGroup,
):
    """
    Recursively build SQLAlchemy conditions from Filter/FilterGroup.
    """
    if isinstance(item, Filter):
        # 1. Check for JSON path (dot notation)
        if "." in item.field:
            base, *path = item.field.split(".")
            if hasattr(model, base):
                col = getattr(model, base)
                # We assume it's a JSONB column (data, tags)

                # For equality, use JSON containment (efficient with GIN index)
                if item.op == "eq":
                    val = item.value
                    for p in reversed(path):
                        val = {p: val}
                    return col.contains(val)

                # For other ops, we need to extract the value.
                # This is tricky without knowing the type.
                # We'll implement basic extraction as text.
                expr = col
                for p in path[:-1]:
                    expr = expr[p]
                # Final element as text
                expr = expr[path[-1]].astext

                # Attempt to cast based on value type
                if isinstance(item.value, int):
                    expr = expr.cast(Integer)
                elif isinstance(item.value, float):
                    # Import Float/Numeric if needed, or just cast to Float
                    from sqlalchemy import Float

                    expr = expr.cast(Float)
                elif isinstance(item.value, bool):
                    from sqlalchemy import Boolean

                    expr = expr.cast(Boolean)
            else:
                # Field looks like path but base doesn't exist
                return False
        else:
            # Standard column
            if not hasattr(model, item.field):
                return False
            expr = getattr(model, item.field)

        # 2. Apply Operator
        if item.op == "eq":
            return expr == item.value
        if item.op == "gt":
            return expr > item.value
        if item.op == "lt":
            return expr < item.value
        if item.op == "gte":
            return expr >= item.value
        if item.op == "lte":
            return expr <= item.value
        if item.op == "ne":
            return expr != item.value
        if item.op == "contains":
            return expr.ilike(f"%{item.value}%")
        if item.op == "in" and isinstance(item.value, (list, tuple)):
            if hasattr(expr.type, "as_generic") and hasattr(
                expr.type, "python_type"
            ):
                # Check if it's JSONB (e.g. tags list)
                if expr.type.python_type in (dict, list):
                    # For JSONB arrays, "IN" implies overlap (has_any / ?|)
                    # e.g. "tags IN (a, b)" matches if tags contains "a" OR "b".
                    return expr.has_any(array(item.value))

            return expr.in_(item.value)

        return expr == item.value

    elif isinstance(item, FilterGroup):
        conditions = [_build_condition(model, f) for f in item.filters]

        # Filter out explicit False (invalid fields)
        valid_conditions = [c for c in conditions if c is not False]

        if not valid_conditions:
            # If group is empty or all invalid
            return True if item.logic == Logic.AND else False

        if item.logic == Logic.OR:
            return or_(*valid_conditions)
        return and_(*valid_conditions)


async def _search(
    model: Any,
    query: SearchQuery,
    session_getter=None,
    extra_options: List[Any] = None,
) -> Page[Any]:
    """
    Internal: Generic search for Nodes or Edges.
    """
    if query.select:
        cols = []
        for field in query.select:
            if "." in field:
                base, *path = field.split(".")
                if hasattr(model, base):
                    col = getattr(model, base)
                    # Descend into JSON path
                    expr = col
                    for p in path:
                        expr = expr[p]
                    cols.append(expr.label(field))
            elif hasattr(model, field):
                cols.append(getattr(model, field).label(field))

        if cols:
            stmt = select(*cols)
        else:
            # Fallback to full model if select list yielded no valid columns
            stmt = select(model)
    else:
        stmt = select(model)

    # 1. Apply Filters
    if query.filter:
        cond = _build_condition(model, query.filter)

        # Check if condition is valid (not True/False literals unless supported by DB)
        if cond is not True and cond is not False:
            stmt = stmt.where(cond)
        elif cond is False:
            # If condition resolved to False, return empty result immediately
            return Page(items=[], total=0, limit=query.limit, offset=query.offset)

    # 2. Apply Sorts
    for s in query.sort:
        col = getattr(model, s.field, None)
        if col is not None:
            stmt = stmt.order_by(col.desc() if s.desc else col.asc())

    # Default sort if none provided
    if not query.sort:
        stmt = stmt.order_by(model.created_at.desc())

    # 3. Apply Options (e.g. undefer)
    # Options usually apply to Model entities. If we are projecting specific columns,
    # some options might not be relevant, but we'll apply them if we are selecting the model (fallback)
    if extra_options and not query.select:
        stmt = stmt.options(*extra_options)

    # 4. Execute (Count + Fetch)
    async with session_getter() as session:
        # Total count
        # We use a subquery count to support complex where clauses safely
        subq = stmt.subquery()
        count_stmt = select(func.count()).select_from(subq)

        total = (await session.execute(count_stmt)).scalar_one()

        # Paged items
        paged_stmt = stmt.limit(query.limit).offset(query.offset)

        result = await session.execute(paged_stmt)

        if query.select:
            # Return list of dicts
            items = [r._asdict() for r in result.all()]
        else:
            # Return list of models
            items = result.scalars().all()

        return Page(
            items=items,
            total=total,
            limit=query.limit,
            offset=query.offset,
        )


async def search_nodes(
    query: SearchQuery,
    model: Any,
    session_getter,
    converter: Callable[[Any], Any] = None,
) -> Page[Any]:
    """
    Search for Nodes. Returns NodeRead without payload.

    For nodes with payload, use get_node_payloads() on individual results.
    For column projection, use search_nodes_projection().
    """
    if query.select:
        raise ValueError(
            "query.select is not supported in search_nodes(). "
            "Use search_nodes_projection() instead."
        )
    page = await _search(model, query, session_getter=session_getter)
    if converter:
        return Page(
            items=[converter(orm) for orm in page.items],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
        )
    return page


async def search_nodes_projection(
    query: SearchQuery,
    model: Any,
    session_getter,
) -> Page[Dict[str, Any]]:
    """
    Search for Nodes with field projection.
    query.select determines returned fields.
    Returns paginated dict results.
    """
    if not query.select:
        raise ValueError("query.select is required for projection search")
    return await _search(model, query, session_getter=session_getter)


async def search_edges(
    query: SearchQuery,
    model: Any,
    session_getter,
    converter: Callable[[Any], Any] = None,
) -> Page[Any]:
    """
    Search for Edges. Returns paginated EdgeRead results.

    For column projection, use search_edges_projection().
    """
    if query.select:
        raise ValueError(
            "query.select is not supported in search_edges(). "
            "Use search_edges_projection() instead."
        )
    page = await _search(model, query, session_getter=session_getter)
    if converter:
        return Page(
            items=[converter(orm) for orm in page.items],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
        )
    return page


async def search_edges_projection(
    query: SearchQuery,
    model: Any,
    session_getter,
) -> Page[Dict[str, Any]]:
    """
    Search for Edges with field projection.
    query.select determines returned fields.
    Returns paginated dict results.
    """
    if not query.select:
        raise ValueError("query.select is required for projection search")
    return await _search(model, query, session_getter=session_getter)
