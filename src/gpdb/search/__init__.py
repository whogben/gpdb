"""
Search package - re-exports for backward compatibility.
"""

from __future__ import annotations

from gpdb.search.query import (
    Op,
    Logic,
    Filter,
    FilterGroup,
    Sort,
    SearchQuery,
    Page,
)
from gpdb.search.parser import (
    _value_to_dsl,
    _tokenize,
    _parse_value,
    _parse_list,
    _parse_filter,
    _parse_primary,
    _parse_and_expr,
    _parse_expr,
)
from gpdb.search.engine import (
    _build_condition,
    _search,
    search_nodes,
    search_edges,
    search_nodes_projection,
    search_edges_projection,
)

__all__ = [
    # Query types
    "Op",
    "Logic",
    "Filter",
    "FilterGroup",
    "Sort",
    "SearchQuery",
    "Page",
    # Parser functions
    "_value_to_dsl",
    "_tokenize",
    "_parse_value",
    "_parse_list",
    "_parse_filter",
    "_parse_primary",
    "_parse_and_expr",
    "_parse_expr",
    # Engine functions
    "_build_condition",
    "_search",
    # Public search methods
    "search_nodes",
    "search_edges",
    "search_nodes_projection",
    "search_edges_projection",
]
