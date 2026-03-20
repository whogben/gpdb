"""
Pure functions for schema inheritance operations.

This module provides deterministic, side-effect-free functions for:
- Extracting property keys from JSON schemas
- Merging object-style JSON schemas
- Computing effective schemas from inheritance
- Building and validating inheritance graphs
"""

from __future__ import annotations

from typing import Any


def top_level_property_keys(schema: dict) -> frozenset[str]:
    """
    Extract keys of properties only from a JSON schema.

    If properties is missing, treat as empty (return empty frozenset).
    This is for the additive rule that applies to declared top-level property names only.

    Args:
        schema: A JSON schema dict

    Returns:
        frozenset of property keys at the top level
    """
    properties = schema.get("properties")
    if properties is None:
        return frozenset()
    return frozenset(properties.keys())


def merge_object_json_schemas(partials: list[dict]) -> dict:
    """
    Deterministic merge for type: object style GPDB schemas.

    Merges multiple partial schemas into a single effective schema:
    - Union properties from all partials
    - Merge required as union of all required fields
    - Reconcile additionalProperties conservatively: if any parent disallows extras,
      effective disallows
    - Handle $defs / local refs by requiring merged branches to keep disjoint
      $defs keys or inline consistently

    Args:
        partials: List of JSON schema dicts to merge

    Returns:
        A merged JSON schema dict
    """
    if not partials:
        return {"type": "object"}

    # Start with the first partial as base
    merged: dict[str, Any] = {"type": "object"}

    # Merge properties (union of all properties)
    all_properties: dict[str, Any] = {}
    for partial in partials:
        properties = partial.get("properties")
        if properties:
            all_properties.update(properties)

    if all_properties:
        merged["properties"] = all_properties

    # Merge required (union of all required fields)
    all_required: set[str] = set()
    for partial in partials:
        required = partial.get("required")
        if required:
            all_required.update(required)

    if all_required:
        merged["required"] = sorted(all_required)

    # Reconcile additionalProperties conservatively
    # If any parent disallows extras (additionalProperties: false),
    # the effective schema also disallows extras
    additional_properties = None
    for partial in partials:
        partial_ap = partial.get("additionalProperties")
        if partial_ap is False:
            # Any false means effective is false
            additional_properties = False
            break
        elif partial_ap is not None and additional_properties is None:
            # If we haven't seen false yet, use the first non-None value
            additional_properties = partial_ap

    if additional_properties is not None:
        merged["additionalProperties"] = additional_properties

    # Merge $defs (require disjoint keys - if there are conflicts,
    # the last one wins, but this should be validated separately)
    all_defs: dict[str, Any] = {}
    for partial in partials:
        defs = partial.get("$defs")
        if defs:
            all_defs.update(defs)

    if all_defs:
        merged["$defs"] = all_defs

    return merged


def compute_effective_row(
    own_json_schema: dict, parent_effectives: list[dict]
) -> dict | None:
    """
    Compute the effective JSON schema for a schema with parents.

    Returns None if no parents (parent_effectives is empty).
    Otherwise returns merged effective dict by merging own_json_schema
    with all parent_effectives.

    Args:
        own_json_schema: The schema's own json_schema
        parent_effectives: List of effective schemas from parent schemas

    Returns:
        Merged effective schema dict, or None if no parents
    """
    if not parent_effectives:
        return None

    # Merge own schema with all parent effective schemas
    all_schemas = [own_json_schema] + parent_effectives
    return merge_object_json_schemas(all_schemas)


def build_inheritance_graph(schemas: dict[str, dict]) -> dict[str, list[str]]:
    """
    Build a graph mapping schema name to list of parent names it extends.

    Input: dict mapping schema name to schema record (with extends field)
    Output: adjacency list: each key is a schema; values are direct parents
    (schemas it extends). This is the input shape expected by
    ``topological_sort`` and ``detect_cycles``.

    Args:
        schemas: Dict mapping schema name to schema record dict

    Returns:
        Dict mapping schema name to list of parent schema names
    """
    graph: dict[str, list[str]] = {}
    for name, schema_record in schemas.items():
        extends = schema_record.get("extends", [])
        if extends:
            graph[name] = list(extends)
        else:
            graph[name] = []
    return graph


def detect_cycles(graph: dict[str, list[str]]) -> list[str] | None:
    """
    Detect cycles in the inheritance graph.

    Returns a list of schema names forming a cycle if found, else None.
    Uses DFS-based cycle detection.

    Args:
        graph: Adjacency list representation of inheritance graph

    Returns:
        List of schema names in the cycle, or None if no cycle
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {node: WHITE for node in graph}
    parent: dict[str, str | None] = {node: None for node in graph}

    def dfs(node: str) -> list[str] | None:
        color[node] = GRAY
        for neighbor in graph.get(node, []):
            if neighbor not in color:
                # Node not in graph - skip (will be caught by validation)
                continue
            if color[neighbor] == GRAY:
                # Found a cycle - reconstruct it
                cycle = [neighbor]
                current = node
                while current != neighbor:
                    cycle.append(current)
                    current = parent[current]  # type: ignore
                cycle.append(neighbor)
                return cycle
            if color[neighbor] == WHITE:
                parent[neighbor] = node
                result = dfs(neighbor)
                if result:
                    return result
        color[node] = BLACK
        return None

    for node in graph:
        if color[node] == WHITE:
            cycle = dfs(node)
            if cycle:
                return cycle

    return None


def topological_sort(graph: dict[str, list[str]]) -> list[str]:
    """
    Return schema names so every parent appears before any schema that extends it.

    ``graph[child]`` is the list of direct parents (same shape as
    ``build_inheritance_graph``).

    Raises ValueError if cycle detected.

    Args:
        graph: child -> list of parent names

    Returns:
        List of schema names in topological order (roots / parents first)

    Raises:
        ValueError: If a cycle is detected in the graph
    """
    cycle = detect_cycles(graph)
    if cycle:
        raise ValueError(f"Cycle detected in inheritance graph: {' -> '.join(cycle)}")

    # Kahn: in_degree[schema] = number of parents not yet placed (must be 0 to emit).
    in_degree: dict[str, int] = {node: len(graph.get(node, ())) for node in graph}

    children: dict[str, list[str]] = {node: [] for node in graph}
    for child in graph:
        for parent in graph[child]:
            if parent in children:
                children[parent].append(child)

    queue = [node for node, degree in in_degree.items() if degree == 0]
    result: list[str] = []

    while queue:
        queue.sort()
        node = queue.pop(0)
        result.append(node)

        for child in children.get(node, ()):
            if child in in_degree:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

    if len(result) != len(graph):
        raise ValueError("Graph has a cycle")

    return result


def validate_additive_invariant(
    schemas: dict[str, dict]
) -> tuple[bool, str | None]:
    """
    Validate the global additive inheritance invariant.

    For every schema U, let Ancestors(U) be the transitive closure of extends.
    Let Keys(S) be top_level_property_keys(S.json_schema).
    Then for every U, the sets { Keys(S) : S in {U} ∪ Ancestors(U) } must be
    pairwise disjoint.

    This ensures:
    - Multiple parents of one child have disjoint top-level keys
    - A child's own keys don't overlap any ancestor's keys
    - Editing parent A cannot introduce a key that appears on co-ancestor B
      for some descendant C

    Args:
        schemas: Dict mapping schema name to schema record dict with json_schema

    Returns:
        Tuple of (is_valid, error_message) where error_message names conflicting
        schemas and fields if invalid
    """
    # Build the inheritance graph
    graph = build_inheritance_graph(schemas)

    # Compute transitive closure of ancestors for each schema
    ancestors: dict[str, set[str]] = {}
    for node in graph:
        ancestors[node] = set()

    # Compute ancestors using DFS
    def collect_ancestors(node: str, visited: set[str]) -> set[str]:
        if node in visited:
            return set()
        visited.add(node)
        result = set()
        for parent in graph.get(node, []):
            if parent in schemas:
                result.add(parent)
                result.update(collect_ancestors(parent, visited))
        return result

    for node in graph:
        ancestors[node] = collect_ancestors(node, set())

    # Check the invariant for each schema
    for schema_name, schema_record in schemas.items():
        # Get all schemas in the inheritance chain: self + ancestors
        chain_schemas = {schema_name} | ancestors[schema_name]

        # Collect property keys for each schema in the chain
        schema_keys: dict[str, frozenset[str]] = {}
        for name in chain_schemas:
            if name in schemas:
                json_schema = schemas[name].get("json_schema", {})
                schema_keys[name] = top_level_property_keys(json_schema)

        # Check for overlaps between any pair of schemas in the chain
        for i, name1 in enumerate(chain_schemas):
            for name2 in list(chain_schemas)[i + 1 :]:
                if name1 not in schema_keys or name2 not in schema_keys:
                    continue
                keys1 = schema_keys[name1]
                keys2 = schema_keys[name2]
                overlap = keys1 & keys2
                if overlap:
                    error_fields = ", ".join(sorted(overlap))
                    error_msg = (
                        f"Schema '{schema_name}' violates additive inheritance: "
                        f"schemas '{name1}' and '{name2}' have overlapping "
                        f"properties: {error_fields}"
                    )
                    return False, error_msg

    return True, None
