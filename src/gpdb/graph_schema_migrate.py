"""
Schema migration with descendant types and effective_json_schema recompute.
"""

from __future__ import annotations

from typing import Any, Dict, Union

import jsonschema
from sqlalchemy import select

from gpdb.models import (
    SchemaBreakingChangeError,
    SchemaProtectedError,
    SchemaUpsert,
    SchemaValidationError,
    _normalize_schema_kind,
)
from gpdb.schema import _bump_semver, _detect_semver_change
from gpdb.schema_inheritance import compute_effective_row, topological_sort


async def run_migrate_schema(
    graph: Any,
    name: str,
    migration_func: callable,
    new_schema: Union[Dict[str, Any], type],
    kind: str,
) -> None:
    if name == "__default__":
        raise SchemaProtectedError(
            f"Cannot migrate protected schema '{name}'"
        )

    async with graph.sqla_sessionmaker() as session:
        async with session.begin():
            resolved_kind = _normalize_schema_kind(kind)
            existing = await session.get(
                graph._Schema, {"name": name, "kind": resolved_kind}
            )
            schema_upsert = SchemaUpsert(
                name=name,
                json_schema=new_schema,
                kind=kind,
            )
            json_schema, resolved_kind = graph._prepare_schema_registration(
                schema_upsert.json_schema,
                kind=schema_upsert.kind,
                existing=existing,
            )
            if existing is not None:
                existing_kind = graph._schema_kind_from_record(existing)
                if resolved_kind != existing_kind:
                    raise SchemaBreakingChangeError(
                        f"Schema '{name}' cannot change kind from "
                        f"'{existing_kind}' to '{resolved_kind}'."
                    )

            kind_stmt = select(graph._Schema).where(graph._Schema.kind == resolved_kind)
            kind_result = await session.execute(kind_stmt)
            all_schemas = kind_result.scalars().all()

            schema_map = {s.name: s for s in all_schemas}

            inheritance_graph: dict[str, list[str]] = {}
            for schema in all_schemas:
                inheritance_graph[schema.name] = schema.extends or []

            def get_descendants(
                schema_name: str, visited: set[str] | None = None
            ) -> set[str]:
                if visited is None:
                    visited = set()
                descendants: set[str] = set()
                for child_name, parents in inheritance_graph.items():
                    if schema_name in parents and child_name not in visited:
                        visited.add(child_name)
                        descendants.add(child_name)
                        descendants.update(get_descendants(child_name, visited))
                return descendants

            descendant_names = get_descendants(name)
            all_affected_names = {name} | descendant_names

            node_stmt = select(graph._Node).where(
                graph._Node.type.in_(all_affected_names)
            )
            node_result = await session.execute(node_stmt)
            nodes = node_result.scalars().all()

            for node in nodes:
                new_data = migration_func(node.data)
                node.data = new_data

            edge_stmt = select(graph._Edge).where(
                graph._Edge.type.in_(all_affected_names)
            )
            edge_result = await session.execute(edge_stmt)
            edges = edge_result.scalars().all()

            for edge in edges:
                new_data = migration_func(edge.data)
                edge.data = new_data

            if existing:
                change_type = _detect_semver_change(
                    existing.json_schema, json_schema
                )
                new_version = _bump_semver(existing.version, change_type)
                existing.json_schema = json_schema
                existing.kind = resolved_kind
                existing.version = new_version
            else:
                new_schema_record = graph._Schema(
                    name=name,
                    json_schema=json_schema,
                    kind=resolved_kind,
                    version="1.0.0",
                )
                session.add(new_schema_record)
                existing = new_schema_record

            sorted_names = topological_sort(inheritance_graph)

            for schema_name in sorted_names:
                if schema_name not in all_affected_names:
                    continue

                schema = schema_map.get(schema_name)
                if schema is None:
                    continue

                parent_effectives = []
                for parent_name in (schema.extends or []):
                    parent_schema = schema_map.get(parent_name)
                    if parent_schema:
                        if parent_schema.effective_json_schema is not None:
                            parent_effectives.append(parent_schema.effective_json_schema)
                        else:
                            parent_effectives.append(parent_schema.json_schema)

                effective = compute_effective_row(schema.json_schema, parent_effectives)
                schema.effective_json_schema = effective

            for node in nodes:
                node_schema = schema_map.get(node.type)
                if node_schema is None:
                    continue
                schema_to_validate = (
                    node_schema.effective_json_schema
                    if node_schema.effective_json_schema is not None
                    else node_schema.json_schema
                )
                validator = jsonschema.Draft7Validator(schema_to_validate)
                try:
                    validator.validate(node.data)
                except jsonschema.exceptions.ValidationError as e:
                    raise SchemaValidationError(
                        f"Migration produced invalid data for node {node.id} "
                        f"(type {node.type}): {e.message}"
                    )

            for edge in edges:
                edge_schema = schema_map.get(edge.type)
                if edge_schema is None:
                    continue
                schema_to_validate = (
                    edge_schema.effective_json_schema
                    if edge_schema.effective_json_schema is not None
                    else edge_schema.json_schema
                )
                validator = jsonschema.Draft7Validator(schema_to_validate)
                try:
                    validator.validate(edge.data)
                except jsonschema.exceptions.ValidationError as e:
                    raise SchemaValidationError(
                        f"Migration produced invalid data for edge {edge.id} "
                        f"(type {edge.type}): {e.message}"
                    )

            cache_key = (name, resolved_kind)
            graph._validators.pop(cache_key, None)
            graph._schema_kinds.pop(cache_key, None)
            graph._schema_display_cache.pop(cache_key, None)
