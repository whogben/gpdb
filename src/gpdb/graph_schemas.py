"""
Schema-related methods for GPGraph.
"""

from __future__ import annotations

from typing import Any, Dict, List, Union

import jsonschema
from pydantic import BaseModel
import sqlalchemy as sa
from sqlalchemy import select

from gpdb.models import (
    SchemaKind,
    SchemaNotFoundError,
    SchemaValidationError,
    SchemaKindMismatchError,
    SchemaBreakingChangeError,
    SchemaProtectedError,
    SchemaInheritanceError,
    SchemaUpsert,
    SchemaRef,
    _normalize_schema_kind,
)
from gpdb.schema import (
    _bump_semver,
    _detect_semver_change,
    _check_breaking_changes,
)
from gpdb.schema_inheritance import (
    build_inheritance_graph,
    compute_effective_row,
    detect_cycles,
    topological_sort,
    validate_additive_invariant,
)
from gpdb.graph_schema_migrate import run_migrate_schema
from gpdb.svg_sanitizer import normalize_svg_icon_for_display, sanitize_svg


class SchemaMixin:
    """Mixin class providing schema-related methods for GPGraph."""

    def _schema_kind_from_record(self, schema: Any) -> SchemaKind:
        """Extract the schema kind from a schema record."""
        if schema.kind is None:
            raise SchemaValidationError(
                f"Schema '{schema.name}' is missing kind metadata. "
                "Re-register it as a node or edge schema."
            )
        return _normalize_schema_kind(schema.kind)

    def _prepare_schema_registration(
        self,
        schema: Union[Dict[str, Any], type],
        *,
        kind: str | None,
        existing: Any | None = None,
    ) -> tuple[Dict[str, Any], SchemaKind]:
        """Normalize a schema payload and resolve the schema kind."""
        if isinstance(schema, type) and issubclass(schema, BaseModel):
            json_schema = schema.model_json_schema()
        else:
            import copy

            json_schema = copy.deepcopy(schema)

        if kind is not None:
            resolved_kind = _normalize_schema_kind(kind)
        elif existing is not None:
            resolved_kind = self._schema_kind_from_record(existing)
        else:
            resolved_kind = "node"

        return json_schema, resolved_kind

    async def set_schemas(self, schemas: list[SchemaUpsert]) -> list[Any]:
        """
        Register multiple JSON schemas in the schema registry.

        The system automatically detects the type of change and bumps the version:
        - Major: Breaking changes (removed fields, type changes, newly required fields)
        - Minor: Backward compatible changes (new optional fields)
        - Patch: Non-consequential changes (descriptions, titles, examples)

        Args:
            schemas: List of SchemaUpsert models containing name, json_schema, and kind

        Raises:
            SchemaBreakingChangeError: If breaking changes are detected
            SchemaProtectedError: If attempting to modify a protected schema
            SchemaInheritanceError: If inheritance validation fails
            ValueError: If duplicate schema names are provided

        Returns:
            List of schema ORM objects with updated versions
        """
        # Reject duplicate names before any database writes
        names = [s.name for s in schemas]
        if len(names) != len(set(names)):
            raise ValueError("Duplicate schema names are not allowed")

        # Reject operations on protected schemas
        for schema_upsert in schemas:
            if schema_upsert.name == "__default__":
                raise SchemaProtectedError(
                    f"Cannot modify protected schema '{schema_upsert.name}'"
                )

        async with self._get_session() as session:
            # Build proposed registry: DB state overlaid by batch
            # First, load all existing schemas of the same kinds
            kinds_in_batch = {_normalize_schema_kind(s.kind) for s in schemas}
            stmt = select(self._Schema).where(self._Schema.kind.in_(kinds_in_batch))
            result = await session.execute(stmt)
            existing_schemas = result.scalars().all()
            
            # Build map of existing schemas by (name, kind)
            existing_map = {(s.name, s.kind): s for s in existing_schemas}
            
            # Build proposed rows (in-memory objects with proposed changes)
            proposed_rows = []
            for schema_upsert in schemas:
                resolved_kind = _normalize_schema_kind(schema_upsert.kind)
                key = (schema_upsert.name, resolved_kind)
                
                # Get existing or create new
                existing = existing_map.get(key)
                json_schema, resolved_kind = self._prepare_schema_registration(
                    schema_upsert.json_schema,
                    kind=schema_upsert.kind,
                    existing=existing,
                )
                
                # Sanitize svg_icon if provided
                sanitized_svg_icon = None
                if schema_upsert.svg_icon is not None:
                    sanitized_svg_icon = sanitize_svg(schema_upsert.svg_icon)
                
                # extends: None on update = leave unchanged; on create = []; [] = clear
                if existing is not None and schema_upsert.extends is None:
                    extends = list(existing.extends or [])
                else:
                    extends = list(schema_upsert.extends or [])

                # Validate extends constraints
                if schema_upsert.name == "__default__" and extends:
                    raise SchemaInheritanceError(
                        f"Schema '__default__' cannot extend other schemas"
                    )
                if "__default__" in extends:
                    raise SchemaInheritanceError(
                        f"Schema '{schema_upsert.name}' cannot extend '__default__'"
                    )
                
                # Build proposed row
                if existing:
                    existing_kind = self._schema_kind_from_record(existing)
                    if resolved_kind != existing_kind:
                        raise SchemaBreakingChangeError(
                            f"Schema '{schema_upsert.name}' cannot change kind from "
                            f"'{existing_kind}' to '{resolved_kind}'."
                        )
                    # Detect type of change
                    change_type = _detect_semver_change(
                        existing.json_schema, json_schema
                    )

                    # Fail on breaking changes
                    if change_type == "major":
                        _check_breaking_changes(
                            existing.json_schema, json_schema, schema_upsert.name
                        )

                    # Bump version
                    new_version = _bump_semver(existing.version, change_type)

                    # Create proposed row with updates
                    proposed_row = {
                        "name": existing.name,
                        "kind": existing.kind,
                        "version": new_version,
                        "json_schema": json_schema,
                        "extends": extends,
                        "effective_json_schema": None,  # Will compute
                        "alias": schema_upsert.alias if schema_upsert.alias is not None else existing.alias,
                        "svg_icon": sanitized_svg_icon if sanitized_svg_icon is not None else existing.svg_icon,
                        "existing": existing,
                    }
                else:
                    # Create new schema with version 1.0.0
                    proposed_row = {
                        "name": schema_upsert.name,
                        "kind": resolved_kind,
                        "version": "1.0.0",
                        "json_schema": json_schema,
                        "extends": extends,
                        "effective_json_schema": None,  # Will compute
                        "alias": schema_upsert.alias,
                        "svg_icon": sanitized_svg_icon,
                        "existing": None,
                    }
                
                proposed_rows.append(proposed_row)
            
            # Build proposed registry for each kind
            for kind in kinds_in_batch:
                # Build map of name -> row for this kind
                kind_rows = [r for r in proposed_rows if r["kind"] == kind]
                kind_existing = {s.name: s for s in existing_schemas if s.kind == kind}
                
                # Build full proposed registry (existing + proposed)
                proposed_registry = {}
                for name, schema in kind_existing.items():
                    if not any(r["name"] == name and r["kind"] == kind for r in proposed_rows):
                        # This schema is not being updated, include as-is
                        proposed_registry[name] = {
                            "json_schema": schema.json_schema,
                            "extends": schema.extends or [],
                        }
                
                # Add/update proposed rows
                for row in kind_rows:
                    proposed_registry[row["name"]] = {
                        "json_schema": row["json_schema"],
                        "extends": row["extends"],
                    }
                
                # Validate parent existence and kind matching
                for row in kind_rows:
                    for parent_name in row["extends"]:
                        if parent_name not in proposed_registry:
                            raise SchemaInheritanceError(
                                f"Schema '{row['name']}' extends non-existent schema '{parent_name}'"
                            )
                        # Parent must have same kind (already filtered by kind_rows)
                
                # Build inheritance graph
                graph = build_inheritance_graph(proposed_registry)
                
                # Detect cycles
                cycle = detect_cycles(graph)
                if cycle:
                    raise SchemaInheritanceError(
                        f"Cycle detected in inheritance: {' -> '.join(cycle)}"
                    )
                
                # Validate additive invariant
                is_valid, error_msg = validate_additive_invariant(proposed_registry)
                if not is_valid:
                    raise SchemaInheritanceError(error_msg)
                
                # Topologically sort to ensure parents before children
                sorted_names = topological_sort(graph)
                
                # Compute effective_json_schema for each schema in topological order
                for name in sorted_names:
                    row = next((r for r in kind_rows if r["name"] == name), None)
                    if row is None:
                        # This is an existing schema not being updated
                        continue
                    
                    # Get parent effective schemas (batch parents may have effective None = use json_schema)
                    parent_effectives = []
                    for parent_name in row["extends"]:
                        parent_row = next((r for r in kind_rows if r["name"] == parent_name), None)
                        if parent_row is not None:
                            if parent_row["effective_json_schema"] is not None:
                                parent_effectives.append(parent_row["effective_json_schema"])
                            else:
                                parent_effectives.append(parent_row["json_schema"])
                        elif parent_name in kind_existing:
                            parent_schema = kind_existing[parent_name]
                            if parent_schema.effective_json_schema is not None:
                                parent_effectives.append(parent_schema.effective_json_schema)
                            else:
                                parent_effectives.append(parent_schema.json_schema)
                    
                    # Compute effective schema
                    effective = compute_effective_row(row["json_schema"], parent_effectives)
                    row["effective_json_schema"] = effective
            
            # Persist all changes
            results = []
            for row in proposed_rows:
                if row["existing"]:
                    # Update existing schema
                    row["existing"].json_schema = row["json_schema"]
                    row["existing"].kind = row["kind"]
                    row["existing"].version = row["version"]
                    row["existing"].extends = row["extends"]
                    row["existing"].effective_json_schema = row["effective_json_schema"]
                    if row["alias"] is not None:
                        row["existing"].alias = row["alias"]
                    if row["svg_icon"] is not None:
                        row["existing"].svg_icon = row["svg_icon"]
                    results.append(row["existing"])
                else:
                    # Create new schema
                    new_schema = self._Schema(
                        name=row["name"],
                        json_schema=row["json_schema"],
                        kind=row["kind"],
                        version=row["version"],
                        extends=row["extends"],
                        effective_json_schema=row["effective_json_schema"],
                        alias=row["alias"],
                        svg_icon=row["svg_icon"],
                    )
                    session.add(new_schema)
                    results.append(new_schema)

            await session.flush()
            
            # Clear entire validators cache on any successful schema write
            self._validators.clear()
            self._schema_kinds.clear()
            self._schema_display_cache.clear()
            
            for result in results:
                await session.refresh(result)
            return results

    async def get_schemas(self, refs: list[SchemaRef]) -> list[Any]:
        """
        Retrieve registered schemas by name and kind.

        Args:
            refs: List of SchemaRef objects containing name and kind

        Returns:
            List of schema ORM objects in the same order as input refs

        Raises:
            ValueError: If duplicate refs are provided
            SchemaNotFoundError: If any requested schema is not found
        """
        # Reject duplicate refs before doing any work
        ref_keys = [(r.name, _normalize_schema_kind(r.kind)) for r in refs]
        if len(ref_keys) != len(set(ref_keys)):
            duplicates = [
                ref for ref in ref_keys if ref_keys.count(ref) > 1
            ]
            raise ValueError(f"Duplicate schema refs provided: {set(duplicates)}")

        async with self._get_session() as session:
            # Build composite key conditions for the query
            conditions = [
                (self._Schema.name == r.name) & (self._Schema.kind == _normalize_schema_kind(r.kind))
                for r in refs
            ]
            stmt = select(self._Schema).where(
                conditions[0] if len(conditions) == 1 else sa.or_(*conditions)
            )
            result = await session.execute(stmt)
            found_schemas = {
                (schema.name, schema.kind): schema
                for schema in result.scalars().all()
            }

            # Check if any requested schema is missing
            missing = [
                ref for ref in ref_keys if ref not in found_schemas
            ]
            if missing:
                raise SchemaNotFoundError(f"Schemas not found: {missing}")

            # Return schemas in the same order as input refs
            return [found_schemas[ref] for ref in ref_keys]

    async def delete_schemas(self, refs: list[SchemaRef]) -> None:
        """
        Delete multiple schemas from the registry.

        Args:
            refs: List of SchemaRef objects containing name and kind

        Raises:
            ValueError: If duplicate refs are provided
            SchemaInUseError: If any nodes or edges reference any of the schemas
            SchemaProtectedError: If attempting to delete a protected schema
            SchemaInheritanceError: If attempting to delete a schema that has descendants
        """
        from gpdb.models import SchemaInUseError

        # Reject duplicate refs before doing any work
        ref_keys = [(r.name, _normalize_schema_kind(r.kind)) for r in refs]
        if len(ref_keys) != len(set(ref_keys)):
            duplicates = [
                ref for ref in ref_keys if ref_keys.count(ref) > 1
            ]
            raise ValueError(f"Duplicate schema refs provided: {set(duplicates)}")

        # Reject operations on protected schemas
        for ref in refs:
            if ref.name == "__default__":
                raise SchemaProtectedError(
                    f"Cannot delete protected schema '{ref.name}'"
                )

        async with self._get_session() as session:
            # Verify all requested schemas exist before checking usage or deleting.
            # This keeps bulk deletes strictly all-or-nothing even when
            # the caller includes a missing ref.
            conditions = [
                (self._Schema.name == r.name) & (self._Schema.kind == _normalize_schema_kind(r.kind))
                for r in refs
            ]
            stmt = select(self._Schema).where(
                conditions[0] if len(conditions) == 1 else sa.or_(*conditions)
            )
            result = await session.execute(stmt)
            found_schemas = {
                (schema.name, schema.kind): schema
                for schema in result.scalars().all()
            }
            missing = [ref for ref in ref_keys if ref not in found_schemas]
            if missing:
                raise SchemaNotFoundError(f"Schemas not found: {missing}")

            # Check all schemas for usage before deleting any
            for ref in refs:
                # Check if any nodes use this schema
                node_stmt = select(self._Node).where(self._Node.type == ref.name)
                node_result = await session.execute(node_stmt)
                if node_result.scalars().first() is not None:
                    raise SchemaInUseError(
                        f"Cannot delete schema '{ref.name}': it is referenced by one or more nodes"
                    )

                # Check if any edges use this schema
                edge_stmt = select(self._Edge).where(self._Edge.type == ref.name)
                edge_result = await session.execute(edge_stmt)
                if edge_result.scalars().first() is not None:
                    raise SchemaInUseError(
                        f"Cannot delete schema '{ref.name}': it is referenced by one or more edges"
                    )

            # Check for descendants (schemas that extend the target schemas)
            for ref in refs:
                # Get all schemas of the same kind
                kind_stmt = select(self._Schema).where(self._Schema.kind == _normalize_schema_kind(ref.kind))
                kind_result = await session.execute(kind_stmt)
                all_schemas = kind_result.scalars().all()
                
                # Check if any schema extends the target
                for schema in all_schemas:
                    if schema.extends and ref.name in schema.extends:
                        raise SchemaInheritanceError(
                            f"Cannot delete schema '{ref.name}': it is extended by schema '{schema.name}'"
                        )

            # Delete all schema records
            for ref in refs:
                schema = await session.get(
                    self._Schema, {"name": ref.name, "kind": _normalize_schema_kind(ref.kind)}
                )
                if schema is not None:
                    await session.delete(schema)
                    cache_key = (ref.name, _normalize_schema_kind(ref.kind))
                    self._validators.pop(cache_key, None)
                    self._schema_kinds.pop(cache_key, None)
                    self._schema_display_cache.pop(cache_key, None)

    async def list_schemas(self, kind: str | None = None) -> List[SchemaRef]:
        """
        List all registered schemas.

        Args:
            kind: Optional filter ("node" or "edge")

        Returns:
            List of SchemaRef objects containing name and kind
        """
        resolved_kind = _normalize_schema_kind(kind) if kind is not None else None
        async with self._get_session() as session:
            stmt = select(self._Schema)
            result = await session.execute(stmt)
            refs: List[SchemaRef] = []
            for schema in result.scalars().all():
                schema_kind = self._schema_kind_from_record(schema)
                if resolved_kind is None or schema_kind == resolved_kind:
                    refs.append(SchemaRef(name=str(schema.name), kind=schema_kind))
            return refs

    async def migrate_schema(
        self,
        name: str,
        migration_func: callable,
        new_schema: Union[Dict[str, Any], type],
        kind: str,
    ):
        """
        Migrate all nodes/edges using a schema to a new schema version.

        This method atomically:
        1. Migrates all data using the provided migration function (including descendants)
        2. Registers the new schema (with auto SemVer bump)
        3. Recomputes effective_json_schema for the migrated schema and all descendants
        4. All in a single transaction for 100% integrity

        Args:
            name: Schema name to migrate
            migration_func: Function that transforms old data to new data: (old_data) -> new_data
            new_schema: New JSON schema or Pydantic model class
            kind: Schema kind ("node" or "edge")

        Raises:
            SchemaProtectedError: If attempting to migrate a protected schema
        """
        await run_migrate_schema(self, name, migration_func, new_schema, kind)

    async def _get_schema_by_ref(self, ref: SchemaRef) -> Any:
        """Get a single schema by name and kind."""
        schemas = await self.get_schemas([ref])
        return schemas[0]

    async def _get_schema_display_info(self, ref: SchemaRef) -> Dict[str, str | None]:
        """
        Get cached display info (alias, svg_icon) for a schema.

        Args:
            ref: SchemaRef containing name and kind

        Returns:
            Dict with 'alias' and 'svg_icon' keys (values may be None)
        """
        cache_key = (ref.name, _normalize_schema_kind(ref.kind))
        if cache_key in self._schema_display_cache:
            return self._schema_display_cache[cache_key]

        schema = await self._get_schema_by_ref(ref)
        display_info = {
            "alias": schema.alias,
            "svg_icon": normalize_svg_icon_for_display(schema.svg_icon),
        }
        self._schema_display_cache[cache_key] = display_info
        return display_info

    async def _get_registered_schema_kind(self, ref: SchemaRef) -> SchemaKind:
        """Return the registered kind for one schema."""
        cache_key = (ref.name, _normalize_schema_kind(ref.kind))
        if cache_key in self._schema_kinds:
            return self._schema_kinds[cache_key]

        schema = await self._get_schema_by_ref(ref)
        kind = self._schema_kind_from_record(schema)
        self._schema_kinds[cache_key] = kind
        return kind

    async def _get_validator(self, ref: SchemaRef) -> Any:
        """
        Get a cached jsonschema validator for the given schema reference.

        Args:
            ref: SchemaRef containing name and kind

        Returns:
            Compiled jsonschema validator

        Raises:
            SchemaNotFoundError: If schema is not found
        """
        cache_key = (ref.name, _normalize_schema_kind(ref.kind))
        if cache_key in self._validators:
            return self._validators[cache_key]

        schema = await self._get_schema_by_ref(ref)
        # Use effective_json_schema if not null, otherwise use json_schema
        schema_to_validate = schema.effective_json_schema if schema.effective_json_schema is not None else schema.json_schema
        validator = jsonschema.Draft7Validator(schema_to_validate)
        self._validators[cache_key] = validator
        return validator

    async def _validate_data(
        self,
        schema_name: str,
        data: Dict[str, Any],
        *,
        expected_kind: SchemaKind,
    ):
        """
        Validate data against a registered schema.

        Args:
            schema_name: Name of the schema to validate against
            data: Data to validate
            expected_kind: Graph record kind the schema must be compatible with

        Raises:
            SchemaNotFoundError: If schema is not found
            SchemaValidationError: If validation fails
        """
        ref = SchemaRef(name=schema_name, kind=expected_kind)
        actual_kind = await self._get_registered_schema_kind(ref)
        if actual_kind != expected_kind:
            raise SchemaKindMismatchError(
                f"Schema '{schema_name}' is a {actual_kind} schema and cannot be "
                f"attached to a {expected_kind}."
            )
        validator = await self._get_validator(ref)
        errors = list(validator.iter_errors(data))
        if errors:
            error_details = [e.message for e in errors]
            raise SchemaValidationError(
                f"Validation failed for schema '{schema_name}': {error_details}"
            )
