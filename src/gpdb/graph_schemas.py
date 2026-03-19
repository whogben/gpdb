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
    SchemaUpsert,
    SchemaRef,
    _normalize_schema_kind,
)
from gpdb.schema import (
    _bump_semver,
    _detect_semver_change,
    _check_breaking_changes,
)


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
            ValueError: If duplicate schema names are provided

        Returns:
            List of schema ORM objects with updated versions
        """
        # Reject duplicate names before any database writes
        names = [s.name for s in schemas]
        if len(names) != len(set(names)):
            raise ValueError("Duplicate schema names are not allowed")

        async with self._get_session() as session:
            results = []
            for schema_upsert in schemas:
                # Check if schema already exists using composite key (name, kind)
                stmt = select(self._Schema).where(
                    self._Schema.name == schema_upsert.name,
                    self._Schema.kind == _normalize_schema_kind(schema_upsert.kind),
                )
                result = await session.execute(stmt)
                existing = result.scalars().first()
                json_schema, resolved_kind = self._prepare_schema_registration(
                    schema_upsert.json_schema,
                    kind=schema_upsert.kind,
                    existing=existing,
                )
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

                    # Update existing schema
                    existing.json_schema = json_schema
                    existing.kind = resolved_kind
                    existing.version = new_version
                    cache_key = (schema_upsert.name, resolved_kind)
                    self._validators.pop(cache_key, None)  # invalidate cache for updated schema
                    self._schema_kinds.pop(cache_key, None)
                    results.append(existing)
                else:
                    # Create new schema with version 1.0.0
                    new_schema = self._Schema(
                        name=schema_upsert.name,
                        json_schema=json_schema,
                        kind=resolved_kind,
                        version="1.0.0",
                    )
                    session.add(new_schema)
                    cache_key = (schema_upsert.name, resolved_kind)
                    self._schema_kinds.pop(cache_key, None)
                    results.append(new_schema)

            await session.flush()
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
        """
        from gpdb.models import SchemaInUseError

        # Reject duplicate refs before doing any work
        ref_keys = [(r.name, _normalize_schema_kind(r.kind)) for r in refs]
        if len(ref_keys) != len(set(ref_keys)):
            duplicates = [
                ref for ref in ref_keys if ref_keys.count(ref) > 1
            ]
            raise ValueError(f"Duplicate schema refs provided: {set(duplicates)}")

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
                node_stmt = select(self._Node).where(self._Node.schema_name == ref.name)
                node_result = await session.execute(node_stmt)
                if node_result.scalars().first() is not None:
                    raise SchemaInUseError(
                        f"Cannot delete schema '{ref.name}': it is referenced by one or more nodes"
                    )

                # Check if any edges use this schema
                edge_stmt = select(self._Edge).where(self._Edge.schema_name == ref.name)
                edge_result = await session.execute(edge_stmt)
                if edge_result.scalars().first() is not None:
                    raise SchemaInUseError(
                        f"Cannot delete schema '{ref.name}': it is referenced by one or more edges"
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

    async def list_schemas(self, kind: str | None = None) -> List[str]:
        """
        List all registered schema names.

        Args:
            kind: Optional compatibility filter ("node" or "edge")

        Returns:
            List of schema names
        """
        resolved_kind = _normalize_schema_kind(kind) if kind is not None else None
        async with self._get_session() as session:
            stmt = select(self._Schema)
            result = await session.execute(stmt)
            names: List[str] = []
            for schema in result.scalars().all():
                schema_kind = self._schema_kind_from_record(schema)
                if resolved_kind is None or schema_kind == resolved_kind:
                    names.append(str(schema.name))
            return names

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
        1. Migrates all data using the provided migration function
        2. Registers the new schema (with auto SemVer bump)
        3. All in a single transaction for 100% integrity

        Args:
            name: Schema name to migrate
            migration_func: Function that transforms old data to new data: (old_data) -> new_data
            new_schema: New JSON schema or Pydantic model class
            kind: Schema kind ("node" or "edge")
        """
        async with self.sqla_sessionmaker() as session:
            async with session.begin():
                existing = await session.get(
                    self._Schema, {"name": name, "kind": _normalize_schema_kind(kind)}
                )
                # Build SchemaUpsert for the new schema
                schema_upsert = SchemaUpsert(
                    name=name,
                    json_schema=new_schema,
                    kind=kind,
                )
                json_schema, resolved_kind = self._prepare_schema_registration(
                    schema_upsert.json_schema,
                    kind=schema_upsert.kind,
                    existing=existing,
                )
                if existing is not None:
                    existing_kind = self._schema_kind_from_record(existing)
                    if resolved_kind != existing_kind:
                        raise SchemaBreakingChangeError(
                            f"Schema '{name}' cannot change kind from "
                            f"'{existing_kind}' to '{resolved_kind}'."
                        )

                # Create validator for new schema directly (not from cache since schema not yet registered)
                validator = jsonschema.Draft7Validator(json_schema)

                # Get all nodes with this schema
                stmt = select(self._Node).where(self._Node.schema_name == name)
                result = await session.execute(stmt)
                nodes = result.scalars().all()

                # Migrate each node's data and validate
                for node in nodes:
                    new_data = migration_func(node.data)
                    try:
                        validator.validate(new_data)
                    except jsonschema.exceptions.ValidationError as e:
                        raise SchemaValidationError(
                            f"Migration produced invalid data for node {node.id}: {e.message}"
                        )
                    node.data = new_data

                # Get all edges with this schema
                stmt = select(self._Edge).where(self._Edge.schema_name == name)
                result = await session.execute(stmt)
                edges = result.scalars().all()

                # Migrate each edge's data and validate
                for edge in edges:
                    new_data = migration_func(edge.data)
                    try:
                        validator.validate(new_data)
                    except jsonschema.exceptions.ValidationError as e:
                        raise SchemaValidationError(
                            f"Migration produced invalid data for edge {edge.id}: {e.message}"
                        )
                    edge.data = new_data

                # Update schema with new version (bump major for breaking changes)
                if existing:
                    # Detect change type and bump version
                    change_type = _detect_semver_change(
                        existing.json_schema, json_schema
                    )
                    new_version = _bump_semver(existing.version, change_type)
                    existing.json_schema = json_schema
                    existing.kind = resolved_kind
                    existing.version = new_version
                else:
                    new_schema_record = self._Schema(
                        name=name,
                        json_schema=json_schema,
                        kind=resolved_kind,
                        version="1.0.0",
                    )
                    session.add(new_schema_record)
                cache_key = (name, resolved_kind)
                self._validators.pop(cache_key, None)  # invalidate cache for updated schema
                self._schema_kinds.pop(cache_key, None)

    async def _get_schema_by_ref(self, ref: SchemaRef) -> Any:
        """Get a single schema by name and kind."""
        schemas = await self.get_schemas([ref])
        return schemas[0]

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
        validator = jsonschema.Draft7Validator(schema.json_schema)
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
