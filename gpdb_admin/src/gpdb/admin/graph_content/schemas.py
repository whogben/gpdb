"""Schema operations for graph-content service."""

from __future__ import annotations

import logging

from gpdb import (
    GPGraph,
    SchemaBreakingChangeError,
    SchemaInheritanceError,
    SchemaInUseError,
    SchemaNotFoundError,
    SchemaProtectedError,
    SchemaRef,
    SchemaUpsert,
    SchemaValidationError,
    sanitize_svg,
)
from gpdb.admin.store import AdminUser

from gpdb.admin.graph_content.exceptions import (
    GraphContentConflictError,
    GraphContentNotFoundError,
    GraphContentValidationError,
)
from gpdb.admin.graph_content.models import (
    GraphSchemaCreateParam,
    GraphSchemaDeleteResult,
    GraphSchemaDetail,
    GraphSchemaList,
    GraphSchemaRecord,
    GraphSchemaUpdateParam,
    GraphSchemaUsage,
)
from gpdb.admin.graph_content._helpers import (
    format_schema_delete_blocker_message,
    inspect_schema_usage,
    normalize_optional_schema_kind,
    normalize_optional_text,
    open_graph,
    require_admin_store,
    schema_kind_from_record,
    serialize_schema_record,
    validate_json_schema,
    validate_schema_kind,
    validate_schema_name,
)


async def get_graph_overview(
    self,
    *,
    graph_id: str,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> GraphOverview:
    """Return managed graph metadata plus live schema/node/edge counts."""
    from gpdb.admin.graph_content.models import GraphContentSummary, GraphOverview
    from gpdb.admin.graph_content._helpers import serialize_graph, serialize_instance
    
    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="view",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    overview = GraphOverview(
        graph=serialize_graph(graph),
        instance=serialize_instance(instance),
    )
    try:
        schema_refs = await db.list_schemas()
        from gpdb import SearchQuery
        node_page = await db.search_nodes(SearchQuery(limit=1))
        edge_page = await db.search_edges(SearchQuery(limit=1))
        overview.summary = GraphContentSummary(
            schema_count=len(schema_refs),
            node_count=node_page.total,
            edge_count=edge_page.total,
        )
    except Exception as exc:
        logging.exception(
            "Failed to load live graph content counts",
            extra={"graph_id": graph_id, "instance_id": instance.id},
        )
        overview.content_status = "unavailable"
        overview.content_error = f"Could not load live graph content counts: {exc}"
    finally:
        await db.sqla_engine.dispose()

    return overview


async def list_graph_schemas(
    self,
    *,
    graph_id: str,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
    kind: str | None = None,
    include_json_schema: bool = False,
) -> GraphSchemaList:
    """Return the current schema registry for one managed graph."""
    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="view",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        items: list[dict[str, object]] = []
        clean_kind = normalize_optional_schema_kind(kind)
        schema_refs = await db.list_schemas(kind=clean_kind)
        if schema_refs:
            try:
                schemas = await db.get_schemas(schema_refs)
                for schema in schemas:
                    items.append(
                        serialize_schema_record(
                            schema,
                            include_json_schema=include_json_schema,
                            usage=GraphSchemaUsage(),
                        )
                    )
            except SchemaNotFoundError:
                # TOCTOU protection: `list_schemas()` may become stale
                # if another user deletes a schema between list+get.
                # Best-effort: skip missing schemas and return the rest.
                for ref in schema_refs:
                    try:
                        schemas = await db.get_schemas([ref])
                        for schema in schemas:
                            items.append(
                                serialize_schema_record(
                                    schema,
                                    include_json_schema=include_json_schema,
                                    usage=GraphSchemaUsage(),
                                )
                            )
                    except SchemaNotFoundError:
                        continue
        return GraphSchemaList(
            items=[GraphSchemaRecord(**item) for item in items],
            total=len(items),
        )
    finally:
        await db.sqla_engine.dispose()


async def get_graph_schemas(
    self,
    *,
    graph_id: str,
    names: list[str],
    kind: str,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphSchemaDetail]:
    """Return multiple graph schemas plus usage detail."""
    if not names:
        raise GraphContentValidationError("At least one schema name is required.")

    # Reject duplicate names
    if len(names) != len(set(names)):
        duplicates = [name for name in names if names.count(name) > 1]
        raise GraphContentValidationError(
            f"Duplicate schema names provided: {set(duplicates)}"
        )

    clean_names = [name.strip() for name in names]
    if not all(clean_names):
        raise GraphContentValidationError("Schema names cannot be empty.")

    clean_kind = validate_schema_kind(kind)

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="view",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        refs = [SchemaRef(name=name, kind=clean_kind) for name in clean_names]
        try:
            schemas = await db.get_schemas(refs)
        except SchemaNotFoundError as exc:
            raise GraphContentNotFoundError(
                f"One or more schemas were not found."
            ) from exc
        results = []
        for schema in schemas:
            results.append(
                GraphSchemaDetail(
                    schema_record=GraphSchemaRecord(
                        **serialize_schema_record(
                            schema,
                            include_json_schema=True,
                            usage=await inspect_schema_usage(db, schema.name),
                        )
                    ),
                )
            )
        return results
    finally:
        await db.sqla_engine.dispose()


async def create_graph_schemas(
    self,
    *,
    graph_id: str,
    schemas: list[GraphSchemaCreateParam],
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphSchemaDetail]:
    """Create multiple schemas in a managed graph."""
    if not schemas:
        raise GraphContentValidationError("At least one schema is required.")

    # Reject duplicate names before doing any work
    names = [s.name for s in schemas]
    if len(names) != len(set(names)):
        duplicates = [name for name in names if names.count(name) > 1]
        raise GraphContentValidationError(
            f"Duplicate schema names provided: {set(duplicates)}"
        )

    # Validate all schemas first
    validated_schemas = []
    for schema_param in schemas:
        clean_name = validate_schema_name(schema_param.name)
        clean_kind = validate_schema_kind(schema_param.kind)
        validate_json_schema(schema_param.json_schema)
        
        # Sanitize SVG icon if provided
        sanitized_svg_icon = None
        if schema_param.svg_icon is not None:
            sanitized_svg_icon = sanitize_svg(schema_param.svg_icon)
        
        validated_schemas.append(
            (clean_name, schema_param.json_schema, clean_kind, schema_param.alias, sanitized_svg_icon, schema_param.extends)
        )
    clean_names = [clean_name for clean_name, _, _, _, _, _ in validated_schemas]

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="manage_schemas",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        # Check for existing schemas
        try:
            refs = [
                SchemaRef(name=clean_name, kind=clean_kind)
                for clean_name, _, clean_kind, _, _, _ in validated_schemas
            ]
            existing = await db.get_schemas(refs)
            existing_names = [s.name for s in existing]
            raise GraphContentConflictError(
                f"Schemas already exist: {existing_names}"
            )
        except SchemaNotFoundError:
            pass

        try:
            schema_upserts = [
                SchemaUpsert(
                    name=name,
                    json_schema=json_schema,
                    kind=kind,
                    alias=alias,
                    svg_icon=svg_icon,
                    extends=extends,
                )
                for name, json_schema, kind, alias, svg_icon, extends in validated_schemas
            ]
            created_schemas = await db.set_schemas(schema_upserts)
        except SchemaProtectedError as exc:
            raise GraphContentConflictError(str(exc)) from exc
        except SchemaInheritanceError as exc:
            raise GraphContentValidationError(str(exc)) from exc
        except (SchemaBreakingChangeError, ValueError) as exc:
            raise GraphContentValidationError(str(exc)) from exc

        results = []
        for schema in created_schemas:
            results.append(
                GraphSchemaDetail(
                    schema_record=GraphSchemaRecord(
                        **serialize_schema_record(
                            schema,
                            include_json_schema=True,
                            usage=GraphSchemaUsage(),
                        )
                    ),
                )
            )
        return results
    finally:
        await db.sqla_engine.dispose()


async def update_graph_schemas(
    self,
    *,
    graph_id: str,
    schemas: list[GraphSchemaUpdateParam],
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphSchemaDetail]:
    """Update multiple schemas in a managed graph when the change is non-breaking. Omitted fields are left unchanged."""
    if not schemas:
        raise GraphContentValidationError("At least one schema is required.")

    # Reject duplicate names before doing any work
    names = [s.name for s in schemas]
    if len(names) != len(set(names)):
        duplicates = [name for name in names if names.count(name) > 1]
        raise GraphContentValidationError(
            f"Duplicate schema names provided: {set(duplicates)}"
        )

    # Validate all schemas first
    validated_schemas = []
    for schema_param in schemas:
        clean_name = validate_schema_name(schema_param.name)
        clean_kind = (
            validate_schema_kind(schema_param.kind)
            if schema_param.kind is not None
            else None
        )
        if schema_param.json_schema is not None:
            validate_json_schema(schema_param.json_schema)
        
        # Sanitize SVG icon if provided
        sanitized_svg_icon = None
        if schema_param.svg_icon is not None:
            sanitized_svg_icon = sanitize_svg(schema_param.svg_icon)
        
        validated_schemas.append(
            (clean_name, schema_param.json_schema, clean_kind, schema_param.alias, sanitized_svg_icon, schema_param.extends)
        )
    clean_names = [clean_name for clean_name, _, _, _, _, _ in validated_schemas]

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="manage_schemas",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        # Check existing schemas
        try:
            # For schemas where kind is not specified, we need to find the existing kind
            # For schemas where kind is specified, we use that kind
            existing_by_name = {}
            
            # First, handle schemas with explicit kind
            refs_with_kind = [
                SchemaRef(name=clean_name, kind=clean_kind)
                for clean_name, _, clean_kind, _, _, _ in validated_schemas
                if clean_kind is not None
            ]
            
            if refs_with_kind:
                schemas_with_kind = await db.get_schemas(refs_with_kind)
                for schema in schemas_with_kind:
                    existing_by_name[schema.name] = schema
            
            # Now handle schemas without explicit kind - need to look up existing kind
            refs_without_kind = [
                clean_name
                for clean_name, _, clean_kind, _, _, _ in validated_schemas
                if clean_kind is None and clean_name not in existing_by_name
            ]
            
            if refs_without_kind:
                # Get all schemas from the database by listing them all
                all_schema_refs = await db.list_schemas()
                
                # Filter to only the names we need
                needed_names = [ref.name for ref in all_schema_refs if ref.name in refs_without_kind]
                
                if needed_names:
                    # Try to get each schema by name, handling missing kinds
                    for name in needed_names:
                        # Try node first, then edge
                        try:
                            schemas = await db.get_schemas([SchemaRef(name=name, kind="node")])
                            if schemas:
                                existing_by_name[name] = schemas[0]
                                continue
                        except SchemaNotFoundError:
                            pass
                        try:
                            schemas = await db.get_schemas([SchemaRef(name=name, kind="edge")])
                            if schemas:
                                existing_by_name[name] = schemas[0]
                                continue
                        except SchemaNotFoundError:
                            pass
            
            # Verify all schemas exist
            missing = [
                name
                for name, _, _, _, _, _ in validated_schemas
                if name not in existing_by_name
            ]
            if missing:
                raise SchemaNotFoundError(f"Schemas not found: {missing}")
        except SchemaNotFoundError as exc:
            raise GraphContentNotFoundError(
                f"One or more schemas not found: {exc}"
            ) from exc

        # Build upserts with preserved values for omitted fields
        schema_upserts = []
        for clean_name, json_schema, clean_kind, alias, svg_icon, extends in validated_schemas:
            existing = existing_by_name[clean_name]
            json_schema_ = (
                json_schema if json_schema is not None else existing.json_schema
            )
            kind_ = (
                clean_kind
                if clean_kind is not None
                else schema_kind_from_record(existing)
            )
            alias_ = alias if alias is not None else getattr(existing, "alias", None)
            svg_icon_ = svg_icon if svg_icon is not None else getattr(existing, "svg_icon", None)
            # None from the client means leave parents unchanged (SchemaUpsert semantics).
            extends_ = extends if extends is not None else getattr(existing, "extends", None)
            schema_upserts.append(
                SchemaUpsert(
                    name=clean_name,
                    json_schema=json_schema_,
                    kind=kind_,
                    alias=alias_,
                    svg_icon=svg_icon_,
                    extends=extends_,
                )
            )

        try:
            updated_schemas = await db.set_schemas(schema_upserts)
        except SchemaProtectedError as exc:
            raise GraphContentConflictError(str(exc)) from exc
        except SchemaInheritanceError as exc:
            raise GraphContentValidationError(str(exc)) from exc
        except SchemaBreakingChangeError as exc:
            raise GraphContentValidationError(
                f"Breaking schema changes are not supported yet. Use a migration workflow."
            ) from exc
        except ValueError as exc:
            raise GraphContentValidationError(str(exc)) from exc

        results = []
        for schema in updated_schemas:
            results.append(
                GraphSchemaDetail(
                    schema_record=GraphSchemaRecord(
                        **serialize_schema_record(
                            schema,
                            include_json_schema=True,
                            usage=await inspect_schema_usage(db, schema.name),
                        )
                    ),
                )
            )
        return results
    finally:
        await db.sqla_engine.dispose()


async def delete_graph_schemas(
    self,
    *,
    graph_id: str,
    names: list[str],
    kind: str,
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphSchemaDeleteResult]:
    """Delete multiple unused schemas from a managed graph."""
    clean_names = [validate_schema_name(name) for name in names]
    clean_kind = validate_schema_kind(kind)

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="manage_schemas",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        refs = [SchemaRef(name=name, kind=clean_kind) for name in clean_names]
        try:
            schemas = await db.get_schemas(refs)
        except SchemaNotFoundError as exc:
            # Translate missing schema names into admin-facing not-found errors
            # (web routes and REST tool handlers catch GraphContentError).
            missing: list[str] = []
            for clean_name in clean_names:
                try:
                    await db.get_schemas([SchemaRef(name=clean_name, kind=clean_kind)])
                except SchemaNotFoundError:
                    missing.append(clean_name)

            if len(missing) == 1:
                raise GraphContentNotFoundError(
                    f"Schema '{missing[0]}' was not found."
                ) from exc
            raise GraphContentNotFoundError(
                f"Schemas not found: {missing}"
            ) from exc
        in_use_messages: list[str] = []
        for schema in schemas:
            usage = await inspect_schema_usage(db, schema.name)
            if usage.node_count or usage.edge_count:
                in_use_messages.append(
                    format_schema_delete_blocker_message(
                        schema.name, usage
                    )
                )
        if in_use_messages:
            # Keep the user-facing error message stable by formatting
            # from computed usage counts (matches UI tests).
            raise GraphContentConflictError("; ".join(in_use_messages))
        try:
            await db.delete_schemas(refs)
        except SchemaProtectedError as exc:
            raise GraphContentConflictError(str(exc)) from exc
        except SchemaInUseError as exc:
            raise GraphContentConflictError(str(exc)) from exc
        return [GraphSchemaDeleteResult(name=name) for name in clean_names]
    finally:
        await db.sqla_engine.dispose()
