"""Schema operations for graph-content service."""

from __future__ import annotations

import logging

from gpdb import (
    GPGraph,
    SchemaBreakingChangeError,
    SchemaInUseError,
    SchemaNotFoundError,
    SchemaUpsert,
    SchemaValidationError,
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
        schema_names = await db.list_schemas()
        from gpdb import SearchQuery
        node_page = await db.search_nodes(SearchQuery(limit=1))
        edge_page = await db.search_edges(SearchQuery(limit=1))
        overview.summary = GraphContentSummary(
            schema_count=len(schema_names),
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
        schema_names = sorted(await db.list_schemas(kind=clean_kind))
        if schema_names:
            try:
                schemas = await db.get_schemas(schema_names)
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
                for schema_name in schema_names:
                    try:
                        schemas = await db.get_schemas([schema_name])
                        schema = schemas[0]
                    except SchemaNotFoundError:
                        continue
                    items.append(
                        serialize_schema_record(
                            schema,
                            include_json_schema=include_json_schema,
                            usage=GraphSchemaUsage(),
                        )
                    )
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

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="view",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        try:
            schemas = await db.get_schemas(clean_names)
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
        validated_schemas.append(
            (clean_name, schema_param.json_schema, clean_kind)
        )
    clean_names = [clean_name for clean_name, _, _ in validated_schemas]

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
            existing = await db.get_schemas(clean_names)
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
                )
                for name, json_schema, kind in validated_schemas
            ]
            created_schemas = await db.set_schemas(schema_upserts)
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
        validated_schemas.append(
            (clean_name, schema_param.json_schema, clean_kind)
        )
    clean_names = [clean_name for clean_name, _, _ in validated_schemas]

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
            existing_schemas = await db.get_schemas(clean_names)
            existing_by_name = {s.name: s for s in existing_schemas}
        except SchemaNotFoundError as exc:
            raise GraphContentNotFoundError(
                f"One or more schemas not found: {exc}"
            ) from exc

        # Build upserts with preserved values for omitted fields
        schema_upserts = []
        for clean_name, json_schema, clean_kind in validated_schemas:
            existing = existing_by_name[clean_name]
            json_schema_ = (
                json_schema if json_schema is not None else existing.json_schema
            )
            kind_ = (
                clean_kind
                if clean_kind is not None
                else schema_kind_from_record(existing)
            )
            schema_upserts.append(
                SchemaUpsert(
                    name=clean_name,
                    json_schema=json_schema_,
                    kind=kind_,
                )
            )

        try:
            updated_schemas = await db.set_schemas(schema_upserts)
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
    current_user: AdminUser | None,
    allow_local_system: bool = False,
) -> list[GraphSchemaDeleteResult]:
    """Delete multiple unused schemas from a managed graph."""
    clean_names = [validate_schema_name(name) for name in names]

    graph, instance, db = await open_graph(
        graph_id=graph_id,
        current_user=current_user,
        allow_local_system=allow_local_system,
        permission_kind="manage_schemas",
        admin_store=require_admin_store(self._admin_store),
        captive_url_factory=self._captive_url_factory,
    )
    try:
        try:
            schemas = await db.get_schemas(clean_names)
        except SchemaNotFoundError as exc:
            # Translate missing schema names into admin-facing not-found errors
            # (web routes and REST tool handlers catch GraphContentError).
            missing: list[str] = []
            for clean_name in clean_names:
                try:
                    await db.get_schemas([clean_name])
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
            await db.delete_schemas(clean_names)
        except SchemaInUseError as exc:
            raise GraphContentConflictError(str(exc)) from exc
        return [GraphSchemaDeleteResult(name=name) for name in clean_names]
    finally:
        await db.sqla_engine.dispose()
