import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from gpdb import GPGraph, NodeUpsert, SchemaNotFoundError
from gpdb.admin import entry
from gpdb.admin.auth import generate_api_key, hash_api_key_secret, hash_password
from gpdb.admin.store import AdminStore


def test_graph_schema_registry_across_surfaces(admin_test_env):
    """Test schema browse/create flow across web, REST, CLI, and MCP."""
    manager = admin_test_env.manager
    client = admin_test_env.client
    graph_id = ""
    api_key_value = ""

    _bootstrap_owner(client)
    _login(client)

    response = client.get("/graphs/new")
    assert response.status_code == 200
    default_instance_id = _extract_instance_option_value(
        response.text, "Default instance"
    )

    response = client.post(
        "/graphs",
        data={
            "instance_id": default_instance_id,
            "table_prefix": "schema_slice",
            "display_name": "Schema Slice",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="schema_slice")
    assert graph is not None
    graph_id = graph.id

    response = client.get(f"/graphs/{graph_id}/schemas/new")
    assert response.status_code == 200
    assert "Create a schema for Schema Slice." in response.text
    assert 'name="kind"' in response.text

    response = client.post(
        f"/graphs/{graph_id}/schemas",
        data={
            "name": "web_schema",
            "kind": "node",
            "json_schema": json.dumps(_schema_definition("web schema")),
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith(
        f"/graphs/{graph_id}/schemas/web_schema/node"
    )

    _seed_schema_usage(manager, table_prefix="schema_slice", schema_name="web_schema")

    response = client.post(
        "/apikeys",
        data={"label": "Schema slice key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]
    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)

    response = client.post(
        "/api/graph_schemas_create",
        json={
            "graph_id": graph_id,
            "schemas": [
                {
                    "name": "rest_schema",
                    "json_schema": _schema_definition("rest schema"),
                    "kind": "node",
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    rest_created_list = response.json()
    rest_created = rest_created_list[0]
    assert rest_created["schema"]["name"] == "rest_schema"

    mcp_created = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schemas_create",
        {
            "graph_id": graph_id,
            "schemas": [
                {
                    "name": "mcp_schema",
                    "json_schema": _schema_definition("mcp schema"),
                    "kind": "node",
                }
            ],
        },
    )
    mcp_created = mcp_created[0]
    assert mcp_created.schema.name == "mcp_schema"
    assert mcp_created.schema.version == "1.0.0"

    _login(client)

    response = client.get(f"/graphs/{graph_id}/schemas")
    assert response.status_code == 200
    assert "Schema Slice" in response.text
    assert "web_schema" in response.text
    assert "rest_schema" in response.text
    assert "mcp_schema" in response.text

    response = client.get(f"/graphs/{graph_id}/schemas/web_schema/node")
    assert response.status_code == 200
    assert "Version 1.0.0" in response.text
    assert "Kind: node." in response.text
    assert "1 node reference this schema." in response.text
    assert "0 edges reference this schema." in response.text
    assert "Sample node IDs:" in response.text

    response = client.post(
        "/api/graph_schema_list",
        json={"graph_id": graph_id},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    assert response.json()["total"] == 5
    assert {item["name"] for item in response.json()["items"]} == {
        "__default__",
        "mcp_schema",
        "rest_schema",
        "web_schema",
    }

    response = client.post(
        "/api/graph_schemas_get",
        json={"graph_id": graph_id, "names": ["web_schema"], "kind": "node"},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    web_detail_list = response.json()
    web_detail = web_detail_list[0]
    assert web_detail["schema"]["kind"] == "node"
    assert web_detail["schema"]["usage"] == {
        "node_count": 1,
        "edge_count": 0,
        "sample_node_ids": [
            web_detail["schema"]["usage"]["sample_node_ids"][0]
        ],
        "sample_edge_ids": [],
    }

    mcp_list = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schema_list",
        {"graph_id": graph_id},
    )
    assert mcp_list.total == 5

    mcp_get = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schemas_get",
        {"graph_id": graph_id, "names": ["web_schema"], "kind": "node"},
    )
    mcp_get = mcp_get[0]
    assert mcp_get.schema.usage.node_count == 1
    assert mcp_get.schema.usage.edge_count == 0


def test_graph_schema_list_tolerates_toctou_delete(admin_test_env, monkeypatch):
    """Schema listing should not 500 if a schema disappears between list+get."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    graph_id = ""
    _bootstrap_owner(client)
    _login(client)

    response = client.get("/graphs/new")
    assert response.status_code == 200
    default_instance_id = _extract_instance_option_value(
        response.text, "Default instance"
    )

    table_prefix = "schema_list_toctou"
    response = client.post(
        "/graphs",
        data={
            "instance_id": default_instance_id,
            "table_prefix": table_prefix,
            "display_name": "Schema List TOCTOU",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix=table_prefix)
    assert graph is not None
    graph_id = graph.id

    response = client.post(
        f"/graphs/{graph_id}/schemas",
        data={
            "name": "web_schema",
            "kind": "node",
            "json_schema": json.dumps(_schema_definition("web schema")),
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    response = client.post(
        f"/graphs/{graph_id}/schemas",
        data={
            "name": "web_unused",
            "kind": "node",
            "json_schema": json.dumps(_schema_definition("web unused")),
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    missing_name = "web_unused"
    missing_kind = "node"
    original_get_schemas = GPGraph.get_schemas

    async def patched_get_schemas(self, refs):
        # Simulate a concurrent delete: bulk get fails, but per-name fallback
        # should ignore the missing entry and still return the rest.
        from gpdb.models import SchemaRef
        if len(refs) > 1 and any(r.name == missing_name and r.kind == missing_kind for r in refs):
            raise SchemaNotFoundError(f"Schemas not found: [{(missing_name, missing_kind)}]")
        if len(refs) == 1 and refs[0].name == missing_name and refs[0].kind == missing_kind:
            raise SchemaNotFoundError(f"Schemas not found: [{(missing_name, missing_kind)}]")
        return await original_get_schemas(self, refs)

    monkeypatch.setattr(GPGraph, "get_schemas", patched_get_schemas, raising=True)

    response = client.get(f"/graphs/{graph_id}/schemas")
    assert response.status_code == 200
    assert "web_schema" in response.text
    assert "web_unused" not in response.text


def test_graph_schema_update_and_delete_across_surfaces(admin_test_env):
    """Test schema update/delete flow, blockers, and breaking-change rejection."""
    manager = admin_test_env.manager
    client = admin_test_env.client
    graph_id = ""
    api_key_value = ""

    _bootstrap_owner(client)
    _login(client)

    response = client.get("/graphs/new")
    assert response.status_code == 200
    default_instance_id = _extract_instance_option_value(
        response.text, "Default instance"
    )

    response = client.post(
        "/graphs",
        data={
            "instance_id": default_instance_id,
            "table_prefix": "schema_slice_phase2",
            "display_name": "Schema Slice Phase 2",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="schema_slice_phase2")
    assert graph is not None
    graph_id = graph.id

    response = client.post(
        f"/graphs/{graph_id}/schemas",
        data={
            "name": "web_schema",
            "kind": "node",
            "json_schema": json.dumps(_schema_definition("web schema")),
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    response = client.post(
        f"/graphs/{graph_id}/schemas",
        data={
            "name": "web_unused",
            "json_schema": json.dumps(_schema_definition("web unused")),
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    _seed_schema_usage(
        manager,
        table_prefix="schema_slice_phase2",
        schema_name="web_schema",
    )

    response = client.post(
        "/apikeys",
        data={"label": "Schema phase 2 key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]
    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)

    response = client.post(
        "/api/graph_schemas_create",
        json={
            "graph_id": graph_id,
            "schemas": [
                {
                    "name": "rest_schema",
                    "json_schema": _schema_definition("rest schema"),
                    "kind": "node",
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200

    mcp_created = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schemas_create",
        {
            "graph_id": graph_id,
            "schemas": [
                {
                    "name": "mcp_schema",
                    "json_schema": _schema_definition("mcp schema"),
                    "kind": "node",
                }
            ],
        },
    )
    mcp_created = mcp_created[0]
    assert mcp_created.schema.name == "mcp_schema"

    _login(client)

    response = client.get(f"/graphs/{graph_id}/schemas/web_schema/node")
    assert response.status_code == 200
    assert (
        "Delete is blocked until all node and edge references are removed."
        in response.text
    )
    assert "Delete schema</button>" in response.text
    assert "disabled" in response.text

    response = client.get(f"/graphs/{graph_id}/schemas/web_unused/node")
    assert response.status_code == 200
    assert (
        "Delete is available because this schema is currently unused." in response.text
    )

    response = client.get(f"/graphs/{graph_id}/schemas/web_schema/node/edit")
    assert response.status_code == 200
    assert "Update schema" in response.text
    assert "non-breaking updates are allowed here" in response.text

    response = client.post(
        f"/graphs/{graph_id}/schemas/web_schema/node",
        data={
            "kind": "node",
            "json_schema": json.dumps(
                _schema_definition(
                    "web schema updated",
                    include_optional_status=True,
                )
            ),
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith(
        f"/graphs/{graph_id}/schemas/web_schema/node"
    )

    response = client.get(f"/graphs/{graph_id}/schemas/web_schema/node")
    assert response.status_code == 200
    assert "Version 1.1.0" in response.text

    response = client.post(
        f"/graphs/{graph_id}/schemas/web_schema/node",
        data={
            "kind": "node",
            "json_schema": json.dumps(
                _schema_definition(
                    "web schema breaking",
                    include_optional_status=True,
                    require_status=True,
                )
            ),
        },
    )
    assert response.status_code == 200
    assert "Breaking schema changes are not supported yet." in response.text
    assert "Use a migration workflow." in response.text

    response = client.post(
        f"/graphs/{graph_id}/schemas/web_schema/node/delete",
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert (
        "Schema &#39;web_schema&#39; cannot be deleted because it is still referenced by 1 node."
        in response.text
    )

    response = client.post(
        f"/graphs/{graph_id}/schemas/web_unused/node/delete",
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith(
        f"/graphs/{graph_id}/schemas?success="
    )

    response = client.post(
        "/api/graph_schemas_update",
        json={
            "graph_id": graph_id,
            "schemas": [
                {
                    "name": "rest_schema",
                    "json_schema": _schema_definition(
                        "rest schema updated",
                        include_optional_status=True,
                    ),
                    "kind": "node",
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    updated_list = response.json()
    updated = updated_list[0]
    assert updated["schema"]["version"] == "1.1.0"
    assert updated["schema"]["json_schema"]["properties"]["status"]["type"] == "string"

    response = client.post(
        "/api/graph_schemas_delete",
        json={"graph_id": graph_id, "names": ["rest_schema"], "kind": "node"},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    deleted_list = response.json()
    deleted = deleted_list[0]
    assert deleted["name"] == "rest_schema"

    mcp_updated = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schemas_update",
        {
            "graph_id": graph_id,
            "schemas": [
                {
                    "name": "mcp_schema",
                    "json_schema": _schema_definition(
                        "mcp schema updated",
                        include_optional_status=True,
                    ),
                }
            ],
        },
    )
    mcp_updated = mcp_updated[0]
    assert mcp_updated.schema.version == "1.1.0"

    mcp_deleted = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schemas_delete",
        {"graph_id": graph_id, "names": ["mcp_schema"], "kind": "node"},
    )
    mcp_deleted = mcp_deleted[0]
    assert mcp_deleted.name == "mcp_schema"

    _login(client)
    response = client.get(f"/graphs/{graph_id}/schemas")
    assert response.status_code == 200
    assert "web_schema" in response.text
    assert "web_unused" not in response.text
    assert "rest_schema" not in response.text
    assert "cli_schema" not in response.text
    assert "mcp_schema" not in response.text


def test_graph_schema_delete_missing_translates_to_not_found(admin_test_env):
    """Deleting an already-missing schema should not 500."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    response = client.get("/graphs/new")
    assert response.status_code == 200
    default_instance_id = _extract_instance_option_value(
        response.text, "Default instance"
    )

    table_prefix = "schema_delete_missing"
    response = client.post(
        "/graphs",
        data={
            "instance_id": default_instance_id,
            "table_prefix": table_prefix,
            "display_name": "Schema Delete Missing",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix=table_prefix)
    assert graph is not None
    graph_id = graph.id

    response = client.post(
        f"/graphs/{graph_id}/schemas/missing_schema/node/delete",
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert "not found" in response.text


def test_schema_partial_update_preserves_omitted_fields(admin_test_env):
    """Partial schema update with only json_schema preserves kind."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    response = client.get("/graphs/new")
    assert response.status_code == 200
    default_instance_id = _extract_instance_option_value(
        response.text, "Default instance"
    )
    response = client.post(
        "/graphs",
        data={
            "instance_id": default_instance_id,
            "table_prefix": "partial_schema",
            "display_name": "Partial Schema",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="partial_schema")
    assert graph is not None
    graph_id = graph.id

    response = client.post(
        "/apikeys",
        data={"label": "Partial schema key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    response = client.get(response.headers["location"])
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)

    response = client.post(
        "/api/graph_schemas_create",
        json={
            "graph_id": graph_id,
            "schemas": [
                {
                    "name": "partial_kind_schema",
                    "json_schema": _schema_definition("original"),
                    "kind": "node",
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    created_list = response.json()
    created = created_list[0]
    assert created["schema"]["kind"] == "node"

    # Update only json_schema; omit kind so it is preserved
    response = client.post(
        "/api/graph_schemas_update",
        json={
            "graph_id": graph_id,
            "schemas": [
                {
                    "name": "partial_kind_schema",
                    "json_schema": _schema_definition(
                        "updated description",
                        include_optional_status=True,
                    ),
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    updated_list = response.json()
    updated = updated_list[0]
    assert updated["schema"]["kind"] == "node"
    assert (
        updated["schema"]["json_schema"]["description"] == "updated description"
    )
    assert "status" in updated["schema"]["json_schema"]["properties"]


def _bootstrap_owner(client: TestClient) -> None:
    response = client.post(
        "/setup",
        data={
            "username": "owner",
            "display_name": "Primary Owner",
            "password": "secret-pass",
            "confirm_password": "secret-pass",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/login")


def _login(
    client: TestClient,
    *,
    username: str = "owner",
    password: str = "secret-pass",
) -> None:
    response = client.post(
        "/login",
        data={"username": username, "password": password},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/"
    assert "gpdb_admin_session" in response.cookies


def _extract_instance_option_value(html: str, label: str) -> str:
    import re

    match = re.search(
        rf'<option[^>]*value="([^"]+)"[^>]*>\s*{re.escape(label)}\s*\([^)]*\)\s*</option>',
        html,
        re.S,
    )
    assert match is not None
    return match.group(1)


def _extract_revealed_api_key(html: str) -> str:
    import re

    match = re.search(r'<input[^>]*readonly[^>]*value="([^"]+)"', html)
    assert match is not None
    return match.group(1)


def _call_persisted_authenticated_mcp_tool(
    manager,
    api_key_value: str,
    tool_name: str,
    arguments: dict[str, object],
):
    async def _call():
        services = manager.app.state.services
        admin_lifespan = entry.create_admin_lifespan(services)
        async with admin_lifespan(manager.app):
            assert services.admin_store is not None
            verified_token = await entry._AdminAPIKeyTokenVerifier(
                SimpleNamespace(admin_store=services.admin_store)
            ).verify_token(api_key_value)
            assert verified_token is not None
            return await _call_authenticated_mcp_tool_in_loop(
                manager,
                verified_token,
                tool_name,
                {"params": arguments},
            )

    return asyncio.run(_call())


async def _call_authenticated_mcp_tool_in_loop(
    manager,
    verified_token,
    tool_name: str,
    arguments: dict[str, object],
):
    from toolaccess import InvocationContext, Principal, get_public_signature
    from gpdb.admin.servers import _invoke_tool_raw

    runtime = manager.app.state.admin_runtime
    # Find the tool in the appropriate service
    tool = None
    for service in [
        runtime.admin_service,
        runtime.graph_service,
        runtime.api_key_service,
    ]:
        for tool_def in service.tools:
            if tool_def.name == tool_name:
                tool = tool_def
                break
        if tool is not None:
            break

    if tool is None:
        raise ValueError(f"Tool {tool_name} not found")

    # Get the user from the verified token
    services = manager.app.state.services
    user_id = verified_token.claims.get("user_id")
    user = await services.admin_store.get_user_by_id(user_id)

    ctx = InvocationContext(
        surface="mcp",
        principal=Principal(
            kind="api_key",
            id=verified_token.client_id,
            name=verified_token.claims.get("username"),
            claims=verified_token.claims,
            is_authenticated=True,
            is_trusted_local=False,
        ),
    )

    # Set the current_user in the context state
    ctx.state["current_user"] = user
    ctx.state["access_token"] = verified_token

    # Get the context parameter name
    _, _, context_param_name = get_public_signature(tool.func)

    # Use _invoke_tool_raw to run principal resolver and set current_user
    result = await _invoke_tool_raw(
        tool,
        arguments,
        ctx,
        context_param_name=context_param_name,
        surface_resolver=None,  # Skip principal resolver since we already set the user
    )
    return result


def _read_graph_by_prefix(manager, *, table_prefix: str):
    from gpdb.admin.store import AdminStore

    services = manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None

    async def _load():
        store = AdminStore(
            services.captive_server.get_uri(),
            instance_secret=services.resolved_config.auth.session_secret,
        )
        try:
            instance = await store.get_instance_by_slug("default")
            assert instance is not None
            return await store.get_graph_by_scope(instance.id, table_prefix)
        finally:
            await store.close()

    return asyncio.run(_load())


def _seed_schema_usage(
    manager,
    *,
    table_prefix: str,
    schema_name: str,
    kind: str = "node",
) -> None:
    services = manager.app.state.services
    assert services.captive_server is not None

    async def _seed() -> None:
        db = GPGraph(services.captive_server.get_uri(), table_prefix=table_prefix)
        try:
            if kind == "node":
                await db.set_nodes(
                    [
                        NodeUpsert(
                            type=schema_name,
                            name="source",
                            data={"name": "Source"},
                        )
                    ]
                )
                return

            from gpdb import EdgeUpsert

            source_list = await db.set_nodes(
                [
                    NodeUpsert(
                        type="__default__",
                        name="source",
                        data={},
                    )
                ]
            )
            source = source_list[0]
            target_list = await db.set_nodes(
                [
                    NodeUpsert(
                        type="__default__",
                        name="target",
                        data={},
                    )
                ]
            )
            target = target_list[0]
            _ = (await db.set_edges(
                [
                    EdgeUpsert(
                        type=schema_name,
                        source_id=source.id,
                        target_id=target.id,
                        data={"name": "Edge"},
                    )
                ]
            ))[0]
        finally:
            await db.sqla_engine.dispose()

    return asyncio.run(_seed())


def _schema_definition(
    description: str,
    *,
    include_optional_status: bool = False,
    require_status: bool = False,
) -> dict[str, object]:
    properties = {
        "name": {"type": "string"},
    }
    required = ["name"]
    if include_optional_status or require_status:
        properties["status"] = {"type": "string"}
    if require_status:
        required.append("status")
    return {
        "type": "object",
        "description": description,
        "properties": properties,
        "required": required,
    }
