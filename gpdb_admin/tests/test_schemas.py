import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from mcp.server.auth.middleware.auth_context import auth_context_var

from gpdb import GPGraph, NodeUpsert
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
            "json_schema": json.dumps(_schema_definition("web schema")),
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith(
        f"/graphs/{graph_id}/schemas/web_schema"
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
        "/api/graph_schema_create",
        params={"graph_id": graph_id, "name": "rest_schema"},
        json=_schema_definition("rest schema"),
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    assert response.json()["schema"]["name"] == "rest_schema"

    mcp_created = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schema_create",
        {
            "graph_id": graph_id,
            "name": "mcp_schema",
            "json_schema": _schema_definition("mcp schema"),
        },
    )
    assert mcp_created["schema"]["name"] == "mcp_schema"
    assert mcp_created["schema"]["version"] == "1.0.0"

    _login(client)

    response = client.get(f"/graphs/{graph_id}/schemas")
    assert response.status_code == 200
    assert "Schema Slice" in response.text
    assert "web_schema" in response.text
    assert "rest_schema" in response.text
    assert "mcp_schema" in response.text

    response = client.get(f"/graphs/{graph_id}/schemas/web_schema")
    assert response.status_code == 200
    assert "Version 1.0.0" in response.text
    assert "Kind: node." in response.text
    assert "1 node reference this schema." in response.text
    assert "0 edges reference this schema." in response.text
    assert "Sample node IDs:" in response.text

    response = client.get(
        "/api/graph_schema_list",
        params={"graph_id": graph_id},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    assert response.json()["total"] == 3
    assert {item["name"] for item in response.json()["items"]} == {
        "mcp_schema",
        "rest_schema",
        "web_schema",
    }

    response = client.get(
        "/api/graph_schema_get",
        params={"graph_id": graph_id, "name": "web_schema"},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    assert response.json()["schema"]["kind"] == "node"
    assert response.json()["schema"]["usage"] == {
        "node_count": 1,
        "edge_count": 0,
        "sample_node_ids": [response.json()["schema"]["usage"]["sample_node_ids"][0]],
        "sample_edge_ids": [],
    }

    mcp_list = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schema_list",
        {"graph_id": graph_id},
    )
    assert mcp_list["total"] == 3

    mcp_get = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schema_get",
        {"graph_id": graph_id, "name": "web_schema"},
    )
    assert mcp_get["schema"]["usage"]["node_count"] == 1
    assert mcp_get["schema"]["usage"]["edge_count"] == 0


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
        "/api/graph_schema_create",
        params={"graph_id": graph_id, "name": "rest_schema"},
        json=_schema_definition("rest schema"),
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200

    mcp_created = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schema_create",
        {
            "graph_id": graph_id,
            "name": "mcp_schema",
            "json_schema": _schema_definition("mcp schema"),
        },
    )
    assert mcp_created["schema"]["name"] == "mcp_schema"

    _login(client)

    response = client.get(f"/graphs/{graph_id}/schemas/web_schema")
    assert response.status_code == 200
    assert (
        "Delete is blocked until all node and edge references are removed."
        in response.text
    )
    assert "Delete schema</button>" in response.text
    assert "disabled" in response.text

    response = client.get(f"/graphs/{graph_id}/schemas/web_unused")
    assert response.status_code == 200
    assert (
        "Delete is available because this schema is currently unused." in response.text
    )

    response = client.get(f"/graphs/{graph_id}/schemas/web_schema/edit")
    assert response.status_code == 200
    assert "Update schema" in response.text
    assert "non-breaking updates are allowed here" in response.text

    response = client.post(
        f"/graphs/{graph_id}/schemas/web_schema",
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
        f"/graphs/{graph_id}/schemas/web_schema"
    )

    response = client.get(f"/graphs/{graph_id}/schemas/web_schema")
    assert response.status_code == 200
    assert "Version 1.1.0" in response.text

    response = client.post(
        f"/graphs/{graph_id}/schemas/web_schema",
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
    assert "Breaking schema changes are not supported here yet." in response.text
    assert "Use a migration workflow for schema &#39;web_schema&#39;." in response.text

    response = client.post(
        f"/graphs/{graph_id}/schemas/web_schema/delete",
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert (
        "Schema &#39;web_schema&#39; cannot be deleted because it is still referenced by 1 node."
        in response.text
    )

    response = client.post(
        f"/graphs/{graph_id}/schemas/web_unused/delete",
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith(
        f"/graphs/{graph_id}/schemas?success="
    )

    response = client.post(
        "/api/graph_schema_update",
        params={"graph_id": graph_id, "name": "rest_schema"},
        json=_schema_definition(
            "rest schema updated",
            include_optional_status=True,
        ),
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    assert response.json()["schema"]["version"] == "1.1.0"
    assert (
        response.json()["schema"]["json_schema"]["properties"]["status"]["type"]
        == "string"
    )

    response = client.post(
        "/api/graph_schema_delete",
        params={"graph_id": graph_id, "name": "rest_schema"},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    assert response.json()["schema"]["name"] == "rest_schema"

    mcp_updated = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schema_update",
        {
            "graph_id": graph_id,
            "name": "mcp_schema",
            "json_schema": _schema_definition(
                "mcp schema updated",
                include_optional_status=True,
            ),
        },
    )
    assert mcp_updated["schema"]["version"] == "1.1.0"

    mcp_deleted = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schema_delete",
        {"graph_id": graph_id, "name": "mcp_schema"},
    )
    assert mcp_deleted["schema"]["name"] == "mcp_schema"

    _login(client)
    response = client.get(f"/graphs/{graph_id}/schemas")
    assert response.status_code == 200
    assert "web_schema" in response.text
    assert "web_unused" not in response.text
    assert "rest_schema" not in response.text
    assert "cli_schema" not in response.text
    assert "mcp_schema" not in response.text


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
                arguments,
            )

    return asyncio.run(_call())


async def _call_authenticated_mcp_tool_in_loop(
    manager,
    verified_token,
    tool_name: str,
    arguments: dict[str, object],
):
    token = auth_context_var.set(SimpleNamespace(access_token=verified_token))
    try:
        result = await manager.mcp_servers["gpdb"].call_tool(tool_name, arguments)
    finally:
        auth_context_var.reset(token)
    assert result.content
    return json.loads(result.content[0].text)


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
                await db.set_node(
                    NodeUpsert(
                        type="schema-test",
                        name="source",
                        schema_name=schema_name,
                        data={"name": "Source"},
                    )
                )
                return

            from gpdb import EdgeUpsert

            source = await db.set_node(
                NodeUpsert(
                    type="schema-test",
                    name="source",
                    data={},
                )
            )
            target = await db.set_node(
                NodeUpsert(
                    type="schema-test",
                    name="target",
                    data={},
                )
            )
            await db.set_edge(
                EdgeUpsert(
                    type="schema-link",
                    source_id=source.id,
                    target_id=target.id,
                    schema_name=schema_name,
                    data={"name": "Edge"},
                )
            )
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
