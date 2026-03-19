"""Tests for node payload behavior: payload_set, payload_get, and error paths."""

import asyncio
import base64
import re
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from gpdb import GPGraph, NodeUpsert, SchemaUpsert
from gpdb.admin import entry
from gpdb.admin.store import AdminStore


def test_rest_payload_set_then_get_returns_stored_payload(admin_test_env):
    """graph_node_payloads_set (REST) then payloads_get returns same bytes and metadata."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id, node_id, api_key_value = _graph_and_node_without_payload(client, manager)

    payload_bytes = b"rest payload set only"
    response = client.post(
        "/api/graph_node_payloads_set",
        json={
            "graph_id": graph_id,
            "payloads": [
                {
                    "node_id": node_id,
                    "payload_base64": base64.b64encode(payload_bytes).decode(
                        "ascii"
                    ),
                    "payload_mime": "text/plain",
                    "payload_filename": "set-via-rest.txt",
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    data_list = response.json()
    data = data_list[0]
    assert data["node"]["payload_size"] == len(payload_bytes)
    assert data["node"]["payload_filename"] == "set-via-rest.txt"

    response = client.post(
        "/api/graph_node_payloads_get",
        json={"graph_id": graph_id, "node_ids": [node_id]},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    got_list = response.json()
    got = got_list[0]
    assert base64.b64decode(got["payload_base64"]) == payload_bytes
    assert got["node"]["payload_mime"] == "text/plain"
    assert got["node"]["payload_filename"] == "set-via-rest.txt"
    assert got["filename"] == "set-via-rest.txt"


def test_mcp_payload_set_then_get_returns_stored_payload(admin_test_env):
    """graph_node_payloads_set (MCP) then payloads_get returns same bytes and metadata."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id, node_id, api_key_value = _graph_and_node_without_payload(client, manager)

    payload_bytes = b"mcp payload set only"
    _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_node_payloads_set",
        {
            "graph_id": graph_id,
            "payloads": [
                {
                    "node_id": node_id,
                    "payload_base64": base64.b64encode(payload_bytes).decode(
                        "ascii"
                    ),
                    "payload_mime": "application/octet-stream",
                    "payload_filename": "set-via-mcp.bin",
                }
            ],
        },
    )

    got = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_node_payloads_get",
        {"graph_id": graph_id, "node_ids": [node_id]},
    )
    got_list = got
    got = got_list[0]
    assert base64.b64decode(got.payload_base64) == payload_bytes
    assert got.node.payload_mime == "application/octet-stream"
    assert got.node.payload_filename == "set-via-mcp.bin"
    assert got.filename == "set-via-mcp.bin"


def test_payload_get_without_payload_returns_400_rest(admin_test_env):
    """REST graph_node_payloads_get for node with no payload returns 400."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id, node_id, api_key_value = _graph_and_node_without_payload(client, manager)

    response = client.post(
        "/api/graph_node_payloads_get",
        json={"graph_id": graph_id, "node_ids": [node_id]},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 400
    assert "does not have a payload" in response.json()["detail"]


def test_payload_get_without_payload_returns_error_mcp(admin_test_env):
    """MCP graph_node_payloads_get for node with no payload raises/returns error."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id, node_id, api_key_value = _graph_and_node_without_payload(client, manager)

    with pytest.raises(Exception) as exc_info:
        _call_persisted_authenticated_mcp_tool(
            manager,
            api_key_value,
            "graph_node_payloads_get",
            {"graph_id": graph_id, "node_ids": [node_id]},
        )
    assert "does not have a payload" in str(exc_info.value)


def test_payload_get_nonexistent_node_returns_404(admin_test_env):
    """REST graph_node_payloads_get for non-existent node returns 404."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id, _, api_key_value = _graph_and_node_without_payload(client, manager)
    nonexistent_id = "nonexistent-node-id"

    response = client.post(
        "/api/graph_node_payloads_get",
        json={"graph_id": graph_id, "node_ids": [nonexistent_id]},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 404


def test_payload_set_invalid_base64_returns_400(admin_test_env):
    """graph_node_payloads_set with invalid base64 returns 400."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id, node_id, api_key_value = _graph_and_node_without_payload(client, manager)

    response = client.post(
        "/api/graph_node_payloads_set",
        json={
            "graph_id": graph_id,
            "payloads": [
                {
                    "node_id": node_id,
                    "payload_base64": "not-valid-base64!!",
                    "payload_mime": "text/plain",
                    "payload_filename": "x.txt",
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 400


def test_payload_binary_roundtrip(admin_test_env):
    """Binary (non-UTF-8) payload round-trips correctly via payload_set/get."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id, node_id, api_key_value = _graph_and_node_without_payload(client, manager)

    payload_bytes = bytes(range(256))
    response = client.post(
        "/api/graph_node_payloads_set",
        json={
            "graph_id": graph_id,
            "payloads": [
                {
                    "node_id": node_id,
                    "payload_base64": base64.b64encode(payload_bytes).decode(
                        "ascii"
                    ),
                    "payload_mime": "application/octet-stream",
                    "payload_filename": "binary.bin",
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200

    response = client.post(
        "/api/graph_node_payloads_get",
        json={"graph_id": graph_id, "node_ids": [node_id]},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    got = response.json()[0]
    assert base64.b64decode(got["payload_base64"]) == payload_bytes


def test_payload_set_replaces_existing_payload(admin_test_env):
    """Calling payloads_set twice stores the second payload."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id, node_id, api_key_value = _graph_and_node_without_payload(client, manager)

    first = b"first payload"
    response = client.post(
        "/api/graph_node_payloads_set",
        json={
            "graph_id": graph_id,
            "payloads": [
                {
                    "node_id": node_id,
                    "payload_base64": base64.b64encode(first).decode("ascii"),
                    "payload_mime": "text/plain",
                    "payload_filename": "first.txt",
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200

    second = b"second payload replaced"
    response = client.post(
        "/api/graph_node_payloads_set",
        json={
            "graph_id": graph_id,
            "payloads": [
                {
                    "node_id": node_id,
                    "payload_base64": base64.b64encode(second).decode("ascii"),
                    "payload_mime": "application/octet-stream",
                    "payload_filename": "second.txt",
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200

    response = client.post(
        "/api/graph_node_payloads_get",
        json={"graph_id": graph_id, "node_ids": [node_id]},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    got = response.json()[0]
    assert base64.b64decode(got["payload_base64"]) == second
    assert got["node"]["payload_filename"] == "second.txt"


def test_web_payload_upload_no_file_redirects_with_error(admin_test_env):
    """Web POST to payload upload with no file redirects with error message."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id, node_id, _ = _graph_and_node_without_payload(client, manager)

    response = client.post(
        f"/graphs/{graph_id}/nodes/{node_id}/payload",
        data={"mime": "text/plain"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "error=" in response.headers.get("location", "")
    response = client.get(response.headers["location"])
    assert "Choose a file to upload" in response.text


def test_create_node_without_payload_has_no_payload(admin_test_env):
    """REST graph_nodes_create without payload_base64 yields has_payload false."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id, api_key_value = _graph_and_api_key(client, manager)

    response = client.post(
        "/api/graph_nodes_create",
        json={
            "graph_id": graph_id,
            "nodes": [
                {
                    "type": "task",
                    "name": "no-payload-node",
                    "data": {"name": "No payload"},
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    node_detail = response.json()[0]
    assert node_detail["node"]["has_payload"] is False
    assert node_detail["node"]["payload_size"] == 0
    assert node_detail["node"].get("payload_filename") is None


def _graph_and_node_without_payload(client, manager):
    """Create graph + node without payload, return (graph_id, node_id, api_key)."""
    graph_id, api_key_value = _graph_and_api_key(client, manager)
    response = client.post(
        "/api/graph_nodes_create",
        json={
            "graph_id": graph_id,
            "nodes": [
                {
                    "type": "task",
                    "name": "payload-test-node",
                    "data": {"name": "Payload test"},
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    node_id = response.json()[0]["node"]["id"]
    return graph_id, node_id, api_key_value


def _graph_and_api_key(client, manager):
    """Create payload_tests graph and return (graph_id, api_key_value)."""
    response = client.get("/graphs/new")
    assert response.status_code == 200
    default_instance_id = _extract_instance_option_value(
        response.text, "Default instance"
    )
    response = client.post(
        "/graphs",
        data={
            "instance_id": default_instance_id,
            "table_prefix": "payload_tests",
            "display_name": "Payload Tests",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    graph = _read_graph_by_prefix(manager, table_prefix="payload_tests")
    assert graph is not None
    graph_id = graph.id
    _seed_graph_schema(manager, table_prefix="payload_tests", schema_name="task")
    response = client.post(
        "/apikeys",
        data={"label": "Payload test key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    response = client.get(response.headers["location"])
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)
    return graph_id, api_key_value


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
    match = re.search(
        rf'<option[^>]*value="([^"]+)"[^>]*>\s*{re.escape(label)}\s*\([^)]*\)\s*</option>',
        html,
        re.S,
    )
    assert match is not None
    return match.group(1)


def _extract_revealed_api_key(html: str) -> str:
    match = re.search(r'<input[^>]*readonly[^>]*value="([^"]+)"', html)
    assert match is not None
    return match.group(1)


def _read_graph_by_prefix(manager, *, table_prefix: str):
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


def _seed_graph_schema(
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
            await db.set_schemas(
                [
                    SchemaUpsert(
                        name=schema_name,
                        json_schema=_schema_definition(f"{schema_name} schema"),
                        kind=kind,
                    )
                ]
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
    ctx.state["current_user"] = user
    ctx.state["access_token"] = verified_token
    _, _, context_param_name = get_public_signature(tool.func)
    result = await _invoke_tool_raw(
        tool,
        arguments,
        ctx,
        context_param_name=context_param_name,
        surface_resolver=None,
    )
    return result
