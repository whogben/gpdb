import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from mcp.server.auth.middleware.auth_context import auth_context_var
from sqlalchemy.engine import make_url

from gpdb import EdgeUpsert, GPGraph, NodeUpsert, SchemaUpsert
from gpdb.admin import entry
from gpdb.admin.auth import generate_api_key, hash_api_key_secret, hash_password
from gpdb.admin.config import ConfigStore
from gpdb.admin.store import AdminStore


def test_graph_overview_across_surfaces(admin_test_env):
    """Test the shared graph overview flow across web, REST, CLI, and MCP."""
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
            "table_prefix": "slice_one",
            "display_name": "Slice One",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="slice_one")
    assert graph is not None
    graph_id = graph.id
    _seed_graph_content(manager, table_prefix="slice_one")

    response = client.get(f"/graphs/{graph_id}")
    assert response.status_code == 200
    assert "Slice One" in response.text
    assert "1 registered schema." in response.text
    assert "2 nodes." in response.text
    assert "1 edge." in response.text

    response = client.post(
        "/apikeys",
        data={"label": "Graph overview key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]
    response = client.get(api_key_detail_path)
    api_key_value = _extract_revealed_api_key(response.text)

    response = client.post(
        "/api/graph_overview",
        json={"graph_id": graph_id},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    assert response.json()["summary"] == {
        "schema_count": 1,
        "node_count": 2,
        "edge_count": 1,
    }

    mcp_result = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_overview",
        {"graph_id": graph_id},
    )
    mcp_result_dict = mcp_result.model_dump()
    assert mcp_result_dict["summary"] == {
        "schema_count": 1,
        "node_count": 2,
        "edge_count": 1,
    }


def _create_test_manager(tmp_path: Path):
    """Create a manager backed by a temporary config and captive data dir."""
    data_dir = tmp_path / "admin data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "admin.toml").write_text(
        (
            "[server]\n"
            'host = "127.0.0.1"\n'
            "port = 8747\n"
            "[auth]\n"
            'session_secret = "test-session-secret"\n'
        ),
        encoding="utf-8",
    )
    config_store = ConfigStore.from_sources(cli_data_dir=data_dir)
    resolved_config = config_store.load()
    return entry.create_manager(
        resolved_config=resolved_config, config_store=config_store
    )


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


def _seed_graph_content(manager, *, table_prefix: str) -> None:
    services = manager.app.state.services
    assert services.captive_server is not None

    async def _seed() -> None:
        db = GPGraph(services.captive_server.get_uri(), table_prefix=table_prefix)
        try:
            await db.upsert_schema(
                SchemaUpsert(
                    name="task_schema",
                    json_schema={
                        "type": "object",
                        "properties": {
                            "label": {"type": "string"},
                        },
                        "required": ["label"],
                    },
                )
            )
            source = await db.set_node(
                NodeUpsert(
                    type="task",
                    name="source",
                    schema_name="task_schema",
                    data={"label": "Source task"},
                )
            )
            target = await db.set_node(
                NodeUpsert(
                    type="task",
                    name="target",
                    data={},
                )
            )
            await db.set_edge(
                EdgeUpsert(
                    type="depends_on",
                    source_id=source.id,
                    target_id=target.id,
                    data={},
                )
            )
        finally:
            await db.sqla_engine.dispose()

    return asyncio.run(_seed())


def _verify_api_key_with_mcp_verifier(manager, api_key_value: str):
    services = manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None

    async def _verify():
        store = AdminStore(
            services.captive_server.get_uri(),
            instance_secret=services.resolved_config.auth.session_secret,
        )
        try:
            verifier = entry._AdminAPIKeyTokenVerifier(
                SimpleNamespace(admin_store=store)
            )
            return await verifier.verify_token(api_key_value)
        finally:
            await store.close()

    return asyncio.run(_verify())


def _call_authenticated_mcp_tool(
    manager,
    api_key_value: str,
    tool_name: str,
    arguments: dict[str, object],
):
    verified_token = _verify_api_key_with_mcp_verifier(manager, api_key_value)
    assert verified_token is not None

    async def _call():
        token = auth_context_var.set(SimpleNamespace(access_token=verified_token))
        try:
            result = await manager.mcp_servers["gpdb"].call_tool(tool_name, arguments)
        finally:
            auth_context_var.reset(token)
        assert result.content
        return json.loads(result.content[0].text)

    return asyncio.run(_call())


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

    # Set current_user in the context state
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
