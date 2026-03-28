import asyncio
import re
from datetime import datetime, timezone
from types import SimpleNamespace

from mcp.server.auth.middleware.auth_context import auth_context_var

from gpdb import EdgeUpsert, GPGraph, NodeUpsert, SchemaUpsert
from gpdb.admin import entry
from gpdb.admin.servers import _invoke_tool_raw
from gpdb.admin.store import AdminStore
from toolaccess import InvocationContext, Principal, get_public_signature


def test_graph_change_events_list_across_rest_mcp_cli(admin_test_env):
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_slice")
    api_key_value = _create_api_key(client, label="events-key")
    since_time = _seed_event_data(manager, table_prefix="events_slice")

    response = client.post(
        "/api/graph_change_events_list",
        json={
            "graph_id": graph_id,
            "since_time": since_time.isoformat(),
            "limit": 100,
            "offset": 0,
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    rest_page = response.json()
    assert rest_page["total"] >= 4
    kinds = {item["kind"] for item in rest_page["items"]}
    assert "node_created" in kinds
    assert "node_origin_edge_created" in kinds
    assert "node_destination_edge_created" in kinds

    edge_ids = {
        item["edge_id"]
        for item in rest_page["items"]
        if item["kind"] in {"node_origin_edge_created", "node_destination_edge_created"}
    }
    for edge_id in edge_ids:
        pair_kinds = {
            item["kind"]
            for item in rest_page["items"]
            if item.get("edge_id") == edge_id
            and item["kind"]
            in {"node_origin_edge_created", "node_destination_edge_created"}
        }
        assert pair_kinds == {
            "node_origin_edge_created",
            "node_destination_edge_created",
        }

    mcp_page = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_change_events_list",
        {
            "graph_id": graph_id,
            "since_time": since_time.isoformat(),
            "limit": 100,
            "offset": 0,
        },
    ).model_dump()
    assert mcp_page["total"] == rest_page["total"]
    assert [item["kind"] for item in mcp_page["items"]] == [
        item["kind"] for item in rest_page["items"]
    ]

    cli_page = _call_local_cli_tool(
        manager,
        "graph_change_events_list",
        {
            "graph_id": graph_id,
            "since_time": since_time.isoformat(),
            "limit": 100,
            "offset": 0,
        },
    ).model_dump()
    assert cli_page["total"] == rest_page["total"]
    assert [item["kind"] for item in cli_page["items"]] == [
        item["kind"] for item in rest_page["items"]
    ]

    filtered_response = client.post(
        "/api/graph_change_events_list",
        json={
            "graph_id": graph_id,
            "since_time": since_time.isoformat(),
            "event_filter": {
                "node_created": True,
                "node_updated": False,
                "node_deleted": False,
                "node_origin_edge_created": False,
                "node_origin_edge_updated": False,
                "node_origin_edge_deleted": False,
                "node_destination_edge_created": False,
                "node_destination_edge_updated": False,
                "node_destination_edge_deleted": False,
                "node_types": ["task_base*"],
            },
            "limit": 100,
            "offset": 0,
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert filtered_response.status_code == 200
    filtered_items = filtered_response.json()["items"]
    assert filtered_items
    assert {item["kind"] for item in filtered_items} == {"node_created"}
    assert {item["node_type"] for item in filtered_items} == {
        "task_base",
        "task_child",
    }

    paged_response = client.post(
        "/api/graph_change_events_list",
        json={
            "graph_id": graph_id,
            "since_time": since_time.isoformat(),
            "limit": 1,
            "offset": 1,
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert paged_response.status_code == 200
    paged_page = paged_response.json()
    assert paged_page["total"] == rest_page["total"]
    assert len(paged_page["items"]) == 1

    first_occurred_at = datetime.fromisoformat(rest_page["items"][0]["occurred_at"])
    boundary_response = client.post(
        "/api/graph_change_events_list",
        json={
            "graph_id": graph_id,
            "since_time": first_occurred_at.isoformat(),
            "limit": 100,
            "offset": 0,
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert boundary_response.status_code == 200
    boundary_items = boundary_response.json()["items"]
    assert all(
        datetime.fromisoformat(item["occurred_at"]) > first_occurred_at
        for item in boundary_items
    )


def test_graph_change_events_list_validation_and_auth(admin_test_env):
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_validation")
    api_key_value = _create_api_key(client, label="events-validation-key")
    since_time = _seed_event_data(manager, table_prefix="events_validation")

    unauthorized = client.post(
        "/api/graph_change_events_list",
        json={
            "graph_id": graph_id,
            "since_time": since_time.isoformat(),
            "limit": 10,
            "offset": 0,
        },
    )
    assert unauthorized.status_code == 401

    invalid_limit = client.post(
        "/api/graph_change_events_list",
        json={
            "graph_id": graph_id,
            "since_time": since_time.isoformat(),
            "limit": 0,
            "offset": 0,
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert invalid_limit.status_code == 400
    assert "Limit must be at least 1." in invalid_limit.text


def _bootstrap_owner(client) -> None:
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


def _login(client) -> None:
    response = client.post(
        "/login",
        data={"username": "owner", "password": "secret-pass"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/"


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


def _create_graph(client, manager, *, table_prefix: str) -> str:
    response = client.get("/graphs/new")
    assert response.status_code == 200
    instance_id = _extract_instance_option_value(response.text, "Default instance")
    response = client.post(
        "/graphs",
        data={
            "instance_id": instance_id,
            "table_prefix": table_prefix,
            "display_name": "Events Graph",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    graph = _read_graph_by_prefix(manager, table_prefix=table_prefix)
    assert graph is not None
    return graph.id


def _create_api_key(client, *, label: str) -> str:
    response = client.post("/apikeys", data={"label": label}, follow_redirects=False)
    assert response.status_code == 303
    detail = client.get(response.headers["location"])
    assert detail.status_code == 200
    return _extract_revealed_api_key(detail.text)


def _read_graph_by_prefix(manager, *, table_prefix: str):
    services = manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.instance_secret is not None

    async def _load():
        store = AdminStore(
            services.captive_server.get_uri(),
            instance_secret=services.resolved_config.auth.instance_secret,
        )
        try:
            instance = await store.get_instance_by_slug("default")
            assert instance is not None
            return await store.get_graph_by_scope(instance.id, table_prefix)
        finally:
            await store.close()

    return asyncio.run(_load())


def _seed_event_data(manager, *, table_prefix: str) -> datetime:
    services = manager.app.state.services
    assert services.captive_server is not None

    async def _seed() -> datetime:
        db = GPGraph(services.captive_server.get_uri(), table_prefix=table_prefix)
        try:
            await db.set_schemas(
                [
                    SchemaUpsert(
                        name="task_base",
                        kind="node",
                        json_schema={
                            "type": "object",
                            "properties": {"name": {"type": "string"}},
                            "required": ["name"],
                        },
                    ),
                    SchemaUpsert(
                        name="task_child",
                        kind="node",
                        extends=["task_base"],
                        json_schema={
                            "type": "object",
                            "properties": {"child_label": {"type": "string"}},
                            "required": ["child_label"],
                        },
                    ),
                    SchemaUpsert(
                        name="rel_base",
                        kind="edge",
                        json_schema={"type": "object"},
                    ),
                    SchemaUpsert(
                        name="rel_child",
                        kind="edge",
                        extends=["rel_base"],
                        json_schema={
                            "type": "object",
                            "properties": {"weight": {"type": "number"}},
                        },
                    ),
                ]
            )
            since_time = datetime.now(timezone.utc)
            source = (
                await db.set_nodes(
                    [
                        NodeUpsert(type="task_base", name="source", data={"name": "Source"}),
                    ]
                )
            )[0]
            target = (
                await db.set_nodes(
                    [
                        NodeUpsert(
                            type="task_child",
                            name="target",
                            data={"name": "Target", "child_label": "Target"},
                        ),
                    ]
                )
            )[0]
            await db.set_edges(
                [
                    EdgeUpsert(
                        type="rel_child",
                        source_id=source.id,
                        target_id=target.id,
                        data={},
                    )
                ]
            )
            return since_time
        finally:
            await db.sqla_engine.dispose()

    return asyncio.run(_seed())


def _find_tool(manager, tool_name: str):
    runtime = manager.app.state.admin_runtime
    for service in [runtime.admin_service, runtime.graph_service, runtime.api_key_service]:
        for tool_def in service.tools:
            if tool_def.name == tool_name:
                return tool_def
    raise ValueError(f"Tool {tool_name} not found")


def _call_local_cli_tool(manager, tool_name: str, arguments: dict[str, object]):
    async def _call():
        services = manager.app.state.services
        admin_lifespan = entry.create_admin_lifespan(services)
        async with admin_lifespan(manager.app):
            tool = _find_tool(manager, tool_name)
            ctx = InvocationContext(
                surface="cli",
                principal=Principal(
                    kind="local",
                    is_authenticated=True,
                    is_trusted_local=True,
                ),
            )
            _, _, context_param_name = get_public_signature(tool.func)
            return await _invoke_tool_raw(
                tool,
                {"params": arguments},
                ctx,
                context_param_name=context_param_name,
                surface_resolver=None,
            )

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
                manager, verified_token, tool_name, {"params": arguments}
            )

    return asyncio.run(_call())


async def _call_authenticated_mcp_tool_in_loop(
    manager,
    verified_token,
    tool_name: str,
    arguments: dict[str, object],
):
    tool = _find_tool(manager, tool_name)
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
    token = auth_context_var.set(verified_token)
    try:
        _, _, context_param_name = get_public_signature(tool.func)
        return await _invoke_tool_raw(
            tool,
            arguments,
            ctx,
            context_param_name=context_param_name,
            surface_resolver=None,
        )
    finally:
        auth_context_var.reset(token)
