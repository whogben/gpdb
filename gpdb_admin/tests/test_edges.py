import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from mcp.server.auth.middleware.auth_context import auth_context_var

from gpdb import EdgeUpsert, GPGraph, NodeUpsert, SchemaUpsert
from gpdb.admin import entry
from gpdb.admin.auth import generate_api_key, hash_api_key_secret, hash_password
from gpdb.admin.config import ConfigStore
from gpdb.admin.store import AdminStore


def test_graph_edge_schema_editor_renders_ui(admin_test_env):
    """Test that edge forms and detail pages expose the schema-driven web UI."""
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
            "table_prefix": "edge_schema_editor",
            "display_name": "Edge Schema Editor",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="edge_schema_editor")
    assert graph is not None
    graph_id = graph.id
    _seed_graph_schema(
        manager,
        table_prefix="edge_schema_editor",
        schema_name="edge_schema",
        kind="edge",
    )
    _seed_graph_schema(
        manager,
        table_prefix="edge_schema_editor",
        schema_name="node_only_schema",
        kind="node",
    )
    source_id = _seed_node_record(
        manager,
        table_prefix="edge_schema_editor",
        type="task",
        name="source-node",
        data={"name": "Source"},
    )
    target_id = _seed_node_record(
        manager,
        table_prefix="edge_schema_editor",
        type="task",
        name="target-node",
        data={"name": "Target"},
    )
    edge_id = _seed_edge_record(
        manager,
        table_prefix="edge_schema_editor",
        type="depends_on",
        source_id=source_id,
        target_id=target_id,
        schema_name="edge_schema",
        data={"name": "Schema backed edge"},
    )

    response = client.get(f"/graphs/{graph_id}/edges/new")
    assert response.status_code == 200
    assert "Schema Editor" in response.text
    assert "Raw JSON" in response.text
    assert "jedison.umd.js" in response.text
    assert "jedison-form.js" in response.text
    assert '"edge_schema"' in response.text
    assert '"description": "edge_schema schema"' in response.text
    assert '"node_only_schema"' not in response.text

    response = client.get(f"/graphs/{graph_id}/edges/{edge_id}/edit")
    assert response.status_code == 200
    assert "Schema Editor" in response.text
    assert "Schema backed edge" in response.text
    assert '<option value="edge_schema" selected' in response.text

    response = client.get(f"/graphs/{graph_id}/edges/{edge_id}")
    assert response.status_code == 200
    assert "Show schema view" in response.text
    assert '<p class="resource-subtitle">edge_schema</p>' in response.text
    assert "jedison.umd.js" in response.text
    assert "jedison-form.js" in response.text
    assert '"description": "edge_schema schema"' in response.text


def test_graph_edge_browse_and_create_across_surfaces(admin_test_env):
    """Test edge browse/create flow across web, REST, CLI, and MCP."""
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
            "table_prefix": "edge_slice",
            "display_name": "Edge Slice",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="edge_slice")
    assert graph is not None
    graph_id = graph.id
    _seed_graph_schema(
        manager,
        table_prefix="edge_slice",
        schema_name="edge_schema",
        kind="edge",
    )
    _seed_graph_schema(
        manager,
        table_prefix="edge_slice",
        schema_name="node_only_schema",
        kind="node",
    )

    seeded_source_id = _seed_node_record(
        manager,
        table_prefix="edge_slice",
        type="task",
        name="seeded-source",
        data={"name": "Seeded source"},
    )
    seeded_target_id = _seed_node_record(
        manager,
        table_prefix="edge_slice",
        type="task",
        name="seeded-target",
        data={"name": "Seeded target"},
    )
    seeded_edge_id = _seed_edge_record(
        manager,
        table_prefix="edge_slice",
        type="depends_on",
        source_id=seeded_source_id,
        target_id=seeded_target_id,
        schema_name="edge_schema",
        data={"name": "Seeded edge"},
        tags=["seeded"],
    )

    web_source_id = _seed_node_record(
        manager,
        table_prefix="edge_slice",
        type="task",
        name="web-source",
        data={"name": "Web source"},
    )
    web_target_id = _seed_node_record(
        manager,
        table_prefix="edge_slice",
        type="task",
        name="web-target",
        data={"name": "Web target"},
    )
    rest_source_id = _seed_node_record(
        manager,
        table_prefix="edge_slice",
        type="task",
        name="rest-source",
        data={"name": "Rest source"},
    )
    rest_target_id = _seed_node_record(
        manager,
        table_prefix="edge_slice",
        type="task",
        name="rest-target",
        data={"name": "Rest target"},
    )
    cli_source_id = _seed_node_record(
        manager,
        table_prefix="edge_slice",
        type="task",
        name="cli-source",
        data={"name": "CLI source"},
    )
    cli_target_id = _seed_node_record(
        manager,
        table_prefix="edge_slice",
        type="task",
        name="cli-target",
        data={"name": "CLI target"},
    )
    mcp_source_id = _seed_node_record(
        manager,
        table_prefix="edge_slice",
        type="task",
        name="mcp-source",
        data={"name": "MCP source"},
    )
    mcp_target_id = _seed_node_record(
        manager,
        table_prefix="edge_slice",
        type="task",
        name="mcp-target",
        data={"name": "MCP target"},
    )

    response = client.get(f"/graphs/{graph_id}/edges/new")
    assert response.status_code == 200
    assert "Create an edge for Edge Slice." in response.text
    assert '<option value="edge_schema"' in response.text
    assert 'value="node_only_schema"' not in response.text

    response = client.post(
        f"/graphs/{graph_id}/edges",
        data={
            "type": "depends_on",
            "source_id": web_source_id,
            "target_id": web_target_id,
            "schema_name": "edge_schema",
            "tags": "alpha, beta",
            "data": json.dumps({"name": "Web edge"}),
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith(f"/graphs/{graph_id}/edges/")
    web_edge_id = response.headers["location"].split("?", 1)[0].rsplit("/", 1)[-1]

    response = client.get(response.headers["location"])
    assert response.status_code == 200
    assert web_edge_id in response.text
    assert web_source_id in response.text
    assert web_target_id in response.text
    assert "Tags: alpha, beta" in response.text

    response = client.get(
        f"/graphs/{graph_id}/edges", params={"type": "depends_on", "limit": 1}
    )
    assert response.status_code == 200
    assert "Next page" in response.text

    response = client.post(
        "/apikeys",
        data={"label": "Edge slice key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]
    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)

    response = client.post(
        "/api/graph_edge_create",
        json={
            "graph_id": graph_id,
            "type": "depends_on",
            "source_id": rest_source_id,
            "target_id": rest_target_id,
            "schema_name": "edge_schema",
            "tags": ["rest"],
            "data": {"name": "Rest edge"},
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    rest_created = response.json()
    assert rest_created["edge"]["type"] == "depends_on"
    assert rest_created["edge"]["schema_name"] == "edge_schema"
    assert rest_created["edge"]["tags"] == ["rest"]

    response = client.post(
        "/api/graph_edge_list",
        json={"graph_id": graph_id, "type": "depends_on", "limit": 10},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    edge_list_payload = response.json()
    assert edge_list_payload["total"] == 3
    assert {item["id"] for item in edge_list_payload["items"]} == {
        seeded_edge_id,
        web_edge_id,
        rest_created["edge"]["id"],
    }

    response = client.post(
        "/api/graph_edge_get",
        json={"graph_id": graph_id, "edge_id": web_edge_id},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    assert response.json()["edge"]["source_id"] == web_source_id
    assert response.json()["edge"]["target_id"] == web_target_id

    mcp_created = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_edge_create",
        {
            "graph_id": graph_id,
            "type": "depends_on",
            "source_id": mcp_source_id,
            "target_id": mcp_target_id,
            "schema_name": "edge_schema",
            "tags": ["mcp", "final"],
            "data": {"name": "MCP edge"},
        },
    )
    assert mcp_created.edge.schema_name == "edge_schema"
    assert mcp_created.edge.tags == ["mcp", "final"]

    mcp_get = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_edge_get",
        {
            "graph_id": graph_id,
            "edge_id": mcp_created.edge.id,
        },
    )
    assert mcp_get.edge.source_id == mcp_source_id

    mcp_list = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_edge_list",
        {
            "graph_id": graph_id,
            "type": "depends_on",
            "schema_name": "",
            "source_id": "",
            "target_id": "",
            "filter": "",
            "limit": 10,
            "offset": 0,
            "sort": "created_at_desc",
        },
    )
    assert mcp_list.total == 4

    _login(client)
    response = client.get(f"/graphs/{graph_id}/edges")
    assert response.status_code == 200
    assert seeded_source_id in response.text
    assert web_edge_id in response.text
    assert rest_created["edge"]["id"] in response.text
    assert mcp_created.edge.id in response.text


def test_edge_list_filter_dsl(admin_test_env):
    """Test edge list page with valid and invalid DSL filter."""
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
            "table_prefix": "dsl_filter_edges",
            "display_name": "DSL Filter Edges",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="dsl_filter_edges")
    assert graph is not None
    graph_id = graph.id

    a_id = _seed_node_record(
        manager,
        table_prefix="dsl_filter_edges",
        type="n",
        name="a",
        data={},
    )
    b_id = _seed_node_record(
        manager,
        table_prefix="dsl_filter_edges",
        type="n",
        name="b",
        data={},
    )
    c_id = _seed_node_record(
        manager,
        table_prefix="dsl_filter_edges",
        type="n",
        name="c",
        data={},
    )
    _seed_edge_record(
        manager,
        table_prefix="dsl_filter_edges",
        type="follows",
        source_id=a_id,
        target_id=b_id,
        data={},
    )
    _seed_edge_record(
        manager,
        table_prefix="dsl_filter_edges",
        type="blocks",
        source_id=b_id,
        target_id=c_id,
        data={},
    )

    response = client.get(
        f"/graphs/{graph_id}/edges",
        params={"filter": "type = follows"},
    )
    assert response.status_code == 200
    assert "1 edge" in response.text

    response = client.get(
        f"/graphs/{graph_id}/edges",
        params={"filter": "("},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "error=" in response.headers.get("location", "")


def test_graph_edge_update_and_delete_across_surfaces(admin_test_env):
    """Test edge update/delete flow across web, REST, CLI, and MCP."""
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
            "table_prefix": "edge_slice_phase2",
            "display_name": "Edge Slice Phase 2",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="edge_slice_phase2")
    assert graph is not None
    graph_id = graph.id
    _seed_graph_schema(
        manager,
        table_prefix="edge_slice_phase2",
        schema_name="edge_schema",
        kind="edge",
    )

    web_source_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="web-source",
        data={"name": "Web source"},
    )
    web_target_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="web-target",
        data={"name": "Web target"},
    )
    web_new_source_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="web-new-source",
        data={"name": "Web new source"},
    )
    web_new_target_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="web-new-target",
        data={"name": "Web new target"},
    )
    web_edit_id = _seed_edge_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="depends_on",
        source_id=web_source_id,
        target_id=web_target_id,
        schema_name="edge_schema",
        data={"name": "Web edit"},
        tags=["stale"],
    )
    web_delete_id = _seed_edge_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="depends_on",
        source_id=web_target_id,
        target_id=web_source_id,
        schema_name="edge_schema",
        data={"name": "Web delete"},
        tags=["remove"],
    )

    rest_source_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="rest-source",
        data={"name": "Rest source"},
    )
    rest_target_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="rest-target",
        data={"name": "Rest target"},
    )
    rest_new_source_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="rest-new-source",
        data={"name": "Rest new source"},
    )
    rest_new_target_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="rest-new-target",
        data={"name": "Rest new target"},
    )
    rest_edge_id = _seed_edge_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="depends_on",
        source_id=rest_source_id,
        target_id=rest_target_id,
        schema_name="edge_schema",
        data={"name": "Rest edit"},
        tags=["rest"],
    )

    cli_source_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="cli-source",
        data={"name": "CLI source"},
    )
    cli_target_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="cli-target",
        data={"name": "CLI target"},
    )
    cli_new_source_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="cli-new-source",
        data={"name": "CLI new source"},
    )
    cli_new_target_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="cli-new-target",
        data={"name": "CLI new target"},
    )
    cli_edge_id = _seed_edge_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="depends_on",
        source_id=cli_source_id,
        target_id=cli_target_id,
        schema_name="edge_schema",
        data={"name": "CLI edit"},
        tags=["cli"],
    )

    mcp_source_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="mcp-source",
        data={"name": "MCP source"},
    )
    mcp_target_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="mcp-target",
        data={"name": "MCP target"},
    )
    mcp_new_source_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="mcp-new-source",
        data={"name": "MCP new source"},
    )
    mcp_new_target_id = _seed_node_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="task",
        name="mcp-new-target",
        data={"name": "MCP new target"},
    )
    mcp_edge_id = _seed_edge_record(
        manager,
        table_prefix="edge_slice_phase2",
        type="depends_on",
        source_id=mcp_source_id,
        target_id=mcp_target_id,
        schema_name="edge_schema",
        data={"name": "MCP edit"},
        tags=["mcp"],
    )

    response = client.get(f"/graphs/{graph_id}/edges/{web_edit_id}")
    assert response.status_code == 200
    assert "Delete removes this relationship immediately." in response.text
    assert "Edit edge" in response.text

    response = client.get(f"/graphs/{graph_id}/edges/{web_edit_id}/edit")
    assert response.status_code == 200
    assert "Update edge" in response.text
    assert f'value="{web_edit_id}" readonly' in response.text

    response = client.post(
        f"/graphs/{graph_id}/edges/{web_edit_id}",
        data={
            "type": "blocks",
            "source_id": web_new_source_id,
            "target_id": web_new_target_id,
            "schema_name": "edge_schema",
            "tags": "alpha, beta",
            "data": json.dumps({"name": "Web edge updated", "status": "active"}),
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    response = client.get(response.headers["location"])
    assert response.status_code == 200
    assert web_new_source_id in response.text
    assert web_new_target_id in response.text
    assert "Tags: alpha, beta" in response.text

    response = client.post(
        f"/graphs/{graph_id}/edges/{web_delete_id}/delete",
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith(f"/graphs/{graph_id}/edges?success=")

    response = client.post(
        "/apikeys",
        data={"label": "Edge phase 2 key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]
    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)

    response = client.post(
        "/api/graph_edge_update",
        json={
            "graph_id": graph_id,
            "edge_id": rest_edge_id,
            "type": "blocks",
            "source_id": rest_new_source_id,
            "target_id": rest_new_target_id,
            "schema_name": "edge_schema",
            "tags": ["rest", "updated"],
            "data": {"name": "Rest edge updated", "status": "active"},
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    assert response.json()["edge"]["type"] == "blocks"
    assert response.json()["edge"]["source_id"] == rest_new_source_id
    assert response.json()["edge"]["target_id"] == rest_new_target_id
    assert response.json()["edge"]["tags"] == ["rest", "updated"]

    response = client.post(
        "/api/graph_edge_delete",
        json={"graph_id": graph_id, "edge_id": rest_edge_id},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    assert response.json()["edge"]["id"] == rest_edge_id

    mcp_updated = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_edge_update",
        {
            "graph_id": graph_id,
            "edge_id": mcp_edge_id,
            "type": "blocks",
            "source_id": mcp_new_source_id,
            "target_id": mcp_new_target_id,
            "schema_name": "edge_schema",
            "tags": ["mcp", "updated"],
            "data": {"name": "MCP edge updated", "status": "active"},
        },
    )
    assert mcp_updated.edge.type == "blocks"
    assert mcp_updated.edge.source_id == mcp_new_source_id
    assert mcp_updated.edge.target_id == mcp_new_target_id
    assert mcp_updated.edge.tags == ["mcp", "updated"]

    mcp_deleted = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_edge_delete",
        {"graph_id": graph_id, "edge_id": mcp_edge_id},
    )
    assert mcp_deleted.edge.id == mcp_edge_id

    _login(client)
    response = client.get(f"/graphs/{graph_id}/edges")
    assert response.status_code == 200
    assert web_edit_id in response.text
    assert web_delete_id not in response.text
    assert rest_edge_id not in response.text
    assert cli_edge_id in response.text
    assert mcp_edge_id not in response.text


def test_edge_partial_update_preserves_omitted_fields(admin_test_env):
    """Partial edge update with only data changes data but preserves type, source, target, tags."""
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
            "table_prefix": "partial_edge",
            "display_name": "Partial Edge",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="partial_edge")
    assert graph is not None
    graph_id = graph.id
    src_id = _seed_node_record(
        manager,
        table_prefix="partial_edge",
        type="task",
        name="src",
        data={"n": 1},
    )
    tgt_id = _seed_node_record(
        manager,
        table_prefix="partial_edge",
        type="task",
        name="tgt",
        data={"n": 2},
    )
    edge_id = _seed_edge_record(
        manager,
        table_prefix="partial_edge",
        type="depends_on",
        source_id=src_id,
        target_id=tgt_id,
        data={"old": True},
        tags=["keep", "me"],
    )

    response = client.post(
        "/apikeys",
        data={"label": "Partial edge key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    response = client.get(response.headers["location"])
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)

    # Update only data; omit type, source_id, target_id, tags
    response = client.post(
        "/api/graph_edge_update",
        json={
            "graph_id": graph_id,
            "edge_id": edge_id,
            "data": {"new": True, "updated": True},
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["edge"]["type"] == "depends_on"
    assert data["edge"]["source_id"] == src_id
    assert data["edge"]["target_id"] == tgt_id
    assert data["edge"]["tags"] == ["keep", "me"]
    assert data["edge"]["data"] == {"new": True, "updated": True}


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


def _seed_node_record(
    manager,
    *,
    table_prefix: str,
    type: str,
    name: str,
    data: dict[str, object],
    schema_name: str | None = None,
    tags: list[str] | None = None,
    parent_id: str | None = None,
) -> str:
    services = manager.app.state.services
    assert services.captive_server is not None

    async def _seed() -> str:
        db = GPGraph(services.captive_server.get_uri(), table_prefix=table_prefix)
        try:
            node_list = await db.set_nodes(
                [
                    NodeUpsert(
                        type=type,
                        name=name,
                        parent_id=parent_id,
                        schema_name=schema_name,
                        data=data,
                        tags=list(tags or []),
                    )
                ]
            )
            node = node_list[0]
            return node.id
        finally:
            await db.sqla_engine.dispose()

    return asyncio.run(_seed())


def _seed_edge_record(
    manager,
    *,
    table_prefix: str,
    type: str,
    source_id: str,
    target_id: str,
    data: dict[str, object],
    schema_name: str | None = None,
    tags: list[str] | None = None,
) -> str:
    services = manager.app.state.services
    assert services.captive_server is not None

    async def _seed() -> str:
        db = GPGraph(services.captive_server.get_uri(), table_prefix=table_prefix)
        try:
            edge = (await db.set_edges(
                [EdgeUpsert(
                    type=type,
                    source_id=source_id,
                    target_id=target_id,
                    schema_name=schema_name,
                    data=data,
                    tags=list(tags or []),
                )]
            ))[0]
            return edge.id
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
