import asyncio
import base64
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from mcp.server.auth.middleware.auth_context import auth_context_var

from gpdb import EdgeUpsert, GPGraph, NodeUpsert, SchemaUpsert
from gpdb.admin import entry
from gpdb.admin.auth import generate_api_key, hash_api_key_secret, hash_password
from gpdb.admin.store import AdminStore


def test_graph_node_schema_editor_renders_ui(admin_test_env):
    """Test that node forms and detail pages expose the schema-driven web UI."""
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
            "table_prefix": "node_schema_editor",
            "display_name": "Node Schema Editor",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="node_schema_editor")
    assert graph is not None
    graph_id = graph.id
    _seed_graph_schema(
        manager, table_prefix="node_schema_editor", schema_name="task_schema"
    )
    _seed_graph_schema(
        manager,
        table_prefix="node_schema_editor",
        schema_name="edge_only_schema",
        kind="edge",
    )
    node_id = _seed_node_record(
        manager,
        table_prefix="node_schema_editor",
        type="task",
        name="schema-backed-node",
        schema_name="task_schema",
        data={"name": "Schema backed node"},
    )

    response = client.get(f"/graphs/{graph_id}/nodes/new")
    assert response.status_code == 200
    assert "Schema Editor" in response.text
    assert "Raw JSON" in response.text
    assert "jedison.umd.js" in response.text
    assert "jedison-form.js" in response.text
    assert "data-jedison-root" in response.text
    assert '"task_schema"' in response.text
    assert '"description": "task_schema schema"' in response.text
    assert '"edge_only_schema"' not in response.text

    response = client.get(f"/graphs/{graph_id}/nodes/{node_id}/edit")
    assert response.status_code == 200
    assert "Schema Editor" in response.text
    assert "Schema backed node" in response.text
    assert '<option value="task_schema" selected' in response.text

    response = client.get(f"/graphs/{graph_id}/nodes/{node_id}")
    assert response.status_code == 200
    assert "Show schema view" in response.text
    assert '<p class="resource-subtitle">task_schema</p>' in response.text
    assert "jedison.umd.js" in response.text
    assert "jedison-form.js" in response.text
    assert '"description": "task_schema schema"' in response.text


def test_graph_node_browse_and_create_across_surfaces(admin_test_env):
    """Test node browse/create flow across web, REST, CLI, and MCP."""
    manager = admin_test_env.manager
    client = admin_test_env.client
    graph_id = ""
    api_key_value = ""
    web_node_id = ""

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
            "table_prefix": "node_slice",
            "display_name": "Node Slice",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="node_slice")
    assert graph is not None
    graph_id = graph.id
    _seed_graph_schema(manager, table_prefix="node_slice", schema_name="task_schema")
    _seed_graph_schema(
        manager,
        table_prefix="node_slice",
        schema_name="edge_only_schema",
        kind="edge",
    )
    _seed_node_record(
        manager,
        table_prefix="node_slice",
        type="task",
        name="seeded-node",
        schema_name="task_schema",
        data={"name": "Seeded node"},
        tags=["seeded"],
    )

    response = client.get(f"/graphs/{graph_id}/nodes/new")
    assert response.status_code == 200
    assert "Create a node for Node Slice." in response.text
    assert '<option value="task_schema"' in response.text
    assert 'value="edge_only_schema"' not in response.text

    response = client.post(
        f"/graphs/{graph_id}/nodes",
        data={
            "type": "task",
            "name": "web-node",
            "schema_name": "task_schema",
            "owner_id": "",
            "parent_id": "",
            "tags": "alpha, beta",
            "data": json.dumps({"name": "Web node"}),
            "mime": "text/plain",
        },
        files={"payload_file": ("empty.txt", b"", "text/plain")},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith(f"/graphs/{graph_id}/nodes/")
    web_node_id = response.headers["location"].split("?", 1)[0].rsplit("/", 1)[-1]

    response = client.get(response.headers["location"])
    assert response.status_code == 200
    assert "web-node" in response.text
    assert "Tags: alpha, beta" in response.text
    assert "A binary payload is stored on this node." in response.text
    assert "0 bytes" in response.text

    response = client.get(f"/graphs/{graph_id}/nodes/{web_node_id}/payload")
    assert response.status_code == 200
    assert response.content == b""
    assert response.headers["content-type"].startswith("text/plain")

    response = client.get(
        f"/graphs/{graph_id}/nodes", params={"type": "task", "limit": 1}
    )
    assert response.status_code == 200
    assert "Next page" in response.text

    response = client.post(
        "/apikeys",
        data={"label": "Node slice key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]
    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)

    response = client.post(
        "/api/graph_nodes_create",
        json={
            "graph_id": graph_id,
            "nodes": [
                {
                    "type": "task",
                    "name": "rest-node",
                    "schema_name": "task_schema",
                    "tags": ["rest"],
                    "payload_base64": base64.b64encode(b"rest payload").decode(
                        "ascii"
                    ),
                    "payload_mime": "text/plain",
                    "payload_filename": "rest.txt",
                    "data": {"name": "Rest node"},
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    rest_created_list = response.json()
    rest_created = rest_created_list[0]
    assert rest_created["node"]["name"] == "rest-node"
    assert rest_created["node"]["schema_name"] == "task_schema"
    assert rest_created["node"]["tags"] == ["rest"]
    assert rest_created["node"]["payload_size"] == 12
    assert rest_created["node"]["payload_filename"] == "rest.txt"

    response = client.post(
        "/api/graph_node_list",
        json={"graph_id": graph_id, "type": "task", "limit": 10},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    assert response.json()["total"] == 3
    assert {item["name"] for item in response.json()["items"]} == {
        "seeded-node",
        "web-node",
        "rest-node",
    }

    response = client.post(
        "/api/graph_nodes_get",
        json={"graph_id": graph_id, "node_ids": [web_node_id]},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    web_detail_list = response.json()
    web_detail = web_detail_list[0]
    assert web_detail["node"]["name"] == "web-node"
    assert web_detail["node"]["tags"] == ["alpha", "beta"]

    mcp_created = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_nodes_create",
        {
            "graph_id": graph_id,
            "nodes": [
                {
                    "type": "task",
                    "name": "mcp-node",
                    "schema_name": "task_schema",
                    "tags": ["mcp", "final"],
                    "data": {"name": "MCP node"},
                    "payload_base64": base64.b64encode(b"mcp payload").decode(
                        "ascii"
                    ),
                    "payload_mime": "text/plain",
                    "payload_filename": "mcp.txt",
                }
            ],
        },
    )
    mcp_created = mcp_created[0]
    assert mcp_created.node.name == "mcp-node"
    assert mcp_created.node.tags == ["mcp", "final"]
    assert mcp_created.node.payload_size == 11
    assert mcp_created.node.payload_filename == "mcp.txt"

    mcp_get = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_nodes_get",
        {
            "graph_id": graph_id,
            "node_ids": [mcp_created.node.id],
        },
    )
    mcp_get = mcp_get[0]
    assert mcp_get.node.name == "mcp-node"

    mcp_list = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_node_list",
        {
            "graph_id": graph_id,
            "type": "task",
            "limit": 10,
        },
    )
    assert mcp_list.total == 4

    _login(client)
    response = client.get(f"/graphs/{graph_id}/nodes")
    assert response.status_code == 200
    assert "seeded-node" in response.text
    assert "web-node" in response.text
    assert "rest-node" in response.text
    assert "mcp-node" in response.text


def test_node_list_filter_dsl(admin_test_env):
    """Test node list page with valid and invalid DSL filter."""
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
            "table_prefix": "dsl_filter_nodes",
            "display_name": "DSL Filter Nodes",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="dsl_filter_nodes")
    assert graph is not None
    graph_id = graph.id

    _seed_node_record(
        manager,
        table_prefix="dsl_filter_nodes",
        type="task",
        name="a-task",
        data={"n": 1},
    )
    _seed_node_record(
        manager,
        table_prefix="dsl_filter_nodes",
        type="other",
        name="other-node",
        data={"n": 2},
    )

    response = client.get(
        f"/graphs/{graph_id}/nodes",
        params={"filter": "type = task"},
    )
    assert response.status_code == 200
    assert "a-task" in response.text
    assert "1 node" in response.text
    assert "other-node" not in response.text

    response = client.get(
        f"/graphs/{graph_id}/nodes",
        params={"filter": "("},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "error=" in response.headers.get("location", "")


def test_graph_node_update_delete_and_payload_across_surfaces(
    admin_test_env,
):
    """Test node update/delete/payload flow, blockers, and downloads across surfaces."""
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
            "table_prefix": "node_slice_phase2",
            "display_name": "Node Slice Phase 2",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="node_slice_phase2")
    assert graph is not None
    graph_id = graph.id
    _seed_graph_schema(
        manager, table_prefix="node_slice_phase2", schema_name="task_schema"
    )

    blocked_id = _seed_node_record(
        manager,
        table_prefix="node_slice_phase2",
        type="task",
        name="blocked-root",
        schema_name="task_schema",
        data={"name": "Blocked root"},
    )
    _seed_node_record(
        manager,
        table_prefix="node_slice_phase2",
        type="task",
        name="blocked-child",
        data={"name": "Blocked child"},
        parent_id=blocked_id,
    )
    blocked_target_id = _seed_node_record(
        manager,
        table_prefix="node_slice_phase2",
        type="task",
        name="blocked-target",
        data={"name": "Blocked target"},
    )
    _seed_edge_record(
        manager,
        table_prefix="node_slice_phase2",
        type="depends_on",
        source_id=blocked_id,
        target_id=blocked_target_id,
        data={"kind": "blocker"},
    )
    web_edit_id = _seed_node_record(
        manager,
        table_prefix="node_slice_phase2",
        type="task",
        name="web-edit",
        schema_name="task_schema",
        data={"name": "Web edit"},
        tags=["before"],
    )
    web_delete_id = _seed_node_record(
        manager,
        table_prefix="node_slice_phase2",
        type="task",
        name="web-delete",
        data={"name": "Web delete"},
    )
    rest_node_id = _seed_node_record(
        manager,
        table_prefix="node_slice_phase2",
        type="task",
        name="rest-edit",
        data={"name": "Rest edit"},
    )
    cli_node_id = _seed_node_record(
        manager,
        table_prefix="node_slice_phase2",
        type="task",
        name="cli-edit",
        data={"name": "CLI edit"},
    )
    mcp_node_id = _seed_node_record(
        manager,
        table_prefix="node_slice_phase2",
        type="task",
        name="mcp-edit",
        data={"name": "MCP edit"},
    )

    response = client.get(f"/graphs/{graph_id}/nodes/{blocked_id}")
    assert response.status_code == 200
    assert (
        "Delete is blocked until child nodes and incident edges are removed."
        in response.text
    )
    assert "Child nodes: 1." in response.text
    assert "Incident edges: 1." in response.text
    assert "Sample child IDs:" in response.text
    assert "Sample edge IDs:" in response.text

    response = client.post(
        f"/graphs/{graph_id}/nodes/{blocked_id}/delete",
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert (
        "Node &#39;{node_id}&#39; cannot be deleted because it still has 1 child node and 1 incident edge.".format(
            node_id=blocked_id
        )
        in response.text
    )

    response = client.get(f"/graphs/{graph_id}/nodes/{web_edit_id}/edit")
    assert response.status_code == 200
    assert "Update node" in response.text

    response = client.post(
        f"/graphs/{graph_id}/nodes/{web_edit_id}",
        data={
            "type": "task",
            "name": "web-edit-renamed",
            "schema_name": "task_schema",
            "owner_id": "owner-1",
            "parent_id": "",
            "tags": "alpha, beta",
            "data": json.dumps({"name": "Web edit updated", "status": "active"}),
            "mime": "text/plain",
        },
        files={"payload_file": ("web.txt", b"web payload", "text/plain")},
        follow_redirects=False,
    )
    assert response.status_code == 303

    response = client.get(response.headers["location"])
    assert response.status_code == 200
    assert "web-edit-renamed" in response.text
    assert "Tags: alpha, beta" in response.text
    assert "owner-1" in response.text
    assert "A binary payload is stored on this node." in response.text
    assert "11 bytes" in response.text
    assert "Download payload" in response.text

    response = client.get(f"/graphs/{graph_id}/nodes/{web_edit_id}/payload")
    assert response.status_code == 200
    assert response.content == b"web payload"
    assert response.headers["content-type"].startswith("text/plain")
    assert "attachment;" in response.headers["content-disposition"]
    assert 'filename="web.txt"' in response.headers["content-disposition"]

    response = client.post(
        f"/graphs/{graph_id}/nodes/{web_edit_id}",
        data={
            "type": "task",
            "name": "web-edit-renamed",
            "schema_name": "task_schema",
            "owner_id": "owner-1",
            "parent_id": "",
            "tags": "alpha, beta",
            "data": json.dumps({"name": "Web edit updated", "status": "active"}),
            "clear_payload": "true",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    response = client.get(response.headers["location"])
    assert response.status_code == 200
    assert "No binary payload is stored on this node." in response.text

    response = client.post(
        f"/graphs/{graph_id}/nodes/{web_delete_id}/delete",
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith(f"/graphs/{graph_id}/nodes?success=")

    response = client.post(
        "/apikeys",
        data={"label": "Node phase 2 key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]
    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)

    response = client.post(
        "/api/graph_nodes_update",
        json={
            "graph_id": graph_id,
            "nodes": [
                {
                    "node_id": rest_node_id,
                    "type": "task",
                    "name": "rest-edit-renamed",
                    "schema_name": "task_schema",
                    "tags": ["rest", "updated"],
                    "payload_base64": base64.b64encode(b"rest payload").decode(
                        "ascii"
                    ),
                    "payload_mime": "text/plain",
                    "payload_filename": "rest.txt",
                    "data": {"name": "Rest edit updated", "status": "active"},
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    updated_rest = response.json()[0]
    assert updated_rest["node"]["name"] == "rest-edit-renamed"
    assert updated_rest["node"]["schema_name"] == "task_schema"
    assert updated_rest["node"]["tags"] == ["rest", "updated"]
    assert updated_rest["node"]["payload_size"] == 12
    assert updated_rest["node"]["payload_filename"] == "rest.txt"

    response = client.post(
        "/api/graph_node_payloads_get",
        json={"graph_id": graph_id, "node_ids": [rest_node_id]},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    got = response.json()[0]
    assert got["payload_base64"] == base64.b64encode(b"rest payload").decode(
        "ascii"
    )
    assert got["node"]["payload_mime"] == "text/plain"
    assert got["node"]["payload_filename"] == "rest.txt"
    assert got["filename"] == "rest.txt"

    response = client.post(
        "/api/graph_nodes_update",
        json={
            "graph_id": graph_id,
            "nodes": [
                {
                    "node_id": rest_node_id,
                    "type": "task",
                    "name": "rest-edit-renamed",
                    "schema_name": "task_schema",
                    "tags": ["rest", "updated"],
                    "clear_payload": True,
                    "data": {"name": "Rest edit updated", "status": "active"},
                }
            ],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    cleared_rest = response.json()[0]
    assert cleared_rest["node"]["payload_size"] == 0
    assert cleared_rest["node"]["payload_filename"] is None
    assert cleared_rest["node"]["has_payload"] is False

    response = client.post(
        "/api/graph_nodes_delete",
        json={"graph_id": graph_id, "node_ids": [rest_node_id]},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    deleted_rest = response.json()[0]
    assert deleted_rest["node"]["id"] == rest_node_id

    mcp_updated = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_nodes_update",
        {
            "graph_id": graph_id,
            "nodes": [
                {
                    "node_id": mcp_node_id,
                    "type": "task",
                    "name": "mcp-edit-renamed",
                    "schema_name": "task_schema",
                    "tags": ["mcp", "updated"],
                    "data": {"name": "MCP edit updated", "status": "active"},
                    "payload_base64": base64.b64encode(b"mcp payload").decode(
                        "ascii"
                    ),
                    "payload_mime": "text/plain",
                    "payload_filename": "mcp.txt",
                }
            ],
        },
    )
    mcp_updated = mcp_updated[0]
    assert mcp_updated.node.name == "mcp-edit-renamed"
    assert mcp_updated.node.tags == ["mcp", "updated"]
    assert mcp_updated.node.payload_size == 11
    assert mcp_updated.node.payload_filename == "mcp.txt"

    mcp_payload_get = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_node_payloads_get",
        {"graph_id": graph_id, "node_ids": [mcp_node_id]},
    )
    mcp_payload_get = mcp_payload_get[0]
    assert mcp_payload_get.payload_base64 == base64.b64encode(b"mcp payload").decode(
        "ascii"
    )
    assert mcp_payload_get.node.payload_mime == "text/plain"
    assert mcp_payload_get.node.payload_filename == "mcp.txt"
    assert mcp_payload_get.filename == "mcp.txt"

    mcp_cleared = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_nodes_update",
        {
            "graph_id": graph_id,
            "nodes": [
                {
                    "node_id": mcp_node_id,
                    "type": "task",
                    "name": "mcp-edit-renamed",
                    "schema_name": "task_schema",
                    "tags": ["mcp", "updated"],
                    "data": {"name": "MCP edit updated", "status": "active"},
                    "clear_payload": True,
                }
            ],
        },
    )
    mcp_cleared = mcp_cleared[0]
    assert mcp_cleared.node.payload_size == 0
    assert mcp_cleared.node.payload_filename is None
    assert mcp_cleared.node.has_payload is False

    mcp_deleted = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_nodes_delete",
        {"graph_id": graph_id, "node_ids": [mcp_node_id]},
    )
    mcp_deleted = mcp_deleted[0]
    assert mcp_deleted.node.id == mcp_node_id

    _login(client)
    response = client.get(f"/graphs/{graph_id}/nodes")
    assert response.status_code == 200
    assert "web-edit-renamed" in response.text
    assert "web-delete" not in response.text
    assert "rest-edit-renamed" not in response.text
    assert "cli-edit-renamed" not in response.text
    assert "mcp-edit-renamed" not in response.text


def test_node_partial_update_preserves_omitted_fields(admin_test_env):
    """Partial node update with only name changes name but preserves tags and data."""
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
            "table_prefix": "partial_node",
            "display_name": "Partial Node",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="partial_node")
    assert graph is not None
    graph_id = graph.id
    _seed_graph_schema(manager, table_prefix="partial_node", schema_name="task_schema")
    node_id = _seed_node_record(
        manager,
        table_prefix="partial_node",
        type="task",
        name="partial-a",
        schema_name="task_schema",
        data={"name": "partial-a", "x": 1, "label": "original"},
        tags=["keep", "me"],
    )

    response = client.post(
        "/apikeys",
        data={"label": "Partial update key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    response = client.get(response.headers["location"])
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)

    # Update only name; omit type, data, tags so they are preserved
    response = client.post(
        "/api/graph_nodes_update",
        json={
            "graph_id": graph_id,
            "nodes": [{"node_id": node_id, "name": "partial-b"}],
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    data = response.json()[0]
    assert data["node"]["name"] == "partial-b"
    assert data["node"]["tags"] == ["keep", "me"]
    assert (
        data["node"]["data"] == {"name": "partial-a", "x": 1, "label": "original"}
    )
    assert data["node"]["type"] == "task"
    assert data["node"]["schema_name"] == "task_schema"


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
                [
                    EdgeUpsert(
                        type=type,
                        source_id=source_id,
                        target_id=target_id,
                        schema_name=schema_name,
                        data=data,
                        tags=list(tags or []),
                    )
                ]
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
