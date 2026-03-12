import asyncio
import base64
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from mcp.server.auth.middleware.auth_context import auth_context_var
from sqlalchemy.engine import make_url

from gpdb import EdgeUpsert, GPGraph, NodeUpsert
from gpdb.admin import entry
from gpdb.admin.auth import generate_api_key, hash_api_key_secret, hash_password
from gpdb.admin.config import AdminConfig, ConfigPathSource, ConfigStore, extract_config_arg, resolve_config_location
from gpdb.admin.store import AdminStore


def test_cli_status_command():
    """Test that the status command is accessible via CLI."""
    admin_service = entry.ToolService("admin", [entry.status])
    cli = entry.CLIServer("gpdb")
    cli.mount(admin_service)

    manager = entry.ServerManager(name="gpdb-admin")
    manager.add_server(cli)

    result = manager.cli(["gpdb", "status"], standalone_mode=False)
    assert result == "OK"


def test_fastapi_status_command():
    """Test that the status command is accessible via FastAPI REST API."""
    admin_service = entry.ToolService("admin", [entry.status])
    rest_api = entry.OpenAPIServer(path_prefix="/api", title="GPDB Admin API")
    rest_api.mount(admin_service)

    manager = entry.ServerManager(name="gpdb-admin")
    manager.add_server(rest_api)

    client = TestClient(manager.app)
    response = client.post("/api/status")
    assert response.status_code == 200
    assert response.json() == "OK"


def test_mcp_status_command():
    """Test that the status command is accessible via MCP server."""
    admin_service = entry.ToolService("admin", [entry.status])
    mcp_server = entry.SSEMCPServer("gpdb")
    mcp_server.mount(admin_service)

    manager = entry.ServerManager(name="gpdb-admin")
    manager.add_server(mcp_server)

    assert "gpdb" in manager.mcp_servers
    assert manager.mcp_servers["gpdb"] is not None


def test_health_endpoint():
    """Test that the health endpoint returns registered MCP servers."""
    admin_service = entry.ToolService("admin", [entry.status])
    mcp_server = entry.SSEMCPServer("gpdb")
    mcp_server.mount(admin_service)

    manager = entry.ServerManager(name="gpdb-admin")
    manager.add_server(mcp_server)

    client = TestClient(manager.app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"mcp_servers": ["gpdb"]}


def test_first_run_setup_and_login_flow(tmp_path):
    """Test owner bootstrap and subsequent login-gated home page access."""
    manager = _create_test_manager(tmp_path)

    with TestClient(manager.app) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert "Create the initial owner user." in response.text

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
        assert response.headers["location"] == "/login"

        response = client.get("/", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/login"

        response = client.get("/login")
        assert response.status_code == 200
        assert "Log in to GPDB admin." in response.text

        _login(client, username="owner", password="secret-pass")

        response = client.get("/")
        assert response.status_code == 200
        assert "All graphs across all managed instances." in response.text
        assert "Primary Owner" in response.text
        assert "Default instance" in response.text
        assert "Default graph" in response.text


def test_instance_and_graph_crud_flow(tmp_path):
    """Test owner-managed instance and graph CRUD from the web UI."""
    manager = _create_test_manager(tmp_path)

    with TestClient(manager.app) as client:
        _bootstrap_owner(client)
        _login(client)

        response = client.get("/graphs/new")
        assert response.status_code == 200
        default_instance_id = _extract_instance_option_value(response.text, "Default instance")

        response = client.post(
            "/graphs",
            data={
                "instance_id": default_instance_id,
                "table_prefix": "scratch",
                "display_name": "Scratch graph",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303

        response = client.get("/")
        assert "Scratch graph" in response.text
        assert "scratch" in response.text

        response = client.post(
            "/instances",
            data=_captive_instance_form(
                manager,
                slug="mirror",
                display_name="Mirror instance",
                description="Shared test connection",
            ),
            follow_redirects=False,
        )
        assert response.status_code == 303

        raw_password = _captive_instance_form(
            manager,
            slug="mirror",
            display_name="Mirror instance",
            description="Shared test connection",
        )["password"]
        stored_password = _read_stored_instance_password(
            manager,
            slug="mirror",
        )
        assert stored_password != raw_password
        assert str(stored_password).startswith("fernet:")

        response = client.get("/")
        assert "Mirror instance" in response.text

        instance_edit_page = _extract_instance_action(response.text, "Mirror instance", "edit")
        response = client.get(instance_edit_page)
        assert response.status_code == 200
        assert 'name="password" type="password"' in response.text
        assert 'value="' not in response.text.split('name="password" type="password"', 1)[1].split(">", 1)[0]

        response = client.get("/graphs/new")
        mirror_instance_id = _extract_instance_option_value(response.text, "Mirror instance")

        response = client.post(
            "/graphs",
            data={
                "instance_id": mirror_instance_id,
                "table_prefix": "mirror_scratch",
                "display_name": "Mirror scratch",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303

        response = client.get("/")
        assert "Mirror scratch" in response.text

        graph_edit_path = _extract_graph_action(response.text, "Mirror scratch", "edit")
        response = client.post(
            graph_edit_path,
            data={"display_name": "Mirror scratch renamed"},
            follow_redirects=False,
        )
        assert response.status_code == 303

        response = client.get("/")
        assert "Mirror scratch renamed" in response.text

        instance_edit_path = _extract_instance_action(response.text, "Mirror instance", "edit")
        response = client.post(
            instance_edit_path,
            data={
                **_captive_instance_form(
                    manager,
                    slug="mirror",
                    display_name="Mirror instance renamed",
                    description="Shared test connection updated",
                ),
                "display_name": "Mirror instance renamed",
                "description": "Shared test connection updated",
                "is_active": "true",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303

        response = client.get("/")
        assert "Mirror instance renamed" in response.text

        graph_delete_path = _extract_graph_action(
            response.text,
            "Mirror scratch renamed",
            "delete",
        )
        response = client.post(graph_delete_path, follow_redirects=False)
        assert response.status_code == 303

        response = client.get("/")
        assert "Mirror scratch renamed" not in response.text

        instance_delete_path = _extract_instance_action(
            response.text,
            "Mirror instance renamed",
            "delete",
        )
        response = client.post(instance_delete_path, follow_redirects=False)
        assert response.status_code == 303

        response = client.get("/")
        assert "Mirror instance renamed" not in response.text


def test_api_key_lifecycle_for_web_rest_and_mcp(tmp_path):
    """Test API key create, reveal, use, last-used update, and revoke flow."""
    manager = _create_test_manager(tmp_path)

    with TestClient(manager.app) as client:
        _bootstrap_owner(client)
        _login(client)

        response = client.post("/api/status")
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == "Bearer"

        response = client.post(
            "/apikeys",
            data={"label": "Automation key"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        api_key_detail_path = response.headers["location"]
        assert api_key_detail_path.startswith("/apikeys/")

        response = client.get(api_key_detail_path)
        assert response.status_code == 200
        assert "Automation key" in response.text
        api_key_value = _extract_revealed_api_key(response.text)
        assert api_key_value.startswith("gpdb_")

        response = client.get("/apikeys")
        assert response.status_code == 200
        assert "Automation key" in response.text

        stored_key = _read_stored_api_key(manager, label="Automation key")
        assert str(stored_key["key_value"]).startswith("fernet:")
        assert stored_key["key_value"] != api_key_value
        assert stored_key["secret_hash"] != api_key_value
        assert stored_key["last_used_at"] is None
        assert stored_key["revoked_at"] is None

        response = client.post(
            "/api/status",
            headers={"Authorization": "Bearer gpdb_invalid_deadbeef"},
        )
        assert response.status_code == 401

        response = client.post(
            "/api/status",
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        assert response.json() == "OK"

        stored_key = _read_stored_api_key(manager, label="Automation key")
        assert stored_key["last_used_at"] is not None

        assert manager.mcp_servers["gpdb"].auth is not None
        verified_token = _verify_api_key_with_mcp_verifier(manager, api_key_value)
        assert verified_token is not None
        assert verified_token.claims["username"] == "owner"
        assert verified_token.claims["api_key_label"] == "Automation key"

        api_key_id = api_key_detail_path.split("?", 1)[0].rsplit("/", 1)[-1]
        response = client.post(
            f"/apikeys/{api_key_id}/revoke",
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert response.headers["location"].startswith("/apikeys")

        response = client.get(api_key_detail_path)
        assert response.status_code == 200
        assert api_key_value in response.text
        assert "revoked" in response.text

        response = client.post(
            "/api/status",
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 401
        assert _verify_api_key_with_mcp_verifier(manager, api_key_value) is None


def test_graph_overview_vertical_slice_across_surfaces(tmp_path):
    """Test the shared graph overview flow across web, REST, CLI, and MCP."""
    manager = _create_test_manager(tmp_path)
    graph_id = ""
    api_key_value = ""

    with TestClient(manager.app) as client:
        _bootstrap_owner(client)
        _login(client)

        response = client.get("/graphs/new")
        assert response.status_code == 200
        default_instance_id = _extract_instance_option_value(response.text, "Default instance")

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

        response = client.get(
            "/api/graph_overview",
            params={"graph_id": graph_id},
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        assert response.json()["summary"] == {
            "schema_count": 1,
            "node_count": 2,
            "edge_count": 1,
        }

    cli_result = manager.cli(
        ["gpdb", "graph_overview", graph_id],
        standalone_mode=False,
    )
    assert cli_result["summary"] == {
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
    assert mcp_result["summary"] == {
        "schema_count": 1,
        "node_count": 2,
        "edge_count": 1,
    }


def test_graph_schema_registry_vertical_slice_across_surfaces(tmp_path):
    """Test schema browse/create flow across web, REST, CLI, and MCP."""
    manager = _create_test_manager(tmp_path)
    graph_id = ""
    api_key_value = ""

    with TestClient(manager.app) as client:
        _bootstrap_owner(client)
        _login(client)

        response = client.get("/graphs/new")
        assert response.status_code == 200
        default_instance_id = _extract_instance_option_value(response.text, "Default instance")

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

        response = client.post(
            f"/graphs/{graph_id}/schemas",
            data={
                "name": "web_schema",
                "json_schema": json.dumps(_schema_definition("web schema")),
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert response.headers["location"].startswith(f"/graphs/{graph_id}/schemas/web_schema")

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

    cli_created = manager.cli(
        [
            "gpdb",
            "graph_schema_create",
            graph_id,
            "cli_schema",
            json.dumps(_schema_definition("cli schema")),
        ],
        standalone_mode=False,
    )
    assert cli_created["schema"]["name"] == "cli_schema"
    assert cli_created["schema"]["version"] == "1.0.0"

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

    with TestClient(manager.app) as client:
        _login(client)

        response = client.get(f"/graphs/{graph_id}/schemas")
        assert response.status_code == 200
        assert "Schema Slice" in response.text
        assert "web_schema" in response.text
        assert "rest_schema" in response.text
        assert "cli_schema" in response.text
        assert "mcp_schema" in response.text

        response = client.get(f"/graphs/{graph_id}/schemas/web_schema")
        assert response.status_code == 200
        assert "Version 1.0.0" in response.text
        assert "1 node reference this schema." in response.text
        assert "1 edge reference this schema." in response.text
        assert "Sample node IDs:" in response.text
        assert "Sample edge IDs:" in response.text

        response = client.get(
            "/api/graph_schema_list",
            params={"graph_id": graph_id},
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        assert response.json()["total"] == 4
        assert {item["name"] for item in response.json()["items"]} == {
            "cli_schema",
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
        assert response.json()["schema"]["usage"] == {
            "node_count": 1,
            "edge_count": 1,
            "sample_node_ids": [response.json()["schema"]["usage"]["sample_node_ids"][0]],
            "sample_edge_ids": [response.json()["schema"]["usage"]["sample_edge_ids"][0]],
        }

    cli_list = manager.cli(
        ["gpdb", "graph_schema_list", graph_id],
        standalone_mode=False,
    )
    assert cli_list["total"] == 4

    cli_get = manager.cli(
        ["gpdb", "graph_schema_get", graph_id, "rest_schema"],
        standalone_mode=False,
    )
    assert cli_get["schema"]["name"] == "rest_schema"
    assert cli_get["schema"]["json_schema"]["description"] == "rest schema"

    mcp_list = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schema_list",
        {"graph_id": graph_id},
    )
    assert mcp_list["total"] == 4

    mcp_get = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_schema_get",
        {"graph_id": graph_id, "name": "web_schema"},
    )
    assert mcp_get["schema"]["usage"]["node_count"] == 1
    assert mcp_get["schema"]["usage"]["edge_count"] == 1


def test_graph_schema_update_and_delete_vertical_slice_across_surfaces(tmp_path):
    """Test schema update/delete flow, blockers, and breaking-change rejection."""
    manager = _create_test_manager(tmp_path)
    graph_id = ""
    api_key_value = ""

    with TestClient(manager.app) as client:
        _bootstrap_owner(client)
        _login(client)

        response = client.get("/graphs/new")
        assert response.status_code == 200
        default_instance_id = _extract_instance_option_value(response.text, "Default instance")

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

    cli_created = manager.cli(
        [
            "gpdb",
            "graph_schema_create",
            graph_id,
            "cli_schema",
            json.dumps(_schema_definition("cli schema")),
        ],
        standalone_mode=False,
    )
    assert cli_created["schema"]["name"] == "cli_schema"

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

    with TestClient(manager.app) as client:
        _login(client)

        response = client.get(f"/graphs/{graph_id}/schemas/web_schema")
        assert response.status_code == 200
        assert "Delete is blocked until all node and edge references are removed." in response.text
        assert "Delete schema</button>" in response.text
        assert "disabled" in response.text

        response = client.get(f"/graphs/{graph_id}/schemas/web_unused")
        assert response.status_code == 200
        assert "Delete is available because this schema is currently unused." in response.text

        response = client.get(f"/graphs/{graph_id}/schemas/web_schema/edit")
        assert response.status_code == 200
        assert "Update schema" in response.text
        assert "non-breaking updates are allowed here" in response.text

        response = client.post(
            f"/graphs/{graph_id}/schemas/web_schema",
            data={
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
        assert response.headers["location"].startswith(f"/graphs/{graph_id}/schemas/web_schema")

        response = client.get(f"/graphs/{graph_id}/schemas/web_schema")
        assert response.status_code == 200
        assert "Version 1.1.0" in response.text

        response = client.post(
            f"/graphs/{graph_id}/schemas/web_schema",
            data={
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
            "Schema &#39;web_schema&#39; cannot be deleted because it is still referenced by 1 node and 1 edge."
            in response.text
        )

        response = client.post(
            f"/graphs/{graph_id}/schemas/web_unused/delete",
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert response.headers["location"].startswith(f"/graphs/{graph_id}/schemas?success=")

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
        assert response.json()["schema"]["json_schema"]["properties"]["status"]["type"] == "string"

        response = client.post(
            "/api/graph_schema_delete",
            params={"graph_id": graph_id, "name": "rest_schema"},
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        assert response.json()["schema"]["name"] == "rest_schema"

    cli_updated = manager.cli(
        [
            "gpdb",
            "graph_schema_update",
            graph_id,
            "cli_schema",
            json.dumps(
                _schema_definition(
                    "cli schema updated",
                    include_optional_status=True,
                )
            ),
        ],
        standalone_mode=False,
    )
    assert cli_updated["schema"]["version"] == "1.1.0"

    cli_deleted = manager.cli(
        ["gpdb", "graph_schema_delete", graph_id, "cli_schema"],
        standalone_mode=False,
    )
    assert cli_deleted["schema"]["name"] == "cli_schema"

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

    with TestClient(manager.app) as client:
        _login(client)
        response = client.get(f"/graphs/{graph_id}/schemas")
        assert response.status_code == 200
        assert "web_schema" in response.text
        assert "web_unused" not in response.text
        assert "rest_schema" not in response.text
        assert "cli_schema" not in response.text
        assert "mcp_schema" not in response.text


def test_graph_node_browse_and_create_vertical_slice_across_surfaces(tmp_path):
    """Test node browse/create flow across web, REST, CLI, and MCP."""
    manager = _create_test_manager(tmp_path)
    graph_id = ""
    api_key_value = ""
    web_node_id = ""

    with TestClient(manager.app) as client:
        _bootstrap_owner(client)
        _login(client)

        response = client.get("/graphs/new")
        assert response.status_code == 200
        default_instance_id = _extract_instance_option_value(response.text, "Default instance")

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
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert response.headers["location"].startswith(f"/graphs/{graph_id}/nodes/")
        web_node_id = response.headers["location"].split("?", 1)[0].rsplit("/", 1)[-1]

        response = client.get(response.headers["location"])
        assert response.status_code == 200
        assert "web-node" in response.text
        assert "Tags: alpha, beta" in response.text
        assert "No binary payload is stored on this node." in response.text

        response = client.get(f"/graphs/{graph_id}/nodes", params={"type": "task", "limit": 1})
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
            "/api/graph_node_create",
            params={
                "graph_id": graph_id,
                "type": "task",
                "name": "rest-node",
                "schema_name": "task_schema",
                "tags": "rest",
            },
            json={"name": "Rest node"},
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        rest_created = response.json()
        assert rest_created["node"]["name"] == "rest-node"
        assert rest_created["node"]["schema_name"] == "task_schema"
        assert rest_created["node"]["tags"] == ["rest"]

        response = client.get(
            "/api/graph_node_list",
            params={"graph_id": graph_id, "type": "task", "limit": 10},
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        assert response.json()["total"] == 3
        assert {item["name"] for item in response.json()["items"]} == {
            "seeded-node",
            "web-node",
            "rest-node",
        }

        response = client.get(
            "/api/graph_node_get",
            params={"graph_id": graph_id, "node_id": web_node_id},
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        assert response.json()["node"]["name"] == "web-node"
        assert response.json()["node"]["tags"] == ["alpha", "beta"]

    cli_created = manager.cli(
        [
            "gpdb",
            "graph_node_create",
            graph_id,
            "task",
            json.dumps({"name": "CLI node"}),
        ],
        standalone_mode=False,
    )
    assert cli_created["node"]["type"] == "task"
    assert cli_created["node"]["data"] == {"name": "CLI node"}

    cli_get = manager.cli(
        ["gpdb", "graph_node_get", graph_id, cli_created["node"]["id"]],
        standalone_mode=False,
    )
    assert cli_get["node"]["data"] == {"name": "CLI node"}

    cli_list = manager.cli(
        ["gpdb", "graph_node_list", graph_id],
        standalone_mode=False,
    )
    assert cli_list["total"] == 4

    mcp_created = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_node_create",
        {
            "graph_id": graph_id,
            "type": "task",
            "name": "mcp-node",
            "schema_name": "task_schema",
            "tags": "mcp, final",
            "data": {"name": "MCP node"},
        },
    )
    assert mcp_created["node"]["name"] == "mcp-node"
    assert mcp_created["node"]["tags"] == ["mcp", "final"]

    mcp_get = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_node_get",
        {
            "graph_id": graph_id,
            "node_id": mcp_created["node"]["id"],
        },
    )
    assert mcp_get["node"]["name"] == "mcp-node"

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
    assert mcp_list["total"] == 5

    with TestClient(manager.app) as client:
        _login(client)
        response = client.get(f"/graphs/{graph_id}/nodes")
        assert response.status_code == 200
        assert "seeded-node" in response.text
        assert "web-node" in response.text
        assert "rest-node" in response.text
        assert cli_created["node"]["id"] in response.text
        assert "mcp-node" in response.text


def test_graph_node_update_delete_and_payload_vertical_slice_across_surfaces(tmp_path):
    """Test node update/delete/payload flow, blockers, and downloads across surfaces."""
    manager = _create_test_manager(tmp_path)
    graph_id = ""
    api_key_value = ""

    with TestClient(manager.app) as client:
        _bootstrap_owner(client)
        _login(client)

        response = client.get("/graphs/new")
        assert response.status_code == 200
        default_instance_id = _extract_instance_option_value(response.text, "Default instance")

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
        _seed_graph_schema(manager, table_prefix="node_slice_phase2", schema_name="task_schema")

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
        assert "Delete is blocked until child nodes and incident edges are removed." in response.text
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
            },
            follow_redirects=False,
        )
        assert response.status_code == 303

        response = client.get(response.headers["location"])
        assert response.status_code == 200
        assert "web-edit-renamed" in response.text
        assert "Tags: alpha, beta" in response.text
        assert "owner-1" in response.text

        response = client.post(
            f"/graphs/{graph_id}/nodes/{web_edit_id}/payload",
            data={"mime": "text/plain"},
            files={"payload_file": ("web.txt", b"web payload", "text/plain")},
            follow_redirects=False,
        )
        assert response.status_code == 303

        response = client.get(response.headers["location"])
        assert response.status_code == 200
        assert "A binary payload is stored on this node." in response.text
        assert "11 bytes" in response.text
        assert "Download payload" in response.text

        response = client.get(f"/graphs/{graph_id}/nodes/{web_edit_id}/payload")
        assert response.status_code == 200
        assert response.content == b"web payload"
        assert response.headers["content-type"].startswith("text/plain")
        assert "attachment;" in response.headers["content-disposition"]

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
            "/api/graph_node_update",
            params={
                "graph_id": graph_id,
                "node_id": rest_node_id,
                "type": "task",
                "name": "rest-edit-renamed",
                "schema_name": "task_schema",
                "tags": "rest, updated",
            },
            json={"name": "Rest edit updated", "status": "active"},
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        assert response.json()["node"]["name"] == "rest-edit-renamed"
        assert response.json()["node"]["schema_name"] == "task_schema"
        assert response.json()["node"]["tags"] == ["rest", "updated"]

        response = client.post(
            "/api/graph_node_payload_set",
            params={
                "graph_id": graph_id,
                "node_id": rest_node_id,
                "payload_base64": base64.b64encode(b"rest payload").decode("ascii"),
                "mime": "text/plain",
            },
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        assert response.json()["node"]["payload_size"] == 12

        response = client.get(
            "/api/graph_node_payload_get",
            params={"graph_id": graph_id, "node_id": rest_node_id},
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        assert response.json()["payload_base64"] == base64.b64encode(b"rest payload").decode("ascii")
        assert response.json()["node"]["payload_mime"] == "text/plain"

        response = client.post(
            "/api/graph_node_delete",
            params={"graph_id": graph_id, "node_id": rest_node_id},
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        assert response.json()["node"]["id"] == rest_node_id

    cli_updated = manager.cli(
        [
            "gpdb",
            "graph_node_update",
            graph_id,
            cli_node_id,
            "task",
            json.dumps({"name": "CLI edit updated", "status": "ready"}),
            "--name",
            "cli-edit-renamed",
        ],
        standalone_mode=False,
    )
    assert cli_updated["node"]["name"] == "cli-edit-renamed"
    assert cli_updated["node"]["data"] == {"name": "CLI edit updated", "status": "ready"}

    cli_payload_set = manager.cli(
        [
            "gpdb",
            "graph_node_payload_set",
            graph_id,
            cli_node_id,
            base64.b64encode(b"cli payload").decode("ascii"),
            "--mime",
            "text/plain",
        ],
        standalone_mode=False,
    )
    assert cli_payload_set["node"]["payload_size"] == 11

    cli_payload_get = manager.cli(
        ["gpdb", "graph_node_payload_get", graph_id, cli_node_id],
        standalone_mode=False,
    )
    assert cli_payload_get["payload_base64"] == base64.b64encode(b"cli payload").decode("ascii")
    assert cli_payload_get["node"]["payload_mime"] == "text/plain"

    cli_deleted = manager.cli(
        ["gpdb", "graph_node_delete", graph_id, cli_node_id],
        standalone_mode=False,
    )
    assert cli_deleted["node"]["id"] == cli_node_id

    mcp_updated = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_node_update",
        {
            "graph_id": graph_id,
            "node_id": mcp_node_id,
            "type": "task",
            "name": "mcp-edit-renamed",
            "schema_name": "task_schema",
            "tags": "mcp, updated",
            "data": {"name": "MCP edit updated", "status": "active"},
        },
    )
    assert mcp_updated["node"]["name"] == "mcp-edit-renamed"
    assert mcp_updated["node"]["tags"] == ["mcp", "updated"]

    mcp_payload_set = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_node_payload_set",
        {
            "graph_id": graph_id,
            "node_id": mcp_node_id,
            "payload_base64": base64.b64encode(b"mcp payload").decode("ascii"),
            "mime": "text/plain",
        },
    )
    assert mcp_payload_set["node"]["payload_size"] == 11

    mcp_payload_get = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_node_payload_get",
        {"graph_id": graph_id, "node_id": mcp_node_id},
    )
    assert mcp_payload_get["payload_base64"] == base64.b64encode(b"mcp payload").decode("ascii")
    assert mcp_payload_get["node"]["payload_mime"] == "text/plain"

    mcp_deleted = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_node_delete",
        {"graph_id": graph_id, "node_id": mcp_node_id},
    )
    assert mcp_deleted["node"]["id"] == mcp_node_id

    with TestClient(manager.app) as client:
        _login(client)
        response = client.get(f"/graphs/{graph_id}/nodes")
        assert response.status_code == 200
        assert "web-edit-renamed" in response.text
        assert "web-delete" not in response.text
        assert "rest-edit-renamed" not in response.text
        assert "cli-edit-renamed" not in response.text
        assert "mcp-edit-renamed" not in response.text


def test_graph_edge_browse_and_create_vertical_slice_across_surfaces(tmp_path):
    """Test edge browse/create flow across web, REST, CLI, and MCP."""
    manager = _create_test_manager(tmp_path)
    graph_id = ""
    api_key_value = ""

    with TestClient(manager.app) as client:
        _bootstrap_owner(client)
        _login(client)

        response = client.get("/graphs/new")
        assert response.status_code == 200
        default_instance_id = _extract_instance_option_value(response.text, "Default instance")

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
        _seed_graph_schema(manager, table_prefix="edge_slice", schema_name="edge_schema")

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

        response = client.get(f"/graphs/{graph_id}/edges", params={"type": "depends_on", "limit": 1})
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
            params={
                "graph_id": graph_id,
                "type": "depends_on",
                "source_id": rest_source_id,
                "target_id": rest_target_id,
                "schema_name": "edge_schema",
                "tags": "rest",
            },
            json={"name": "Rest edge"},
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        rest_created = response.json()
        assert rest_created["edge"]["type"] == "depends_on"
        assert rest_created["edge"]["schema_name"] == "edge_schema"
        assert rest_created["edge"]["tags"] == ["rest"]

        response = client.get(
            "/api/graph_edge_list",
            params={"graph_id": graph_id, "type": "depends_on", "limit": 10},
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

        response = client.get(
            "/api/graph_edge_get",
            params={"graph_id": graph_id, "edge_id": web_edge_id},
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 200
        assert response.json()["edge"]["source_id"] == web_source_id
        assert response.json()["edge"]["target_id"] == web_target_id

    cli_created = manager.cli(
        [
            "gpdb",
            "graph_edge_create",
            graph_id,
            "depends_on",
            cli_source_id,
            cli_target_id,
            json.dumps({"name": "CLI edge"}),
            "--schema-name",
            "edge_schema",
            "--tags",
            "cli, linked",
        ],
        standalone_mode=False,
    )
    assert cli_created["edge"]["schema_name"] == "edge_schema"
    assert cli_created["edge"]["tags"] == ["cli", "linked"]

    cli_get = manager.cli(
        ["gpdb", "graph_edge_get", graph_id, cli_created["edge"]["id"]],
        standalone_mode=False,
    )
    assert cli_get["edge"]["source_id"] == cli_source_id
    assert cli_get["edge"]["target_id"] == cli_target_id

    cli_list = manager.cli(
        ["gpdb", "graph_edge_list", graph_id],
        standalone_mode=False,
    )
    assert cli_list["total"] == 4

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
            "tags": "mcp, final",
            "data": {"name": "MCP edge"},
        },
    )
    assert mcp_created["edge"]["schema_name"] == "edge_schema"
    assert mcp_created["edge"]["tags"] == ["mcp", "final"]

    mcp_get = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_edge_get",
        {
            "graph_id": graph_id,
            "edge_id": mcp_created["edge"]["id"],
        },
    )
    assert mcp_get["edge"]["source_id"] == mcp_source_id

    mcp_list = _call_persisted_authenticated_mcp_tool(
        manager,
        api_key_value,
        "graph_edge_list",
        {
            "graph_id": graph_id,
            "type": "depends_on",
            "limit": 10,
        },
    )
    assert mcp_list["total"] == 5

    with TestClient(manager.app) as client:
        _login(client)
        response = client.get(f"/graphs/{graph_id}/edges")
        assert response.status_code == 200
        assert seeded_source_id in response.text
        assert web_edge_id in response.text
        assert rest_created["edge"]["id"] in response.text
        assert cli_created["edge"]["id"] in response.text
        assert mcp_created["edge"]["id"] in response.text


def test_cli_api_key_management_commands(tmp_path):
    """Test trusted local CLI API key management commands."""
    manager = _create_test_manager(tmp_path)

    with TestClient(manager.app) as client:
        _bootstrap_owner(client)

    created = manager.cli(
        ["gpdb", "api_key_create", "owner", "CLI key"],
        standalone_mode=False,
    )
    assert created["label"] == "CLI key"
    assert str(created["api_key"]).startswith("gpdb_")
    key_id = str(created["key_id"])

    listed = manager.cli(
        ["gpdb", "api_key_list", "owner"],
        standalone_mode=False,
    )
    assert any(item["key_id"] == key_id for item in listed)

    revealed = manager.cli(
        ["gpdb", "api_key_reveal", "owner", key_id],
        standalone_mode=False,
    )
    assert revealed["api_key"] == created["api_key"]

    revoked = manager.cli(
        ["gpdb", "api_key_revoke", "owner", key_id],
        standalone_mode=False,
    )
    assert revoked["is_active"] is False


def test_mcp_api_key_management_tools(tmp_path):
    """Test authenticated MCP API key management tools."""
    manager = _create_test_manager(tmp_path)

    async def _run():
        assert manager.lifespan_ctx is not None
        async with manager.lifespan_ctx(manager.app):
            services = manager.app.state.services
            assert services.admin_store is not None

            owner = await services.admin_store.create_initial_owner(
                username="owner",
                password_hash=hash_password("secret-pass"),
                display_name="Primary Owner",
            )
            bootstrap_generated = generate_api_key()
            bootstrap_key = await services.admin_store.create_api_key(
                user_id=owner.id,
                label="MCP bootstrap",
                key_id=bootstrap_generated.key_id,
                preview=bootstrap_generated.preview,
                secret_hash=hash_api_key_secret(bootstrap_generated.secret),
                key_value=bootstrap_generated.token,
            )

            verified_token = await entry._AdminAPIKeyTokenVerifier(
                SimpleNamespace(admin_store=services.admin_store)
            ).verify_token(bootstrap_generated.token)
            assert verified_token is not None

            listed = await _call_authenticated_mcp_tool_in_loop(
                manager,
                verified_token,
                "api_key_list",
                {},
            )
            assert any(item["key_id"] == bootstrap_key.key_id for item in listed)

            created = await _call_authenticated_mcp_tool_in_loop(
                manager,
                verified_token,
                "api_key_create",
                {"label": "MCP managed"},
            )
            assert created["label"] == "MCP managed"
            assert str(created["api_key"]).startswith("gpdb_")
            created_key_id = str(created["key_id"])

            revealed = await _call_authenticated_mcp_tool_in_loop(
                manager,
                verified_token,
                "api_key_reveal",
                {"key_id": created_key_id},
            )
            assert revealed["api_key"] == created["api_key"]

            revoked = await _call_authenticated_mcp_tool_in_loop(
                manager,
                verified_token,
                "api_key_revoke",
                {"key_id": created_key_id},
            )
            assert revoked["is_active"] is False

    asyncio.run(_run())


def test_rest_api_public_docs_and_health_endpoints(tmp_path):
    """Test that documented public REST endpoints stay accessible without API keys."""
    manager = _create_test_manager(tmp_path)

    with TestClient(manager.app) as client:
        for path in sorted(entry.REST_API_PUBLIC_PATHS):
            response = client.get(f"/api{path}")
            assert response.status_code == 200

        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"mcp_servers": ["gpdb"]}

        response = client.post("/api/status")
        assert response.status_code == 401


def test_startup_requires_session_secret(tmp_path):
    """Test that startup fails clearly when the session secret is missing."""
    config_path = tmp_path / "admin.toml"
    data_dir = tmp_path / "admin data"
    config_path.write_text(
        (
            "[server]\n"
            "host = \"127.0.0.1\"\n"
            "port = 8747\n"
            "[runtime]\n"
            f"data_dir = \"{data_dir.as_posix()}\"\n"
        ),
        encoding="utf-8",
    )

    config_store = ConfigStore.from_sources(cli_path=config_path)
    resolved_config = config_store.load()
    manager = entry.create_manager(
        resolved_config=resolved_config,
        config_store=config_store,
    )

    with pytest.raises(RuntimeError, match="auth.session_secret"):
        with TestClient(manager.app):
            pass


def test_extract_config_arg_strips_global_option():
    """Test that the global config option is removed before CLI dispatch."""
    config_path, remaining = extract_config_arg(["start", "--config", "./admin.toml", "--port", "9000"])

    assert config_path == Path("./admin.toml")
    assert remaining == ["start", "--port", "9000"]


def test_resolve_config_location_prefers_environment(monkeypatch):
    """Test that config resolution uses the env var when no CLI path is passed."""
    monkeypatch.setenv("GPDB_CONFIG", "/tmp/gpdb-admin.toml")

    location = resolve_config_location()

    assert location.path == Path("/tmp/gpdb-admin.toml")
    assert location.source == ConfigPathSource.ENV


def test_config_store_round_trip(tmp_path):
    """Test that file-backed config can be saved and loaded."""
    path = tmp_path / "admin.toml"
    store = ConfigStore.from_sources(cli_path=path)

    store.save(
        AdminConfig.model_validate(
            {
                "server": {"host": "0.0.0.0", "port": 9010},
                "runtime": {"data_dir": str(tmp_path / "data")},
                "auth": {"session_secret": "test-secret"},
            }
        )
    )
    resolved = store.load()

    assert resolved.location.path == path
    assert resolved.location.exists is True
    assert resolved.server.host == "0.0.0.0"
    assert resolved.server.port == 9010
    assert resolved.runtime.data_dir == str(tmp_path / "data")
    assert resolved.auth.session_secret == "test-secret"


def test_bootstrap_runtime_uses_config_file(tmp_path):
    """Test that bootstrap loads config before creating the runtime."""
    path = tmp_path / "admin.toml"
    path.write_text(
        (
            "[server]\n"
            "host = \"0.0.0.0\"\n"
            "port = 9011\n"
            "[runtime]\n"
            f"data_dir = \"{(tmp_path / 'runtime-data').as_posix()}\"\n"
        ),
        encoding="utf-8",
    )

    manager, resolved_config, remaining_args = entry.bootstrap_runtime(
        ["--config", str(path), "status"]
    )

    assert remaining_args == ["status"]
    assert resolved_config.location.path == path
    assert resolved_config.server.host == "0.0.0.0"
    assert resolved_config.server.port == 9011
    assert resolved_config.runtime.data_dir == str(tmp_path / "runtime-data")
    assert resolved_config.auth.session_secret is not None
    assert manager.app.state.config.server.port == 9011


def _create_test_manager(tmp_path: Path):
    """Create a manager backed by a temporary config and captive data dir."""
    config_path = tmp_path / "admin.toml"
    data_dir = tmp_path / "admin data"
    config_path.write_text(
        (
            "[server]\n"
            "host = \"127.0.0.1\"\n"
            "port = 8747\n"
            "[runtime]\n"
            f"data_dir = \"{data_dir.as_posix()}\"\n"
            "[auth]\n"
            "session_secret = \"test-session-secret\"\n"
        ),
        encoding="utf-8",
    )
    config_store = ConfigStore.from_sources(cli_path=config_path)
    resolved_config = config_store.load()
    return entry.create_manager(resolved_config=resolved_config, config_store=config_store)


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
    assert response.headers["location"] == "/login"


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


def _captive_instance_form(
    manager,
    *,
    slug: str,
    display_name: str,
    description: str,
    password: str = "top-secret",
) -> dict[str, str]:
    uri = manager.app.state.services.captive_server.get_uri()
    parsed = make_url(uri)
    host = str(parsed.query.get("host") or parsed.host or "127.0.0.1")
    port = str(parsed.query.get("port") or parsed.port or "")
    return {
        "slug": slug,
        "display_name": display_name,
        "description": description,
        "host": host,
        "port": port,
        "database": str(parsed.database or "postgres"),
        "username": str(parsed.username or "postgres"),
        "password": password,
    }


def _extract_instance_option_value(html: str, label: str) -> str:
    match = re.search(
        rf'<option[^>]*value="([^"]+)"[^>]*>\s*{re.escape(label)}\s*\([^)]*\)\s*</option>',
        html,
        re.S,
    )
    assert match is not None
    return match.group(1)


def _extract_graph_action(html: str, graph_name: str, action: str) -> str:
    if action == "edit":
        pattern = rf"<h3>{re.escape(graph_name)}</h3>.*?href=\"([^\"]*?/graphs/[^\"]+/edit)\""
    else:
        pattern = rf"<h3>{re.escape(graph_name)}</h3>.*?action=\"([^\"]*?/graphs/[^\"]+/delete)\""
    match = re.search(pattern, html, re.S)
    assert match is not None
    return match.group(1)


def _extract_instance_action(html: str, instance_name: str, action: str) -> str:
    if action == "edit":
        pattern = (
            rf"<h3>{re.escape(instance_name)}</h3>.*?resource-meta.*?"
            rf"href=\"([^\"]*?/instances/[^\"]+/edit)\""
        )
    else:
        pattern = (
            rf"<h3>{re.escape(instance_name)}</h3>.*?resource-meta.*?"
            rf"action=\"([^\"]*?/instances/[^\"]+/delete)\""
        )
    match = re.search(pattern, html, re.S)
    assert match is not None
    return match.group(1)


def _extract_revealed_api_key(html: str) -> str:
    match = re.search(r'<input readonly value="([^"]+)"', html)
    assert match is not None
    return match.group(1)


def _read_stored_api_key(manager, *, label: str) -> dict[str, object]:
    services = manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None

    async def _load() -> dict[str, object]:
        store = AdminStore(
            services.captive_server.get_uri(),
            instance_secret=services.resolved_config.auth.session_secret,
        )
        try:
            owner = await store.get_user_by_username("owner")
            assert owner is not None
            api_keys = await store.list_api_keys_for_user(owner.id)
            api_key = next(item for item in api_keys if item.label == label)
            node = await store.db.get_node(api_key.id)
            assert node is not None
            return dict(node.data)
        finally:
            await store.close()

    return asyncio.run(_load())


def _read_api_key_key_id(manager, *, api_key_node_id: str) -> str:
    services = manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None

    async def _load() -> str:
        store = AdminStore(
            services.captive_server.get_uri(),
            instance_secret=services.resolved_config.auth.session_secret,
        )
        try:
            node = await store.db.get_node(api_key_node_id)
            assert node is not None
            return str(node.data["key_id"])
        finally:
            await store.close()

    return asyncio.run(_load())


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
            verifier = entry._AdminAPIKeyTokenVerifier(SimpleNamespace(admin_store=store))
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
        assert manager.lifespan_ctx is not None
        async with manager.lifespan_ctx(manager.app):
            services = manager.app.state.services
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


def _read_stored_instance_password(manager, *, slug: str) -> object:
    services = manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None

    async def _load() -> object:
        store = AdminStore(
            services.captive_server.get_uri(),
            instance_secret=services.resolved_config.auth.session_secret,
        )
        try:
            instance = await store.get_instance_by_slug(slug)
            assert instance is not None
            node = await store.db.get_node(instance.id)
            assert node is not None
            return node.data["password"]
        finally:
            await store.close()

    return asyncio.run(_load())


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
            await db.register_schema(
                "task_schema",
                {
                    "type": "object",
                    "properties": {
                        "label": {"type": "string"},
                    },
                    "required": ["label"],
                },
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


def _seed_schema_usage(manager, *, table_prefix: str, schema_name: str) -> None:
    services = manager.app.state.services
    assert services.captive_server is not None

    async def _seed() -> None:
        db = GPGraph(services.captive_server.get_uri(), table_prefix=table_prefix)
        try:
            source = await db.set_node(
                NodeUpsert(
                    type="schema-test",
                    name="source",
                    schema_name=schema_name,
                    data={"name": "Source"},
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


def _seed_graph_schema(manager, *, table_prefix: str, schema_name: str) -> None:
    services = manager.app.state.services
    assert services.captive_server is not None

    async def _seed() -> None:
        db = GPGraph(services.captive_server.get_uri(), table_prefix=table_prefix)
        try:
            await db.register_schema(
                schema_name,
                _schema_definition(f"{schema_name} schema"),
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
            node = await db.set_node(
                NodeUpsert(
                    type=type,
                    name=name,
                    parent_id=parent_id,
                    schema_name=schema_name,
                    data=data,
                    tags=list(tags or []),
                )
            )
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
            edge = await db.set_edge(
                EdgeUpsert(
                    type=type,
                    source_id=source_id,
                    target_id=target_id,
                    schema_name=schema_name,
                    data=data,
                    tags=list(tags or []),
                )
            )
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
