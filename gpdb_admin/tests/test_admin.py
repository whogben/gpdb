import asyncio

import pytest
from fastapi.testclient import TestClient

from gpdb.admin import entry


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

    with TestClient(manager.app) as client:
        response = client.post("/api/status")
    assert response.status_code == 200
    assert response.json() == "OK"


def test_mcp_status_command():
    """Test that the status command is accessible via MCP server."""
    admin_service = entry.ToolService("admin", [entry.status])
    mcp_server = entry.AuthMCPServer("gpdb")
    mcp_server.mount(admin_service)

    manager = entry.ServerManager(name="gpdb-admin")
    manager.add_server(mcp_server)

    assert "gpdb" in manager.mcp_servers
    assert manager.mcp_servers["gpdb"] is not None


def test_health_endpoint(admin_test_env):
    """Test that the health endpoint returns registered MCP servers."""
    response = admin_test_env.client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"mcp_servers": ["gpdb"]}


def test_rest_api_public_docs_and_health_endpoints(admin_test_env):
    """Test that documented public REST endpoints stay accessible without API keys."""
    client = admin_test_env.client

    for path in sorted(entry.REST_API_PUBLIC_PATHS):
        response = client.get(f"/api{path}")
        assert response.status_code == 200

    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"mcp_servers": ["gpdb"]}

    response = client.post("/api/status")
    assert response.status_code == 401


def test_nested_admin_lifespan_reuses_active_services(admin_test_env):
    """Ensure nested admin lifespans do not replace the active captive server."""
    manager = admin_test_env.manager
    client = admin_test_env.client
    services = manager.app.state.services
    assert services.captive_server is not None
    active_uri = services.captive_server.get_uri()

    async def _run():
        admin_lifespan = entry.create_admin_lifespan(services)
        async with admin_lifespan(manager.app):
            assert services.captive_server is not None
            assert services.captive_server.get_uri() == active_uri
            assert manager.app.state.admin_lifespan_active is True
            assert manager.app.state.admin_lifespan_depth == 2

    asyncio.run(_run())

    assert services.captive_server is not None
    assert services.captive_server.get_uri() == active_uri
    assert manager.app.state.admin_lifespan_active is True
    assert manager.app.state.admin_lifespan_depth == 1

    response = client.get("/")
    assert response.status_code == 200
    assert "Create the initial owner user." in response.text
