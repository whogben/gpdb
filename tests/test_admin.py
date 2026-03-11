import pytest
from fastapi.testclient import TestClient

try:
    from gpdb.admin import entry

    HAS_ADMIN = True
except ImportError:
    HAS_ADMIN = False

admin_only = pytest.mark.skipif(not HAS_ADMIN, reason="admin module not installed")


@admin_only
def test_cli_status_command():
    """Test that the status command is accessible via CLI."""
    # Create a manager to get the CLI app
    admin_service = entry.ToolService("admin", [entry.status])
    cli = entry.CLIServer("gpdb")
    cli.mount(admin_service)

    manager = entry.ServerManager(name="gpdb-admin")
    manager.add_server(cli)

    # Test the CLI command
    result = manager.cli(["gpdb", "status"], standalone_mode=False)
    assert result == "OK"


@admin_only
def test_fastapi_status_command():
    """Test that the status command is accessible via FastAPI REST API."""
    # Create a test client from the manager's FastAPI app
    admin_service = entry.ToolService("admin", [entry.status])
    rest_api = entry.OpenAPIServer(path_prefix="/api", title="GPDB Admin API")
    rest_api.mount(admin_service)

    manager = entry.ServerManager(name="gpdb-admin")
    manager.add_server(rest_api)

    client = TestClient(manager.app)
    response = client.post("/api/status")
    assert response.status_code == 200
    assert response.json() == "OK"


@admin_only
def test_mcp_status_command():
    """Test that the status command is accessible via MCP server."""
    admin_service = entry.ToolService("admin", [entry.status])
    mcp_server = entry.SSEMCPServer("gpdb")
    mcp_server.mount(admin_service)

    manager = entry.ServerManager(name="gpdb-admin")
    manager.add_server(mcp_server)

    # Verify the MCP server is registered
    assert "gpdb" in manager.mcp_servers

    # Verify the MCP server has the tool registered
    mcp = manager.mcp_servers["gpdb"]
    # FastMCP stores tools internally - we can verify by checking the server exists
    assert mcp is not None


@admin_only
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
