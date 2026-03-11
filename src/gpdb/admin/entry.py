"""Entry point for the `gpdp` console command."""

from toolaccess import (
    ServerManager,
    ToolService,
    OpenAPIServer,
    SSEMCPServer,
    CLIServer,
)


def status() -> str:
    """Return the current status of the GPDB admin service."""
    return "OK"


def main():
    admin_service = ToolService("admin", [status])

    rest_api = OpenAPIServer(path_prefix="/api", title="GPDB Admin API")
    rest_api.mount(admin_service)

    mcp_server = SSEMCPServer("gpdb")
    mcp_server.mount(admin_service)

    cli = CLIServer("gpdb")
    cli.mount(admin_service)

    manager = ServerManager(name="gpdb-admin")
    manager.add_server(rest_api)
    manager.add_server(mcp_server)
    manager.add_server(cli)

    manager.run()


if __name__ == "__main__":
    main()
