"""Entry point for the `gpdb` console command."""

from toolaccess import CLIServer, OpenAPIServer, SSEMCPServer, ServerManager, ToolService
from toolaccess.toolaccess import MountableApp

from gpdb.admin.web import create_web_app


def status() -> str:
    """Return the current status of the GPDB admin service."""
    return "OK"


def create_manager() -> ServerManager:
    """Build the combined admin runtime."""
    admin_service = ToolService("admin", [status])

    rest_api = OpenAPIServer(path_prefix="/api", title="GPDB Admin API")
    rest_api.mount(admin_service)

    mcp_server = SSEMCPServer("gpdb")
    mcp_server.mount(admin_service)

    cli = CLIServer("gpdb")
    cli.mount(admin_service)

    web_app = MountableApp(create_web_app(), path_prefix="", name="web")

    manager = ServerManager(name="gpdb-admin")
    manager.add_server(web_app)
    manager.add_server(rest_api)
    manager.add_server(mcp_server)
    manager.add_server(cli)
    return manager


def main():
    create_manager().run()


if __name__ == "__main__":
    main()
