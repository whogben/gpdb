"""Runtime services for the gpdb-admin process."""

from __future__ import annotations

from contextlib import ExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pixeltable_pgserver import PostgresServer
from pixeltable_pgserver import postgres_server as postgres_server_module

from gpdb import GPGraph
from gpdb.admin.auth import SessionSigner
from gpdb.admin.config import ConfigStore, ResolvedConfig
from gpdb.admin.graph_content import GraphContentService
from gpdb.admin.instances import ManagedInstanceMonitor
from gpdb.admin.store import AdminStore


@dataclass
class AdminServices:
    """Shared runtime services used by the web app and API layers."""

    resolved_config: ResolvedConfig
    config_store: ConfigStore
    admin_store: AdminStore | None = None
    session_signer: SessionSigner | None = None
    captive_server: PostgresServer | None = None
    instance_monitor: ManagedInstanceMonitor | None = None
    graph_content: GraphContentService | None = None


class _PathSafePostgresServer(PostgresServer):
    """PostgresServer wrapper that quotes socket paths correctly."""

    def ensure_postgres_running(self) -> None:
        """Start postgres while preserving paths that contain spaces."""
        postmaster_info = postgres_server_module.PostmasterInfo.read_from_pgdata(
            self.pgdata
        )
        if postmaster_info is not None and postmaster_info.is_running():
            postgres_server_module._logger.info(
                f"a postgres server is already running: {postmaster_info=} {postmaster_info.process=}"
            )
            self._postmaster_info = postmaster_info
        else:
            if postmaster_info is not None and not postmaster_info.is_running():
                postgres_server_module._logger.info(
                    f"found a postmaster.pid file, but the server is not running: {postmaster_info=}"
                )
            if postmaster_info is None:
                postgres_server_module._logger.info(
                    f"no postmaster.pid file found in {self.pgdata}"
                )

            postgres_args: str
            subprocess_kwargs: dict[str, Any]

            if postgres_server_module.platform.system() != "Windows":
                socket_dir = postgres_server_module.find_suitable_socket_dir(
                    self.pgdata, self.runtime_path
                )

                if self.system_user is not None and socket_dir != self.pgdata:
                    postgres_server_module.ensure_prefix_permissions(socket_dir)
                    socket_dir.chmod(0o777)

                postgres_args = (
                    f'-h "" -k "{_escape_postgres_option_value(socket_dir)}"'
                )
                subprocess_kwargs = {}
            else:
                host = "127.0.0.1"
                port = postgres_server_module.find_suitable_port(host)
                postgres_args = f'-h "{host}" -p {port}'
                subprocess_kwargs = {
                    "close_fds": True,
                    "creationflags": (
                        postgres_server_module.CREATE_NEW_PROCESS_GROUP
                        | postgres_server_module.CREATE_NO_WINDOW
                    ),
                }

            try:
                pg_ctl_args = (
                    "-w",
                    "-o",
                    postgres_args,
                    "-l",
                    str(self.log),
                    "-D",
                    str(self.pgdata),
                    "start",
                )
                postgres_server_module._logger.info(f"running pg_ctl... {pg_ctl_args=}")
                postgres_server_module.pgexec(
                    "pg_ctl",
                    pg_ctl_args,
                    user=self.system_user,
                    timeout=10,
                    **subprocess_kwargs,
                )
            except postgres_server_module.subprocess.SubprocessError:
                postgres_server_module._logger.error(
                    f"Failed to start server.\nShowing contents of postgres server log ({self.log.absolute()}) "
                    f"below:\n{self.log.read_text()}"
                )
                raise

            while True:
                postgres_server_module._logger.info(
                    "Waiting for postmaster info to show a running process."
                )
                pinfo = postgres_server_module.PostmasterInfo.read_from_pgdata(
                    self.pgdata
                )
                postgres_server_module._logger.info(
                    f"Running; checking if ready {pinfo=}"
                )
                if pinfo is not None and pinfo.is_running() and pinfo.status == "ready":
                    self._postmaster_info = pinfo
                    break

                postgres_server_module._logger.info(
                    "Not ready yet; waiting a bit longer."
                )
                postgres_server_module.time.sleep(1.0)

        if (
            self._postmaster_info is None
            or not self._postmaster_info.is_running()
            or self._postmaster_info.status != "ready"
        ):
            raise RuntimeError("Postgres server failed to reach ready state")


def create_admin_lifespan(services: AdminServices):
    """Create a FastAPI lifespan that boots the captive admin instance."""

    @asynccontextmanager
    async def lifespan(app):
        if not hasattr(app.state, "admin_lifespan_active"):
            app.state.admin_lifespan_active = False
        if not hasattr(app.state, "admin_lifespan_depth"):
            app.state.admin_lifespan_depth = 0

        if app.state.admin_lifespan_active:
            app.state.admin_lifespan_depth += 1
            previous_admin_store = services.admin_store
            previous_graph_content = services.graph_content
            nested_admin_store: AdminStore | None = None
            try:
                session_secret = services.resolved_config.auth.session_secret
                if services.captive_server is not None and session_secret:
                    nested_admin_store = AdminStore(
                        services.captive_server.get_uri(),
                        instance_secret=session_secret,
                    )
                    await nested_admin_store.initialize()
                    services.admin_store = nested_admin_store
                    services.graph_content = GraphContentService(
                        admin_store=nested_admin_store,
                        captive_url_factory=services.captive_server.get_uri,
                        instance_monitor=services.instance_monitor,
                    )
                yield
            finally:
                if nested_admin_store is not None:
                    await nested_admin_store.close()
                services.admin_store = previous_admin_store
                services.graph_content = previous_graph_content
                app.state.admin_lifespan_depth -= 1
            return

        app.state.admin_lifespan_active = True
        app.state.admin_lifespan_depth = 1

        data_dir = Path(services.resolved_config.runtime.data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        pgdata = data_dir / "pgdata"
        pgdata.mkdir(parents=True, exist_ok=True)

        with ExitStack() as stack:
            server = _PathSafePostgresServer(pgdata)
            stack.enter_context(server)

            session_secret = services.resolved_config.auth.session_secret
            if not session_secret:
                raise RuntimeError(
                    "gpdb-admin requires auth.session_secret before startup"
                )

            admin_store = AdminStore(server.get_uri(), instance_secret=session_secret)
            await admin_store.initialize()
            default_graph = GPGraph(server.get_uri())
            await default_graph.create_tables()
            await default_graph.sqla_engine.dispose()
            # Create schemas for admin node types (instance, graph, user, api_key)
            # These are used as type identifiers, not for validation
            from gpdb import SchemaUpsert
            try:
                await admin_store.db.set_schemas([
                    SchemaUpsert(name="instance", json_schema={"type": "object"}, kind="node"),
                    SchemaUpsert(name="graph", json_schema={"type": "object"}, kind="node"),
                    SchemaUpsert(name="user", json_schema={"type": "object"}, kind="node"),
                    SchemaUpsert(name="api_key", json_schema={"type": "object"}, kind="node"),
                ])
            except Exception as e:
                print(f"Error creating schemas: {e}")
                raise
            builtin_instance = await admin_store.ensure_builtin_instance()
            await admin_store.upsert_graph_metadata(
                instance_id=builtin_instance.id,
                table_prefix="",
                display_name="Default graph",
                exists_in_instance=True,
                source="managed",
            )

            instance_monitor = ManagedInstanceMonitor(
                admin_store=admin_store,
                captive_url_factory=server.get_uri,
            )
            await instance_monitor.refresh_all()
            await instance_monitor.start()

            services.captive_server = server
            services.admin_store = admin_store
            services.session_signer = SessionSigner(session_secret)
            services.instance_monitor = instance_monitor
            services.graph_content = GraphContentService(
                admin_store=admin_store,
                captive_url_factory=server.get_uri,
                instance_monitor=instance_monitor,
            )
            app.state.services = services
            try:
                yield
            finally:
                await instance_monitor.stop()
                await admin_store.close()
                app.state.admin_lifespan_depth = 0
                app.state.admin_lifespan_active = False

    return lifespan


def _escape_postgres_option_value(value: Path | str) -> str:
    """Escape a value embedded inside the pg_ctl -o option string."""
    return str(value).replace("\\", "\\\\").replace('"', '\\"')
