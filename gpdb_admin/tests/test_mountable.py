"""Tests for mountable GPDB admin runtime."""

from pathlib import Path

import pytest
from toolaccess import ServerManager

from gpdb.admin import entry
from gpdb.admin.config import ConfigStore


def _create_test_config(tmp_path: Path) -> ConfigStore:
    """Create a test config store backed by a temporary file."""
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
    return ConfigStore.from_sources(cli_data_dir=data_dir)


def test_create_admin_runtime_returns_runtime(tmp_path):
    """Test that create_admin_runtime() returns an AdminRuntime with all expected components."""
    config_store = _create_test_config(tmp_path)
    resolved_config = config_store.load()

    runtime = entry.create_admin_runtime(
        config_store=config_store,
        resolved_config=resolved_config,
    )

    # Verify all fields are populated
    assert runtime.services is not None
    assert isinstance(runtime.services, entry.AdminServices)
    assert runtime.resolved_config is not None
    assert isinstance(runtime.resolved_config, entry.ResolvedConfig)
    assert runtime.config_store is not None
    assert isinstance(runtime.config_store, ConfigStore)
    assert runtime.lifespan is not None
    assert callable(runtime.lifespan)
    assert runtime.web_app is not None
    assert runtime.rest_api is not None
    assert runtime.mcp_server is not None
    assert runtime.cli_server is not None
    assert runtime.admin_service is not None
    assert runtime.graph_service is not None
    assert runtime.api_key_service is not None

    # Verify services are properly configured
    assert runtime.services.resolved_config is resolved_config
    assert runtime.services.config_store is config_store


def test_create_admin_runtime_with_http_root(tmp_path):
    """Test that http_root parameter affects paths."""
    config_store = _create_test_config(tmp_path)
    resolved_config = config_store.load()

    runtime = entry.create_admin_runtime(
        config_store=config_store,
        resolved_config=resolved_config,
        http_root="/gpdb",
        api_path_prefix="/api",
    )

    # Verify web_app has correct path_prefix
    assert runtime.web_app.path_prefix == "/gpdb"

    # Verify rest_api has correct path_prefix (http_root + api_path_prefix)
    assert runtime.rest_api.path_prefix == "/gpdb/api"


def test_create_admin_runtime_without_cli(tmp_path):
    """Test that cli_root_name=None skips CLI creation."""
    config_store = _create_test_config(tmp_path)
    resolved_config = config_store.load()

    runtime = entry.create_admin_runtime(
        config_store=config_store,
        resolved_config=resolved_config,
        cli_root_name=None,
    )

    # Verify cli_server is None when cli_root_name=None
    assert runtime.cli_server is None

    # Verify other components are still created
    assert runtime.web_app is not None
    assert runtime.rest_api is not None
    assert runtime.mcp_server is not None


def test_attach_admin_to_manager(tmp_path):
    """Test attaching admin to existing ServerManager."""
    config_store = _create_test_config(tmp_path)
    resolved_config = config_store.load()

    # Create a host ServerManager
    manager = ServerManager(name="host-app")

    # Attach admin to the manager
    runtime = entry.attach_admin_to_manager(
        manager=manager,
        config_store=config_store,
        resolved_config=resolved_config,
        http_root="/gpdb",
        api_path_prefix="/api",
        mcp_name="gpdb",
        cli_root_name=None,
    )

    # Verify runtime is returned
    assert runtime is not None
    assert isinstance(runtime, entry.AdminRuntime)

    # Verify all servers are attached to the manager
    assert runtime.web_app in manager.active_servers.values()
    assert runtime.rest_api in manager.active_servers.values()
    assert runtime.mcp_server in manager.active_servers.values()

    # Verify CLI is not attached (cli_root_name=None)
    assert runtime.cli_server is None


def test_admin_runtime_exposes_services(tmp_path):
    """Test that ToolServices are exposed for host CLI integration."""
    config_store = _create_test_config(tmp_path)
    resolved_config = config_store.load()

    runtime = entry.create_admin_runtime(
        config_store=config_store,
        resolved_config=resolved_config,
    )

    # Verify all ToolServices are accessible
    assert runtime.admin_service is not None
    assert runtime.graph_service is not None
    assert runtime.api_key_service is not None

    # Verify services have the expected names
    assert runtime.admin_service.name == "admin"
    assert runtime.graph_service.name == "admin-graph"
    assert runtime.api_key_service.name == "admin-apikeys"


def test_create_manager_still_works(tmp_path):
    """Backwards compatibility test - verify create_manager() still works."""
    config_store = _create_test_config(tmp_path)
    resolved_config = config_store.load()

    # Verify create_manager() still works and returns ServerManager
    manager = entry.create_manager(
        resolved_config=resolved_config,
        config_store=config_store,
    )

    assert manager is not None
    assert isinstance(manager, ServerManager)
    assert manager.name == "gpdb-admin"

    # Verify manager.state.admin_runtime is exposed
    assert hasattr(manager.app.state, "admin_runtime")
    assert manager.app.state.admin_runtime is not None
    assert isinstance(manager.app.state.admin_runtime, entry.AdminRuntime)

    # Verify backwards-compatible state attributes
    assert hasattr(manager.app.state, "config")
    assert manager.app.state.config is resolved_config
    assert hasattr(manager.app.state, "config_store")
    assert manager.app.state.config_store is config_store
    assert hasattr(manager.app.state, "services")
    assert manager.app.state.services is not None
    assert isinstance(manager.app.state.services, entry.AdminServices)

    # Verify all servers are attached
    assert manager.app.state.admin_runtime.web_app in manager.active_servers.values()
    assert manager.app.state.admin_runtime.rest_api in manager.active_servers.values()
    assert manager.app.state.admin_runtime.mcp_server in manager.active_servers.values()
    assert manager.app.state.admin_runtime.cli_server in manager.active_servers.values()
