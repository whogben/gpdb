from pathlib import Path

import pytest

from gpdb.admin import entry
from gpdb.admin.config import (
    AdminConfig,
    ConfigStore,
    DataDirSource,
    default_data_dir,
    extract_data_dir_arg,
    resolve_data_dir_location,
)


def test_extract_data_dir_arg_strips_global_option():
    """Test that the global --data-dir option is removed before CLI dispatch."""
    data_dir, remaining = extract_data_dir_arg(
        ["start", "--data-dir", "./data", "--port", "9000"]
    )

    assert data_dir == Path("./data")
    assert remaining == ["start", "--port", "9000"]


def test_resolve_data_dir_location_prefers_environment(monkeypatch):
    """Test that data-dir resolution uses the env var when no CLI path is passed."""
    monkeypatch.setenv("GPDB_DATA_DIR", "/var/lib/gpdb")

    location = resolve_data_dir_location()

    assert location.data_dir == Path("/var/lib/gpdb")
    assert location.source == DataDirSource.ENV
    assert location.path == Path("/var/lib/gpdb/admin.toml")


def test_config_store_round_trip(tmp_path):
    """Test that file-backed config can be saved and loaded."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    store = ConfigStore.from_sources(cli_data_dir=data_dir)

    store.save(
        AdminConfig.model_validate(
            {
                "server": {"host": "0.0.0.0", "port": 9010},
                "auth": {"session_secret": "test-secret"},
            }
        )
    )
    resolved = store.load()

    assert resolved.location.data_dir == data_dir
    assert resolved.location.path == data_dir / "admin.toml"
    assert resolved.location.exists is True
    assert resolved.server.host == "0.0.0.0"
    assert resolved.server.port == 9010
    assert resolved.runtime.data_dir == str(data_dir)
    assert resolved.auth.session_secret == "test-secret"


def test_data_dir_always_from_resolved_not_from_file(tmp_path):
    """Effective data_dir is always the resolved location; file [runtime].data_dir is ignored."""
    data_dir = tmp_path / "actual"
    data_dir.mkdir()
    config_path = data_dir / "admin.toml"
    config_path.write_text(
        '[runtime]\ndata_dir = "/ignored/path"\n',
        encoding="utf-8",
    )
    store = ConfigStore.from_sources(cli_data_dir=data_dir)
    resolved = store.load()
    assert resolved.runtime.data_dir == str(data_dir)


def test_data_dir_uses_platform_default_when_no_cli_or_env(monkeypatch):
    """When no CLI or env is set, data_dir is the platform default."""
    monkeypatch.delenv("GPDB_DATA_DIR", raising=False)
    store = ConfigStore.from_sources(cli_data_dir=None)
    resolved = store.load()
    assert resolved.location.source == DataDirSource.DEFAULT
    assert resolved.runtime.data_dir == str(default_data_dir())


def test_bootstrap_runtime_uses_data_dir(tmp_path):
    """Test that bootstrap loads config from the given data dir."""
    data_dir = tmp_path
    (data_dir / "admin.toml").write_text(
        (
            "[server]\n"
            'host = "0.0.0.0"\n'
            "port = 9011\n"
        ),
        encoding="utf-8",
    )

    manager, resolved_config, remaining_args = entry.bootstrap_runtime(
        ["--data-dir", str(data_dir), "status"]
    )

    assert remaining_args == ["status"]
    assert resolved_config.location.data_dir == data_dir
    assert resolved_config.location.path == data_dir / "admin.toml"
    assert resolved_config.server.host == "0.0.0.0"
    assert resolved_config.server.port == 9011
    assert resolved_config.runtime.data_dir == str(data_dir)
    assert resolved_config.auth.session_secret is not None
    assert manager.app.state.config.server.port == 9011
