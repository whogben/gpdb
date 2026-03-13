from pathlib import Path

import pytest

from gpdb.admin import entry
from gpdb.admin.config import (
    AdminConfig,
    ConfigPathSource,
    ConfigStore,
    extract_config_arg,
    resolve_config_location,
)


def test_extract_config_arg_strips_global_option():
    """Test that the global config option is removed before CLI dispatch."""
    config_path, remaining = extract_config_arg(
        ["start", "--config", "./admin.toml", "--port", "9000"]
    )

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
            'host = "0.0.0.0"\n'
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
