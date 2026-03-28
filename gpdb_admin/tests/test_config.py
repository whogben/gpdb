from pathlib import Path

import pytest

from gpdb.admin import entry
from gpdb.admin.config import (
    AdminConfig,
    ConfigStore,
    DataDirSource,
    PostgresMode,
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
                "auth": {"session_secret": "test-secret", "instance_secret": "test-instance-secret"},
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
    assert resolved.auth.instance_secret == "test-instance-secret"


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
    assert resolved_config.auth.instance_secret is not None
    assert manager.app.state.config.server.port == 9011


def test_postgres_config_defaults(tmp_path):
    """Test that postgres config has correct defaults."""
    store = ConfigStore.from_sources(cli_data_dir=tmp_path)
    resolved = store.load()

    assert resolved.postgres.mode == PostgresMode.CAPTIVE
    assert resolved.postgres.url is None
    assert resolved.postgres.expose_postgres is False
    assert resolved.postgres.expose_port == 5433


def test_postgres_config_file_loading(tmp_path):
    """Test that postgres config can be loaded from file."""
    config_path = tmp_path / "admin.toml"
    config_path.write_text(
        (
            "[postgres]\n"
            'mode = "external"\n'
            'url = "postgresql://user:pass@host:5432/db"\n'
            "expose_postgres = true\n"
            "expose_port = 5434\n"
            "[auth]\n"
            'instance_secret = "test-instance-secret"\n'
        ),
        encoding="utf-8",
    )

    store = ConfigStore.from_sources(cli_data_dir=tmp_path)
    resolved = store.load()

    assert resolved.postgres.mode == PostgresMode.EXTERNAL
    assert resolved.postgres.url == "postgresql://user:pass@host:5432/db"
    assert resolved.postgres.expose_postgres is True
    assert resolved.postgres.expose_port == 5434


def test_postgres_config_env_overrides(tmp_path, monkeypatch):
    """Test that environment variables override postgres config."""
    config_path = tmp_path / "admin.toml"
    config_path.write_text(
        (
            "[postgres]\n"
            'mode = "captive"\n'
            'url = "postgresql://file:override@host:5432/db"\n'
            "expose_postgres = false\n"
            "expose_port = 5433\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("GPDB_POSTGRES_MODE", "external")
    monkeypatch.setenv("GPDB_POSTGRES_URL", "postgresql://env:override@host:5432/db")
    monkeypatch.setenv("GPDB_EXPOSE_POSTGRES", "true")
    monkeypatch.setenv("GPDB_EXPOSE_PORT", "5435")
    monkeypatch.setenv("GPDB_INSTANCE_SECRET", "test-instance-secret")

    store = ConfigStore.from_sources(cli_data_dir=tmp_path)
    resolved = store.load()

    assert resolved.postgres.mode == PostgresMode.EXTERNAL
    assert resolved.postgres.url == "postgresql://env:override@host:5432/db"
    assert resolved.postgres.expose_postgres is True
    assert resolved.postgres.expose_port == 5435


def test_postgres_config_external_mode_requires_url(tmp_path, monkeypatch):
    """Test that external mode raises ValueError when url is not set."""
    monkeypatch.setenv("GPDB_POSTGRES_MODE", "external")

    store = ConfigStore.from_sources(cli_data_dir=tmp_path)

    with pytest.raises(ValueError, match="postgres.url must be set when postgres.mode is 'external'"):
        store.load()


def test_instance_secret_env_override(tmp_path, monkeypatch):
    """Test that GPDB_INSTANCE_SECRET env var overrides the config value."""
    config_path = tmp_path / "admin.toml"
    config_path.write_text(
        '[auth]\ninstance_secret = "file-secret"\n',
        encoding="utf-8",
    )

    monkeypatch.setenv("GPDB_INSTANCE_SECRET", "env-secret")

    store = ConfigStore.from_sources(cli_data_dir=tmp_path)
    resolved = store.load()

    assert resolved.auth.instance_secret == "env-secret"


def test_instance_secret_persisted_to_file(tmp_path):
    """Test that instance_secret is persisted to admin.toml via ConfigStore.save()."""
    store = ConfigStore.from_sources(cli_data_dir=tmp_path)

    store.save(
        AdminConfig.model_validate(
            {
                "auth": {"session_secret": "s1", "instance_secret": "i1"},
            }
        )
    )
    resolved = store.load()

    assert resolved.auth.session_secret == "s1"
    assert resolved.auth.instance_secret == "i1"


def test_external_mode_requires_instance_secret(tmp_path, monkeypatch):
    """Test that external mode raises ValueError when instance_secret is not set."""
    monkeypatch.setenv("GPDB_POSTGRES_MODE", "external")
    monkeypatch.setenv("GPDB_POSTGRES_URL", "postgresql://user:pass@host:5432/db")

    store = ConfigStore.from_sources(cli_data_dir=tmp_path)

    with pytest.raises(ValueError, match="auth.instance_secret must be set when postgres.mode is 'external'"):
        store.load()


def test_external_mode_with_instance_secret_env(tmp_path, monkeypatch):
    """Test that external mode works when instance_secret is provided via env."""
    monkeypatch.setenv("GPDB_POSTGRES_MODE", "external")
    monkeypatch.setenv("GPDB_POSTGRES_URL", "postgresql://user:pass@host:5432/db")
    monkeypatch.setenv("GPDB_INSTANCE_SECRET", "shared-secret")

    store = ConfigStore.from_sources(cli_data_dir=tmp_path)
    resolved = store.load()

    assert resolved.auth.instance_secret == "shared-secret"
