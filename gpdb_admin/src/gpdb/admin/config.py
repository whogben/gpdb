"""Configuration loading and persistence for gpdb-admin."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Mapping, Sequence

from platformdirs import user_data_path
from pydantic import BaseModel, Field
import tomli_w

try:  # pragma: no cover - exercised on Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.9/3.10
    import tomli as tomllib


DATA_DIR_ENV_VAR = "GPDB_DATA_DIR"
PUBLIC_URL_ENV_VAR = "GPDB_PUBLIC_URL"
DEFAULT_CONFIG_FILENAME = "admin.toml"


class DataDirSource(str, Enum):
    """Where the selected data directory came from."""

    CLI = "cli"
    ENV = "env"
    DEFAULT = "default"


class ServerConfig(BaseModel):
    """Config values that shape how the admin server starts."""

    host: str = "127.0.0.1"
    port: int = 8747
    public_url: str | None = None


class RuntimeConfig(BaseModel):
    """Config values for local runtime-owned state."""

    data_dir: str = Field(default_factory=lambda: str(default_data_dir()))


class AuthConfig(BaseModel):
    """File-backed auth settings for the admin runtime."""

    session_secret: str | None = None


class AdminConfig(BaseModel):
    """File-backed gpdb-admin configuration."""

    server: ServerConfig = Field(default_factory=ServerConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)


@dataclass(frozen=True)
class ConfigLocation:
    """Metadata about the selected data directory; config file is always {data_dir}/admin.toml."""

    data_dir: Path
    source: DataDirSource
    exists: bool
    writable: bool

    @property
    def path(self) -> Path:
        """Path to the config file (always data_dir / admin.toml)."""
        return self.data_dir / DEFAULT_CONFIG_FILENAME


@dataclass(frozen=True)
class ResolvedConfig:
    """Effective configuration used by the runtime."""

    file_config: AdminConfig
    location: ConfigLocation
    server: ServerConfig
    runtime: RuntimeConfig
    auth: AuthConfig


def default_data_dir() -> Path:
    """Return the default local data directory for gpdb-admin."""
    return user_data_path("gpdb") / "admin"


def extract_data_dir_arg(argv: Sequence[str]) -> tuple[Path | None, list[str]]:
    """Remove a global --data-dir/-d option from argv and return it."""
    data_dir: Path | None = None
    remaining: list[str] = []
    index = 0

    while index < len(argv):
        arg = argv[index]
        if arg in {"--data-dir", "-d"}:
            if index + 1 >= len(argv):
                raise ValueError("--data-dir requires a path")
            data_dir = Path(argv[index + 1]).expanduser()
            index += 2
            continue
        if arg.startswith("--data-dir="):
            value = arg.split("=", 1)[1]
            if not value:
                raise ValueError("--data-dir requires a path")
            data_dir = Path(value).expanduser()
            index += 1
            continue
        remaining.append(arg)
        index += 1

    return data_dir, remaining


def resolve_data_dir_location(
    cli_data_dir: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> ConfigLocation:
    """Resolve the data directory from CLI, env, or the default location."""
    env = environ if environ is not None else os.environ

    if cli_data_dir is not None:
        data_dir = cli_data_dir.expanduser()
        source = DataDirSource.CLI
    elif env.get(DATA_DIR_ENV_VAR):
        data_dir = Path(env[DATA_DIR_ENV_VAR]).expanduser()
        source = DataDirSource.ENV
    else:
        data_dir = default_data_dir()
        source = DataDirSource.DEFAULT

    data_dir = _normalize_path(data_dir)
    config_path = data_dir / DEFAULT_CONFIG_FILENAME
    return ConfigLocation(
        data_dir=data_dir,
        source=source,
        exists=config_path.exists(),
        writable=_path_is_writable(config_path),
    )


class ConfigStore:
    """Load and save the file-backed admin configuration (config file is always {data_dir}/admin.toml)."""

    def __init__(self, location: ConfigLocation):
        self.location = location

    @classmethod
    def from_sources(
        cls,
        cli_data_dir: Path | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> "ConfigStore":
        """Create a store from CLI/env/default data-dir resolution."""
        return cls(resolve_data_dir_location(cli_data_dir=cli_data_dir, environ=environ))

    def load(self) -> ResolvedConfig:
        """Load config from disk and merge it with defaults and environment variables."""
        file_config = AdminConfig()
        if self.location.path.exists():
            with self.location.path.open("rb") as handle:
                data = tomllib.load(handle)
            file_config = AdminConfig.model_validate(data)

        # Override with environment variables
        if os.environ.get(PUBLIC_URL_ENV_VAR):
            file_config.server.public_url = os.environ.get(PUBLIC_URL_ENV_VAR)

        runtime = file_config.runtime.model_copy(deep=True)
        # Effective data dir is always the resolved location (never from file).
        runtime.data_dir = str(self.location.data_dir)

        self.location = self._refresh_location()
        return ResolvedConfig(
            file_config=file_config,
            location=self.location,
            server=file_config.server.model_copy(deep=True),
            runtime=runtime,
            auth=file_config.auth.model_copy(deep=True),
        )

    def save(self, config: AdminConfig) -> Path:
        """Persist file-backed config atomically."""
        path = self.location.path
        path.parent.mkdir(parents=True, exist_ok=True)

        data = config.model_dump(mode="python")
        data = _drop_none_values(data)

        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        temp_path.write_text(tomli_w.dumps(data), encoding="utf-8")
        temp_path.replace(path)

        self.location = self._refresh_location()
        return path

    def _refresh_location(self) -> ConfigLocation:
        return ConfigLocation(
            data_dir=self.location.data_dir,
            source=self.location.source,
            exists=self.location.path.exists(),
            writable=_path_is_writable(self.location.path),
        )


def _drop_none_values(obj: dict) -> dict:
    """Return a copy of the dict with keys whose value is None removed (recursive)."""
    out: dict = {}
    for k, v in obj.items():
        if v is None:
            continue
        if isinstance(v, dict):
            v = _drop_none_values(v)
        out[k] = v
    return out


def _path_is_writable(path: Path) -> bool:
    """Return whether the target file can be created or updated."""
    if path.exists():
        return os.access(path, os.W_OK)

    parent = path.parent
    while not parent.exists() and parent != parent.parent:
        parent = parent.parent
    return os.access(parent, os.W_OK)


def _normalize_path(path: Path) -> Path:
    """Expand the path without resolving symlinks like /tmp -> /private/tmp."""
    path = path.expanduser()
    if path.is_absolute():
        return path
    return Path.cwd() / path
