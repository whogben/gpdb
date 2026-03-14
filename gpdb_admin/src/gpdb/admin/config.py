"""Configuration loading and persistence for gpdb-admin."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Mapping, Sequence

from platformdirs import user_config_path, user_data_path
from pydantic import BaseModel, Field
import tomli_w

try:  # pragma: no cover - exercised on Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.9/3.10
    import tomli as tomllib


CONFIG_ENV_VAR = "GPDB_CONFIG"
PUBLIC_URL_ENV_VAR = "GPDB_PUBLIC_URL"
DEFAULT_CONFIG_FILENAME = "admin.toml"


class ConfigPathSource(str, Enum):
    """Where the selected config path came from."""

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
    """Metadata about the selected config file location."""

    path: Path
    source: ConfigPathSource
    exists: bool
    writable: bool


@dataclass(frozen=True)
class ResolvedConfig:
    """Effective configuration used by the runtime."""

    file_config: AdminConfig
    location: ConfigLocation
    server: ServerConfig
    runtime: RuntimeConfig
    auth: AuthConfig


def default_config_path() -> Path:
    """Return the default config path for the current user."""
    return user_config_path("gpdb") / DEFAULT_CONFIG_FILENAME


def default_data_dir() -> Path:
    """Return the default local data directory for gpdb-admin."""
    return user_data_path("gpdb") / "admin"


def extract_config_arg(argv: Sequence[str]) -> tuple[Path | None, list[str]]:
    """Remove a global --config/-c option from argv and return it."""
    config_path: Path | None = None
    remaining: list[str] = []
    index = 0

    while index < len(argv):
        arg = argv[index]
        if arg in {"--config", "-c"}:
            if index + 1 >= len(argv):
                raise ValueError("--config requires a path")
            config_path = Path(argv[index + 1]).expanduser()
            index += 2
            continue
        if arg.startswith("--config="):
            value = arg.split("=", 1)[1]
            if not value:
                raise ValueError("--config requires a path")
            config_path = Path(value).expanduser()
            index += 1
            continue
        remaining.append(arg)
        index += 1

    return config_path, remaining


def resolve_config_location(
    cli_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> ConfigLocation:
    """Resolve the config path from CLI, env, or the default location."""
    env = environ if environ is not None else os.environ

    if cli_path is not None:
        path = cli_path.expanduser()
        source = ConfigPathSource.CLI
    elif env.get(CONFIG_ENV_VAR):
        path = Path(env[CONFIG_ENV_VAR]).expanduser()
        source = ConfigPathSource.ENV
    else:
        path = default_config_path()
        source = ConfigPathSource.DEFAULT

    path = _normalize_path(path)
    return ConfigLocation(
        path=path,
        source=source,
        exists=path.exists(),
        writable=_path_is_writable(path),
    )


class ConfigStore:
    """Load and save the file-backed admin configuration."""

    def __init__(self, location: ConfigLocation):
        self.location = location

    @classmethod
    def from_sources(
        cls,
        cli_path: Path | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> "ConfigStore":
        """Create a store from CLI/env/default resolution."""
        return cls(resolve_config_location(cli_path=cli_path, environ=environ))

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

        self.location = self._refresh_location()
        return ResolvedConfig(
            file_config=file_config,
            location=self.location,
            server=file_config.server.model_copy(deep=True),
            runtime=file_config.runtime.model_copy(deep=True),
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
            path=self.location.path,
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
