"""Local settings page routes."""

from __future__ import annotations

import os

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

from gpdb.admin.config import (
    AdminConfig,
    AuthConfig,
    PostgresConfig,
    PostgresMode,
    ServerConfig,
)
from gpdb.admin.web.routes.common import (
    ADMIN_VERSION,
    redirect_with_message,
    render,
    require_owner_user,
)

router = APIRouter()

# ENV vars that override file-backed settings
_ENV_OVERRIDES: dict[str, str] = {
    "server.public_url": "GPDB_PUBLIC_URL",
    "postgres.mode": "GPDB_POSTGRES_MODE",
    "postgres.url": "GPDB_POSTGRES_URL",
    "postgres.expose_postgres": "GPDB_EXPOSE_POSTGRES",
    "postgres.expose_port": "GPDB_EXPOSE_PORT",
    "auth.instance_secret": "GPDB_INSTANCE_SECRET",
}


def _env_overrides() -> dict[str, bool]:
    """Return which config keys are overridden by environment variables."""
    return {key: bool(os.environ.get(env_var)) for key, env_var in _ENV_OVERRIDES.items()}


@router.get("/settings", response_class=HTMLResponse, name="settings_page")
async def settings_page(request: Request) -> HTMLResponse:
    """Render the local settings page with current config values."""
    current_user = await require_owner_user(request)
    if isinstance(current_user, HTMLResponse):
        return current_user

    config: AdminConfig = request.app.state.config.file_config
    services = request.app.state.services

    return render(
        request,
        "pages/settings.html",
        page_title="Settings",
        current_user=current_user,
        config=config,
        env_overrides=_env_overrides(),
        is_host=services.is_host,
        server_version=ADMIN_VERSION,
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


@router.post("/settings", name="settings_save")
async def settings_save(
    request: Request,
    server_host: str = Form(...),
    server_port: str = Form(...),
    server_public_url: str = Form(""),
    postgres_mode: str = Form(...),
    postgres_url: str = Form(""),
    postgres_expose_postgres: str | None = Form(None),
    postgres_expose_port: str = Form(""),
    auth_instance_secret: str = Form(""),
) -> HTMLResponse:
    """Save updated local config values to admin.toml."""
    current_user = await require_owner_user(request)
    if isinstance(current_user, HTMLResponse):
        return current_user

    config_store = request.app.state.config_store
    file_config: AdminConfig = request.app.state.config.file_config

    # Build updated config from form values
    try:
        port = int(server_port.strip()) if server_port.strip() else file_config.server.port
    except ValueError:
        return redirect_with_message(request, "settings_page", error="Port must be a number.")

    new_server = ServerConfig(
        host=server_host.strip(),
        port=port,
        public_url=server_public_url.strip() or None,
    )

    try:
        expose_port = int(postgres_expose_port.strip()) if postgres_expose_port.strip() else file_config.postgres.expose_port
    except ValueError:
        return redirect_with_message(request, "settings_page", error="Expose port must be a number.")

    new_postgres = PostgresConfig(
        mode=PostgresMode(postgres_mode),
        url=postgres_url.strip() or None,
        expose_postgres=bool(postgres_expose_postgres),
        expose_port=expose_port,
    )

    # Validation: external mode requires url
    if new_postgres.mode == PostgresMode.EXTERNAL and not new_postgres.url:
        return redirect_with_message(
            request, "settings_page", error="postgres.url must be set when mode is 'external'."
        )

    # Preserve existing secrets if form value is empty (masked placeholder)
    new_auth = AuthConfig(
        session_secret=file_config.auth.session_secret,
        instance_secret=auth_instance_secret.strip() or file_config.auth.instance_secret,
    )

    updated = AdminConfig(
        server=new_server,
        runtime=file_config.runtime,
        auth=new_auth,
        viz=file_config.viz,
        postgres=new_postgres,
    )

    try:
        config_store.save(updated)
    except Exception as exc:
        return redirect_with_message(request, "settings_page", error=f"Failed to save: {exc}")

    return redirect_with_message(request, "settings_page", success="Settings saved. Restart to apply changes.")
