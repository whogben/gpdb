import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from mcp.server.auth.middleware.auth_context import auth_context_var

from gpdb.admin import entry
from gpdb.admin.auth import generate_api_key, hash_api_key_secret, hash_password
from gpdb.admin.config import ConfigStore
from gpdb.admin.store import AdminStore


def test_api_key_lifecycle_for_web_rest_and_mcp(admin_test_env):
    """Test API key create, reveal, use, last-used update, and revoke flow."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    response = client.post("/api/status")
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"

    response = client.post(
        "/apikeys",
        data={"label": "Automation key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]
    assert api_key_detail_path.startswith("/apikeys/")

    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    assert "Automation key" in response.text
    api_key_value = _extract_revealed_api_key(response.text)
    assert api_key_value.startswith("gpdb_")

    response = client.get("/apikeys")
    assert response.status_code == 200
    assert "Automation key" in response.text

    stored_key = _read_stored_api_key(manager, label="Automation key")
    assert str(stored_key["key_value"]).startswith("fernet:")
    assert stored_key["key_value"] != api_key_value
    assert stored_key["secret_hash"] != api_key_value
    assert stored_key["last_used_at"] is None
    assert stored_key["revoked_at"] is None

    response = client.post(
        "/api/status",
        headers={"Authorization": "Bearer gpdb_invalid_deadbeef"},
    )
    assert response.status_code == 401

    response = client.post(
        "/api/status",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    assert response.json() == "OK"

    stored_key = _read_stored_api_key(manager, label="Automation key")
    assert stored_key["last_used_at"] is not None

    assert manager.mcp_servers["gpdb"].auth is not None
    verified_token = _verify_api_key_with_mcp_verifier(manager, api_key_value)
    assert verified_token is not None
    assert verified_token.claims["username"] == "owner"
    assert verified_token.claims["api_key_label"] == "Automation key"

    api_key_id = api_key_detail_path.split("?", 1)[0].rsplit("/", 1)[-1]
    response = client.post(
        f"/apikeys/{api_key_id}/revoke",
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/apikeys")

    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    assert api_key_value in response.text
    assert "revoked" in response.text

    response = client.post(
        "/api/status",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 401
    assert _verify_api_key_with_mcp_verifier(manager, api_key_value) is None


def test_cli_api_key_management_commands(admin_test_env):
    """Test trusted local CLI API key management commands."""
    manager = admin_test_env.manager
    _bootstrap_owner(admin_test_env.client)

    created = manager.cli(
        ["gpdb", "api_key_create", "owner", "CLI key"],
        standalone_mode=False,
    )
    assert created["label"] == "CLI key"
    assert str(created["api_key"]).startswith("gpdb_")
    key_id = str(created["key_id"])

    listed = manager.cli(
        ["gpdb", "api_key_list", "owner"],
        standalone_mode=False,
    )
    assert any(item["key_id"] == key_id for item in listed)

    revealed = manager.cli(
        ["gpdb", "api_key_reveal", "owner", key_id],
        standalone_mode=False,
    )
    assert revealed["api_key"] == created["api_key"]

    revoked = manager.cli(
        ["gpdb", "api_key_revoke", "owner", key_id],
        standalone_mode=False,
    )
    assert revoked["is_active"] is False


def test_mcp_api_key_management_tools(tmp_path):
    """Test authenticated MCP API key management tools."""
    manager = _create_test_manager(tmp_path)

    async def _run():
        services = manager.app.state.services
        admin_lifespan = entry.create_admin_lifespan(services)
        async with admin_lifespan(manager.app):
            assert services.admin_store is not None

            owner = await services.admin_store.create_initial_owner(
                username="owner",
                password_hash=hash_password("secret-pass"),
                display_name="Primary Owner",
            )
            bootstrap_generated = generate_api_key()
            bootstrap_key = await services.admin_store.create_api_key(
                user_id=owner.id,
                label="MCP bootstrap",
                key_id=bootstrap_generated.key_id,
                preview=bootstrap_generated.preview,
                secret_hash=hash_api_key_secret(bootstrap_generated.secret),
                key_value=bootstrap_generated.token,
            )

            verified_token = await entry._AdminAPIKeyTokenVerifier(
                SimpleNamespace(admin_store=services.admin_store)
            ).verify_token(bootstrap_generated.token)
            assert verified_token is not None

            listed = await _call_authenticated_mcp_tool_in_loop(
                manager,
                verified_token,
                "api_key_list",
                {},
            )
            assert any(item["key_id"] == bootstrap_key.key_id for item in listed)

            created = await _call_authenticated_mcp_tool_in_loop(
                manager,
                verified_token,
                "api_key_create",
                {"label": "MCP managed"},
            )
            assert created["label"] == "MCP managed"
            assert str(created["api_key"]).startswith("gpdb_")
            created_key_id = str(created["key_id"])

            revealed = await _call_authenticated_mcp_tool_in_loop(
                manager,
                verified_token,
                "api_key_reveal",
                {"key_id": created_key_id},
            )
            assert revealed["api_key"] == created["api_key"]

            revoked = await _call_authenticated_mcp_tool_in_loop(
                manager,
                verified_token,
                "api_key_revoke",
                {"key_id": created_key_id},
            )
            assert revoked["is_active"] is False

    asyncio.run(_run())


def _create_test_manager(tmp_path: Path):
    """Create a manager backed by a temporary config and captive data dir."""
    config_path = tmp_path / "admin.toml"
    data_dir = tmp_path / "admin data"
    config_path.write_text(
        (
            "[server]\n"
            'host = "127.0.0.1"\n'
            "port = 8747\n"
            "[runtime]\n"
            f'data_dir = "{data_dir.as_posix()}"\n'
            "[auth]\n"
            'session_secret = "test-session-secret"\n'
        ),
        encoding="utf-8",
    )
    config_store = ConfigStore.from_sources(cli_path=config_path)
    resolved_config = config_store.load()
    return entry.create_manager(
        resolved_config=resolved_config, config_store=config_store
    )


def _bootstrap_owner(client: TestClient) -> None:
    response = client.post(
        "/setup",
        data={
            "username": "owner",
            "display_name": "Primary Owner",
            "password": "secret-pass",
            "confirm_password": "secret-pass",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/login")


def _login(
    client: TestClient,
    *,
    username: str = "owner",
    password: str = "secret-pass",
) -> None:
    response = client.post(
        "/login",
        data={"username": username, "password": password},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/"
    assert "gpdb_admin_session" in response.cookies


def _extract_revealed_api_key(html: str) -> str:
    import re

    match = re.search(r'<input[^>]*readonly[^>]*value="([^"]+)"', html)
    assert match is not None
    return match.group(1)


def _read_stored_api_key(manager, *, label: str) -> dict[str, object]:
    services = manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None

    async def _load() -> dict[str, object]:
        store = AdminStore(
            services.captive_server.get_uri(),
            instance_secret=services.resolved_config.auth.session_secret,
        )
        try:
            owner = await store.get_user_by_username("owner")
            assert owner is not None
            api_keys = await store.list_api_keys_for_user(owner.id)
            api_key = next(item for item in api_keys if item.label == label)
            node = await store.db.get_node(api_key.id)
            assert node is not None
            return dict(node.data)
        finally:
            await store.close()

    return asyncio.run(_load())


def _verify_api_key_with_mcp_verifier(manager, api_key_value: str):
    services = manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None

    async def _verify():
        store = AdminStore(
            services.captive_server.get_uri(),
            instance_secret=services.resolved_config.auth.session_secret,
        )
        try:
            verifier = entry._AdminAPIKeyTokenVerifier(
                SimpleNamespace(admin_store=store)
            )
            return await verifier.verify_token(api_key_value)
        finally:
            await store.close()

    return asyncio.run(_verify())


async def _call_authenticated_mcp_tool_in_loop(
    manager,
    verified_token,
    tool_name: str,
    arguments: dict[str, object],
):
    token = auth_context_var.set(SimpleNamespace(access_token=verified_token))
    try:
        result = await manager.mcp_servers["gpdb"].call_tool(tool_name, arguments)
    finally:
        auth_context_var.reset(token)
    assert result.content
    return json.loads(result.content[0].text)
