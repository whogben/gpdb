import asyncio
import json
import re
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import make_url

from gpdb.admin import entry
from gpdb.admin.auth import hash_password
from gpdb.admin.config import ConfigStore
from gpdb.admin.store import AdminStore


def test_instance_and_graph_crud_flow(admin_test_env):
    """Test owner-managed instance and graph CRUD from the web UI."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    response = client.get("/graphs/new")
    assert response.status_code == 200
    default_instance_id = _extract_instance_option_value(
        response.text, "Default instance"
    )

    response = client.post(
        "/graphs",
        data={
            "instance_id": default_instance_id,
            "table_prefix": "scratch",
            "display_name": "Scratch graph",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    response = client.get("/")
    assert "Scratch graph" in response.text
    assert "scratch" in response.text

    response = client.post(
        "/instances",
        data=_captive_instance_form(
            manager,
            slug="mirror",
            display_name="Mirror instance",
            description="Shared test connection",
        ),
        follow_redirects=False,
    )
    assert response.status_code == 303

    raw_password = _captive_instance_form(
        manager,
        slug="mirror",
        display_name="Mirror instance",
        description="Shared test connection",
    )["password"]
    stored_password = _read_stored_instance_password(
        manager,
        slug="mirror",
    )
    assert stored_password != raw_password
    assert str(stored_password).startswith("fernet:")

    response = client.get("/")
    assert "Mirror instance" in response.text

    instance_edit_page = _extract_instance_action(
        response.text, "Mirror instance", "edit"
    )
    response = client.get(instance_edit_page)
    assert response.status_code == 200
    assert 'name="password" type="password"' in response.text
    assert (
        'value="'
        not in response.text.split('name="password" type="password"', 1)[1].split(
            ">", 1
        )[0]
    )

    response = client.get("/graphs/new")
    mirror_instance_id = _extract_instance_option_value(
        response.text, "Mirror instance"
    )

    response = client.post(
        "/graphs",
        data={
            "instance_id": mirror_instance_id,
            "table_prefix": "mirror_scratch",
            "display_name": "Mirror scratch",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    response = client.get("/")
    assert "Mirror scratch" in response.text

    graph_edit_path = _extract_graph_action(response.text, "Mirror scratch", "edit")
    response = client.post(
        graph_edit_path,
        data={"display_name": "Mirror scratch renamed"},
        follow_redirects=False,
    )
    assert response.status_code == 303

    response = client.get("/")
    assert "Mirror scratch renamed" in response.text

    instance_edit_path = _extract_instance_action(
        response.text, "Mirror instance", "edit"
    )
    response = client.post(
        instance_edit_path,
        data={
            **_captive_instance_form(
                manager,
                slug="mirror",
                display_name="Mirror instance renamed",
                description="Shared test connection updated",
            ),
            "display_name": "Mirror instance renamed",
            "description": "Shared test connection updated",
            "is_active": "true",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    response = client.get("/")
    assert "Mirror instance renamed" in response.text

    graph_delete_path = _extract_graph_action(
        response.text,
        "Mirror scratch renamed",
        "delete",
    )
    response = client.post(graph_delete_path, follow_redirects=False)
    assert response.status_code == 303

    response = client.get("/")
    assert "Mirror scratch renamed" not in response.text

    instance_delete_path = _extract_instance_action(
        response.text,
        "Mirror instance renamed",
        "delete",
    )
    response = client.post(instance_delete_path, follow_redirects=False)
    assert response.status_code == 303

    response = client.get("/")
    assert "Mirror instance renamed" not in response.text


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


def _captive_instance_form(
    manager,
    *,
    slug: str,
    display_name: str,
    description: str,
    password: str = "top-secret",
) -> dict[str, str]:
    uri = manager.app.state.services.captive_server.get_uri()
    parsed = make_url(uri)
    host = str(parsed.query.get("host") or parsed.host or "127.0.0.1")
    port = str(parsed.query.get("port") or parsed.port or "")
    return {
        "slug": slug,
        "display_name": display_name,
        "description": description,
        "host": host,
        "port": port,
        "database": str(parsed.database or "postgres"),
        "username": str(parsed.username or "postgres"),
        "password": password,
    }


def _extract_instance_option_value(html: str, label: str) -> str:
    match = re.search(
        rf'<option[^>]*value="([^"]+)"[^>]*>\s*{re.escape(label)}\s*\([^)]*\)\s*</option>',
        html,
        re.S,
    )
    assert match is not None
    return match.group(1)


def _extract_graph_action(html: str, graph_name: str, action: str) -> str:
    if action == "edit":
        pattern = (
            rf"<h3>{re.escape(graph_name)}</h3>.*?href=\"([^\"]*?/graphs/[^\"]+/edit)\""
        )
    else:
        pattern = rf"<h3>{re.escape(graph_name)}</h3>.*?action=\"([^\"]*?/graphs/[^\"]+/delete)\""
    match = re.search(pattern, html, re.S)
    assert match is not None
    return match.group(1)


def _extract_instance_action(html: str, instance_name: str, action: str) -> str:
    if action == "edit":
        pattern = (
            rf"<h3>{re.escape(instance_name)}</h3>.*?resource-meta.*?"
            rf"href=\"([^\"]*?/instances/[^\"]+/edit)\""
        )
    else:
        pattern = (
            rf"<h3>{re.escape(instance_name)}</h3>.*?resource-meta.*?"
            rf"action=\"([^\"]*?/instances/[^\"]+/delete)\""
        )
    match = re.search(pattern, html, re.S)
    assert match is not None
    return match.group(1)


def _read_stored_instance_password(manager, *, slug: str) -> object:
    services = manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None

    async def _load() -> object:
        store = AdminStore(
            services.captive_server.get_uri(),
            instance_secret=services.resolved_config.auth.session_secret,
        )
        try:
            instance = await store.get_instance_by_slug(slug)
            assert instance is not None
            node = await store.db.get_node(instance.id)
            assert node is not None
            return node.data["password"]
        finally:
            await store.close()

    return asyncio.run(_load())
