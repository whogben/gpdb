"""Tests for the graph viewer page and data endpoint."""

import asyncio
import re

import pytest
from fastapi.testclient import TestClient

from gpdb import EdgeUpsert, GPGraph, NodeUpsert
from gpdb.admin.store import AdminStore


def test_viewer_page_renders_and_linked_from_overview(admin_test_env):
    """Viewer page renders and is linked from graph overview."""
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
            "table_prefix": "viewer_test",
            "display_name": "Viewer Test",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="viewer_test")
    assert graph is not None
    graph_id = graph.id

    response = client.get(f"/graphs/{graph_id}")
    assert response.status_code == 200
    assert "Viewer" in response.text
    assert (
        f'href="/graphs/{graph_id}/viewer"' in response.text
        or f"/viewer" in response.text
    )

    response = client.get(f"/graphs/{graph_id}/viewer")
    assert response.status_code == 200
    assert "Graph Viewer" in response.text
    assert "cytoscape.min.js" in response.text
    assert "graph-viewer.js" in response.text
    assert "viewer-cy" in response.text
    assert "Node filters" in response.text
    assert "Edge filters" in response.text


def test_viewer_data_applies_filters(admin_test_env):
    """Viewer data endpoint returns filtered nodes and edges."""
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
            "table_prefix": "viewer_data",
            "display_name": "Viewer Data",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="viewer_data")
    assert graph is not None
    graph_id = graph.id

    a_id = _seed_node_record(
        manager,
        table_prefix="viewer_data",
        type="user",
        name="alice",
        data={},
    )
    b_id = _seed_node_record(
        manager,
        table_prefix="viewer_data",
        type="user",
        name="bob",
        data={},
    )
    _seed_node_record(
        manager,
        table_prefix="viewer_data",
        type="task",
        name="task1",
        data={},
    )
    _seed_edge_record(
        manager,
        table_prefix="viewer_data",
        type="follows",
        source_id=a_id,
        target_id=b_id,
        data={},
    )

    response = client.get(f"/graphs/{graph_id}/viewer/data")
    assert response.status_code == 200
    data = response.json()
    assert "elements" in data
    assert data["node_count"] == 3
    assert data["edge_count"] == 1

    response = client.get(
        f"/graphs/{graph_id}/viewer/data",
        params={"node_type": "user"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["node_count"] == 2
    assert data["edge_count"] == 1

    response = client.get(
        f"/graphs/{graph_id}/viewer/data",
        params={"edge_type": "follows"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["edge_count"] == 1


def test_viewer_data_invalid_dsl_returns_error(admin_test_env):
    """Viewer data endpoint returns user-visible error for invalid filter DSL."""
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
            "table_prefix": "viewer_dsl",
            "display_name": "Viewer DSL",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="viewer_dsl")
    assert graph is not None
    graph_id = graph.id

    response = client.get(
        f"/graphs/{graph_id}/viewer/data",
        params={"node_filter": "("},
    )
    assert response.status_code == 400
    data = response.json()
    assert "error" in data
    assert data["error"]
    assert "elements" in data
    assert data["elements"] == []


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


def _extract_instance_option_value(html: str, label: str) -> str:
    match = re.search(
        rf'<option[^>]*value="([^"]+)"[^>]*>\s*{re.escape(label)}\s*\([^)]*\)\s*</option>',
        html,
        re.S,
    )
    assert match is not None
    return match.group(1)


def _read_graph_by_prefix(manager, *, table_prefix: str):
    services = manager.app.state.services
    assert services.captive_server is not None
    assert services.resolved_config.auth.session_secret is not None

    async def _load():
        store = AdminStore(
            services.captive_server.get_uri(),
            instance_secret=services.resolved_config.auth.session_secret,
        )
        try:
            instance = await store.get_instance_by_slug("default")
            assert instance is not None
            return await store.get_graph_by_scope(instance.id, table_prefix)
        finally:
            await store.close()

    return asyncio.run(_load())


def _seed_node_record(
    manager,
    *,
    table_prefix: str,
    type: str,
    name: str,
    data: dict,
    schema_name: str | None = None,
    tags: list | None = None,
    parent_id: str | None = None,
) -> str:
    services = manager.app.state.services
    assert services.captive_server is not None

    async def _seed() -> str:
        db = GPGraph(services.captive_server.get_uri(), table_prefix=table_prefix)
        try:
            node = await db.set_node(
                NodeUpsert(
                    type=type,
                    name=name,
                    parent_id=parent_id,
                    schema_name=schema_name,
                    data=data,
                    tags=list(tags or []),
                )
            )
            return node.id
        finally:
            await db.sqla_engine.dispose()

    return asyncio.run(_seed())


def _seed_edge_record(
    manager,
    *,
    table_prefix: str,
    type: str,
    source_id: str,
    target_id: str,
    data: dict,
    schema_name: str | None = None,
    tags: list | None = None,
) -> str:
    services = manager.app.state.services
    assert services.captive_server is not None

    async def _seed() -> str:
        db = GPGraph(services.captive_server.get_uri(), table_prefix=table_prefix)
        try:
            edge = await db.set_edge(
                EdgeUpsert(
                    type=type,
                    source_id=source_id,
                    target_id=target_id,
                    schema_name=schema_name,
                    data=data,
                    tags=list(tags or []),
                )
            )
            return edge.id
        finally:
            await db.sqla_engine.dispose()

    return asyncio.run(_seed())
