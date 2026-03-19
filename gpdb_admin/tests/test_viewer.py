"""Tests for the graph viewer page and data endpoint."""

import asyncio
import re
from urllib.parse import unquote

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

    # Create schemas for the node and edge types
    _create_schema(manager, table_prefix="viewer_data", name="user", kind="node")
    _create_schema(manager, table_prefix="viewer_data", name="task", kind="node")
    _create_schema(manager, table_prefix="viewer_data", name="follows", kind="edge")

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


def test_viewer_data_includes_svg_icon_data_uri(admin_test_env):
    """Viewer JSON includes server-built percent-encoded SVG data URIs for Cytoscape."""
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
            "table_prefix": "viewer_svg_uri",
            "display_name": "Viewer SVG URI",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="viewer_svg_uri")
    assert graph is not None
    graph_id = graph.id

    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10">'
        '<circle cx="5" cy="5" r="4" fill="blue"/></svg>'
    )
    _create_schema(
        manager,
        table_prefix="viewer_svg_uri",
        name="iconnode",
        kind="node",
        svg_icon=svg,
    )
    _seed_node_record(
        manager,
        table_prefix="viewer_svg_uri",
        type="iconnode",
        name="n1",
        data={},
    )

    response = client.get(f"/graphs/{graph_id}/viewer/data")
    assert response.status_code == 200
    data = response.json()
    assert "schemas" in data
    assert "node:iconnode" in data["schemas"]
    entry = data["schemas"]["node:iconnode"]
    assert entry.get("svg_icon")
    uri = entry.get("svg_icon_data_uri")
    assert uri and uri.startswith("data:image/svg+xml;charset=utf-8,")
    decoded = unquote(uri.split(",", 1)[1])
    assert "<circle" in decoded

    node_elts = [e for e in data["elements"] if e.get("group") == "nodes"]
    assert node_elts
    icon_nodes = [e for e in node_elts if e.get("data", {}).get("type") == "iconnode"]
    assert icon_nodes
    assert icon_nodes[0]["data"].get("iconUri") == uri


def test_viewer_data_includes_edge_schema_svg_icon_data_uri(admin_test_env):
    """Edge schema SVGs appear as iconUri on edge elements (viewer uses midpoint nodes)."""
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
            "table_prefix": "viewer_edge_svg",
            "display_name": "Viewer Edge SVG",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    graph = _read_graph_by_prefix(manager, table_prefix="viewer_edge_svg")
    assert graph is not None
    graph_id = graph.id

    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10">'
        '<rect x="1" y="1" width="8" height="8" fill="green"/></svg>'
    )
    _create_schema(
        manager,
        table_prefix="viewer_edge_svg",
        name="n",
        kind="node",
    )
    _create_schema(
        manager,
        table_prefix="viewer_edge_svg",
        name="iconedge",
        kind="edge",
        svg_icon=svg,
    )
    a_id = _seed_node_record(
        manager,
        table_prefix="viewer_edge_svg",
        type="n",
        name="a",
        data={},
    )
    b_id = _seed_node_record(
        manager,
        table_prefix="viewer_edge_svg",
        type="n",
        name="b",
        data={},
    )
    _seed_edge_record(
        manager,
        table_prefix="viewer_edge_svg",
        type="iconedge",
        source_id=a_id,
        target_id=b_id,
        data={},
    )

    response = client.get(f"/graphs/{graph_id}/viewer/data")
    assert response.status_code == 200
    data = response.json()
    assert "edge:iconedge" in data["schemas"]
    entry = data["schemas"]["edge:iconedge"]
    uri = entry.get("svg_icon_data_uri")
    assert uri and uri.startswith("data:image/svg+xml;charset=utf-8,")
    decoded = unquote(uri.split(",", 1)[1])
    assert "<rect" in decoded

    edge_elts = [e for e in data["elements"] if e.get("group") == "edges"]
    assert len(edge_elts) == 1
    assert edge_elts[0]["data"].get("iconUri") == uri


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
    tags: list | None = None,
    parent_id: str | None = None,
) -> str:
    services = manager.app.state.services
    assert services.captive_server is not None

    async def _seed() -> str:
        db = GPGraph(services.captive_server.get_uri(), table_prefix=table_prefix)
        try:
            node_list = await db.set_nodes(
                [
                    NodeUpsert(
                        type=type,
                        name=name,
                        parent_id=parent_id,
                        data=data,
                        tags=list(tags or []),
                    )
                ]
            )
            node = node_list[0]
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
    tags: list | None = None,
) -> str:
    services = manager.app.state.services
    assert services.captive_server is not None

    async def _seed() -> str:
        db = GPGraph(services.captive_server.get_uri(), table_prefix=table_prefix)
        try:
            edge = (await db.set_edges(
                [
                    EdgeUpsert(
                        type=type,
                        source_id=source_id,
                        target_id=target_id,
                        data=data,
                        tags=list(tags or []),
                    )
                ]
            ))[0]
            return edge.id
        finally:
            await db.sqla_engine.dispose()

    return asyncio.run(_seed())


def _create_schema(
    manager,
    *,
    table_prefix: str,
    name: str,
    kind: str,
    svg_icon: str | None = None,
) -> None:
    services = manager.app.state.services
    assert services.captive_server is not None

    async def _create() -> None:
        db = GPGraph(services.captive_server.get_uri(), table_prefix=table_prefix)
        try:
            from gpdb import SchemaUpsert

            await db.set_schemas(
                [
                    SchemaUpsert(
                        name=name,
                        json_schema={"type": "object"},
                        kind=kind,
                        svg_icon=svg_icon,
                    )
                ]
            )
        finally:
            await db.sqla_engine.dispose()

    return asyncio.run(_create())
