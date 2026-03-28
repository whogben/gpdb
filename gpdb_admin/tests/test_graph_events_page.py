"""Tests for graph events page rendering and functionality."""

import re
from datetime import datetime, timezone

from gpdb import EdgeUpsert, GPGraph, NodeUpsert, SchemaUpsert
from gpdb.admin.store import AdminStore


def test_events_page_renders_with_authentication(admin_test_env):
    """Test that the events page loads successfully with authentication."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_render")
    since_time = _seed_event_data(manager, table_prefix="events_render")

    response = client.get(
        f"/graphs/{graph_id}/events",
        params={"since_time": since_time.isoformat()},
    )
    assert response.status_code == 200
    html = response.text

    # Verify page structure
    assert "Events" in html
    assert "Events Graph" in html
    assert "Event list" in html

    # Verify filter section
    assert "Since time (ISO 8601)" in html
    assert "Event kinds" in html
    assert "Node created" in html
    assert "Node updated" in html
    assert "Node deleted" in html

    # Verify results section
    assert "Results" in html
    assert "Event list" in html


def test_events_page_redirects_unauthenticated_users(admin_test_env):
    """Test that unauthenticated users are redirected to login."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_auth")

    # Clear cookies to simulate unauthenticated state
    client.cookies.clear()

    response = client.get(f"/graphs/{graph_id}/events", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"].startswith("/login")


def test_events_page_displays_events_with_links(admin_test_env):
    """Test that events are displayed with correct links to nodes/edges."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_links")
    since_time = _seed_event_data(manager, table_prefix="events_links")

    response = client.get(
        f"/graphs/{graph_id}/events",
        params={"since_time": since_time.isoformat()},
    )
    assert response.status_code == 200
    html = response.text

    # Verify event kinds are displayed
    assert "node_created" in html
    assert "node_origin_edge_created" in html
    assert "node_destination_edge_created" in html

    # Verify node links are present
    assert f"/graphs/{graph_id}/nodes/" in html

    # Verify edge links are present
    assert f"/graphs/{graph_id}/edges/" in html


def test_events_page_empty_state(admin_test_env):
    """Test the 'No events matched' state when no events exist."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_empty")

    # Use a future time to ensure no events match
    future_time = datetime.now(timezone.utc).replace(microsecond=0)

    response = client.get(
        f"/graphs/{graph_id}/events",
        params={"since_time": future_time.isoformat()},
    )
    assert response.status_code == 200
    html = response.text

    # Verify empty state message
    assert "No events matched" in html
    assert "Adjust the current filters or since_time to see events." in html


def test_events_page_filter_parsing(admin_test_env):
    """Test that filters are parsed correctly from query params."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_filters")
    since_time = _seed_event_data(manager, table_prefix="events_filters")

    # Test with node_created filter
    response = client.get(
        f"/graphs/{graph_id}/events",
        params={
            "since_time": since_time.isoformat(),
            "node_created": "true",
            "node_updated": "false",
            "node_deleted": "false",
        },
    )
    assert response.status_code == 200
    html = response.text

    # Verify checkbox is checked
    assert 'name="node_created"' in html
    assert 'checked' in html

    # Test with node_types filter
    response = client.get(
        f"/graphs/{graph_id}/events",
        params={
            "since_time": since_time.isoformat(),
            "node_types": "task_base",
        },
    )
    assert response.status_code == 200
    html = response.text

    # Verify node_types input has value
    assert 'name="node_types"' in html
    assert 'value="task_base"' in html


def test_events_page_pagination_previous(admin_test_env):
    """Test that previous page link works correctly."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_prev")
    since_time = _seed_event_data(manager, table_prefix="events_prev")

    # Request second page with offset
    response = client.get(
        f"/graphs/{graph_id}/events",
        params={
            "since_time": since_time.isoformat(),
            "limit": 1,
            "offset": 1,
        },
    )
    assert response.status_code == 200
    html = response.text

    # Verify previous page link exists
    assert "Previous page" in html
    assert 'href=' in html

    # Extract previous URL and verify it has correct offset
    match = re.search(r'href="([^"]*offset=0[^"]*)"', html)
    assert match is not None
    previous_url = match.group(1)
    assert "offset=0" in previous_url


def test_events_page_pagination_next(admin_test_env):
    """Test that next page link works correctly."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_next")
    since_time = _seed_event_data(manager, table_prefix="events_next")

    # Request first page with small limit
    response = client.get(
        f"/graphs/{graph_id}/events",
        params={
            "since_time": since_time.isoformat(),
            "limit": 1,
            "offset": 0,
        },
    )
    assert response.status_code == 200
    html = response.text

    # Verify next page link exists
    assert "Next page" in html
    assert 'href=' in html

    # Extract next URL and verify it has correct offset
    match = re.search(r'href="([^"]*offset=1[^"]*)"', html)
    assert match is not None
    next_url = match.group(1)
    assert "offset=1" in next_url


def test_events_page_no_pagination_on_last_page(admin_test_env):
    """Test that pagination links are hidden on last page."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_last")
    since_time = _seed_event_data(manager, table_prefix="events_last")

    # Request with large limit to get all events on one page
    response = client.get(
        f"/graphs/{graph_id}/events",
        params={
            "since_time": since_time.isoformat(),
            "limit": 100,
            "offset": 0,
        },
    )
    assert response.status_code == 200
    html = response.text

    # Verify no pagination links when all events fit on one page
    assert "Previous page" not in html
    assert "Next page" not in html


def test_events_page_clear_filters(admin_test_env):
    """Test that clear filters link works correctly."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_clear")
    since_time = _seed_event_data(manager, table_prefix="events_clear")

    # Request with filters
    response = client.get(
        f"/graphs/{graph_id}/events",
        params={
            "since_time": since_time.isoformat(),
            "node_created": "true",
            "node_types": "task_base",
        },
    )
    assert response.status_code == 200
    html = response.text

    # Verify clear filters link exists
    assert "Clear" in html
    assert 'href=' in html

    # Extract clear URL from the Clear button link
    # Look for the link with text "Clear" and extract its href
    match = re.search(r'<a[^>]*href="([^"]*)"[^>]*>Clear</a>', html)
    assert match is not None
    clear_url = match.group(1)
    assert "node_created" not in clear_url
    assert "node_types" not in clear_url


def test_events_page_displays_event_count(admin_test_env):
    """Test that event count is displayed correctly."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_count")
    since_time = _seed_event_data(manager, table_prefix="events_count")

    response = client.get(
        f"/graphs/{graph_id}/events",
        params={"since_time": since_time.isoformat()},
    )
    assert response.status_code == 200
    html = response.text

    # Verify event count is displayed
    assert "event" in html or "events" in html
    assert "matched" in html


def test_events_page_displays_page_range(admin_test_env):
    """Test that page range is displayed correctly."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_range")
    since_time = _seed_event_data(manager, table_prefix="events_range")

    response = client.get(
        f"/graphs/{graph_id}/events",
        params={
            "since_time": since_time.isoformat(),
            "limit": 10,
            "offset": 0,
        },
    )
    assert response.status_code == 200
    html = response.text

    # Verify page range is displayed
    assert "Showing" in html


def test_events_page_invalid_since_time_defaults_to_now(admin_test_env):
    """Test that invalid since_time defaults to current time."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_invalid_time")

    # Request with invalid since_time
    response = client.get(
        f"/graphs/{graph_id}/events",
        params={"since_time": "invalid-time"},
    )
    assert response.status_code == 200
    html = response.text

    # Page should still render (with default time)
    assert "Events" in html
    assert "Events Graph" in html


def test_events_page_preserves_filters_in_pagination(admin_test_env):
    """Test that filters are preserved in pagination links."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)
    graph_id = _create_graph(client, manager, table_prefix="events_preserve")
    since_time = _seed_event_data(manager, table_prefix="events_preserve")

    # Request with filters and pagination
    response = client.get(
        f"/graphs/{graph_id}/events",
        params={
            "since_time": since_time.isoformat(),
            "node_created": "true",
            "node_types": "task_base",
            "limit": 1,
            "offset": 0,
        },
    )
    assert response.status_code == 200
    html = response.text

    # Verify filters are preserved in next page link
    assert "node_created=true" in html
    assert "node_types=task_base" in html


def _bootstrap_owner(client) -> None:
    """Bootstrap the owner user."""
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


def _login(client) -> None:
    """Login as the owner user."""
    response = client.post(
        "/login",
        data={"username": "owner", "password": "secret-pass"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/"


def _extract_instance_option_value(html: str, label: str) -> str:
    """Extract instance option value from HTML."""
    match = re.search(
        rf'<option[^>]*value="([^"]+)"[^>]*>\s*{re.escape(label)}\s*\([^)]*\)\s*</option>',
        html,
        re.S,
    )
    assert match is not None
    return match.group(1)


def _create_graph(client, manager, *, table_prefix: str) -> str:
    """Create a test graph and return its ID."""
    response = client.get("/graphs/new")
    assert response.status_code == 200
    instance_id = _extract_instance_option_value(response.text, "Default instance")
    response = client.post(
        "/graphs",
        data={
            "instance_id": instance_id,
            "table_prefix": table_prefix,
            "display_name": "Events Graph",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    graph = _read_graph_by_prefix(manager, table_prefix=table_prefix)
    assert graph is not None
    return graph.id


def _read_graph_by_prefix(manager, *, table_prefix: str):
    """Read graph by table prefix."""
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

    import asyncio
    return asyncio.run(_load())


def _seed_event_data(manager, *, table_prefix: str) -> datetime:
    """Seed test event data and return the since_time."""
    services = manager.app.state.services
    assert services.captive_server is not None

    async def _seed() -> datetime:
        db = GPGraph(services.captive_server.get_uri(), table_prefix=table_prefix)
        try:
            await db.set_schemas(
                [
                    SchemaUpsert(
                        name="task_base",
                        kind="node",
                        json_schema={
                            "type": "object",
                            "properties": {"name": {"type": "string"}},
                            "required": ["name"],
                        },
                    ),
                    SchemaUpsert(
                        name="task_child",
                        kind="node",
                        extends=["task_base"],
                        json_schema={
                            "type": "object",
                            "properties": {"child_label": {"type": "string"}},
                            "required": ["child_label"],
                        },
                    ),
                    SchemaUpsert(
                        name="rel_base",
                        kind="edge",
                        json_schema={"type": "object"},
                    ),
                    SchemaUpsert(
                        name="rel_child",
                        kind="edge",
                        extends=["rel_base"],
                        json_schema={
                            "type": "object",
                            "properties": {"weight": {"type": "number"}},
                        },
                    ),
                ]
            )
            since_time = datetime.now(timezone.utc)
            source = (
                await db.set_nodes(
                    [
                        NodeUpsert(type="task_base", name="source", data={"name": "Source"}),
                    ]
                )
            )[0]
            target = (
                await db.set_nodes(
                    [
                        NodeUpsert(
                            type="task_child",
                            name="target",
                            data={"name": "Target", "child_label": "Target"},
                        ),
                    ]
                )
            )[0]
            await db.set_edges(
                [
                    EdgeUpsert(
                        type="rel_child",
                        source_id=source.id,
                        target_id=target.id,
                        data={},
                    )
                ]
            )
            return since_time
        finally:
            await db.sqla_engine.dispose()

    import asyncio
    return asyncio.run(_seed())
