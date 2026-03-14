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


def test_instance_list_api(admin_test_env):
    """Test the instance_list REST API endpoint."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    # Create an API key for authentication
    response = client.post(
        "/apikeys",
        data={"label": "Test key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]

    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)
    assert api_key_value.startswith("gpdb_")

    # Test the instance_list endpoint with API key authentication
    response = client.get(
        "/api/instance_list",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200

    data = response.json()
    assert "items" in data
    assert "total" in data
    assert isinstance(data["items"], list)
    assert isinstance(data["total"], int)
    assert data["total"] == len(data["items"])

    # Verify the default instance is present
    assert len(data["items"]) >= 1
    default_instance = next(
        (item for item in data["items"] if item["is_default"]), None
    )
    assert default_instance is not None
    assert "id" in default_instance
    assert "slug" in default_instance
    assert "display_name" in default_instance
    assert "status" in default_instance


def test_graph_list_api(admin_test_env):
    """Test the graph_list REST API endpoint."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    # Create an API key for authentication
    response = client.post(
        "/apikeys",
        data={"label": "Test key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]

    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)
    assert api_key_value.startswith("gpdb_")

    # Test the graph_list endpoint with API key authentication
    response = client.get(
        "/api/graph_list",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200

    data = response.json()
    assert "items" in data
    assert "total" in data
    assert isinstance(data["items"], list)
    assert isinstance(data["total"], int)
    assert data["total"] == len(data["items"])

    # Verify at least one graph is present (the default graph)
    assert len(data["items"]) >= 1
    default_graph = next((item for item in data["items"] if item["is_default"]), None)
    assert default_graph is not None
    assert "id" in default_graph
    assert "instance_id" in default_graph
    assert "instance_slug" in default_graph
    assert "instance_display_name" in default_graph
    assert "display_name" in default_graph
    assert "table_prefix" in default_graph
    assert "status" in default_graph
    assert "exists_in_instance" in default_graph
    assert "source" in default_graph
    assert "is_default" in default_graph

    # Test filtering by instance_id
    # First get an instance_id from the instance list
    response = client.get(
        "/api/instance_list",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    instance_list_data = response.json()
    assert len(instance_list_data["items"]) >= 1
    instance_id = instance_list_data["items"][0]["id"]

    # Test graph_list with instance_id parameter
    response = client.get(
        f"/api/graph_list?instance_id={instance_id}",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200

    data = response.json()
    assert "items" in data
    assert "total" in data
    assert isinstance(data["items"], list)
    assert isinstance(data["total"], int)
    assert data["total"] == len(data["items"])

    # Verify all returned graphs belong to the specified instance
    for graph in data["items"]:
        assert graph["instance_id"] == instance_id


def test_graph_get_api(admin_test_env):
    """Test the graph_get REST API endpoint."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    # Create an API key for authentication
    response = client.post(
        "/apikeys",
        data={"label": "Test key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]

    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)
    assert api_key_value.startswith("gpdb_")

    # Get the list of graphs to find a valid graph_id
    response = client.get(
        "/api/graph_list",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    list_data = response.json()
    assert len(list_data["items"]) >= 1
    graph_id = list_data["items"][0]["id"]

    # Test the graph_get endpoint with a valid graph_id
    response = client.get(
        f"/api/graph_get?graph_id={graph_id}",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200

    data = response.json()
    assert "graph" in data
    graph = data["graph"]
    assert "id" in graph
    assert "instance_id" in graph
    assert "instance_slug" in graph
    assert "instance_display_name" in graph
    assert "display_name" in graph
    assert "table_prefix" in graph
    assert "status" in graph
    assert "exists_in_instance" in graph
    assert "source" in graph
    assert "is_default" in graph
    assert graph["id"] == graph_id

    # Test the graph_get endpoint with a non-existent graph_id
    response = client.get(
        "/api/graph_get?graph_id=nonexistent-id",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 404


def test_graph_create_api(admin_test_env):
    """Test the graph_create REST API endpoint."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    # Create an API key for authentication
    response = client.post(
        "/apikeys",
        data={"label": "Test key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]

    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)
    assert api_key_value.startswith("gpdb_")

    # Get the list of instances to find a valid instance_id
    response = client.get(
        "/api/instance_list",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    instance_list_data = response.json()
    assert len(instance_list_data["items"]) >= 1
    instance_id = instance_list_data["items"][0]["id"]

    # Test creating a valid graph
    response = client.post(
        "/api/graph_create",
        params={
            "instance_id": instance_id,
            "table_prefix": "test_graph",
            "display_name": "Test Graph",
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    if response.status_code != 200:
        print(f"Error response: {response.text}")
    assert response.status_code == 200

    data = response.json()
    assert "graph" in data
    graph = data["graph"]
    assert "id" in graph
    assert "instance_id" in graph
    assert "instance_slug" in graph
    assert "instance_display_name" in graph
    assert "display_name" in graph
    assert graph["display_name"] == "Test Graph"
    assert "table_prefix" in graph
    assert graph["table_prefix"] == "test_graph"
    assert "status" in graph
    assert "exists_in_instance" in graph
    assert "source" in graph
    assert "is_default" in graph
    assert graph["instance_id"] == instance_id

    # Test creating a graph with a duplicate table_prefix returns 400
    response = client.post(
        "/api/graph_create",
        params={
            "instance_id": instance_id,
            "table_prefix": "test_graph",
            "display_name": "Another Test Graph",
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 400


def test_graph_update_api(admin_test_env):
    """Test the graph_update REST API endpoint."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    # Create an API key for authentication
    response = client.post(
        "/apikeys",
        data={"label": "Test key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]

    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)
    assert api_key_value.startswith("gpdb_")

    # Get the list of instances to find a valid instance_id
    response = client.get(
        "/api/instance_list",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    instance_list_data = response.json()
    assert len(instance_list_data["items"]) >= 1
    instance_id = instance_list_data["items"][0]["id"]

    # Create a graph to update
    response = client.post(
        "/api/graph_create",
        params={
            "instance_id": instance_id,
            "table_prefix": "test_update_graph",
            "display_name": "Test Update Graph",
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    create_data = response.json()
    graph_id = create_data["graph"]["id"]

    # Test updating the graph with a new display_name
    response = client.put(
        "/api/graph_update",
        params={
            "graph_id": graph_id,
            "display_name": "Updated Display Name",
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    if response.status_code != 200:
        print(f"Error response: {response.text}")
    assert response.status_code == 200

    data = response.json()
    assert "graph" in data
    graph = data["graph"]
    assert graph["id"] == graph_id
    assert graph["display_name"] == "Updated Display Name"

    # Test updating a non-existent graph returns 404
    response = client.put(
        "/api/graph_update",
        params={
            "graph_id": "nonexistent-id",
            "display_name": "Test",
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 404


def test_graph_delete_api(admin_test_env):
    """Test the graph_delete REST API endpoint."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    # Create an API key for authentication
    response = client.post(
        "/apikeys",
        data={"label": "Test key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]

    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)
    assert api_key_value.startswith("gpdb_")

    # Get the list of instances to find a valid instance_id
    response = client.get(
        "/api/instance_list",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    instance_list_data = response.json()
    assert len(instance_list_data["items"]) >= 1
    instance_id = instance_list_data["items"][0]["id"]

    # Create a graph to delete
    response = client.post(
        "/api/graph_create",
        params={
            "instance_id": instance_id,
            "table_prefix": "test_delete_graph",
            "display_name": "Test Delete Graph",
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    create_data = response.json()
    graph_id = create_data["graph"]["id"]

    # Test deleting the graph
    response = client.delete(
        "/api/graph_delete",
        params={"graph_id": graph_id},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    if response.status_code != 200:
        print(f"Error response: {response.text}")
    assert response.status_code == 200

    # Verify the graph is deleted by trying to get it
    response = client.get(
        "/api/graph_get",
        params={"graph_id": graph_id},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 404

    # Test deleting a non-existent graph returns 404
    response = client.delete(
        "/api/graph_delete",
        params={"graph_id": "nonexistent-id"},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 404

    # Test deleting the default graph (table_prefix="") returns 400
    # First, get the default graph
    response = client.get(
        "/api/graph_list",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    graph_list_data = response.json()
    default_graph = next(
        (item for item in graph_list_data["items"] if item.get("table_prefix") == ""),
        None,
    )
    if default_graph is not None:
        response = client.delete(
            "/api/graph_delete",
            params={"graph_id": default_graph["id"]},
            headers={"Authorization": f"Bearer {api_key_value}"},
        )
        assert response.status_code == 400


def test_instance_get_api(admin_test_env):
    """Test the instance_get REST API endpoint."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    # Create an API key for authentication
    response = client.post(
        "/apikeys",
        data={"label": "Test key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]

    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)
    assert api_key_value.startswith("gpdb_")

    # Get the list of instances to find a valid instance_id
    response = client.get(
        "/api/instance_list",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    list_data = response.json()
    assert len(list_data["items"]) >= 1
    instance_id = list_data["items"][0]["id"]

    # Test the instance_get endpoint with a valid instance_id
    response = client.get(
        f"/api/instance_get?instance_id={instance_id}",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200

    data = response.json()
    assert "instance" in data
    instance = data["instance"]
    assert "id" in instance
    assert "slug" in instance
    assert "display_name" in instance
    assert "description" in instance
    assert "mode" in instance
    assert "is_builtin" in instance
    assert "is_default" in instance
    assert "is_active" in instance
    assert "connection_kind" in instance
    assert "status" in instance
    assert instance["id"] == instance_id

    # Test the instance_get endpoint with a non-existent instance_id
    response = client.get(
        "/api/instance_get?instance_id=nonexistent-id",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 404


def test_instance_create_api(admin_test_env):
    """Test the instance_create REST API endpoint."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    # Create an API key for authentication
    response = client.post(
        "/apikeys",
        data={"label": "Test key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]

    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)
    assert api_key_value.startswith("gpdb_")

    # Test creating a valid external instance
    instance_data = _captive_instance_form(
        manager,
        slug="test-external",
        display_name="Test External Instance",
        description="A test external instance",
    )
    # Provide a valid port value for the API call
    api_params = instance_data.copy()
    if api_params["port"] == "":
        api_params["port"] = 5432

    response = client.post(
        "/api/instance_create",
        params=api_params,
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    if response.status_code != 200:
        print(f"Error response: {response.text}")
    assert response.status_code == 200

    data = response.json()
    assert "instance" in data
    instance = data["instance"]
    assert "id" in instance
    assert instance["slug"] == "test-external"
    assert instance["display_name"] == "Test External Instance"
    assert instance["description"] == "A test external instance"
    assert instance["mode"] == "external"
    assert instance["is_builtin"] is False
    assert instance["is_default"] is False
    assert instance["is_active"] is True
    assert instance["connection_kind"] == "postgres"
    assert instance["host"] == instance_data["host"]
    assert instance["port"] == 5432
    assert instance["database"] == instance_data["database"]
    assert instance["username"] == instance_data["username"]
    assert "status" in instance

    # Test creating an instance with a duplicate slug returns 400
    response = client.post(
        "/api/instance_create",
        params=api_params,
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 400


def test_instance_update_api(admin_test_env):
    """Test the instance_update REST API endpoint."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    # Create an API key for authentication
    response = client.post(
        "/apikeys",
        data={"label": "Test key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]

    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)
    assert api_key_value.startswith("gpdb_")

    # First create an instance to update
    instance_data = _captive_instance_form(
        manager,
        slug="test-update",
        display_name="Test Update Instance",
        description="A test instance for update",
    )
    api_params = instance_data.copy()
    if api_params["port"] == "":
        api_params["port"] = 5432

    response = client.post(
        "/api/instance_create",
        params=api_params,
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    create_data = response.json()
    instance_id = create_data["instance"]["id"]

    # Test updating the instance with new values
    update_params = {
        "instance_id": instance_id,
        "display_name": "Updated Display Name",
        "description": "Updated description",
        "is_active": False,
        "host": "updated-host.example.com",
        "port": 5433,
        "database": "updated_db",
        "username": "updated_user",
    }

    response = client.put(
        "/api/instance_update",
        params=update_params,
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    if response.status_code != 200:
        print(f"Error response: {response.text}")
    assert response.status_code == 200

    data = response.json()
    assert "instance" in data
    instance = data["instance"]
    assert instance["id"] == instance_id
    assert instance["display_name"] == "Updated Display Name"
    assert instance["description"] == "Updated description"
    assert instance["is_active"] is False
    assert instance["host"] == "updated-host.example.com"
    assert instance["port"] == 5433
    assert instance["database"] == "updated_db"
    assert instance["username"] == "updated_user"

    # Test updating a non-existent instance returns 404
    response = client.put(
        "/api/instance_update",
        params={
            "instance_id": "nonexistent-id",
            "display_name": "Test",
            "description": "Test",
            "is_active": True,
        },
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 404


def test_instance_delete_api(admin_test_env):
    """Test the instance_delete REST API endpoint."""
    manager = admin_test_env.manager
    client = admin_test_env.client

    _bootstrap_owner(client)
    _login(client)

    # Create an API key for authentication
    response = client.post(
        "/apikeys",
        data={"label": "Test key"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    api_key_detail_path = response.headers["location"]

    response = client.get(api_key_detail_path)
    assert response.status_code == 200
    api_key_value = _extract_revealed_api_key(response.text)
    assert api_key_value.startswith("gpdb_")

    # First create an instance to delete
    instance_data = _captive_instance_form(
        manager,
        slug="test-delete",
        display_name="Test Delete Instance",
        description="A test instance for deletion",
    )
    api_params = instance_data.copy()
    if api_params["port"] == "":
        api_params["port"] = 5432

    response = client.post(
        "/api/instance_create",
        params=api_params,
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    create_data = response.json()
    instance_id = create_data["instance"]["id"]

    # Test deleting the instance
    response = client.delete(
        "/api/instance_delete",
        params={"instance_id": instance_id},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    if response.status_code != 200:
        print(f"Error response: {response.text}")
    assert response.status_code == 200

    # Verify the instance is deleted by trying to get it
    response = client.get(
        f"/api/instance_get?instance_id={instance_id}",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 404

    # Test deleting a non-existent instance returns 404
    response = client.delete(
        "/api/instance_delete",
        params={"instance_id": "nonexistent-id"},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 404

    # Test deleting the built-in instance returns 400
    response = client.get(
        "/api/instance_list",
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 200
    list_data = response.json()
    builtin_instance = next(
        (item for item in list_data["items"] if item["is_builtin"]), None
    )
    assert builtin_instance is not None
    builtin_instance_id = builtin_instance["id"]

    response = client.delete(
        "/api/instance_delete",
        params={"instance_id": builtin_instance_id},
        headers={"Authorization": f"Bearer {api_key_value}"},
    )
    assert response.status_code == 400


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


def _extract_revealed_api_key(html: str) -> str:
    match = re.search(r'<input[^>]*readonly[^>]*value="([^"]+)"', html)
    assert match is not None
    return match.group(1)
