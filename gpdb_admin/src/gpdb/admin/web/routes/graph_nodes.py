"""Server-rendered graph node pages."""

from __future__ import annotations

import base64
import json

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from gpdb.admin.graph_content import GraphContentError
from gpdb.admin.web.routes.common import (
    get_admin_store,
    redirect_with_message,
    render,
    require_authenticated_user,
    require_graph_content_service,
)
from gpdb.admin.web.routes.list_filters import (
    build_node_list_url,
    node_filter_form_from_request,
)


router = APIRouter()
DEFAULT_NODE_DATA = json.dumps({}, indent=2, sort_keys=True)
DEFAULT_NODE_LIMIT = 20
DEFAULT_NODE_SORT = "created_at_desc"
NODE_SORT_OPTIONS = (
    ("created_at_desc", "Created newest first"),
    ("created_at_asc", "Created oldest first"),
    ("updated_at_desc", "Updated newest first"),
    ("updated_at_asc", "Updated oldest first"),
    ("name_asc", "Name A-Z"),
    ("name_desc", "Name Z-A"),
)


@router.get(
    "/graphs/{graph_id}/nodes", response_class=HTMLResponse, name="graph_node_list_page"
)
async def graph_node_list_page(request: Request, graph_id: str) -> HTMLResponse:
    """Render the node list page for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    filter_form = node_filter_form_from_request(
        request,
        default_limit=DEFAULT_NODE_LIMIT,
        default_sort=DEFAULT_NODE_SORT,
    )

    try:
        node_list = await require_graph_content_service(request).list_graph_nodes(
            graph_id=graph_id,
            current_user=current_user,
            type=filter_form["type"],
            schema_name=filter_form["schema_name"],
            parent_id=filter_form["parent_id"],
            filter_dsl=filter_form["filter"] or None,
            limit=filter_form["limit"],
            offset=filter_form["offset"],
            sort=filter_form["sort"],
        )
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    payload = node_list.model_dump(mode="json")
    previous_url = None
    if payload["offset"] > 0:
        previous_url = build_node_list_url(
            request,
            graph_id=graph_id,
            type=filter_form["type"],
            schema_name=filter_form["schema_name"],
            parent_id=filter_form["parent_id"],
            filter=filter_form["filter"],
            sort=filter_form["sort"],
            limit=filter_form["limit"],
            offset=max(0, payload["offset"] - payload["limit"]),
        )
    next_url = None
    if payload["offset"] + payload["limit"] < payload["total"]:
        next_url = build_node_list_url(
            request,
            graph_id=graph_id,
            type=filter_form["type"],
            schema_name=filter_form["schema_name"],
            parent_id=filter_form["parent_id"],
            filter=filter_form["filter"],
            sort=filter_form["sort"],
            limit=filter_form["limit"],
            offset=payload["offset"] + payload["limit"],
        )

    return render(
        request,
        "pages/graph_nodes.html",
        page_title=f"{payload['graph']['display_name']} Nodes",
        current_user=current_user,
        node_list=payload,
        current_graph=payload["graph"],
        filter_form=filter_form,
        sort_options=NODE_SORT_OPTIONS,
        previous_url=previous_url,
        next_url=next_url,
        clear_filters_url=request.app.url_path_for(
            "graph_node_list_page", graph_id=graph_id
        ),
        graphs=await get_admin_store(request).list_graphs(),
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


@router.get(
    "/graphs/{graph_id}/nodes/new",
    response_class=HTMLResponse,
    name="graph_node_create_page",
)
async def graph_node_create_page(request: Request, graph_id: str) -> HTMLResponse:
    """Render the create-node form for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    return await _render_graph_node_form(
        request,
        graph_id=graph_id,
        current_user=current_user,
        form_data={
            "type": "",
            "name": "",
            "schema_name": "",
            "owner_id": "",
            "parent_id": "",
            "tags": "",
            "data": DEFAULT_NODE_DATA,
            "mime": "",
            "clear_payload": "",
        },
    )


@router.post("/graphs/{graph_id}/nodes", name="graph_node_create")
async def graph_node_create(
    request: Request,
    graph_id: str,
    type: str = Form(...),
    name: str = Form(""),
    schema_name: str = Form(""),
    owner_id: str = Form(""),
    parent_id: str = Form(""),
    tags: str = Form(""),
    data: str = Form(...),
    payload_file: UploadFile | None = File(None),
    mime: str = Form(""),
):
    """Create one node in a managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    form_data = {
        "type": type.strip(),
        "name": name.strip(),
        "schema_name": schema_name.strip(),
        "owner_id": owner_id.strip(),
        "parent_id": parent_id.strip(),
        "tags": tags.strip(),
        "data": data.strip(),
        "mime": mime.strip(),
        "clear_payload": "",
    }
    payload_bytes, payload_filename = await _read_optional_payload_upload(payload_file)
    try:
        parsed_data = _parse_node_data_text(form_data["data"])
    except ValueError as exc:
        return await _render_graph_node_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            error_message=str(exc),
        )

    try:
        created = await require_graph_content_service(request).create_graph_node(
            graph_id=graph_id,
            type=form_data["type"],
            name=form_data["name"],
            schema_name=form_data["schema_name"],
            owner_id=form_data["owner_id"],
            parent_id=form_data["parent_id"],
            tags=_parse_tags_text(form_data["tags"]),
            data=parsed_data,
            payload=payload_bytes,
            payload_mime=form_data["mime"],
            payload_filename=payload_filename,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return await _render_graph_node_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            error_message=str(exc),
        )

    return redirect_with_message(
        request,
        "graph_node_detail_page",
        graph_id=graph_id,
        node_id=created.node.id,
        success=f"Node '{created.node.name or created.node.id}' created.",
    )


@router.get(
    "/graphs/{graph_id}/nodes/{node_id}/edit",
    response_class=HTMLResponse,
    name="graph_node_edit_page",
)
async def graph_node_edit_page(
    request: Request,
    graph_id: str,
    node_id: str,
) -> HTMLResponse:
    """Render the edit-node form for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        graph_content = require_graph_content_service(request)
        detail = await graph_content.get_graph_node(
            graph_id=graph_id,
            node_id=node_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_node_list_page",
            graph_id=graph_id,
            error=str(exc),
        )

    return await _render_graph_node_form(
        request,
        graph_id=graph_id,
        current_user=current_user,
        form_data={
            "type": detail.node.type,
            "name": detail.node.name or "",
            "schema_name": detail.node.schema_name or "",
            "owner_id": detail.node.owner_id or "",
            "parent_id": detail.node.parent_id or "",
            "tags": ", ".join(detail.node.tags),
            "data": json.dumps(detail.node.data, indent=2, sort_keys=True),
            "mime": detail.node.payload_mime or "",
            "clear_payload": "",
        },
        node_id=detail.node.id,
        node_detail=detail.model_dump(mode="json", by_alias=True),
    )


@router.post("/graphs/{graph_id}/nodes/{node_id}", name="graph_node_update")
async def graph_node_update(
    request: Request,
    graph_id: str,
    node_id: str,
    type: str = Form(...),
    name: str = Form(""),
    schema_name: str = Form(""),
    owner_id: str = Form(""),
    parent_id: str = Form(""),
    tags: str = Form(""),
    data: str = Form(...),
    payload_file: UploadFile | None = File(None),
    mime: str = Form(""),
    clear_payload: bool = Form(False),
):
    """Update one node in a managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    form_data = {
        "type": type.strip(),
        "name": name.strip(),
        "schema_name": schema_name.strip(),
        "owner_id": owner_id.strip(),
        "parent_id": parent_id.strip(),
        "tags": tags.strip(),
        "data": data.strip(),
        "mime": mime.strip(),
        "clear_payload": "true" if clear_payload else "",
    }
    payload_bytes, payload_filename = await _read_optional_payload_upload(payload_file)
    if payload_bytes is not None and clear_payload:
        return await _render_graph_node_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            node_id=node_id,
            error_message="Choose either a replacement payload or clear_payload.",
        )
    try:
        parsed_data = _parse_node_data_text(form_data["data"])
    except ValueError as exc:
        return await _render_graph_node_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            node_id=node_id,
            error_message=str(exc),
        )

    try:
        updated = await require_graph_content_service(request).update_graph_node(
            graph_id=graph_id,
            node_id=node_id,
            type=form_data["type"],
            name=form_data["name"],
            schema_name=form_data["schema_name"],
            owner_id=form_data["owner_id"],
            parent_id=form_data["parent_id"],
            tags=_parse_tags_text(form_data["tags"]),
            data=parsed_data,
            payload=payload_bytes,
            payload_mime=form_data["mime"],
            payload_filename=payload_filename,
            clear_payload=clear_payload,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return await _render_graph_node_form(
            request,
            graph_id=graph_id,
            current_user=current_user,
            form_data=form_data,
            node_id=node_id,
            error_message=str(exc),
        )

    return redirect_with_message(
        request,
        "graph_node_detail_page",
        graph_id=graph_id,
        node_id=updated.node.id,
        success=f"Node '{updated.node.name or updated.node.id}' updated.",
    )


@router.get(
    "/graphs/{graph_id}/nodes/{node_id}",
    response_class=HTMLResponse,
    name="graph_node_detail_page",
)
async def graph_node_detail_page(
    request: Request,
    graph_id: str,
    node_id: str,
) -> HTMLResponse:
    """Render one node detail page."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        graph_content = require_graph_content_service(request)
        detail = await graph_content.get_graph_node(
            graph_id=graph_id,
            node_id=node_id,
            current_user=current_user,
        )
        schema_json = None
        if detail.node.schema_name:
            schema_detail = await graph_content.get_graph_schema(
                graph_id=graph_id,
                name=detail.node.schema_name,
                current_user=current_user,
            )
            schema_json = schema_detail.schema.json_schema
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_node_list_page",
            graph_id=graph_id,
            error=str(exc),
        )

    payload = detail.model_dump(mode="json", by_alias=True)
    return render(
        request,
        "pages/node_detail.html",
        page_title=f"{payload['node']['name'] or payload['node']['id']} Node",
        current_user=current_user,
        node_detail=payload,
        current_graph=payload["graph"],
        schema_json=schema_json,
        node_json=json.dumps(payload["node"]["data"], indent=2, sort_keys=True),
        graphs=await get_admin_store(request).list_graphs(),
        error_message=request.query_params.get("error"),
        success_message=request.query_params.get("success"),
    )


@router.post("/graphs/{graph_id}/nodes/{node_id}/delete", name="graph_node_delete")
async def graph_node_delete(
    request: Request,
    graph_id: str,
    node_id: str,
):
    """Delete one node when it is not blocked by related records."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        deleted = await require_graph_content_service(request).delete_graph_node(
            graph_id=graph_id,
            node_id=node_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_node_detail_page",
            graph_id=graph_id,
            node_id=node_id,
            error=str(exc),
        )

    return redirect_with_message(
        request,
        "graph_node_list_page",
        graph_id=graph_id,
        success=f"Node '{deleted.node.name or deleted.node.id}' deleted.",
    )


@router.post(
    "/graphs/{graph_id}/nodes/{node_id}/payload", name="graph_node_payload_upload"
)
async def graph_node_payload_upload(
    request: Request,
    graph_id: str,
    node_id: str,
    payload_file: UploadFile | None = File(None),
    mime: str = Form(""),
):
    """Upload or replace a node payload from the browser."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    if payload_file is None:
        return redirect_with_message(
            request,
            "graph_node_detail_page",
            graph_id=graph_id,
            node_id=node_id,
            error="Choose a file to upload.",
        )
    payload_bytes = await payload_file.read()

    try:
        updated = await require_graph_content_service(request).set_graph_node_payload(
            graph_id=graph_id,
            node_id=node_id,
            payload=payload_bytes,
            mime=mime,
            payload_filename=payload_file.filename,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_node_detail_page",
            graph_id=graph_id,
            node_id=node_id,
            error=str(exc),
        )

    return redirect_with_message(
        request,
        "graph_node_detail_page",
        graph_id=graph_id,
        node_id=node_id,
        success=(f"Payload updated for node '{updated.node.name or updated.node.id}'."),
    )


@router.get(
    "/graphs/{graph_id}/nodes/{node_id}/payload",
    name="graph_node_payload_download",
)
async def graph_node_payload_download(
    request: Request,
    graph_id: str,
    node_id: str,
):
    """Download one node payload from the browser."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    try:
        payload = await require_graph_content_service(request).get_graph_node_payload(
            graph_id=graph_id,
            node_id=node_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(
            request,
            "graph_node_detail_page",
            graph_id=graph_id,
            node_id=node_id,
            error=str(exc),
        )

    return Response(
        content=base64.b64decode(payload.payload_base64),
        media_type=payload.node.payload_mime or "application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{payload.filename}"',
        },
    )


async def _render_graph_node_form(
    request: Request,
    *,
    graph_id: str,
    current_user,
    form_data: dict[str, str],
    node_id: str | None = None,
    node_detail: dict[str, object] | None = None,
    error_message: str | None = None,
) -> HTMLResponse | RedirectResponse:
    """Render the create or edit node form with live graph context."""
    try:
        graph_content = require_graph_content_service(request)
        if node_id is None:
            overview = await graph_content.get_graph_overview(
                graph_id=graph_id,
                current_user=current_user,
            )
            overview_payload = overview.model_dump(mode="json")
        else:
            detail = await graph_content.get_graph_node(
                graph_id=graph_id,
                node_id=node_id,
                current_user=current_user,
            )
            overview_payload = {
                "graph": detail.graph,
                "instance": detail.instance,
            }
            node_detail = detail.model_dump(mode="json", by_alias=True)
        schema_list = await graph_content.list_graph_schemas(
            graph_id=graph_id,
            current_user=current_user,
            kind="node",
            include_json_schema=True,
        )
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    is_edit = node_id is not None
    return render(
        request,
        "pages/node_form.html",
        page_title="Edit Node" if is_edit else "Create Node",
        current_user=current_user,
        overview=overview_payload,
        current_graph=overview_payload["graph"],
        node_detail=node_detail,
        form_data=form_data,
        schema_names=[item.name for item in schema_list.items],
        schema_json_map={
            item.name: item.json_schema
            for item in schema_list.items
            if item.json_schema is not None
        },
        is_edit=is_edit,
        submit_url=(
            request.app.url_path_for(
                "graph_node_update",
                graph_id=graph_id,
                node_id=node_id,
            )
            if is_edit
            else request.app.url_path_for("graph_node_create", graph_id=graph_id)
        ),
        graphs=await get_admin_store(request).list_graphs(),
        error_message=error_message,
    )


def _parse_node_data_text(json_text: str) -> dict[str, object]:
    text = json_text.strip()
    if not text:
        raise ValueError("Node data JSON is required.")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Node data JSON must be valid JSON: {exc.msg}.") from exc
    if not isinstance(parsed, dict):
        raise ValueError("Node data JSON must be a JSON object.")
    return parsed


def _parse_tags_text(tags_text: str) -> list[str]:
    return [item.strip() for item in tags_text.split(",") if item.strip()]


async def _read_optional_payload_upload(
    payload_file: UploadFile | None,
) -> tuple[bytes | None, str | None]:
    if payload_file is None:
        return None, None
    payload_bytes = await payload_file.read()
    payload_filename = (payload_file.filename or "").strip() or None
    if payload_filename is None and payload_bytes == b"":
        return None, None
    return payload_bytes, payload_filename
