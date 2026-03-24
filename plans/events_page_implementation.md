# Events Page Implementation Plan

## Overview
Add a new "Events" page to the admin web UI that displays graph change events in a filterable, paginated list. The page will appear in the side navigation after the other 4 graph pages (Viewer, Nodes, Edges, Schemas) with the icon "𐄖".

## Navigation Changes

### File: `gpdb_admin/src/gpdb/admin/web/templates/partials/nav_menu_content.html`
Add a new nav link after the Schemas link (after line 32):

```html
<a class="nav-link"
    href="{{ mount_prefix }}{{ web_app.url_path_for('graph_events_page', graph_id=active_graph.id) }}"
    data-nav-link="events">
    𐄖 Events
</a>
```

## New Route Module

### File: `gpdb_admin/src/gpdb/admin/web/routes/graph_events.py` (NEW)
Create a new router module following the pattern of `graph_nodes.py` and `graph_edges.py`:

```python
"""Server-rendered graph events pages."""

from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from gpdb.admin.graph_content import (
    GraphChangeEventFilter,
    GraphContentError,
)
from gpdb.admin.web.routes.common import (
    redirect_with_message,
    render,
    require_authenticated_user,
    require_graph_content_service,
)
from gpdb.admin.web.routes.list_filters import parse_int_query_param


router = APIRouter()
DEFAULT_EVENT_LIMIT = 50


def event_filter_form_from_request(
    request: Request,
    *,
    default_limit: int = DEFAULT_EVENT_LIMIT,
) -> dict[str, str | int | bool | list[str]]:
    """Build events list filter form state from request query params."""
    return {
        "since_time": request.query_params.get("since_time", "").strip(),
        "node_created": request.query_params.get("node_created") == "true",
        "node_updated": request.query_params.get("node_updated") == "true",
        "node_deleted": request.query_params.get("node_deleted") == "true",
        "node_origin_edge_created": request.query_params.get("node_origin_edge_created") == "true",
        "node_origin_edge_updated": request.query_params.get("node_origin_edge_updated") == "true",
        "node_origin_edge_deleted": request.query_params.get("node_origin_edge_deleted") == "true",
        "node_destination_edge_created": request.query_params.get("node_destination_edge_created") == "true",
        "node_destination_edge_updated": request.query_params.get("node_destination_edge_updated") == "true",
        "node_destination_edge_deleted": request.query_params.get("node_destination_edge_deleted") == "true",
        "node_types": request.query_params.getlist("node_types"),
        "edge_types": request.query_params.getlist("edge_types"),
        "origin_types": request.query_params.getlist("origin_types"),
        "destination_types": request.query_params.getlist("destination_types"),
        "limit": parse_int_query_param(
            request.query_params.get("limit"),
            default=default_limit,
            minimum=1,
        ),
        "offset": parse_int_query_param(
            request.query_params.get("offset"),
            default=0,
            minimum=0,
        ),
    }


def build_event_filter_from_form(filter_form: dict) -> GraphChangeEventFilter:
    """Build GraphChangeEventFilter from form state."""
    return GraphChangeEventFilter(
        node_created=filter_form["node_created"] or None,
        node_updated=filter_form["node_updated"] or None,
        node_deleted=filter_form["node_deleted"] or None,
        node_origin_edge_created=filter_form["node_origin_edge_created"] or None,
        node_origin_edge_updated=filter_form["node_origin_edge_updated"] or None,
        node_origin_edge_deleted=filter_form["node_origin_edge_deleted"] or None,
        node_destination_edge_created=filter_form["node_destination_edge_created"] or None,
        node_destination_edge_updated=filter_form["node_destination_edge_updated"] or None,
        node_destination_edge_deleted=filter_form["node_destination_edge_deleted"] or None,
        node_types=filter_form["node_types"] or None,
        edge_types=filter_form["edge_types"] or None,
        origin_types=filter_form["origin_types"] or None,
        destination_types=filter_form["destination_types"] or None,
    )


def build_events_list_url(
    request: Request,
    *,
    graph_id: str,
    since_time: str,
    event_filter: GraphChangeEventFilter | None,
    limit: int,
    offset: int,
) -> str:
    """Build events list URL preserving filter state and pagination."""
    params: dict[str, object] = {
        "limit": limit,
        "offset": offset,
    }
    if since_time:
        params["since_time"] = since_time
    if event_filter:
        if event_filter.node_created is not None:
            params["node_created"] = "true" if event_filter.node_created else "false"
        if event_filter.node_updated is not None:
            params["node_updated"] = "true" if event_filter.node_updated else "false"
        if event_filter.node_deleted is not None:
            params["node_deleted"] = "true" if event_filter.node_deleted else "false"
        if event_filter.node_origin_edge_created is not None:
            params["node_origin_edge_created"] = "true" if event_filter.node_origin_edge_created else "false"
        if event_filter.node_origin_edge_updated is not None:
            params["node_origin_edge_updated"] = "true" if event_filter.node_origin_edge_updated else "false"
        if event_filter.node_origin_edge_deleted is not None:
            params["node_origin_edge_deleted"] = "true" if event_filter.node_origin_edge_deleted else "false"
        if event_filter.node_destination_edge_created is not None:
            params["node_destination_edge_created"] = "true" if event_filter.node_destination_edge_created else "false"
        if event_filter.node_destination_edge_updated is not None:
            params["node_destination_edge_updated"] = "true" if event_filter.node_destination_edge_updated else "false"
        if event_filter.node_destination_edge_deleted is not None:
            params["node_destination_edge_deleted"] = "true" if event_filter.node_destination_edge_deleted else "false"
        if event_filter.node_types:
            params["node_types"] = event_filter.node_types
        if event_filter.edge_types:
            params["edge_types"] = event_filter.edge_types
        if event_filter.origin_types:
            params["origin_types"] = event_filter.origin_types
        if event_filter.destination_types:
            params["destination_types"] = event_filter.destination_types
    return (
        f"{request.app.url_path_for('graph_events_page', graph_id=graph_id)}"
        f"?{urlencode(params, doseq=True)}"
    )


@router.get(
    "/graphs/{graph_id}/events", response_class=HTMLResponse, name="graph_events_page"
)
async def graph_events_page(request: Request, graph_id: str) -> HTMLResponse:
    """Render the events list page for one managed graph."""
    current_user = await require_authenticated_user(request)
    if isinstance(current_user, RedirectResponse):
        return current_user

    filter_form = event_filter_form_from_request(request)

    # Parse since_time - default to 24 hours ago if not provided
    since_time_str = filter_form["since_time"]
    if since_time_str:
        try:
            since_time = datetime.fromisoformat(since_time_str)
        except ValueError:
            since_time = datetime.now(timezone.utc).replace(microsecond=0)
    else:
        since_time = datetime.now(timezone.utc).replace(microsecond=0)

    event_filter = build_event_filter_from_form(filter_form)

    try:
        graph_content = require_graph_content_service(request)
        events_list = await graph_content.list_graph_change_events(
            graph_id=graph_id,
            since_time=since_time,
            event_filter=event_filter,
            limit=filter_form["limit"],
            offset=filter_form["offset"],
            current_user=current_user,
        )
        overview = await graph_content.get_graph_overview(
            graph_id=graph_id,
            current_user=current_user,
        )
    except GraphContentError as exc:
        return redirect_with_message(request, "home", error=str(exc))

    payload = events_list.model_dump(mode="json")
    current_graph = overview.model_dump(mode="json")["graph"]

    previous_url = None
    if payload["offset"] > 0:
        previous_url = build_events_list_url(
            request,
            graph_id=graph_id,
            since_time=since_time.isoformat(),
            event_filter=event_filter,
            limit=filter_form["limit"],
            offset=max(0, payload["offset"] - payload["limit"]),
        )
    next_url = None
    if payload["offset"] + payload["limit"] < payload["total"]:
        next_url = build_events_list_url(
            request,
            graph_id=graph_id,
            since_time=since_time.isoformat(),
            event_filter=event_filter,
            limit=filter_form["limit"],
            offset=payload["offset"] + payload["limit"],
        )

    clear_filters_url = request.app.url_path_for("graph_events_page", graph_id=graph_id)

    return render(
        request,
        "pages/graph_events.html",
        page_title=f"Events - {current_graph['display_name']}",
        current_graph=current_graph,
        current_user=current_user,
        events_list=payload,
        filter_form=filter_form,
        previous_url=previous_url,
        next_url=next_url,
        clear_filters_url=clear_filters_url,
    )
```

## New Template

### File: `gpdb_admin/src/gpdb/admin/web/templates/pages/graph_events.html` (NEW)
Create a template following the pattern of `graph_nodes.html` and `graph_edges.html`:

```html
{% extends "base.html" %}

{% block content %}
<section class="dashboard-section">
  <div class="section-heading">
    <div>
      <p class="eyebrow">Events</p>
      <h2>{{ current_graph.display_name }}</h2>
    </div>
  </div>

  <p class="resource-note">
    Browse change events in this graph, filter by event kind and type, and navigate
    to related nodes and edges.
  </p>

  <ul class="feature-list">
    <li>{{ events_list.total }} event{{ "" if events_list.total == 1 else "s" }} matched.</li>
    <li>Showing {{ events_list.offset + 1 if events_list.total else 0 }}-{{ [events_list.offset + events_list.limit,
      events_list.total] | min }}.</li>
  </ul>
</section>

<section class="dashboard-section">
  <div class="section-heading">
    <div>
      <p class="eyebrow">Browse</p>
      <h2>Filters</h2>
    </div>
    <button class="button button-secondary" type="button" data-filters-toggle>
      Show filters
    </button>
  </div>

  <article class="resource-card" data-filters-panel hidden>
    <form class="auth-form" action="{{ web_app.url_path_for('graph_events_page', graph_id=current_graph.id) }}" method="get">
      <label class="field">
        <span>Since time (ISO 8601)</span>
        <input name="since_time" value="{{ filter_form.since_time }}" placeholder="2024-01-01T00:00:00+00:00">
        <small class="field-hint">Only events after this timestamp are included. Defaults to now.</small>
      </label>

      <label class="field">
        <span>Event kinds</span>
        <div class="checkbox-group">
          <label><input type="checkbox" name="node_created" {% if filter_form.node_created %}checked{% endif %}> Node created</label>
          <label><input type="checkbox" name="node_updated" {% if filter_form.node_updated %}checked{% endif %}> Node updated</label>
          <label><input type="checkbox" name="node_deleted" {% if filter_form.node_deleted %}checked{% endif %}> Node deleted</label>
          <label><input type="checkbox" name="node_origin_edge_created" {% if filter_form.node_origin_edge_created %}checked{% endif %}> Origin edge created</label>
          <label><input type="checkbox" name="node_origin_edge_updated" {% if filter_form.node_origin_edge_updated %}checked{% endif %}> Origin edge updated</label>
          <label><input type="checkbox" name="node_origin_edge_deleted" {% if filter_form.node_origin_edge_deleted %}checked{% endif %}> Origin edge deleted</label>
          <label><input type="checkbox" name="node_destination_edge_created" {% if filter_form.node_destination_edge_created %}checked{% endif %}> Destination edge created</label>
          <label><input type="checkbox" name="node_destination_edge_updated" {% if filter_form.node_destination_edge_updated %}checked{% endif %}> Destination edge updated</label>
          <label><input type="checkbox" name="node_destination_edge_deleted" {% if filter_form.node_destination_edge_deleted %}checked{% endif %}> Destination edge deleted</label>
        </div>
      </label>

      <label class="field">
        <span>Node types (comma-separated)</span>
        <input name="node_types" value="{{ filter_form.node_types | join(', ') }}" placeholder="e.g. user, task*">
        <small class="field-hint">Optional. Use * for wildcard matching.</small>
      </label>

      <label class="field">
        <span>Edge types (comma-separated)</span>
        <input name="edge_types" value="{{ filter_form.edge_types | join(', ') }}" placeholder="e.g. follows, related*">
        <small class="field-hint">Optional. Use * for wildcard matching.</small>
      </label>

      <label class="field">
        <span>Origin types (comma-separated)</span>
        <input name="origin_types" value="{{ filter_form.origin_types | join(', ') }}" placeholder="e.g. user, task*">
        <small class="field-hint">Optional. Use * for wildcard matching.</small>
      </label>

      <label class="field">
        <span>Destination types (comma-separated)</span>
        <input name="destination_types" value="{{ filter_form.destination_types | join(', ') }}" placeholder="e.g. user, task*">
        <small class="field-hint">Optional. Use * for wildcard matching.</small>
      </label>

      <label class="field">
        <span>Page size</span>
        <select name="limit">
          {% for option in (10, 20, 50, 100) %}
          <option value="{{ option }}" {% if filter_form.limit == option %}selected{% endif %}>{{ option }}</option>
          {% endfor %}
        </select>
      </label>

      <input type="hidden" name="offset" value="0">
      <div class="button-row">
        <button class="button" type="submit">Apply filters</button>
        <a class="button button-secondary" href="{{ clear_filters_url }}">Clear</a>
      </div>
    </form>
  </article>
</section>

<section class="dashboard-section">
  <div class="section-heading">
    <div>
      <p class="eyebrow">Results</p>
      <h2>Event list</h2>
    </div>
  </div>

  <div class="card-grid">
    {% for event in events_list["items"] %}
    <article class="resource-card">
      <div class="resource-header">
        <div>
          <h3>{{ event.kind }}</h3>
          <p class="resource-subtitle">{{ event.occurred_at }}</p>
        </div>
      </div>
      <dl class="resource-meta">
        {% if event.node_id %}
        <div>
          <dt>Node</dt>
          <dd>
            <a href="{{ mount_prefix }}{{ web_app.url_path_for('graph_node_detail_page', graph_id=current_graph.id, node_id=event.node_id) }}">
              {{ event.node_id }}
            </a>
            {% if event.node_type %}({{ event.node_type }}){% endif %}
          </dd>
        </div>
        {% endif %}
        {% if event.edge_id %}
        <div>
          <dt>Edge</dt>
          <dd>
            <a href="{{ mount_prefix }}{{ web_app.url_path_for('graph_edge_detail_page', graph_id=current_graph.id, edge_id=event.edge_id) }}">
              {{ event.edge_id }}
            </a>
            {% if event.edge_type %}({{ event.edge_type }}){% endif %}
          </dd>
        </div>
        {% endif %}
        {% if event.source_id %}
        <div>
          <dt>Source</dt>
          <dd>
            <a href="{{ mount_prefix }}{{ web_app.url_path_for('graph_node_detail_page', graph_id=current_graph.id, node_id=event.source_id) }}">
              {{ event.source_id }}
            </a>
            {% if event.source_node_type %}({{ event.source_node_type }}){% endif %}
          </dd>
        </div>
        {% endif %}
        {% if event.target_id %}
        <div>
          <dt>Target</dt>
          <dd>
            <a href="{{ mount_prefix }}{{ web_app.url_path_for('graph_node_detail_page', graph_id=current_graph.id, node_id=event.target_id) }}">
              {{ event.target_id }}
            </a>
            {% if event.target_node_type %}({{ event.target_node_type }}){% endif %}
          </dd>
        </div>
        {% endif %}
      </dl>
    </article>
    {% else %}
    <article class="resource-card resource-card-empty">
      <h3>No events matched</h3>
      <p class="resource-note">
        Adjust the current filters or the since_time to see events.
      </p>
    </article>
    {% endfor %}
  </div>

  {% if previous_url or next_url %}
  <div class="button-row">
    {% if previous_url %}
    <a class="button button-secondary" href="{{ previous_url }}">Previous page</a>
    {% endif %}
    {% if next_url %}
    <a class="button button-secondary" href="{{ next_url }}">Next page</a>
    {% endif %}
  </div>
  {% endif %}
</section>
{% endblock %}

{% block scripts %}
{% include 'partials/filters_toggle_script.html' %}
<script>
  // Show info strap on page load if error/success query params exist
  const urlParams = new URLSearchParams(window.location.search);
  const error = urlParams.get('error');
  const success = urlParams.get('success');
  if (error) {
    showInfoStrap(error, 'error');
  } else if (success) {
    showInfoStrap(success, 'success');
  }
</script>
{% endblock %}
```

## App Registration

### File: `gpdb_admin/src/gpdb/admin/web/app.py`
Add the new router import and include it:

```python
from .routes.graph_events import router as graph_events_router
```

Add to the router includes (after graph_edges_router, before graph_viewer_router):

```python
app.include_router(graph_events_router)
```

## CSS Considerations

The existing CSS in `gpdb_admin/src/gpdb/admin/web/static/css/app.css` should support the new page without modifications, as it uses the same classes:
- `.dashboard-section`
- `.section-heading`
- `.eyebrow`
- `.resource-note`
- `.feature-list`
- `.resource-card`
- `.resource-header`
- `.resource-subtitle`
- `.resource-meta`
- `.card-grid`
- `.button-row`
- `.button`
- `.button-secondary`

The only new CSS needed is for the checkbox group styling, which can be added if the default checkbox layout doesn't look good:

```css
.checkbox-group {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.checkbox-group label {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}
```

## Implementation Order

1. Create `gpdb_admin/src/gpdb/admin/web/routes/graph_events.py`
2. Create `gpdb_admin/src/gpdb/admin/web/templates/pages/graph_events.html`
3. Update `gpdb_admin/src/gpdb/admin/web/app.py` to import and include the new router
4. Update `gpdb_admin/src/gpdb/admin/web/templates/partials/nav_menu_content.html` to add the Events nav link
5. Test the page with various filters and pagination
6. Add checkbox group CSS if needed for better styling
