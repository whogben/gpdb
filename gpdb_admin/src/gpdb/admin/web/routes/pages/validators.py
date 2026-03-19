"""Form validation helpers for page routes."""

from __future__ import annotations

import re

TABLE_PREFIX_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


def _validate_instance_form(
    form_data: dict[str, str],
    *,
    require_connection_fields: bool = True,
) -> str | None:
    """Return a validation error message for the instance form, if any."""
    if not form_data["display_name"]:
        return "Display name is required."
    if not form_data["slug"] or not TABLE_PREFIX_PATTERN.match(form_data["slug"]):
        return "Slug must contain only letters, numbers, underscores, or hyphens."
    if require_connection_fields:
        for field_name in ("host", "database", "username"):
            if not form_data[field_name]:
                return "Host, database, and username are required."
    if form_data["port"]:
        try:
            port = int(form_data["port"])
        except ValueError:
            return "Port must be a number."
        if port <= 0:
            return "Port must be a positive number."
    return None


def _validate_graph_form(
    form_data: dict[str, str],
    instances,
) -> str | None:
    """Return a validation error message for the graph form, if any."""
    if not form_data["instance_id"] or not any(
        item.id == form_data["instance_id"] for item in instances
    ):
        return "Choose an instance."
    if not form_data["table_prefix"]:
        return "Table prefix is required."
    if not TABLE_PREFIX_PATTERN.match(form_data["table_prefix"]):
        return (
            "Table prefix must contain only letters, numbers, underscores, or hyphens."
        )
    return None
