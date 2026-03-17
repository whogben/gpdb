from __future__ import annotations

import base64
import json
from typing import Any

from toolaccess import (
    AccessPolicy,
    InvocationContext,
    PydanticJsonRenderer,
    SurfaceSpec,
)
from toolaccess.codecs import ArgumentCodec


GRAPH_TOOL_ACCESS = AccessPolicy(
    require_authenticated=True,
    allow_anonymous=False,
)

API_KEY_TOOL_ACCESS = AccessPolicy(
    require_authenticated=True,
    allow_anonymous=False,
)

CLI_JSON_RENDERER = PydanticJsonRenderer(indent=2, sort_keys=True)
CLI_ALIAS_JSON_RENDERER = PydanticJsonRenderer(
    by_alias=True,
    indent=2,
    sort_keys=True,
)


class _JsonObjectArgumentCodec(ArgumentCodec):
    """Use gpdb's strict JSON-object coercion within ToolAccess."""

    def decode(self, value: Any, *, parameter_name: str, ctx: InvocationContext):
        return _coerce_json_object_argument(value, argument_name=parameter_name)


class _TagsArgumentCodec(ArgumentCodec):
    """Use gpdb's existing tags coercion rules within ToolAccess."""

    def decode(self, value: Any, *, parameter_name: str, ctx: InvocationContext):
        return _coerce_tags_argument(value)


class _PayloadBase64ArgumentCodec(ArgumentCodec):
    """Use gpdb's payload validation rules within ToolAccess."""

    def __init__(self, *, optional: bool = False):
        self._optional = optional

    def decode(self, value: Any, *, parameter_name: str, ctx: InvocationContext):
        if self._optional:
            return _coerce_optional_payload_base64_argument(value)
        return _coerce_payload_base64_argument(value)


JSON_OBJECT_CODEC = _JsonObjectArgumentCodec()
TAGS_CODEC = _TagsArgumentCodec()
PAYLOAD_BASE64_CODEC = _PayloadBase64ArgumentCodec()
OPTIONAL_PAYLOAD_BASE64_CODEC = _PayloadBase64ArgumentCodec(optional=True)


def _graph_surface_specs(
    *,
    http_method: str = "POST",
    cli_renderer=CLI_JSON_RENDERER,
) -> dict[str, SurfaceSpec]:
    """Return the standard REST/MCP/CLI surface configuration for graph tools."""
    return {
        "rest": SurfaceSpec(http_method=http_method),
        "mcp": SurfaceSpec(),
        "cli": SurfaceSpec(renderer=cli_renderer),
    }


def _api_key_surface_specs(
    *,
    http_method: str = "POST",
    cli_renderer=CLI_JSON_RENDERER,
) -> dict[str, SurfaceSpec]:
    """Return the standard REST/MCP/CLI surface configuration for API key tools."""
    return {
        "rest": SurfaceSpec(http_method=http_method),
        "mcp": SurfaceSpec(),
        "cli": SurfaceSpec(renderer=cli_renderer),
    }


def _coerce_json_object_argument(raw_value, *, argument_name: str) -> dict[str, object]:
    """Accept either a parsed dict or JSON text and return a JSON object."""
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            raise ValueError(f"{argument_name} is required.")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{argument_name} must be valid JSON: {exc.msg}.") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"{argument_name} must be a JSON object.")
        return parsed
    raise ValueError(f"{argument_name} must be a JSON object.")


def _coerce_tags_argument(raw_value) -> list[str]:
    """Accept blank values, comma-delimited text, or a string list."""
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [str(item).strip() for item in raw_value if str(item).strip()]
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return []
        return [item.strip() for item in text.split(",") if item.strip()]
    raise ValueError("tags must be blank, comma-delimited text, or a list of strings.")


def _coerce_payload_base64_argument(raw_value) -> bytes:
    """Accept a base64-encoded payload body and return decoded bytes."""
    if not isinstance(raw_value, str):
        raise ValueError("payload_base64 must be a base64 string.")
    text = raw_value.strip()
    try:
        return base64.b64decode(text, validate=True)
    except ValueError as exc:
        raise ValueError("payload_base64 must be valid base64.") from exc


def _coerce_optional_payload_base64_argument(raw_value) -> bytes | None:
    """Accept an optional base64-encoded payload body and return decoded bytes."""
    if raw_value is None:
        return None
    return _coerce_payload_base64_argument(raw_value)
