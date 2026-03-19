"""
Schema versioning utilities using SemVer.
"""

from __future__ import annotations

from typing import Any, Dict

from gpdb.models.base import SchemaBreakingChangeError


def _check_breaking_changes(
    old_schema: Dict[str, Any], new_schema: Dict[str, Any], name: str
):
    """
    Check if new schema contains breaking changes compared to old schema.

    Breaking changes:
    - Adding a required field
    - Removing a field
    - Changing a field's type

    Args:
        old_schema: Existing JSON schema
        new_schema: New JSON schema to validate
        name: Schema name (for error messages)

    Raises:
        SchemaBreakingChangeError: If breaking changes detected
    """
    old_props = old_schema.get("properties", {})
    new_props = new_schema.get("properties", {})
    old_required = set(old_schema.get("required", []))
    new_required = set(new_schema.get("required", []))

    # Check for removed fields
    removed_fields = set(old_props.keys()) - set(new_props.keys())
    if removed_fields:
        raise SchemaBreakingChangeError(
            f"Schema '{name}' has breaking changes: removed fields {removed_fields}"
        )

    # Check for type changes
    for field in old_props:
        if field in new_props:
            old_type = old_props[field].get("type")
            new_type = new_props[field].get("type")
            if old_type != new_type:
                raise SchemaBreakingChangeError(
                    f"Schema '{name}' has breaking changes: field '{field}' type changed from {old_type} to {new_type}"
                )

    # Check for newly required fields
    newly_required = new_required - old_required
    if newly_required:
        raise SchemaBreakingChangeError(
            f"Schema '{name}' has breaking changes: newly required fields {newly_required}"
        )


def _bump_semver(old_version: str, change_type: str) -> str:
    """
    Bump a semantic version string.

    Args:
        old_version: Current version string (e.g., "1.2.3")
        change_type: "major", "minor", or "patch"

    Returns:
        New version string
    """
    parts = old_version.split(".")
    major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])

    if change_type == "major":
        major += 1
        minor = 0
        patch = 0
    elif change_type == "minor":
        minor += 1
        patch = 0
    elif change_type == "patch":
        patch += 1

    return f"{major}.{minor}.{patch}"


def _detect_semver_change(
    old_schema: Dict[str, Any], new_schema: Dict[str, Any]
) -> str:
    """
    Detect the type of SemVer change between two schemas.

    Returns:
        "major" if breaking changes detected
        "minor" if backward compatible changes (e.g., new optional field)
        "patch" if only non-consequential changes (descriptions, titles, examples)
    """
    old_props = old_schema.get("properties", {})
    new_props = new_schema.get("properties", {})
    old_required = set(old_schema.get("required", []))
    new_required = set(new_schema.get("required", []))

    # Check for breaking changes (major)
    removed_fields = set(old_props.keys()) - set(new_props.keys())
    if removed_fields:
        return "major"

    for field in old_props:
        if field in new_props:
            old_type = old_props[field].get("type")
            new_type = new_props[field].get("type")
            if old_type != new_type:
                return "major"

    newly_required = new_required - old_required
    if newly_required:
        return "major"

    # Check for backward compatible changes (minor)
    added_fields = set(new_props.keys()) - set(old_props.keys())
    if added_fields:
        # If any added field is required, it's a major change (e.g. was implicitly required before)
        for field in added_fields:
            if field in new_required:
                return "major"

        # If all added fields are optional, it's a minor change
        return "minor"

    # Otherwise, it's a patch change (descriptions, titles, examples)
    return "patch"
