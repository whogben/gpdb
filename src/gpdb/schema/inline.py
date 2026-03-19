"""
Schema reference inlining utilities.
"""

from __future__ import annotations

import copy
from typing import Any, Dict


def _inline_refs(schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inline all $ref references in a JSON Schema to make it standalone.

    Resolves references from the $defs section (or $definitions for older schemas).

    Args:
        schema: JSON Schema dictionary that may contain $ref

    Returns:
        JSON Schema with all $ref references inlined
    """
    # Extract definitions from $defs or $definitions
    defs = schema.get("$defs", schema.get("definitions", {}))

    def inline(obj: Any) -> Any:
        if isinstance(obj, dict):
            if "$ref" in obj:
                ref = obj["$ref"]
                # Handle #/$defs/name or #/definitions/name format
                if ref.startswith("#/$defs/"):
                    def_name = ref[len("#/$defs/") :]
                elif ref.startswith("#/definitions/"):
                    def_name = ref[len("#/definitions/") :]
                else:
                    # Simple name reference
                    def_name = ref

                # Get the definition and recursively inline it
                if def_name in defs:
                    return inline(copy.deepcopy(defs[def_name]))
                return {}
            return {k: inline(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [inline(item) for item in obj]
        return obj

    # Inline all references
    result = inline(copy.deepcopy(schema))

    # Remove $defs/$definitions from the result since everything is inlined
    if "$defs" in result:
        del result["$defs"]
    if "definitions" in result:
        del result["definitions"]

    return result
