"""
Search query DSL parser.
"""

from __future__ import annotations

import re
from typing import Any, List, Union

from gpdb.search.query import Filter, FilterGroup, Op, Logic


_OP_MAP = {
    # Symbols & Aliases
    ":": Op.EQ,
    "=": Op.EQ,
    "==": Op.EQ,
    ">": Op.GT,
    ">=": Op.GTE,
    "after": Op.GT,
    "<": Op.LT,
    "<=": Op.LTE,
    "before": Op.LT,
    "!=": Op.NE,
    "~": Op.CONTAINS,
}

# Automatically register all Op values as themselves (e.g. "eq", "gt", "contains")
for op in Op:
    _OP_MAP[op.value] = op


def _value_to_dsl(value: Any) -> str:
    """Convert a Python value to DSL string representation."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        if re.search(r'[\s()"\']', value):
            escaped = value.replace('"', '\\"')
            return f'"{escaped}"'
        return value
    if isinstance(value, (list, tuple)):
        items = [_value_to_dsl(v) for v in value]
        return f"({', '.join(items)})"
    return str(value)


def _tokenize(text: str) -> List[Any]:
    """Split DSL text into tokens."""
    tokens = []
    i = 0
    while i < len(text):
        if text[i].isspace():
            i += 1
            continue

        char = text[i]

        # 1. Structural chars and operators (including multi-char like >=, <=, ==, !=)
        if char in "()=<>:,!":
            # Check for multi-character operators
            if char in "=<>!" and i + 1 < len(text) and text[i + 1] == "=":
                tokens.append(char + "=")
                i += 2
            else:
                tokens.append(char)
                i += 1
            continue

        # 2. Strings
        if char in "\"'":
            quote = char
            i += 1
            val = ""
            while i < len(text) and text[i] != quote:
                if text[i] == "\\" and i + 1 < len(text):
                    i += 1
                val += text[i]
                i += 1
            i += 1
            tokens.append(("STRING", val))
            continue

        # 3. Unquoted text (identifiers, keywords, numbers)
        j = i
        while j < len(text) and not text[j].isspace() and text[j] not in "()=<>:,":
            j += 1

        word = text[i:j]
        tokens.append(word)
        i = j

    return tokens


def _parse_value(token: Any) -> Any:
    """Convert a token to its Python value."""
    if isinstance(token, tuple) and token[0] == "STRING":
        return token[1]
    if token == "null":
        return None
    if token == "true":
        return True
    if token == "false":
        return False
    # Try int
    try:
        return int(token)
    except ValueError:
        pass
    # Try float
    try:
        return float(token)
    except ValueError:
        pass
    return token


def _parse_list(tokens: List[Any], pos: int) -> tuple[List[Any], int]:
    """Parse a list of values: (v1, v2, ...)"""
    if pos >= len(tokens) or tokens[pos] != "(":
        raise ValueError("Expected list starting with '('")
    pos += 1
    values = []
    while pos < len(tokens) and tokens[pos] != ")":
        # Handle comma if present
        if values and tokens[pos] == ",":
            pos += 1

        if pos >= len(tokens) or tokens[pos] == ")":
            break

        val = _parse_value(tokens[pos])
        values.append(val)
        pos += 1

    if pos >= len(tokens) or tokens[pos] != ")":
        raise ValueError("Missing closing parenthesis for list")
    pos += 1
    return values, pos


def _parse_filter(tokens: List[Any], pos: int) -> tuple[Filter, int]:
    """Parse a single filter: field op value"""
    if pos >= len(tokens):
        raise ValueError("Unexpected end of input, expected field name")

    field = tokens[pos]
    if isinstance(field, tuple):
        raise ValueError(f"Expected field name, got string: {field[1]}")
    # Check if this is actually the start of a parenthesized group handled by _parse_primary
    # But _parse_filter is called ONLY when _parse_primary sees a non-paren.

    pos += 1
    if pos >= len(tokens):
        # Implicit boolean true? e.g. "active" -> active=true
        return Filter(field=field, op=Op.EQ, value=True), pos

    op_token = tokens[pos]

    # Check for operator
    op = None
    if isinstance(op_token, str):
        if op_token.lower() in _OP_MAP:
            op = _OP_MAP[op_token.lower()]
        elif op_token in _OP_MAP:  # Case sensitive fallback
            op = _OP_MAP[op_token]

    if op:
        pos += 1
        if pos >= len(tokens):
            raise ValueError(f"Unexpected end of input after operator '{op_token}'")

        # Special handling for IN operator which expects a list
        if op == Op.IN:
            if tokens[pos] == "(":
                value, pos = _parse_list(tokens, pos)
            else:
                # Single value IN? Treat as single item list
                value = _parse_value(tokens[pos])
                value = [value]
                pos += 1
        else:
            value = _parse_value(tokens[pos])
            pos += 1
        return Filter(field=field, op=op, value=value), pos

    # No operator found.
    # We will assume "field" alone means "field=True" (boolean flag)
    return Filter(field=field, op=Op.EQ, value=True), pos


def _parse_primary(
    tokens: List[Any], pos: int
) -> tuple[Union[Filter, FilterGroup], int]:
    """Parse a primary expression: filter or (expr)"""
    if pos >= len(tokens):
        raise ValueError("Unexpected end of input")

    if tokens[pos] == "(":
        pos += 1
        expr, pos = _parse_expr(tokens, pos)
        if pos >= len(tokens) or tokens[pos] != ")":
            raise ValueError("Missing closing parenthesis")
        pos += 1
        return expr, pos

    return _parse_filter(tokens, pos)


def _parse_and_expr(
    tokens: List[Any], pos: int
) -> tuple[Union[Filter, FilterGroup], int]:
    """Parse AND expression (left-associative, implicit AND)."""
    left, pos = _parse_primary(tokens, pos)
    filters = [left]

    while pos < len(tokens) and tokens[pos] != ")":
        token = tokens[pos]

        # Check for explicit OR (terminates AND group)
        if isinstance(token, str) and token.lower() == "or":
            break

        # Check for explicit AND
        if isinstance(token, str) and token.lower() == "and":
            pos += 1
            if pos >= len(tokens):
                raise ValueError("Unexpected end of input after AND")

        # Parse next primary (implicit AND)
        right, pos = _parse_primary(tokens, pos)
        filters.append(right)

    if len(filters) == 1:
        return filters[0], pos
    return FilterGroup(logic=Logic.AND, filters=filters), pos


def _parse_expr(tokens: List[Any], pos: int) -> tuple[Union[Filter, FilterGroup], int]:
    """Parse OR expression (left-associative)."""
    from gpdb.search.query import Logic

    left, pos = _parse_and_expr(tokens, pos)
    filters = [left]

    while pos < len(tokens):
        token = tokens[pos]
        if isinstance(token, str) and token.lower() == "or":
            pos += 1
            right, pos = _parse_and_expr(tokens, pos)
            filters.append(right)
        else:
            break

    if len(filters) == 1:
        return filters[0], pos
    return FilterGroup(logic=Logic.OR, filters=filters), pos
