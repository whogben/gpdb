# Plan: Pydantic Models for Tool Parameters

## Problem

Tools today use individual function parameters with type hints (e.g. `json_schema: dict[str, object]`) and custom codecs for complex types. That leads to:

- **Poor OpenAPI docs**: Complex parameters show as generic schemas (e.g. `{"additionalProp1": {}}`) instead of real structure, so AI systems and users can’t see what’s required or allowed.
- **Weak MCP tool descriptions**: Tool schemas lack proper field descriptions, constraints, and structure.
- **Manual validation**: No single source of truth for shape, required fields, or error messages.

We want rich, self-documenting parameter schemas and automatic validation without losing the current behavior on any surface.

## Why the Current Approach No Longer Works

The current design was intentional: individual parameters and custom codecs gave a good CLI experience (flags like `--graph-id`, `--name`, `--json-schema`) and kept ToolAccess simple (no model flattening). It worked while REST and MCP could tolerate loose types and poor docs.

That trade-off is no longer acceptable: consumers (especially AI) rely on accurate OpenAPI and MCP schemas. Generic or undocumented parameter shapes make the API harder to use and less reliable. We need proper schemas and validation while preserving CLI UX.

## Objectives

1. **ToolAccess and Pydantic params**
   - ToolAccess must accept tools that take **Pydantic parameter models** (e.g. a single `params: SomeParams` argument, or any number of args which may be pydantic models or not).
   - For **REST (FastAPI) and MCP (FastMCP)**: pass those params through unchanged so FastAPI/FastMCP can use them for docs and validation.
   - For **CLI**: convert between the CLI’s world (individual flags, strings, codecs) and the Pydantic model (flatten for flags on the way in, reconstruct and validate on the way into the tool). How to do that (e.g. in `get_cli_signature`/`decode_args` or elsewhere) is left to the implementer.

2. **Migrate tools to Pydantic params**
   - Move all admin tools to use Pydantic parameter models where appropriate (one or more `BaseModel` types as the tool’s input instead of many loose parameters).
   - Rely on those models for validation and for OpenAPI/MCP schema generation; remove or replace custom codecs where they become redundant.

Implementers decide: where in ToolAccess to do flatten/reconstruct, which helpers to add, where to put the Pydantic models in gpdb-admin, and how to migrate each tool and its tests.
