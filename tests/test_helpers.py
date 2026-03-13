"""Helper functions for tests."""


def schema_with_kind(schema: dict, kind: str = "node") -> dict:
    """Add x-gpdb-kind to a schema dict."""
    return {**schema, "x-gpdb-kind": kind}
