"""
Schema package - re-exports for backward compatibility.
"""

from __future__ import annotations

from gpdb.schema.versioning import (
    _bump_semver,
    _check_breaking_changes,
    _detect_semver_change,
)
from gpdb.schema.inline import (
    _inline_refs,
)

__all__ = [
    # Versioning
    "_bump_semver",
    "_check_breaking_changes",
    "_detect_semver_change",
    # Inline
    "_inline_refs",
]
