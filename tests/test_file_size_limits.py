from __future__ import annotations

from pathlib import Path

import pytest


# Single configuration variable controlling the acceptable file length (lines).
ACCEPTABLE_MAX_FILE_LINES = 750

_CODE_DIRS = [
    Path("src/gpdb"),
    Path("gpdb_admin/src/gpdb"),
]
_EXTENSIONS = {".py", ".js", ".html"}


def _count_lines(path: Path) -> int:
    # Treat files as text; if an input is not valid UTF-8 we replace
    # invalid bytes so the test can still compute line counts.
    text = path.read_text(encoding="utf-8", errors="replace")
    return len(text.splitlines())


def _iter_code_files(repo_root: Path):
    for rel_dir in _CODE_DIRS:
        abs_dir = repo_root / rel_dir
        if not abs_dir.exists():
            continue
        for p in abs_dir.rglob("*"):
            if not p.is_file():
                continue
            # Skip minified/bundled JS artifacts; they aren't meant to be refactored.
            if p.name.endswith(".bundle.js"):
                continue
            if p.suffix not in _EXTENSIONS:
                continue
            yield p


def test_no_source_files_exceed_acceptable_length():
    repo_root = Path(__file__).resolve().parents[1]

    oversized: list[tuple[int, Path]] = []
    for path in _iter_code_files(repo_root):
        line_count = _count_lines(path)
        if line_count > ACCEPTABLE_MAX_FILE_LINES:
            oversized.append((line_count, path))

    if oversized:
        oversized.sort(key=lambda t: (t[1].as_posix()))
        details = "\n".join(
            f"- {path.relative_to(repo_root).as_posix()}: {line_count} lines"
            for line_count, path in oversized
        )
        guidance = (
            "Split the oversized file(s) into logical, organized, and "
            "future-maintainable sub-files. Consider carefully the appropriate "
            "organization and examine the filestructure of the project to produce "
            "a consistent and efficient organization of subfiles."
        )
        pytest.fail(
            "Found source files exceeding the acceptable line length "
            f"(>{ACCEPTABLE_MAX_FILE_LINES} lines):\n{details}"
            f"\n\n{guidance}"
        )

