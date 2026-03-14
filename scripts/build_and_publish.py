#!/usr/bin/env python3
"""
Build and publish script for gpdb and gpdb-admin packages.

This script handles the complete build and publish process:
1. Validates version consistency between packages
2. Runs test suites
3. Cleans old build artifacts
4. Builds packages in correct order (gpdb first, then gpdb-admin)
5. Publishes to PyPI

Requirements:
    pip install -r scripts/requirements.txt

Usage:
    python scripts/build_and_publish.py              # Publish to production PyPI
    python scripts/build_and_publish.py --test-pypi   # Publish to test PyPI
    python scripts/build_and_publish.py --dry-run     # Build but don't publish
    python scripts/build_and_publish.py -y            # Auto-confirm all prompts (for CI/CD)
"""

import argparse
import getpass
import os
import shutil
import subprocess
import sys
from pathlib import Path


# Colors for terminal output
class Colors:
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


def print_success(msg: str):
    print(f"{Colors.GREEN}✓{Colors.RESET} {msg}")


def print_warning(msg: str):
    print(f"{Colors.YELLOW}⚠{Colors.RESET} {msg}")


def print_error(msg: str):
    print(f"{Colors.RED}✗{Colors.RESET} {msg}")


def print_info(msg: str):
    print(f"{Colors.BLUE}ℹ{Colors.RESET} {msg}")


def print_header(msg: str):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{msg}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.RESET}\n")


def run_command(
    cmd: list[str],
    cwd: Path | None = None,
    check: bool = True,
    timeout: int | None = None,
) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    print_info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(
        cmd, cwd=cwd, check=check, capture_output=True, text=True, timeout=timeout
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    return result


def read_version_from_pyproject(pyproject_path: Path) -> str:
    """Read version from pyproject.toml file."""
    import tomli

    with open(pyproject_path, "rb") as f:
        data = tomli.load(f)
    return data["project"]["version"]


def check_git_clean(auto_confirm: bool = False) -> bool:
    """Check if git working directory is clean."""
    try:
        result = run_command(["git", "status", "--porcelain"], check=False)
        if result.returncode != 0:
            print_warning("Could not check git status (not a git repo?)")
            return True
        if result.stdout.strip():
            print_warning("Git working directory has uncommitted changes:")
            print(result.stdout)
            if auto_confirm:
                print_info("Auto-confirm enabled, continuing...")
                return True
            response = input("Continue anyway? (y/N): ").strip().lower()
            return response == "y"
        return True
    except FileNotFoundError:
        print_warning("Git not found, skipping git check")
        return True


def clean_dist_dirs() -> None:
    """Clean dist directories for both packages."""
    print_header("Cleaning old build artifacts")

    for dist_dir in [Path("dist"), Path("gpdb_admin/dist")]:
        if dist_dir.exists():
            print_info(f"Removing {dist_dir}")
            shutil.rmtree(dist_dir)
            print_success(f"Removed {dist_dir}")
        else:
            print_info(f"No {dist_dir} directory to clean")


def validate_versions(auto_confirm: bool = False) -> tuple[str, str]:
    """Validate that versions are consistent between packages."""
    print_header("Validating versions")

    root_pyproject = Path("pyproject.toml")
    admin_pyproject = Path("gpdb_admin/pyproject.toml")

    if not root_pyproject.exists():
        print_error(f"Root pyproject.toml not found at {root_pyproject}")
        sys.exit(1)

    if not admin_pyproject.exists():
        print_error(f"Admin pyproject.toml not found at {admin_pyproject}")
        sys.exit(1)

    try:
        import tomli
    except ImportError:
        print_error("tomli not installed. Install with: pip install tomli")
        sys.exit(1)

    gpdb_version = read_version_from_pyproject(root_pyproject)
    admin_version = read_version_from_pyproject(admin_pyproject)

    print_info(f"gpdb version: {gpdb_version}")
    print_info(f"gpdb-admin version: {admin_version}")

    # Check that gpdb-admin depends on the correct gpdb version
    with open(admin_pyproject, "rb") as f:
        admin_data = tomli.load(f)

    gpdb_dep = None
    for dep in admin_data["project"]["dependencies"]:
        # Match "gpdb", "gpdb[extras]", or "gpdb==version" patterns
        if (
            dep == "gpdb"
            or dep.startswith("gpdb[")
            or dep.startswith("gpdb==")
            or dep.startswith("gpdb>=")
            or dep.startswith("gpdb~=")
        ):
            gpdb_dep = dep
            break

    if gpdb_dep is None:
        print_error("gpdb-admin does not depend on gpdb")
        sys.exit(1)

    print_info(f"gpdb-admin dependency: {gpdb_dep}")

    # Check if the dependency version matches the actual version
    if f"gpdb=={gpdb_version}" in gpdb_dep or f"gpdb>={gpdb_version}" in gpdb_dep:
        print_success("Version dependency is consistent")
    else:
        print_warning(
            f"gpdb-admin depends on {gpdb_dep} but gpdb version is {gpdb_version}"
        )
        print_warning("This may cause issues if versions don't match")
        if auto_confirm:
            print_info("Auto-confirm enabled, continuing...")
        else:
            response = input("Continue anyway? (y/N): ").strip().lower()
            if response != "y":
                sys.exit(1)

    return gpdb_version, admin_version


def run_tests() -> None:
    """Run test suites for both packages."""
    print_header("Running tests")

    # Check if .venv exists
    venv_python = Path(".venv/bin/python")
    if not venv_python.exists():
        print_error("Virtual environment not found at .venv")
        print_info("Create one with: python -m venv .venv")
        sys.exit(1)

    # Run core tests
    print_info("Running core gpdb tests...")
    try:
        run_command([str(venv_python), "-m", "pytest", "tests/", "-v"])
        print_success("Core tests passed")
    except subprocess.CalledProcessError:
        print_error("Core tests failed")
        sys.exit(1)

    # Run admin tests
    print_info("Running gpdb-admin tests...")
    try:
        run_command([str(venv_python), "-m", "pytest", "gpdb_admin/tests/", "-v"])
        print_success("Admin tests passed")
    except subprocess.CalledProcessError:
        print_error("Admin tests failed")
        sys.exit(1)


def build_package(package_dir: Path, package_name: str) -> None:
    """Build a package."""
    print_header(f"Building {package_name}")

    # Get absolute path to venv python
    venv_python = Path.cwd() / ".venv/bin/python"

    # Check if build is installed
    result = run_command([str(venv_python), "-m", "build", "--version"], check=False)
    if result.returncode != 0:
        print_error("build module not installed")
        print_info("Install with: pip install -r scripts/requirements.txt")
        sys.exit(1)

    # Build the package
    try:
        run_command([str(venv_python), "-m", "build"], cwd=package_dir)
        print_success(f"Built {package_name}")
    except subprocess.CalledProcessError:
        print_error(f"Failed to build {package_name}")
        sys.exit(1)

    # Verify build artifacts
    dist_dir = package_dir / "dist"
    if not dist_dir.exists():
        print_error(f"No dist directory found at {dist_dir}")
        sys.exit(1)

    artifacts = list(dist_dir.glob("*"))
    if not artifacts:
        print_error(f"No build artifacts found in {dist_dir}")
        sys.exit(1)

    print_info(f"Build artifacts:")
    for artifact in artifacts:
        print_info(f"  - {artifact.name}")


def publish_package(
    dist_dir: Path, package_name: str, pypi_token: str, test_pypi: bool = False
) -> None:
    """Publish a package to PyPI."""
    print_header(f"Publishing {package_name}")

    # Get absolute path to venv python
    venv_python = Path.cwd() / ".venv/bin/python"

    # Check if twine is installed
    result = run_command([str(venv_python), "-m", "twine", "--version"], check=False)
    if result.returncode != 0:
        print_error("twine not installed")
        print_info("Install with: pip install -r scripts/requirements.txt")
        sys.exit(1)

    # Determine repository URL
    if test_pypi:
        repo_url = "https://test.pypi.org/legacy/"
        print_info(f"Publishing to Test PyPI: {repo_url}")
    else:
        repo_url = "https://upload.pypi.org/legacy/"
        print_info(f"Publishing to PyPI: {repo_url}")

    # Find all distribution files
    artifacts = list(dist_dir.glob("*"))
    if not artifacts:
        print_error(f"No distribution files found in {dist_dir}")
        sys.exit(1)

    # Publish using twine
    try:
        os.environ["TWINE_PASSWORD"] = pypi_token
        cmd = [
            str(venv_python),
            "-m",
            "twine",
            "upload",
            "--repository-url",
            repo_url,
            "--username",
            "__token__",
        ]
        cmd.extend([str(a) for a in artifacts])

        run_command(cmd)
        print_success(f"Published {package_name}")
    except subprocess.CalledProcessError:
        print_error(f"Failed to publish {package_name}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Build and publish gpdb packages")
    parser.add_argument(
        "--test-pypi",
        action="store_true",
        help="Publish to Test PyPI instead of production",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Build but don't publish"
    )
    parser.add_argument("--skip-tests", action="store_true", help="Skip running tests")
    parser.add_argument(
        "--skip-git-check", action="store_true", help="Skip git clean check"
    )
    parser.add_argument(
        "-y", "--yes", action="store_true", help="Auto-confirm all prompts (for CI/CD)"
    )

    args = parser.parse_args()

    print_header("GPDB Build and Publish Script")

    # Check git status
    if not args.skip_git_check:
        if not check_git_clean(auto_confirm=args.yes):
            print_error("Git check failed")
            sys.exit(1)

    # Validate versions
    gpdb_version, admin_version = validate_versions(auto_confirm=args.yes)

    # Run tests
    if not args.skip_tests:
        run_tests()

    # Clean old builds
    clean_dist_dirs()

    # Build gpdb
    build_package(Path("."), "gpdb")

    # Build gpdb-admin
    build_package(Path("gpdb_admin"), "gpdb-admin")

    # Publish if not dry run
    if not args.dry_run:
        # Get PyPI token from env var or prompt
        pypi_token = os.environ.get("TWINE_PASSWORD") or os.environ.get("PYPI_TOKEN")
        if not pypi_token:
            pypi_token = getpass.getpass("Enter PyPI API token: ")
        if not pypi_token:
            print_error("PyPI token is required")
            sys.exit(1)

        # Publish gpdb first
        publish_package(Path("dist"), "gpdb", pypi_token, args.test_pypi)

        # Publish gpdb-admin
        publish_package(
            Path("gpdb_admin/dist"), "gpdb-admin", pypi_token, args.test_pypi
        )

        print_header("Publish Complete!")
        print_success(f"gpdb {gpdb_version} published successfully")
        print_success(f"gpdb-admin {admin_version} published successfully")
    else:
        print_header("Dry Run Complete!")
        print_success("Packages built successfully (not published)")
        print_info("To publish manually, run:")
        print_info(f"  python -m twine upload dist/*")
        print_info(f"  python -m twine upload gpdb_admin/dist/*")


if __name__ == "__main__":
    main()
