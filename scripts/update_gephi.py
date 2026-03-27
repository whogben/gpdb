#!/usr/bin/env python3
"""
Build script for Gephi Lite static assets.

Clones/updates the gephi-lite repo, builds the app and broadcast driver,
and copies output into the admin web static directory.

Usage:
    python scripts/update_gephi.py                    # Full build at pinned version
    python scripts/update_gephi.py --skip-update      # Build without git pull
    python scripts/update_gephi.py --version v1.0.3   # Build a specific tag/commit
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

GEPHI_LITE_REPO = "https://github.com/gephi/gephi-lite.git"
DEFAULT_VERSION = "@gephi/gephi-lite@1.0.2"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GEPHI_SOURCE = PROJECT_ROOT / "temp_refs" / "gephi-lite"
GEPHI_APP_DIR = GEPHI_SOURCE / "packages" / "gephi-lite"
BROADCAST_DIR = GEPHI_SOURCE / "packages" / "broadcast"
SDK_DIR = GEPHI_SOURCE / "packages" / "sdk"

STATIC_ROOT = PROJECT_ROOT / "gpdb_admin" / "src" / "gpdb" / "admin" / "web" / "static"
GEPHI_OUTPUT = STATIC_ROOT / "gephi-lite"
DRIVER_OUTPUT = STATIC_ROOT / "js" / "gephi-driver.js"

BROWSER_ENTRY = BROADCAST_DIR / "src" / "browser-entry.ts"


def run(cmd: list[str], cwd: Path, env: dict[str, str] | None = None) -> None:
    """Run a command, raising on failure."""
    merged = os.environ.copy()
    if env:
        merged.update(env)
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, env=merged)
    if result.returncode != 0:
        sys.exit(f"Command failed with exit code {result.returncode}: {' '.join(cmd)}")


def ensure_repo(version: str, skip_update: bool) -> None:
    """Clone the gephi-lite repo if missing, or checkout the requested version."""
    if not GEPHI_SOURCE.exists():
        print(f"Cloning gephi-lite into {GEPHI_SOURCE} ...")
        run(["git", "clone", "--depth", "1", "--branch", version, GEPHI_LITE_REPO, str(GEPHI_SOURCE)], cwd=PROJECT_ROOT)
    else:
        if not skip_update:
            print("Fetching latest from origin ...")
            run(["git", "fetch", "--tags"], cwd=GEPHI_SOURCE)
        print(f"Checking out {version} ...")
        run(["git", "checkout", version], cwd=GEPHI_SOURCE)


def build_gephi_lite() -> None:
    """Install deps and build the gephi-lite Vite app.

    We run ``vite build --base ./`` directly (not ``npm run build``): the package
    script is ``vite build && npm run generate-json-schema``, and npm forwards
    ``-- --base ./`` to the second command instead of Vite.

    Relative base keeps asset URLs prefix-safe when the admin app is mounted
    (e.g. ``/gpdb/gephi-lite/``).
    """
    print("Building gephi-lite app ...")
    run(["npm", "install"], cwd=GEPHI_SOURCE)
    run(
        ["npx", "vite", "build", "--base", "./"],
        cwd=GEPHI_APP_DIR,
    )
    run(
        ["npm", "run", "generate-json-schema"],
        cwd=GEPHI_APP_DIR,
    )


def _write_browser_entry() -> None:
    """Write the browser entry point that re-exports everything we need."""
    BROWSER_ENTRY.write_text(
        'export { GephiLiteDriver } from "./driver";\n'
        'export { deserializeDataset } from "@gephi/gephi-lite-sdk";\n'
        'export { MultiGraph } from "graphology";\n',
    )


def build_broadcast_driver() -> None:
    """Build a self-contained IIFE bundle of the broadcast driver + graphology."""
    print("Building broadcast driver (esbuild IIFE) ...")
    _write_browser_entry()
    # Ensure graphology peer dep is available for bundling
    run(["npm", "install", "--no-save", "graphology"], cwd=SDK_DIR)
    run([
        "npx", "esbuild",
        str(BROWSER_ENTRY),
        "--bundle",
        "--format=iife",
        "--global-name=GephiLiteBroadcast",
        "--target=es2020",
        "--minify",
        f"--outfile={DRIVER_OUTPUT}",
    ], cwd=GEPHI_SOURCE)


def patch_gephi_index_for_iframe_embed() -> None:
    """Ensure #root fills the iframe.

    The Vite bundle sets ``#root { height: 100dvh }``, which follows the top-level
    viewport in common browsers, not the iframe — the graph UI stays a short strip
    while the iframe grows. Override with height 100% on html/body/#root.
    """
    path = GEPHI_OUTPUT / "index.html"
    text = path.read_text(encoding="utf-8")
    marker = "<!-- gpdb: iframe embed height -->"
    if marker in text:
        return
    inject = (
        f"    {marker}\n"
        "    <style>\n"
        "      html,\n"
        "      body {\n"
        "        height: 100%;\n"
        "        margin: 0;\n"
        "      }\n"
        "\n"
        "      #root {\n"
        "        height: 100% !important;\n"
        "        min-height: 0;\n"
        "        max-height: 100%;\n"
        "      }\n"
        "    </style>\n"
    )
    i = text.rfind("</head>")
    if i == -1:
        sys.exit(f"No </head> in {path}")
    path.write_text(text[:i] + inject + text[i:], encoding="utf-8")
    print(f"Patched {path.name} for iframe-relative height")


def copy_gephi_lite_app() -> None:
    """Copy the Vite build output to our static directory."""
    build_dir = GEPHI_APP_DIR / "build"
    if not build_dir.exists():
        sys.exit(f"Build output not found: {build_dir}")
    if GEPHI_OUTPUT.exists():
        shutil.rmtree(GEPHI_OUTPUT)
    shutil.copytree(build_dir, GEPHI_OUTPUT)
    print(f"Copied gephi-lite build to {GEPHI_OUTPUT}")
    patch_gephi_index_for_iframe_embed()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Gephi Lite static assets")
    parser.add_argument("--version", default=DEFAULT_VERSION, help="Git tag or commit to checkout (default: %(default)s)")
    parser.add_argument("--skip-update", action="store_true", help="Skip git fetch/pull, just checkout and build")
    args = parser.parse_args()

    ensure_repo(args.version, args.skip_update)
    build_gephi_lite()
    build_broadcast_driver()
    copy_gephi_lite_app()
    print("Done.")


if __name__ == "__main__":
    main()
