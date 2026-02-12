"""Export OpenAPI schema directly from bt FastAPI source code.

This script generates the OpenAPI document without starting the HTTP server.

Usage:
    uv run python scripts/export_openapi.py
    uv run python scripts/export_openapi.py --output /path/to/openapi.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# Ensure `src` package can be imported when running this script directly.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
project_root_str = str(PROJECT_ROOT)
if project_root_str not in sys.path:
    sys.path.insert(0, project_root_str)


def build_openapi_schema() -> dict[str, Any]:
    """Create FastAPI app and return its OpenAPI schema."""
    from src.server.app import create_app

    app = create_app()
    return app.openapi()


def write_schema(schema: dict[str, Any], output: Path | None) -> None:
    """Write schema to file or stdout."""
    rendered = f"{json.dumps(schema, ensure_ascii=False, indent=2)}\n"

    if output is None:
        sys.stdout.write(rendered)
        return

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Trading25 bt FastAPI OpenAPI schema",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file path. If omitted, prints JSON to stdout.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    schema = build_openapi_schema()
    write_schema(schema, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
