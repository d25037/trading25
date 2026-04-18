"""Shared helpers for runner-first research scripts."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.domains.analytics.research_bundle import ResearchBundleInfo


def add_bundle_output_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--output-root",
        default=None,
        help="Optional override for the research bundle root directory.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional explicit run id. Defaults to a timestamp + git short SHA.",
    )
    parser.add_argument(
        "--notes",
        default=None,
        help="Optional free-form note stored in manifest.json.",
    )


def emit_bundle_payload(bundle: ResearchBundleInfo) -> None:
    print(
        json.dumps(
            {
                "experimentId": bundle.experiment_id,
                "runId": bundle.run_id,
                "bundlePath": str(bundle.bundle_dir),
                "manifestPath": str(bundle.manifest_path),
                "resultsDbPath": str(bundle.results_db_path),
                "summaryPath": str(bundle.summary_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def ensure_bt_workdir(bt_root: str | Path) -> Path:
    resolved = Path(bt_root).resolve()
    if Path.cwd().resolve() != resolved:
        os.chdir(resolved)
    return resolved
