"""Shared helpers for bundle-viewer research notebooks."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, TypeVar

ResultT = TypeVar("ResultT")


def ensure_bt_project_root_on_path(cwd: Path, sys_path: list[str]) -> Path:
    project_root = cwd
    if project_root.name == "playground":
        project_root = project_root.parent.parent
    elif project_root.name == "notebooks":
        project_root = project_root.parent

    project_root_str = str(project_root)
    if project_root_str not in sys_path:
        sys_path.insert(0, project_root_str)
    return project_root


def get_latest_bundle_defaults(
    get_latest_bundle_path: Callable[[], Path | None],
) -> tuple[str, str]:
    try:
        latest_bundle_path = get_latest_bundle_path()
    except Exception:
        latest_bundle_path = None
    latest_run_id = latest_bundle_path.name if latest_bundle_path else ""
    latest_bundle_path_str = str(latest_bundle_path) if latest_bundle_path else ""
    return latest_bundle_path_str, latest_run_id


def build_bundle_viewer_controls(
    mo: Any,
    *,
    latest_run_id: str,
    latest_bundle_path_str: str,
    runner_path: str,
    docs_readme_path: str | None = None,
    extra_note_lines: list[str] | None = None,
    header_widgets: list[Any] | None = None,
) -> tuple[Any, Any, Any]:
    run_id = mo.ui.text(value=latest_run_id, label="Run ID")
    bundle_path = mo.ui.text(
        value=latest_bundle_path_str,
        label="Bundle Path (optional)",
    )
    note_lines = [
        "### Research Runner",
        "",
        "- Notebook path is **viewer-only**: load an existing bundle by `Run ID` or `Bundle Path`.",
        f"- Canonical runner: `{runner_path}`",
        "- Fresh analysis should be executed outside the notebook via the runner script.",
        "- Expected bundle surface: `manifest.json`, `results.duckdb`, `summary.md`, and `summary.json` when a structured summary is published.",
    ]
    if docs_readme_path:
        note_lines.append(f"- Canonical note: `{docs_readme_path}`")
    if extra_note_lines:
        note_lines.extend(extra_note_lines)

    first_row = [run_id]
    if header_widgets:
        first_row.extend(header_widgets)

    controls_view = mo.vstack(
        [
            mo.md("\n".join(note_lines)),
            mo.hstack(first_row),
            bundle_path,
        ]
    )
    return run_id, bundle_path, controls_view


def resolve_selected_bundle_path(
    bundle_path_value: str,
    run_id_value: str,
    get_bundle_path_for_run_id: Callable[[str], Path],
) -> str | None:
    resolved_bundle_path = bundle_path_value.strip()
    trimmed_run_id = run_id_value.strip()
    if not resolved_bundle_path and trimmed_run_id:
        resolved_bundle_path = str(get_bundle_path_for_run_id(trimmed_run_id))
    return resolved_bundle_path or None


def load_bundle_selection(
    *,
    selected_bundle_path: str | None,
    load_research_bundle_info: Callable[[str | Path], Any],
    load_research_bundle: Callable[[str | Path], ResultT],
) -> tuple[Any, ResultT]:
    if not selected_bundle_path:
        raise ValueError("Set a bundle path or run id.")
    bundle_info = load_research_bundle_info(selected_bundle_path)
    result = load_research_bundle(selected_bundle_path)
    return bundle_info, result
