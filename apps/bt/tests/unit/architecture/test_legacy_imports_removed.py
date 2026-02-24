"""Guardrail: legacy src package prefixes must not remain."""

from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]

LEGACY_SEGMENTS = (
    "server",
    "cli_bt",
    "lib",
    "api",
    "data",
    "backtest",
    "strategy_config",
)
LEGACY_PREFIXES = tuple(f"src.{segment}" for segment in LEGACY_SEGMENTS)

SEARCH_ROOTS = (
    PROJECT_ROOT / "src",
    PROJECT_ROOT / "tests",
    PROJECT_ROOT / "scripts",
)


def _iter_python_files() -> list[Path]:
    files: list[Path] = []
    for root in SEARCH_ROOTS:
        if not root.exists():
            continue
        for py_file in root.rglob("*.py"):
            if "__pycache__" in py_file.parts:
                continue
            files.append(py_file)
    return files


def test_legacy_src_prefixes_removed() -> None:
    violations: list[str] = []

    for py_file in _iter_python_files():
        text = py_file.read_text(encoding="utf-8")
        for prefix in LEGACY_PREFIXES:
            if prefix in text:
                relative = py_file.relative_to(PROJECT_ROOT)
                violations.append(f"{relative}: contains `{prefix}`")

    assert not violations, "Legacy src prefixes found:\n" + "\n".join(sorted(violations))
