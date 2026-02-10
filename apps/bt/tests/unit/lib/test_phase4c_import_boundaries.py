"""Import boundary checks for Phase 4C Step2."""

from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Step2で `src.lib.*` 経由に寄せる対象範囲
TARGET_DIRS = [
    PROJECT_ROOT / "src" / "server",
    PROJECT_ROOT / "src" / "cli_bt",
    PROJECT_ROOT / "src" / "agent",
    PROJECT_ROOT / "src" / "optimization",
    PROJECT_ROOT / "src" / "strategies",
]

DISALLOWED_IMPORT_FRAGMENTS = [
    "from src.backtest",
    "import src.backtest",
    "from src.strategy_config",
    "import src.strategy_config",
    "from src.utils.indicators",
    "import src.utils.indicators",
]


def _iter_python_files() -> list[Path]:
    files: list[Path] = []
    for target_dir in TARGET_DIRS:
        files.extend(target_dir.rglob("*.py"))
    return files


def test_phase4c_step2_runtime_import_boundaries() -> None:
    violations: list[str] = []

    for py_file in _iter_python_files():
        text = py_file.read_text(encoding="utf-8")
        for fragment in DISALLOWED_IMPORT_FRAGMENTS:
            if fragment in text:
                relative = py_file.relative_to(PROJECT_ROOT)
                violations.append(f"{relative}: contains `{fragment}`")

    assert not violations, "Legacy runtime imports found:\n" + "\n".join(sorted(violations))

