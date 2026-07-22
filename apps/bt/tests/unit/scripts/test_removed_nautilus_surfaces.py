from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[5]
THIS_FILE = Path(__file__).resolve()
REMOVED_PATHS = (
    "apps/bt/src/domains/backtest/" + "nauti" + "lus_adapter.py",
    "apps/bt/src/domains/backtest/" + "nauti" + "lus_metrics.py",
    "apps/bt/src/application/services/verification_orchestrator.py",
    "apps/bt/tests/unit/backtest/test_" + "nauti" + "lus_adapter.py",
    "apps/bt/tests/unit/server/services/test_verification_orchestrator.py",
    "apps/bt/tests/smoke/test_" + "nauti" + "lus_runtime_smoke.py",
    ".github/workflows/" + "nauti" + "lus-smoke.yml",
    "scripts/test-" + "nauti" + "lus-smoke.sh",
    "apps/ts/packages/web/src/components/Engine" + "PolicySelector.tsx",
    "apps/ts/packages/web/src/components/Verification" + "SummarySection.tsx",
)
FORBIDDEN = (
    "nauti" + "lus",
    "fast_then_" + "verify",
    "verification_top_" + "k",
    "Engine" + "Policy",
    "Verification" + "Summary",
)
ACTIVE_ROOTS = (
    "apps/bt/src",
    "apps/bt/tests",
    "apps/bt/docs",
    "apps/ts/packages/api-clients/src",
    "apps/ts/packages/contracts/src",
    "apps/ts/packages/web/src",
    ".github/workflows",
    ".codex/skills",
)
ACTIVE_FILES = (
    "AGENTS.md",
    "README.md",
    "apps/bt/AGENTS.md",
    "apps/bt/pyproject.toml",
    "apps/bt/uv.lock",
    "apps/ts/packages/contracts/openapi/bt-openapi.json",
    ".github/dependabot.yml",
    "scripts/dep-direction-allowlist.txt",
)
TEXT_SUFFIXES = {".json", ".md", ".py", ".sh", ".toml", ".ts", ".tsx", ".txt", ".yaml", ".yml"}


def _active_files() -> list[Path]:
    files = [REPO_ROOT / relative for relative in ACTIVE_FILES]
    for relative_root in ACTIVE_ROOTS:
        root = REPO_ROOT / relative_root
        files.extend(
            path
            for path in root.rglob("*")
            if path.is_file()
            and path.suffix in TEXT_SUFFIXES
            and path.resolve() != THIS_FILE
            and "dist" not in path.parts
            and "node_modules" not in path.parts
        )
    return sorted(set(files))


def test_removed_paths_do_not_exist() -> None:
    assert [path for path in REMOVED_PATHS if (REPO_ROOT / path).exists()] == []


def test_active_surfaces_do_not_reference_removed_contracts() -> None:
    violations = []
    for path in _active_files():
        text = path.read_text(encoding="utf-8")
        for token in FORBIDDEN:
            if token.casefold() in text.casefold():
                violations.append(f"{path.relative_to(REPO_ROOT)}: {token}")
    assert violations == []
