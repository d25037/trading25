"""Regression guard for research surfaces deleted due to future leakage."""

from pathlib import Path


def test_removed_future_leaking_transfer_has_no_active_surface() -> None:
    repo_root = Path(__file__).resolve().parents[5]
    forbidden_tokens = (
        "topix100_streak_353" + "_transfer",
        "topix100-streak-3-53" + "-transfer",
        "topix_streak" + "_state",
    )
    active_roots = (
        repo_root / "apps" / "bt" / "src",
        repo_root / "apps" / "bt" / "scripts" / "research",
        repo_root / "apps" / "bt" / "docs" / "experiments",
        repo_root / "apps" / "ts" / "packages" / "web" / "src",
    )
    scanned_suffixes = {".py", ".md", ".toml", ".ts", ".tsx"}

    matches: list[str] = []
    for root in active_roots:
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in scanned_suffixes:
                continue
            relative_path = path.relative_to(repo_root).as_posix()
            text = path.read_text(encoding="utf-8", errors="ignore")
            for token in forbidden_tokens:
                if token in relative_path or token in text:
                    matches.append(f"{relative_path}: {token}")

    assert matches == []
