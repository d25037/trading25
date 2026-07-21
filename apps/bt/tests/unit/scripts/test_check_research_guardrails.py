"""Tests for scripts/check-research-guardrails.py."""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "scripts" / "check-research-guardrails.py"
    spec = importlib.util.spec_from_file_location(
        "check_research_guardrails", module_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load check_research_guardrails module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_research_guardrails"] = module
    spec.loader.exec_module(module)
    return module


def _write_empty_catalog(repo_root: Path) -> None:
    catalog = (
        repo_root
        / "apps"
        / "bt"
        / "docs"
        / "experiments"
        / "research-catalog-metadata.toml"
    )
    catalog.parent.mkdir(parents=True, exist_ok=True)
    catalog.write_text("[experiments]\n", encoding="utf-8")


def test_current_experiment_rerun_guidance_uses_market_v5_provider_contract() -> None:
    repo_root = Path(__file__).resolve().parents[5]
    experiments_root = repo_root / "apps/bt/docs/experiments"
    current_rerun_clause = re.compile(
        r"(?:Any rerun or\s+adoption decision|Current rerun requirement|"
        r"current runner|再実行する場合|高価値候補として残す場合).{0,300}",
        re.IGNORECASE | re.DOTALL,
    )
    invalid_clauses: list[str] = []

    for readme in sorted(experiments_root.rglob("README.md")):
        content = readme.read_text(encoding="utf-8")
        for match in current_rerun_clause.finditer(content):
            clause = match.group(0)
            has_current_contract = re.search(r"Market(?: schema)? v5", clause) and (
                "provider_adjusted_v1" in clause
                or "provider-window/current-basis" in clause
            )
            mentions_market_contract = (
                "Market v" in clause
                or "Market schema v" in clause
                or "local_projection_v2_event_time" in clause
            )
            if (
                "Market v4" in clause
                or "local_projection_v2_event_time" in clause
                or (mentions_market_contract and not has_current_contract)
            ):
                invalid_clauses.append(
                    f"{readme.relative_to(repo_root)}: {clause.splitlines()[0]}"
                )

    assert invalid_clauses == []


def test_daily_ranking_guardrail_rejects_legacy_bridge_and_stock_data() -> None:
    module = _load_module()
    path = Path(
        "apps/bt/src/domains/analytics/ranking_example_evidence.py"
    )
    findings = module.find_research_code_guardrail_findings_in_text(
        path,
        """
from src.domains.analytics.daily_ranking_research_base import DailyRankingPanelRequest

create_daily_ranking_research_panel(conn, event_time_basis_only=True)
conn.execute("SELECT * FROM stock_data")
""".strip(),
    )

    assert {finding.rule_name for finding in findings} == {
        "removed-daily-ranking-compatibility-surface",
        "daily-ranking-stock-data-read",
    }


def test_daily_ranking_guardrail_requires_issued_footprint_basis_pair() -> None:
    module = _load_module()
    path = Path(
        "apps/bt/src/domains/analytics/market_bubble_footprint_support.py"
    )
    findings = module.find_research_code_guardrail_findings_in_text(
        path,
        """
from src.domains.analytics.daily_ranking_research_base import DailyRankingPanelRequest

def run_rerating_bubble_regime_forward_response_research(conn):
    ranking_relations = build_daily_ranking_research_base(
        conn,
        DailyRankingPanelRequest(),
    )
    return _build_footprint_table(
        conn,
        price_history_name=ranking_relations.price_history.name,
    )
""".strip(),
    )

    assert {finding.rule_name for finding in findings} == {
        "daily-ranking-stock-data-read",
    }


def test_scan_research_files_detects_legacy_playground_file(tmp_path: Path) -> None:
    module = _load_module()
    notebook = (
        tmp_path / "apps" / "bt" / "notebooks" / "playground" / "demo_playground.py"
    )
    notebook.parent.mkdir(parents=True)
    notebook.write_text("# legacy notebook\n", encoding="utf-8")

    findings = module.scan_research_files(
        tmp_path,
        [Path("apps/bt/notebooks/playground/demo_playground.py")],
    )

    assert len(findings) == 1
    assert findings[0].rule_name == "legacy-playground-file"


def test_scan_research_files_detects_legacy_notebook_doc_reference(
    tmp_path: Path,
) -> None:
    module = _load_module()
    readme = tmp_path / "apps" / "bt" / "docs" / "experiments" / "demo" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text(
        """
# Demo

```bash
uv run --project apps/bt marimo edit \\
  apps/bt/notebooks/playground/demo_playground.py
```
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_files(
        tmp_path,
        [Path("apps/bt/docs/experiments/demo/README.md")],
    )

    rule_names = {finding.rule_name for finding in findings}
    assert "legacy-notebook-reference" in rule_names
    assert "legacy-marimo-command" in rule_names


def test_main_accepts_clean_runner_bundle_docs(tmp_path: Path, capsys) -> None:
    module = _load_module()
    readme = tmp_path / "apps" / "bt" / "docs" / "experiments" / "demo" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text(
        """
# Demo

## Published Readout

### Decision
- Keep as context.

### Main Findings
- Finding with numbers.

### Interpretation
- This is observational.

### Production Implication
- Use only as a research lens.

### Caveats
- PIT-safe inputs still need cost checks.

### Source Artifacts
- `results.duckdb`

## Reproduce

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_demo.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。
""".strip(),
        encoding="utf-8",
    )
    _write_empty_catalog(tmp_path)

    exit_code = module.main(["--root", str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "[research-guardrails] OK" in captured.out


def test_scan_research_files_detects_incomplete_published_readout(
    tmp_path: Path,
) -> None:
    module = _load_module()
    readme = tmp_path / "apps" / "bt" / "docs" / "experiments" / "demo" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text(
        """
# Demo

## Published Readout

### Decision
- Keep as context.

### Main Findings
- Finding with numbers.
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_files(
        tmp_path,
        [Path("apps/bt/docs/experiments/demo/README.md")],
    )

    assert len(findings) == 1
    assert findings[0].rule_name == "incomplete-published-readout"
    assert "caveats" in findings[0].message


def test_main_accepts_complete_published_readout(tmp_path: Path, capsys) -> None:
    module = _load_module()
    readme = tmp_path / "apps" / "bt" / "docs" / "experiments" / "demo" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text(
        """
# Demo

## Published Readout

### Decision
- Keep as context.

### Main Findings
- Finding with numbers.

### Interpretation
- This is observational.

### Production Implication
- Use only as a research lens.

### Caveats
- PIT-safe inputs still need cost checks.

### Source Artifacts
- `results.duckdb`
""".strip(),
        encoding="utf-8",
    )
    _write_empty_catalog(tmp_path)

    exit_code = module.main(["--root", str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "[research-guardrails] OK" in captured.out


def test_scan_research_files_requires_published_readout_section_content(
    tmp_path: Path,
) -> None:
    module = _load_module()
    readme = tmp_path / "apps" / "bt" / "docs" / "experiments" / "demo" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text(
        """
# Demo

## Published Readout

### Decision

### Main Findings
- Finding with numbers.

### Interpretation
- This is observational.

### Production Implication
- Use only as a research lens.

### Caveats
- PIT-safe inputs still need cost checks.

### Source Artifacts
- `results.duckdb`
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_files(
        tmp_path,
        [Path("apps/bt/docs/experiments/demo/README.md")],
    )

    assert len(findings) == 1
    assert findings[0].rule_name == "incomplete-published-readout"
    assert "decision" in findings[0].message


def test_scan_research_files_rejects_new_published_summary_without_reason(
    tmp_path: Path,
) -> None:
    module = _load_module()
    analytics_module = (
        tmp_path / "apps" / "bt" / "src" / "domains" / "analytics" / "demo.py"
    )
    analytics_module.parent.mkdir(parents=True)
    analytics_module.write_text(
        """
def write_demo_bundle():
    return write_research_bundle(
        summary_markdown="# Demo",
        published_summary=_build_published_summary(),
    )
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_files(
        tmp_path,
        [Path("apps/bt/src/domains/analytics/demo.py")],
    )

    assert len(findings) == 1
    assert findings[0].rule_name == "published-summary-generation"


def test_scan_research_files_rejects_published_summary_even_with_fallback_reason(
    tmp_path: Path,
) -> None:
    module = _load_module()
    analytics_module = (
        tmp_path / "apps" / "bt" / "src" / "domains" / "analytics" / "demo.py"
    )
    analytics_module.parent.mkdir(parents=True)
    analytics_module.write_text(
        """
def write_demo_bundle():
    return write_research_bundle(
        summary_markdown="# Demo",
        # bundle-structured-fallback: keep machine-readable payload for old bundles.
        published_summary=_build_published_summary(),
    )
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_files(
        tmp_path,
        [Path("apps/bt/src/domains/analytics/demo.py")],
    )

    assert len(findings) == 1
    assert findings[0].rule_name == "published-summary-generation"


@pytest.mark.parametrize("schema_version", ("v3", "v4"))
def test_pit_register_guard_rejects_legacy_schema_as_current_valid_rerun(
    schema_version: str,
) -> None:
    module = _load_module()

    findings = module.find_pit_register_contract_findings(
        Path("docs/research-pit-invalidation-register.md"),
        f"""
# Research PIT Invalidation Register

A valid rerun must use `market.duckdb` schema {schema_version}.
""".strip(),
    )

    assert f"pit-register-current-schema-{schema_version}" in {
        finding.rule_name for finding in findings
    }


@pytest.mark.parametrize("schema_version", ("v3", "v4"))
def test_pit_register_guard_allows_archived_historical_legacy_schema(
    schema_version: str,
) -> None:
    module = _load_module()

    findings = module.find_pit_register_contract_findings(
        Path("docs/research-pit-invalidation-register.md"),
        f"Archived historical `market.duckdb` schema {schema_version} provenance.",
    )

    assert f"pit-register-current-schema-{schema_version}" not in {
        finding.rule_name for finding in findings
    }


def test_tracked_pit_register_satisfies_current_market_v5_contract() -> None:
    module = _load_module()
    repo_root = Path(__file__).resolve().parents[5]
    relative_path = Path("docs/research-pit-invalidation-register.md")

    findings = module.scan_research_files(repo_root, [relative_path])

    assert findings == []


def test_main_detects_legacy_playground_by_default(tmp_path: Path, capsys) -> None:
    module = _load_module()
    notebook = (
        tmp_path / "apps" / "bt" / "notebooks" / "playground" / "demo_playground.py"
    )
    notebook.parent.mkdir(parents=True)
    notebook.write_text("# legacy notebook\n", encoding="utf-8")

    exit_code = module.main(["--root", str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "legacy-playground-file" in captured.err


def test_publication_integrity_detects_catalog_key_without_readout(
    tmp_path: Path,
) -> None:
    module = _load_module()
    catalog = (
        tmp_path
        / "apps"
        / "bt"
        / "docs"
        / "experiments"
        / "research-catalog-metadata.toml"
    )
    catalog.parent.mkdir(parents=True)
    catalog.write_text(
        (
            '[experiments."market-behavior/missing"]\n'
            'status = "observed"\nrelatedExperiments = []\n'
        ),
        encoding="utf-8",
    )

    findings = module.scan_research_publication_integrity(tmp_path)

    assert {item.rule_name for item in findings} == {"catalog-readout-missing"}


def test_publication_integrity_detects_dangling_related_experiment(
    tmp_path: Path,
) -> None:
    module = _load_module()
    docs_root = tmp_path / "apps" / "bt" / "docs" / "experiments"
    readme = docs_root / "market-behavior" / "existing" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text("# Existing\n", encoding="utf-8")
    (docs_root / "research-catalog-metadata.toml").write_text(
        """
[experiments."market-behavior/existing"]
status = "observed"
relatedExperiments = ["market-behavior/missing"]
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_publication_integrity(tmp_path)

    assert {item.rule_name for item in findings} == {"dangling-related-experiment"}


def test_publication_integrity_requires_catalog(tmp_path: Path) -> None:
    module = _load_module()
    docs_root = tmp_path / "apps" / "bt" / "docs" / "experiments"
    readme = docs_root / "market-behavior" / "existing" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text("# Existing\n", encoding="utf-8")

    findings = module.scan_research_publication_integrity(tmp_path)

    assert {item.rule_name for item in findings} == {"research-catalog-missing"}


def test_publication_integrity_rejects_invalid_catalog_shapes(
    tmp_path: Path,
) -> None:
    module = _load_module()
    docs_root = tmp_path / "apps" / "bt" / "docs" / "experiments"
    docs_root.mkdir(parents=True)
    catalog = docs_root / "research-catalog-metadata.toml"
    catalog.write_text('experiments = "invalid"\n', encoding="utf-8")

    findings = module.scan_research_publication_integrity(tmp_path)

    assert {item.rule_name for item in findings} == {"catalog-experiments-invalid"}


def test_publication_integrity_reports_invalid_toml_and_missing_experiments(
    tmp_path: Path,
) -> None:
    module = _load_module()
    docs_root = tmp_path / "apps" / "bt" / "docs" / "experiments"
    docs_root.mkdir(parents=True)
    catalog = docs_root / "research-catalog-metadata.toml"
    catalog.write_text("[experiments\n", encoding="utf-8")

    invalid_toml = module.scan_research_publication_integrity(tmp_path)

    assert {item.rule_name for item in invalid_toml} == {
        "invalid-research-catalog-toml"
    }

    catalog.write_text('owner = "research"\n', encoding="utf-8")

    missing_experiments = module.scan_research_publication_integrity(tmp_path)

    assert {item.rule_name for item in missing_experiments} == {
        "catalog-experiments-missing"
    }


def test_publication_integrity_rejects_invalid_entry_and_related_types(
    tmp_path: Path,
) -> None:
    module = _load_module()
    docs_root = tmp_path / "apps" / "bt" / "docs" / "experiments"
    for experiment_id in ("invalid-entry", "invalid-related"):
        readme = docs_root / "market-behavior" / experiment_id / "README.md"
        readme.parent.mkdir(parents=True, exist_ok=True)
        readme.write_text(f"# {experiment_id}\n", encoding="utf-8")
    catalog = docs_root / "research-catalog-metadata.toml"
    catalog.write_text(
        """
[experiments]
"market-behavior/invalid-entry" = "invalid"
[experiments."market-behavior/invalid-related"]
status = "observed"
relatedExperiments = "invalid"
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_publication_integrity(tmp_path)

    assert {item.rule_name for item in findings} == {
        "catalog-entry-invalid",
        "catalog-related-experiments-invalid",
    }


def test_publication_integrity_is_bidirectional_and_excludes_figure_readmes(
    tmp_path: Path,
) -> None:
    module = _load_module()
    docs_root = tmp_path / "apps" / "bt" / "docs" / "experiments"
    readme = docs_root / "market-behavior" / "uncataloged" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text("# Uncataloged\n", encoding="utf-8")
    existing_readme = docs_root / "market-behavior" / "existing" / "README.md"
    existing_readme.parent.mkdir(parents=True)
    existing_readme.write_text("# Existing\n", encoding="utf-8")
    figure_readme = docs_root / "market-behavior" / "existing" / "figures" / "README.md"
    figure_readme.parent.mkdir(parents=True)
    figure_readme.write_text("# Figure Notes\n", encoding="utf-8")
    (docs_root / "research-catalog-metadata.toml").write_text(
        """
[experiments."market-behavior/existing"]
status = "observed"
relatedExperiments = []
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_publication_integrity(tmp_path)

    assert {item.rule_name for item in findings} == {"readout-catalog-missing"}
    assert "figures" not in findings[0].message


def test_publication_integrity_enforces_status_contract(tmp_path: Path) -> None:
    module = _load_module()
    docs_root = tmp_path / "apps" / "bt" / "docs" / "experiments"
    for experiment_id in ("bad-status", "bad-surface"):
        readme = docs_root / "market-behavior" / experiment_id / "README.md"
        readme.parent.mkdir(parents=True, exist_ok=True)
        readme.write_text(f"# {experiment_id}\n", encoding="utf-8")
    (docs_root / "research-catalog-metadata.toml").write_text(
        """
[experiments."market-behavior/bad-status"]
status = "active"
relatedExperiments = []
[experiments."market-behavior/bad-surface"]
status = "ranking_surface"
promotedSurface = "Research"
relatedExperiments = []
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_publication_integrity(tmp_path)

    assert {item.rule_name for item in findings} == {
        "catalog-status-invalid",
        "catalog-status-surface-mismatch",
    }
