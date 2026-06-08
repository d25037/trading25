from __future__ import annotations

from collections.abc import Generator
import os
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.application.services import research_catalog_service
from src.domains.analytics.research_bundle import write_research_bundle
from src.entrypoints.http.app import create_app


@pytest.fixture()
def research_client(tmp_path: Path) -> Generator[TestClient, None, None]:
    from src.shared.config.settings import reload_settings

    data_dir = tmp_path / "data"
    (data_dir / "market-timeseries").mkdir(parents=True, exist_ok=True)

    env_updates = {
        "TRADING25_DATA_DIR": str(data_dir),
        "MARKET_TIMESERIES_DIR": str(data_dir / "market-timeseries"),
        "JQUANTS_API_KEY": "dummy_token_value_0000",
        "JQUANTS_PLAN": "free",
    }
    original_env = {key: os.environ.get(key) for key in env_updates}

    for key, value in env_updates.items():
        os.environ[key] = value
    reload_settings()

    write_research_bundle(
        experiment_id="market-behavior/ranking-short-sector-strength-evidence",
        module="tests.alpha",
        function="run_alpha",
        params={"window": 3},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date="2024-01-01",
        analysis_end_date="2024-12-31",
        result_metadata={"source_mode": "snapshot"},
        result_tables={"summary_df": pd.DataFrame([{"value": 1}])},
        summary_markdown="# Ranking Short Sector Strength Evidence\n\nAlpha purpose paragraph.\n\n- Alpha bullet\n",
        run_id="20260405_100000_alpha0001",
    )
    write_research_bundle(
        experiment_id="market-behavior/ranking-short-sector-strength-evidence",
        module="tests.alpha",
        function="run_alpha",
        params={"window": 5},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date="2025-01-01",
        analysis_end_date="2025-12-31",
        result_metadata={"source_mode": "snapshot"},
        result_tables={"summary_df": pd.DataFrame([{"value": 2}])},
        summary_markdown="# Ranking Short Sector Strength Evidence\n\nLatest alpha purpose.\n\n- Latest alpha bullet\n",
        run_id="20260405_110000_alpha0002",
    )
    write_research_bundle(
        experiment_id="market-behavior/unpublished-bundle-only",
        module="tests.beta",
        function="run_beta",
        params={"window": 8},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date="2023-01-01",
        analysis_end_date="2023-12-31",
        result_metadata={"source_mode": "snapshot"},
        result_tables={"summary_df": pd.DataFrame([{"value": 3}])},
        summary_markdown="# Beta Research\n\nBeta fallback purpose.\n\n- Beta fallback bullet\n",
        run_id="20260405_120000_beta0001",
    )

    app = create_app()
    try:
        with TestClient(app) as client:
            yield client
    finally:
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        reload_settings()


def test_list_research_catalog_returns_latest_per_experiment(
    research_client: TestClient,
) -> None:
    response = research_client.get("/api/analytics/research")

    assert response.status_code == 200
    payload = response.json()
    assert "lastUpdated" in payload
    experiment_ids = [item["experimentId"] for item in payload["items"]]
    assert "market-behavior/ranking-short-sector-strength-evidence" in experiment_ids
    assert "market-behavior/annual-market-fundamental-divergence" in experiment_ids
    published_item = next(
        item
        for item in payload["items"]
        if item["experimentId"] == "market-behavior/ranking-short-sector-strength-evidence"
    )
    assert published_item["runId"] == "20260405_110000_alpha0002"
    assert published_item["title"] == "Ranking Short Sector Strength Evidence"
    assert published_item["hasStructuredSummary"] is True
    assert published_item["family"] == "Ranking"
    assert published_item["status"] == "candidate"
    assert published_item["decision"].startswith("Daily Ranking の short/red 候補")
    assert published_item["promotedSurface"] == "Research"
    assert "sector-regime" in published_item["riskFlags"]
    assert published_item["relatedExperiments"] == [
        "market-behavior/ranking-short-red-evidence",
        "market-behavior/ranking-sector-strength-evidence",
        "market-behavior/ranking-color-evidence",
        "market-behavior/atr-expansion-forward-response",
    ]
    assert (
        published_item["docsReadmePath"]
        == "apps/bt/docs/experiments/market-behavior/ranking-short-sector-strength-evidence/README.md"
    )
    assert "market-behavior/unpublished-bundle-only" not in experiment_ids
    docs_item = next(
        item
        for item in payload["items"]
        if item["experimentId"] == "market-behavior/annual-market-fundamental-divergence"
    )
    assert docs_item["runId"] == "docs"
    assert docs_item["title"] == "Annual Market Fundamental Divergence"
    assert docs_item["headline"] != "Domain:"
    assert docs_item["hasStructuredSummary"] is True
    assert "docs-only" not in docs_item["riskFlags"]
    assert "needs-publication-summary" not in docs_item["riskFlags"]
    assert (
        docs_item["docsReadmePath"]
        == "apps/bt/docs/experiments/market-behavior/annual-market-fundamental-divergence/README.md"
    )


def test_get_research_detail_returns_structured_summary_and_run_history(
    research_client: TestClient,
) -> None:
    response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/ranking-short-sector-strength-evidence"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["runId"] == "20260405_110000_alpha0002"
    assert (
        payload["item"]["docsReadmePath"]
        == "apps/bt/docs/experiments/market-behavior/ranking-short-sector-strength-evidence/README.md"
    )
    assert payload["summary"]["title"] == "Ranking Short Sector Strength Evidence"
    assert payload["summary"]["family"] == "Ranking"
    assert payload["summary"]["status"] == "candidate"
    assert payload["summary"]["decision"].startswith("Daily Ranking の short/red 候補")
    assert payload["summary"]["promotedSurface"] == "Research"
    assert "sector-regime" in payload["summary"]["riskFlags"]
    assert payload["summary"]["relatedExperiments"] == [
        "market-behavior/ranking-short-red-evidence",
        "market-behavior/ranking-sector-strength-evidence",
        "market-behavior/ranking-color-evidence",
        "market-behavior/atr-expansion-forward-response",
    ]
    assert payload["summary"]["readoutSections"][0]["title"] == "Decision"
    assert payload["summary"]["readoutSections"][1]["title"] == "Main Findings"
    assert payload["summary"]["selectedParameters"] == []
    assert [item["runId"] for item in payload["availableRuns"]] == [
        "20260405_110000_alpha0002",
        "20260405_100000_alpha0001",
    ]
    assert payload["availableRuns"][0]["isLatest"] is True
    assert payload["summaryMarkdown"].startswith("# Ranking Short Sector Strength Evidence")


def test_get_research_detail_rejects_unpublished_bundle_without_docs_readout(
    research_client: TestClient,
) -> None:
    response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/unpublished-bundle-only"},
    )

    assert response.status_code == 404


def test_get_research_detail_returns_docs_publication(
    research_client: TestClient,
) -> None:
    response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/annual-market-fundamental-divergence"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["runId"] == "docs"
    assert payload["item"]["title"] == "Annual Market Fundamental Divergence"
    assert payload["summary"] is not None
    assert payload["summary"]["readoutSections"][0]["title"] == "Decision"
    assert payload["summaryMarkdown"].startswith("# Annual Market Fundamental Divergence")
    assert payload["outputTables"] == []
    assert "docs-only" not in payload["item"]["riskFlags"]
    assert "needs-publication-summary" not in payload["item"]["riskFlags"]
    assert payload["availableRuns"] == [
        {
            "runId": "docs",
            "createdAt": payload["item"]["createdAt"],
            "isLatest": True,
        }
    ]
    assert payload["resultMetadata"] == {"source": "docs"}


def test_bundle_with_docs_published_readout_uses_docs_as_sot(
    research_client: TestClient,
    tmp_path: Path,
) -> None:
    write_research_bundle(
        experiment_id="market-behavior/annual-market-fundamental-divergence",
        module="tests.docs_fallback",
        function="run_docs_fallback",
        params={"window": 1},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date="2024-01-01",
        analysis_end_date="2024-12-31",
        result_metadata={"source_mode": "snapshot"},
        result_tables={"summary_df": pd.DataFrame([{"value": 6}])},
        summary_markdown="# Raw Annual Bundle\n\nRaw bundle markdown.\n",
        run_id="20260405_150000_docs0001",
    )

    response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/annual-market-fundamental-divergence"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["runId"] == "20260405_150000_docs0001"
    assert payload["item"]["hasStructuredSummary"] is True
    assert payload["summary"]["title"] == "Annual Market Fundamental Divergence"
    assert payload["summary"]["readoutSections"][0]["title"] == "Decision"
    assert payload["summaryMarkdown"].startswith("# Annual Market Fundamental Divergence")
    assert payload["outputTables"] == ["summary_df"]


def test_research_catalog_ignores_bundle_summary_json_without_docs_readout(
    research_client: TestClient,
    tmp_path: Path,
) -> None:
    write_research_bundle(
        experiment_id="market-behavior/raw-result-summary-json",
        module="tests.raw",
        function="run_raw",
        params={"window": 21},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date="2022-01-01",
        analysis_end_date="2022-12-31",
        result_metadata={"source_mode": "snapshot"},
        result_tables={"summary_df": pd.DataFrame([{"value": 4}])},
        summary_markdown="# Raw Result Summary\n\n## Setup\n\n- Scope: topix500\n\n## Result\n\n- Raw result bullet\n",
        run_id="20260405_130000_raw0001",
    )

    catalog_response = research_client.get("/api/analytics/research")

    assert catalog_response.status_code == 200
    catalog_payload = catalog_response.json()
    assert "market-behavior/raw-result-summary-json" not in {
        item["experimentId"] for item in catalog_payload["items"]
    }

    detail_response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/raw-result-summary-json"},
    )

    assert detail_response.status_code == 404


def test_research_catalog_docs_published_readout_takes_precedence(
    research_client: TestClient,
    tmp_path: Path,
) -> None:
    write_research_bundle(
        experiment_id="market-behavior/annual-market-fundamental-divergence",
        module="tests.value",
        function="run_value",
        params={"top_pct": 10},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date="2020-01-01",
        analysis_end_date="2025-12-31",
        result_metadata={"source_mode": "snapshot"},
        result_tables={"summary_df": pd.DataFrame([{"value": 5}])},
        summary_markdown="# Value Composite\n\nValue purpose.\n\n- Value result\n",
        run_id="20260405_140000_value0001",
    )

    response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/annual-market-fundamental-divergence"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["family"] == "Annual Fundamentals"
    assert payload["item"]["status"] == "observed"
    assert payload["item"]["decision"].startswith("Growth の弱さは高PBRだけでなく")
    assert payload["item"]["promotedSurface"] == "Research"
    assert payload["item"]["riskFlags"] == [
        "retrospective-market-split",
        "not-standalone-alpha",
    ]
    assert payload["summary"]["family"] == "Annual Fundamentals"
    assert payload["summary"]["status"] == "observed"
    assert payload["summary"]["promotedSurface"] == "Research"
    assert payload["summary"]["riskFlags"] == [
        "retrospective-market-split",
        "not-standalone-alpha",
    ]
    assert payload["summaryMarkdown"].startswith("# Annual Market Fundamental Divergence")


def test_markdown_published_readout_becomes_structured_summary() -> None:
    summary = research_catalog_service._load_markdown_published_summary(
        "market-behavior/published-docs",
        """
# Published Docs

Intro paragraph.

## Published Readout

### Decision
- Keep this as the canonical readout.

### Why This Research Was Run
- The catalog needed a human-readable conclusion.

### Data Scope / PIT Assumptions
- Uses PIT-safe annual joins.

### Main Findings
- Primary result was +12.3%.

### Interpretation
- The effect is observational.

### Production Implication
- Use as a ranking diagnostic, not a direct trade rule.

### Caveats
- Capacity needs a follow-up.

### Source Artifacts
- `results.duckdb`
""",
        {
            "decision": "Metadata decision takes precedence.",
            "promotedSurface": "Ranking",
            "tags": ["publication"],
        },
    )

    assert summary is not None
    assert summary.title == "Published Docs"
    assert summary.status == "ranking_surface"
    assert summary.decision == "Metadata decision takes precedence."
    assert summary.purpose == "The catalog needed a human-readable conclusion."
    assert summary.method == ("Uses PIT-safe annual joins.",)
    assert summary.result_headline == "Metadata decision takes precedence."
    assert summary.readout_sections == (
        research_catalog_service.PublishedReadoutSectionData(
            title="Decision",
            items=("Keep this as the canonical readout.",),
        ),
        research_catalog_service.PublishedReadoutSectionData(
            title="Why This Research Was Run",
            items=("The catalog needed a human-readable conclusion.",),
        ),
        research_catalog_service.PublishedReadoutSectionData(
            title="Data Scope / PIT Assumptions",
            items=("Uses PIT-safe annual joins.",),
        ),
        research_catalog_service.PublishedReadoutSectionData(
            title="Main Findings",
            items=("Primary result was +12.3%.",),
        ),
        research_catalog_service.PublishedReadoutSectionData(
            title="Interpretation",
            items=("The effect is observational.",),
        ),
        research_catalog_service.PublishedReadoutSectionData(
            title="Production Implication",
            items=("Use as a ranking diagnostic, not a direct trade rule.",),
        ),
        research_catalog_service.PublishedReadoutSectionData(
            title="Caveats",
            items=("Capacity needs a follow-up.",),
        ),
        research_catalog_service.PublishedReadoutSectionData(
            title="Source Artifacts",
            items=("`results.duckdb`",),
        ),
    )


def test_get_research_detail_returns_404_when_missing(
    research_client: TestClient,
) -> None:
    response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/missing"},
    )

    assert response.status_code == 404
