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
        experiment_id="market-behavior/topix-gap-intraday-distribution",
        module="tests.alpha",
        function="run_alpha",
        params={"window": 3},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date="2024-01-01",
        analysis_end_date="2024-12-31",
        result_metadata={"source_mode": "snapshot"},
        result_tables={"summary_df": pd.DataFrame([{"value": 1}])},
        summary_markdown="# Alpha Research\n\nAlpha purpose paragraph.\n\n- Alpha bullet\n",
        published_summary={
            "title": "Alpha Research",
            "tags": ["TOPIX", "published"],
            "family": "Market Regime",
            "status": "robust",
            "decision": "Keep as regime context.",
            "promotedSurface": "Research",
            "riskFlags": ["portfolio-lens-needed"],
            "relatedExperiments": ["market-behavior/topix-close-stock-overnight"],
            "purpose": "Alpha purpose paragraph.",
            "method": ["Alpha method"],
            "resultHeadline": "Alpha headline",
            "resultBullets": ["Alpha result bullet"],
            "considerations": ["Alpha caution"],
            "selectedParameters": [{"label": "Window", "value": "3"}],
            "highlights": [{"label": "Alpha metric", "value": "+1.23%", "tone": "success"}],
            "tableHighlights": [{"name": "summary_df", "label": "Alpha summary"}],
        },
        run_id="20260405_100000_alpha0001",
    )
    write_research_bundle(
        experiment_id="market-behavior/topix-gap-intraday-distribution",
        module="tests.alpha",
        function="run_alpha",
        params={"window": 5},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date="2025-01-01",
        analysis_end_date="2025-12-31",
        result_metadata={"source_mode": "snapshot"},
        result_tables={"summary_df": pd.DataFrame([{"value": 2}])},
        summary_markdown="# Alpha Research Latest\n\nLatest alpha purpose.\n\n- Latest alpha bullet\n",
        published_summary={
            "title": "Alpha Research Latest",
            "tags": ["TOPIX", "published"],
            "family": "Market Regime",
            "status": "robust",
            "decision": "Keep as regime context.",
            "promotedSurface": "Research",
            "riskFlags": ["portfolio-lens-needed"],
            "relatedExperiments": ["market-behavior/topix-close-stock-overnight"],
            "purpose": "Latest alpha purpose.",
            "method": ["Latest alpha method"],
            "resultHeadline": "Latest alpha headline",
            "resultBullets": ["Latest alpha result bullet"],
            "considerations": ["Latest alpha caution"],
            "selectedParameters": [{"label": "Window", "value": "5"}],
            "highlights": [{"label": "Alpha metric", "value": "+2.34%", "tone": "success"}],
            "tableHighlights": [{"name": "summary_df", "label": "Alpha summary"}],
        },
        run_id="20260405_110000_alpha0002",
    )
    write_research_bundle(
        experiment_id="market-behavior/topix-close-stock-overnight",
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
    assert "market-behavior/topix-close-stock-overnight" in experiment_ids
    assert "market-behavior/topix-gap-intraday-distribution" in experiment_ids
    assert "market-behavior/annual-market-fundamental-divergence" in experiment_ids
    published_item = next(
        item
        for item in payload["items"]
        if item["experimentId"] == "market-behavior/topix-gap-intraday-distribution"
    )
    assert published_item["runId"] == "20260405_110000_alpha0002"
    assert published_item["title"] == "Alpha Research Latest"
    assert published_item["hasStructuredSummary"] is True
    assert published_item["family"] == "Market Regime"
    assert published_item["status"] == "robust"
    assert published_item["decision"] == "Keep as regime context."
    assert published_item["promotedSurface"] == "Research"
    assert published_item["riskFlags"] == ["portfolio-lens-needed"]
    assert published_item["relatedExperiments"] == [
        "market-behavior/topix-close-stock-overnight"
    ]
    assert (
        published_item["docsReadmePath"]
        == "apps/bt/docs/experiments/market-behavior/topix-gap-intraday-distribution/README.md"
    )
    fallback_item = next(
        item
        for item in payload["items"]
        if item["experimentId"] == "market-behavior/topix-close-stock-overnight"
    )
    assert fallback_item["title"] == "Beta Research"
    assert fallback_item["hasStructuredSummary"] is False
    assert fallback_item["family"] == "Market Regime"
    assert fallback_item["status"] == "observed"
    assert "markdown-only" in fallback_item["riskFlags"]
    assert (
        fallback_item["docsReadmePath"]
        == "apps/bt/docs/experiments/market-behavior/topix-close-stock-overnight/README.md"
    )
    docs_item = next(
        item
        for item in payload["items"]
        if item["experimentId"] == "market-behavior/annual-market-fundamental-divergence"
    )
    assert docs_item["runId"] == "docs"
    assert docs_item["title"] == "Annual Market Fundamental Divergence"
    assert docs_item["headline"] != "Domain:"
    assert docs_item["hasStructuredSummary"] is False
    assert "docs-only" in docs_item["riskFlags"]
    assert "needs-publication-summary" in docs_item["riskFlags"]
    assert (
        docs_item["docsReadmePath"]
        == "apps/bt/docs/experiments/market-behavior/annual-market-fundamental-divergence/README.md"
    )


def test_get_research_detail_returns_structured_summary_and_run_history(
    research_client: TestClient,
) -> None:
    response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/topix-gap-intraday-distribution"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["runId"] == "20260405_110000_alpha0002"
    assert (
        payload["item"]["docsReadmePath"]
        == "apps/bt/docs/experiments/market-behavior/topix-gap-intraday-distribution/README.md"
    )
    assert payload["summary"]["title"] == "Alpha Research Latest"
    assert payload["summary"]["family"] == "Market Regime"
    assert payload["summary"]["status"] == "robust"
    assert payload["summary"]["decision"] == "Keep as regime context."
    assert payload["summary"]["promotedSurface"] == "Research"
    assert payload["summary"]["riskFlags"] == ["portfolio-lens-needed"]
    assert payload["summary"]["relatedExperiments"] == [
        "market-behavior/topix-close-stock-overnight"
    ]
    assert payload["summary"]["purpose"] == "Latest alpha purpose."
    assert payload["summary"]["resultHeadline"] == "Latest alpha headline"
    assert payload["summary"]["selectedParameters"] == [{"label": "Window", "value": "5"}]
    assert [item["runId"] for item in payload["availableRuns"]] == [
        "20260405_110000_alpha0002",
        "20260405_100000_alpha0001",
    ]
    assert payload["availableRuns"][0]["isLatest"] is True
    assert payload["summaryMarkdown"].startswith("# Alpha Research Latest")


def test_get_research_detail_returns_markdown_fallback_for_unstructured_bundle(
    research_client: TestClient,
) -> None:
    response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/topix-close-stock-overnight"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["title"] == "Beta Research"
    assert (
        payload["item"]["docsReadmePath"]
        == "apps/bt/docs/experiments/market-behavior/topix-close-stock-overnight/README.md"
    )
    assert payload["summary"] is None
    assert payload["summaryMarkdown"].startswith("# Beta Research")
    assert payload["outputTables"] == ["summary_df"]


def test_get_research_detail_returns_docs_only_publication(
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
    assert payload["summary"] is None
    assert payload["summaryMarkdown"].startswith("# Annual Market Fundamental Divergence")
    assert payload["outputTables"] == []
    assert "needs-publication-summary" in payload["item"]["riskFlags"]
    assert payload["availableRuns"] == [
        {
            "runId": "docs",
            "createdAt": payload["item"]["createdAt"],
            "isLatest": True,
        }
    ]
    assert payload["resultMetadata"] == {"source": "docs"}


def test_research_catalog_treats_raw_summary_json_as_markdown_fallback(
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
        published_summary={
            "selectedMarkets": ["topix500"],
            "eventSummary": [{"mean_return_pct": 1.23}],
        },
        run_id="20260405_130000_raw0001",
    )

    catalog_response = research_client.get("/api/analytics/research")

    assert catalog_response.status_code == 200
    catalog_payload = catalog_response.json()
    raw_item = next(
        item
        for item in catalog_payload["items"]
        if item["experimentId"] == "market-behavior/raw-result-summary-json"
    )
    assert raw_item["title"] == "Raw Result Summary"
    assert raw_item["hasStructuredSummary"] is False
    assert raw_item["objective"] is None
    assert raw_item["headline"] == "Scope: topix500"
    assert "needs-publication-summary" in raw_item["riskFlags"]

    detail_response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/raw-result-summary-json"},
    )

    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["summary"] is None
    assert detail_payload["summaryMarkdown"].startswith("# Raw Result Summary")


def test_research_catalog_metadata_overlay_takes_precedence(
    research_client: TestClient,
    tmp_path: Path,
) -> None:
    write_research_bundle(
        experiment_id="market-behavior/annual-value-composite-selection",
        module="tests.value",
        function="run_value",
        params={"top_pct": 10},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date="2020-01-01",
        analysis_end_date="2025-12-31",
        result_metadata={"source_mode": "snapshot"},
        result_tables={"summary_df": pd.DataFrame([{"value": 5}])},
        summary_markdown="# Value Composite\n\nValue purpose.\n\n- Value result\n",
        published_summary={
            "title": "Value Composite",
            "tags": ["value"],
            "family": "Bundle Family",
            "status": "observed",
            "decision": "Bundle decision",
            "promotedSurface": "Research",
            "riskFlags": ["bundle-risk"],
            "purpose": "Value purpose.",
            "method": ["Value method"],
            "resultHeadline": "Value headline",
            "resultBullets": ["Value result"],
            "considerations": ["Value caution"],
        },
        run_id="20260405_140000_value0001",
    )

    response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/annual-value-composite-selection"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["family"] == "Annual Fundamentals"
    assert payload["item"]["status"] == "ranking_surface"
    assert payload["item"]["decision"].startswith("Use Ranking")
    assert payload["item"]["promotedSurface"] == "Ranking"
    assert payload["item"]["riskFlags"] == ["portfolio-lens", "bundle-risk"]
    assert payload["summary"]["family"] == "Annual Fundamentals"
    assert payload["summary"]["status"] == "ranking_surface"
    assert payload["summary"]["promotedSurface"] == "Ranking"


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
    assert summary.result_bullets == ("Primary result was +12.3%.",)
    assert summary.considerations == (
        "The effect is observational.",
        "Use as a ranking diagnostic, not a direct trade rule.",
        "Capacity needs a follow-up.",
    )


def test_get_research_detail_returns_404_when_missing(
    research_client: TestClient,
) -> None:
    response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/missing"},
    )

    assert response.status_code == 404
