from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.domains.analytics.research_bundle import write_research_bundle
from src.entrypoints.http.app import create_app


@pytest.fixture()
def research_client(tmp_path: Path) -> TestClient:
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
        experiment_id="market-behavior/published-alpha",
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
        experiment_id="market-behavior/published-alpha",
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
        experiment_id="market-behavior/unstructured-beta",
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
    assert [item["experimentId"] for item in payload["items"]] == [
        "market-behavior/unstructured-beta",
        "market-behavior/published-alpha",
    ]
    published_item = next(
        item for item in payload["items"] if item["experimentId"] == "market-behavior/published-alpha"
    )
    assert published_item["runId"] == "20260405_110000_alpha0002"
    assert published_item["title"] == "Alpha Research Latest"
    assert published_item["hasStructuredSummary"] is True
    fallback_item = next(
        item for item in payload["items"] if item["experimentId"] == "market-behavior/unstructured-beta"
    )
    assert fallback_item["title"] == "Beta Research"
    assert fallback_item["hasStructuredSummary"] is False


def test_get_research_detail_returns_structured_summary_and_run_history(
    research_client: TestClient,
) -> None:
    response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/published-alpha"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["runId"] == "20260405_110000_alpha0002"
    assert payload["summary"]["title"] == "Alpha Research Latest"
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
        params={"experimentId": "market-behavior/unstructured-beta"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["title"] == "Beta Research"
    assert payload["summary"] is None
    assert payload["summaryMarkdown"].startswith("# Beta Research")
    assert payload["outputTables"] == ["summary_df"]


def test_get_research_detail_returns_404_when_missing(
    research_client: TestClient,
) -> None:
    response = research_client.get(
        "/api/analytics/research/detail",
        params={"experimentId": "market-behavior/missing"},
    )

    assert response.status_code == 404
