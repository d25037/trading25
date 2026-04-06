from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    build_research_run_id,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    list_research_bundle_infos,
    load_research_bundle_info,
    load_research_bundle_published_summary,
    load_research_bundle_tables,
    write_research_bundle,
)


def test_research_bundle_write_and_load_roundtrip(tmp_path: Path) -> None:
    bundle = write_research_bundle(
        experiment_id="unit-test/example",
        module="tests.example",
        function="run_example",
        params={"window": [20, 50]},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date="2024-01-01",
        analysis_end_date="2024-12-31",
        result_metadata={"source_mode": "snapshot", "valid_date_count": 12},
        result_tables={
            "summary_df": pd.DataFrame(
                [{"feature": "alpha", "mean_return": 0.12}],
            ),
            "detail_df": pd.DataFrame(
                [{"date": "2024-01-05", "code": "1111", "value": 1.0}],
            ),
        },
        summary_markdown="# Example\n",
        published_summary={
            "title": "Example",
            "purpose": "Example purpose.",
            "selectedParameters": [{"label": "Window", "value": "20,50"}],
        },
        output_root=tmp_path,
        run_id="20260331_120000_test1234",
    )

    loaded_info = load_research_bundle_info(bundle.bundle_dir)
    loaded_tables = load_research_bundle_tables(bundle.bundle_dir)
    published_summary = load_research_bundle_published_summary(bundle.bundle_dir)

    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()
    assert bundle.summary_path.exists()
    assert bundle.published_summary_path.exists()
    assert loaded_info.experiment_id == "unit-test/example"
    assert loaded_info.run_id == "20260331_120000_test1234"
    assert loaded_info.result_metadata["source_mode"] == "snapshot"
    assert list(loaded_tables) == ["summary_df", "detail_df"]
    assert loaded_tables["summary_df"].iloc[0]["feature"] == "alpha"
    assert published_summary == {
        "title": "Example",
        "purpose": "Example purpose.",
        "selectedParameters": [{"label": "Window", "value": "20,50"}],
    }


def test_find_latest_research_bundle_path_prefers_latest_mtime(tmp_path: Path) -> None:
    first = write_research_bundle(
        experiment_id="unit-test/example",
        module="tests.example",
        function="run_example",
        params={},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date=None,
        analysis_end_date=None,
        result_metadata={},
        result_tables={"summary_df": pd.DataFrame([{"value": 1}])},
        summary_markdown="# First\n",
        output_root=tmp_path,
        run_id="20260331_120000_first000",
    )
    second = write_research_bundle(
        experiment_id="unit-test/example",
        module="tests.example",
        function="run_example",
        params={},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date=None,
        analysis_end_date=None,
        result_metadata={},
        result_tables={"summary_df": pd.DataFrame([{"value": 2}])},
        summary_markdown="# Second\n",
        output_root=tmp_path,
        run_id="20260331_120500_second00",
    )

    latest = find_latest_research_bundle_path(
        "unit-test/example",
        output_root=tmp_path,
    )

    assert latest == second.bundle_dir
    assert latest != first.bundle_dir


def test_get_research_bundle_dir_and_run_id_helpers(tmp_path: Path) -> None:
    run_id = build_research_run_id(git_commit="abcdef1234567890")
    bundle_dir = get_research_bundle_dir(
        "unit-test/example",
        run_id,
        output_root=tmp_path,
    )

    assert run_id.endswith("_abcdef12")
    assert bundle_dir == tmp_path / "unit-test/example" / run_id


def test_research_bundle_auto_run_id_avoids_same_second_collisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_run_id = "20260331_120000_test1234"
    monkeypatch.setattr(
        "src.domains.analytics.research_bundle.build_research_run_id",
        lambda **_: base_run_id,
    )

    first_bundle = write_research_bundle(
        experiment_id="unit-test/example",
        module="tests.example",
        function="run_example",
        params={},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date=None,
        analysis_end_date=None,
        result_metadata={},
        result_tables={"summary_df": pd.DataFrame([{"value": 1}])},
        summary_markdown="# First\n",
        output_root=tmp_path,
    )
    second_bundle = write_research_bundle(
        experiment_id="unit-test/example",
        module="tests.example",
        function="run_example",
        params={},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date=None,
        analysis_end_date=None,
        result_metadata={},
        result_tables={"summary_df": pd.DataFrame([{"value": 2}])},
        summary_markdown="# Second\n",
        output_root=tmp_path,
    )

    assert isinstance(first_bundle, ResearchBundleInfo)
    assert first_bundle.run_id == base_run_id
    assert second_bundle.run_id == f"{base_run_id}_02"


def test_list_research_bundle_infos_returns_all_manifests(tmp_path: Path) -> None:
    write_research_bundle(
        experiment_id="unit-test/example-a",
        module="tests.example",
        function="run_example",
        params={},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date=None,
        analysis_end_date=None,
        result_metadata={},
        result_tables={"summary_df": pd.DataFrame([{"value": 1}])},
        summary_markdown="# First\n",
        output_root=tmp_path,
        run_id="20260331_120000_first000",
    )
    write_research_bundle(
        experiment_id="unit-test/example-b",
        module="tests.example",
        function="run_example",
        params={},
        db_path=str(tmp_path / "market.duckdb"),
        analysis_start_date=None,
        analysis_end_date=None,
        result_metadata={},
        result_tables={"summary_df": pd.DataFrame([{"value": 2}])},
        summary_markdown="# Second\n",
        output_root=tmp_path,
        run_id="20260331_120100_second00",
    )

    infos = list_research_bundle_infos(output_root=tmp_path)

    assert [info.experiment_id for info in infos] == [
        "unit-test/example-b",
        "unit-test/example-a",
    ]
