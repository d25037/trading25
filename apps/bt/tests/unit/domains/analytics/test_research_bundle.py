from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

import pandas as pd
import pytest

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    build_research_run_id,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    list_research_bundle_infos,
    load_dataclass_research_bundle,
    load_payload_research_bundle,
    load_research_bundle_info,
    load_research_bundle_published_summary,
    load_research_bundle_tables,
    write_dataclass_research_bundle,
    write_payload_research_bundle,
    write_research_bundle,
)


@dataclass(frozen=True)
class _ExampleDataclassResult:
    db_path: str
    source_mode: Literal["snapshot", "live"]
    analysis_start_date: str | None
    analysis_end_date: str | None
    future_horizons: tuple[int, ...]
    top_k_values: tuple[int, ...]
    row_count: int
    summary_df: pd.DataFrame
    detail_df: pd.DataFrame


@dataclass(frozen=True)
class _ExamplePayloadResult:
    db_path: str
    source_mode: Literal["snapshot", "live"]
    analysis_start_date: str | None
    analysis_end_date: str | None
    selected_groups: tuple[str, ...]
    summary_df: pd.DataFrame
    detail_df: pd.DataFrame


def _split_example_payload_result(
    result: _ExamplePayloadResult,
) -> tuple[dict[str, object], dict[str, pd.DataFrame]]:
    return (
        {
            "db_path": result.db_path,
            "source_mode": result.source_mode,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
            "selected_groups": list(result.selected_groups),
        },
        {
            "summary_df": result.summary_df,
            "detail_df": result.detail_df,
        },
    )


def _build_example_payload_result(
    metadata: dict[str, object],
    tables: dict[str, pd.DataFrame],
) -> _ExamplePayloadResult:
    return _ExamplePayloadResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(Literal["snapshot", "live"], metadata["source_mode"]),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        selected_groups=tuple(str(value) for value in cast(list[object], metadata["selected_groups"])),
        summary_df=tables["summary_df"],
        detail_df=tables["detail_df"],
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


def test_dataclass_research_bundle_adapter_roundtrip(tmp_path: Path) -> None:
    result = _ExampleDataclassResult(
        db_path=str(tmp_path / "market.duckdb"),
        source_mode="snapshot",
        analysis_start_date="2024-01-01",
        analysis_end_date="2024-12-31",
        future_horizons=(1, 5, 10),
        top_k_values=(5, 10),
        row_count=2,
        summary_df=pd.DataFrame([{"metric": "alpha", "value": 1.0}]),
        detail_df=pd.DataFrame([{"date": "2024-01-05", "code": "1111"}]),
    )

    bundle = write_dataclass_research_bundle(
        experiment_id="unit-test/dataclass-example",
        module="tests.example",
        function="run_dataclass_example",
        params={"top_k_values": [5, 10]},
        result=result,
        table_field_names=("summary_df", "detail_df"),
        summary_markdown="# Dataclass Example\n",
        output_root=tmp_path,
        run_id="20260406_220000_adapter00",
    )
    loaded = load_dataclass_research_bundle(
        bundle.bundle_dir,
        result_type=_ExampleDataclassResult,
        table_field_names=("summary_df", "detail_df"),
    )

    assert loaded.db_path == result.db_path
    assert loaded.source_mode == "snapshot"
    assert loaded.future_horizons == (1, 5, 10)
    assert loaded.top_k_values == (5, 10)
    assert loaded.row_count == 2
    assert loaded.summary_df.equals(result.summary_df)
    assert loaded.detail_df.equals(result.detail_df)


def test_payload_research_bundle_adapter_roundtrip(tmp_path: Path) -> None:
    result = _ExamplePayloadResult(
        db_path=str(tmp_path / "market.duckdb"),
        source_mode="live",
        analysis_start_date="2024-02-01",
        analysis_end_date="2024-02-29",
        selected_groups=("TOPIX100", "TOPIX500"),
        summary_df=pd.DataFrame([{"metric": "share", "value": 0.4}]),
        detail_df=pd.DataFrame([{"date": "2024-02-05", "code": "7203"}]),
    )

    bundle = write_payload_research_bundle(
        experiment_id="unit-test/payload-example",
        module="tests.example",
        function="run_payload_example",
        params={"selected_groups": ["TOPIX100", "TOPIX500"]},
        result=result,
        split_result_payload=_split_example_payload_result,
        summary_markdown="# Payload Example\n",
        published_summary={"title": "Payload Example"},
        output_root=tmp_path,
        run_id="20260418_220000_payload0",
    )
    loaded = load_payload_research_bundle(
        bundle.bundle_dir,
        build_result_from_payload=_build_example_payload_result,
        table_names=("summary_df", "detail_df"),
    )

    assert loaded.db_path == result.db_path
    assert loaded.source_mode == "live"
    assert loaded.analysis_start_date == "2024-02-01"
    assert loaded.analysis_end_date == "2024-02-29"
    assert loaded.selected_groups == ("TOPIX100", "TOPIX500")
    assert loaded.summary_df.equals(result.summary_df)
    assert loaded.detail_df.equals(result.detail_df)
    assert load_research_bundle_published_summary(bundle.bundle_dir) == {
        "title": "Payload Example"
    }
