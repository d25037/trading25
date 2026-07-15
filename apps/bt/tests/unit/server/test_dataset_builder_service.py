"""Tests for dataset_builder_service module."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.application.services.dataset_builder_service import (
    DatasetJobData,
    DatasetResult,
    _filter_stocks,
    start_dataset_build,
    dataset_job_manager,
)
from src.application.services.dataset_presets import PresetConfig, get_preset


# --- _filter_stocks ---


def test_filter_stocks_by_market() -> None:
    stocks = [
        {"Code": "72030", "MktNm": "プライム", "ScaleCat": "TOPIX Core30"},
        {"Code": "99990", "MktNm": "グロース", "ScaleCat": ""},
    ]
    preset = PresetConfig(markets=["prime"])
    result = _filter_stocks(stocks, preset)
    assert len(result) == 1
    assert result[0]["Code"] == "72030"


def test_filter_stocks_by_scale_categories() -> None:
    stocks = [
        {"Code": "72030", "MktNm": "プライム", "ScaleCat": "TOPIX Core30"},
        {"Code": "66580", "MktNm": "プライム", "ScaleCat": "TOPIX Mid400"},
    ]
    preset = PresetConfig(markets=["prime"], scale_categories=["TOPIX Core30"])
    result = _filter_stocks(stocks, preset)
    assert len(result) == 1
    assert result[0]["Code"] == "72030"


def test_filter_stocks_exclude_scale() -> None:
    stocks = [
        {"Code": "72030", "MktNm": "プライム", "ScaleCat": "TOPIX Core30"},
        {"Code": "66580", "MktNm": "プライム", "ScaleCat": "TOPIX Mid400"},
    ]
    preset = PresetConfig(markets=["prime"], exclude_scale_categories=["TOPIX Core30"])
    result = _filter_stocks(stocks, preset)
    assert len(result) == 1
    assert result[0]["Code"] == "66580"


def test_filter_stocks_max_stocks() -> None:
    stocks = [
        {"Code": f"{i:05d}", "MktNm": "プライム", "ScaleCat": ""}
        for i in range(10)
    ]
    preset = PresetConfig(markets=["prime"], max_stocks=3)
    result = _filter_stocks(stocks, preset)
    assert len(result) == 3


def test_filter_stocks_empty() -> None:
    preset = PresetConfig(markets=["growth"])
    result = _filter_stocks([{"Code": "72030", "MktNm": "プライム"}], preset)
    assert len(result) == 0


def test_filter_stocks_multi_market() -> None:
    stocks = [
        {"Code": "72030", "MktNm": "プライム"},
        {"Code": "99990", "MktNm": "グロース"},
        {"Code": "55550", "MktNm": "スタンダード"},
    ]
    preset = PresetConfig(markets=["prime", "growth"])
    result = _filter_stocks(stocks, preset)
    assert len(result) == 2


def test_filter_stocks_topix500_includes_standard_when_scale_matches() -> None:
    stocks = [
        {"Code": "72030", "MktNm": "プライム", "ScaleCat": "TOPIX Core30"},
        {"Code": "47160", "MktNm": "スタンダード", "ScaleCat": "TOPIX Mid400"},
        {"Code": "85720", "MktNm": "スタンダード", "ScaleCat": "TOPIX Mid400"},
        {"Code": "99990", "MktNm": "グロース", "ScaleCat": ""},
    ]
    preset = get_preset("topix500")
    assert preset is not None

    result = _filter_stocks(stocks, preset)

    assert [row["Code"] for row in result] == ["72030", "47160", "85720"]


# --- start_dataset_build ---


@pytest.mark.asyncio
async def test_start_dataset_build_returns_job(tmp_path: Path) -> None:
    """Job is created and returned."""
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/test.db"
    client = AsyncMock()
    client.get_paginated = AsyncMock(return_value=[])
    source_path = tmp_path / "market.duckdb"
    source_path.touch()
    client.db_path = str(source_path)

    data = DatasetJobData(name="test", preset="quickTesting")

    # Clean up any active jobs
    for job in list(dataset_job_manager._jobs.values()):
        if job.status.value in ("pending", "running"):
            await dataset_job_manager.cancel_job(job.job_id)

    with patch("src.application.services.dataset_builder_service._build_dataset") as mock_build:
        mock_build.return_value = DatasetResult(success=True)
        job = await start_dataset_build(data, resolver, client, str(source_path))
        assert job is not None
        assert job.task is not None
        await job.task
        assert mock_build.call_args.kwargs["source_duckdb_path"] == str(
            source_path.resolve()
        )

    assert job.data.name == "test"
    assert job.data.preset == "quickTesting"

    # Cleanup
    if job.task and not job.task.done():
        job.task.cancel()


@pytest.mark.asyncio
async def test_start_dataset_build_rejects_missing_source_before_creating_job(
    tmp_path: Path,
) -> None:
    resolver = MagicMock()
    client = AsyncMock()
    client.db_path = str(tmp_path / "missing-market.duckdb")
    data = DatasetJobData(name="missing-source", preset="quickTesting")
    before = set(dataset_job_manager._jobs)

    with pytest.raises(FileNotFoundError):
        await start_dataset_build(data, resolver, client, client.db_path)

    assert set(dataset_job_manager._jobs) == before
    resolver.get_dataset_path.assert_not_called()


@pytest.mark.asyncio
async def test_start_dataset_build_rejects_source_that_differs_from_reader(
    tmp_path: Path,
) -> None:
    resolver = MagicMock()
    client = AsyncMock()
    reader_path = tmp_path / "reader-market.duckdb"
    requested_path = tmp_path / "other-market.duckdb"
    reader_path.touch()
    requested_path.touch()
    client.db_path = str(reader_path)
    data = DatasetJobData(name="split-brain-source", preset="quickTesting")
    before = set(dataset_job_manager._jobs)

    with pytest.raises(ValueError, match="must match market_reader.db_path"):
        await start_dataset_build(data, resolver, client, str(requested_path))

    assert set(dataset_job_manager._jobs) == before


@pytest.mark.asyncio
async def test_start_dataset_build_conflict(tmp_path: Path) -> None:
    """Returns None when another job is active."""
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/test.db"
    client = AsyncMock()
    source_path = tmp_path / "market.duckdb"
    source_path.touch()
    client.db_path = str(source_path)

    # Clean up any active jobs
    for job in list(dataset_job_manager._jobs.values()):
        if job.status.value in ("pending", "running"):
            await dataset_job_manager.cancel_job(job.job_id)

    # Create a blocking job
    data1 = DatasetJobData(name="first", preset="quickTesting")
    with patch("src.application.services.dataset_builder_service._build_dataset") as mock_build:
        future: asyncio.Future[DatasetResult] = asyncio.get_event_loop().create_future()
        mock_build.return_value = future
        job1 = await start_dataset_build(data1, resolver, client, str(source_path))

    assert job1 is not None

    # Second job should fail
    data2 = DatasetJobData(name="second", preset="quickTesting")
    job2 = await start_dataset_build(data2, resolver, client, str(source_path))
    assert job2 is None

    # Cleanup
    await dataset_job_manager.cancel_job(job1.job_id)


# --- DatasetResult ---


def test_dataset_result_defaults() -> None:
    r = DatasetResult(success=True)
    assert r.totalStocks == 0
    assert r.processedStocks == 0
    assert r.warnings == []
    assert r.errors == []
    assert r.outputPath == ""


def test_dataset_result_with_data() -> None:
    r = DatasetResult(
        success=True,
        totalStocks=100,
        processedStocks=95,
        warnings=["Stock 1234: timeout"],
        outputPath="/data/test.db",
    )
    assert r.totalStocks == 100
    assert r.warnings is not None
    assert len(r.warnings) == 1


# --- DatasetJobData ---


def test_dataset_job_data() -> None:
    d = DatasetJobData(name="test", preset="quickTesting")
    assert d.overwrite is False
