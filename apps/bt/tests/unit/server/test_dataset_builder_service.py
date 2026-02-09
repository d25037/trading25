"""Tests for dataset_builder_service module."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.dataset_builder_service import (
    DatasetJobData,
    DatasetResult,
    _filter_stocks,
    start_dataset_build,
    dataset_job_manager,
)
from src.server.services.dataset_presets import PresetConfig


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


# --- start_dataset_build ---


@pytest.mark.asyncio
async def test_start_dataset_build_returns_job() -> None:
    """Job is created and returned."""
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/test.db"
    client = AsyncMock()
    client.get_paginated = AsyncMock(return_value=[])

    data = DatasetJobData(name="test", preset="quickTesting")

    # Clean up any active jobs
    for job in list(dataset_job_manager._jobs.values()):
        if job.status.value in ("pending", "running"):
            await dataset_job_manager.cancel_job(job.job_id)

    with patch("src.server.services.dataset_builder_service._build_dataset") as mock_build:
        mock_build.return_value = DatasetResult(success=True)
        job = await start_dataset_build(data, resolver, client)

    assert job is not None
    assert job.data.name == "test"
    assert job.data.preset == "quickTesting"

    # Cleanup
    if job.task:
        job.task.cancel()
        try:
            await job.task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_start_dataset_build_conflict() -> None:
    """Returns None when another job is active."""
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/test.db"
    client = AsyncMock()

    # Clean up any active jobs
    for job in list(dataset_job_manager._jobs.values()):
        if job.status.value in ("pending", "running"):
            await dataset_job_manager.cancel_job(job.job_id)

    # Create a blocking job
    data1 = DatasetJobData(name="first", preset="quickTesting")
    with patch("src.server.services.dataset_builder_service._build_dataset") as mock_build:
        future: asyncio.Future[DatasetResult] = asyncio.get_event_loop().create_future()
        mock_build.return_value = future
        job1 = await start_dataset_build(data1, resolver, client)

    assert job1 is not None

    # Second job should fail
    data2 = DatasetJobData(name="second", preset="quickTesting")
    job2 = await start_dataset_build(data2, resolver, client)
    assert job2 is None

    # Cleanup
    await dataset_job_manager.cancel_job(job1.job_id)


# --- DatasetResult ---


def test_dataset_result_defaults() -> None:
    r = DatasetResult(success=True)
    assert r.totalStocks == 0
    assert r.processedStocks == 0
    assert r.warnings is None
    assert r.errors is None
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
    assert d.resume is False


def test_dataset_job_data_resume() -> None:
    d = DatasetJobData(name="test", preset="quickTesting", resume=True)
    assert d.resume is True
