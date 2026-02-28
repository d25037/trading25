from __future__ import annotations

import asyncio
import hashlib
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

import src.application.services.dataset_builder_service as dataset_builder_service
from src.application.services.dataset_builder_service import (
    DatasetJobData,
    DatasetResult,
    _build_dataset,
    _convert_stocks,
    start_dataset_build,
)
from src.application.services.dataset_presets import PresetConfig
from src.application.services.generic_job_manager import GenericJobManager
from src.entrypoints.http.schemas.job import JobStatus


@pytest.fixture
def isolated_dataset_manager(monkeypatch):
    manager: GenericJobManager = GenericJobManager()
    monkeypatch.setattr(dataset_builder_service, "dataset_job_manager", manager)
    return manager


@pytest.fixture(autouse=True)
def stub_manifest_writer_for_dummy_db_paths(monkeypatch, request):
    if request.node.name in {
        "test_build_dataset_writes_manifest_v1",
        "test_build_dataset_rerun_keeps_logical_checksum_reproducible",
    }:
        return

    def _fake_write_dataset_manifest(
        *,
        db_path: str,
        dataset_name: str,
        preset_name: str,
        manifest_path=None,
    ) -> str:
        del dataset_name, preset_name
        if manifest_path is not None:
            return str(manifest_path)
        return str(dataset_builder_service._manifest_path_for_db(db_path))

    monkeypatch.setattr(dataset_builder_service, "_write_dataset_manifest", _fake_write_dataset_manifest)


async def _create_job(
    manager: GenericJobManager,
    *,
    name: str = "dataset",
    preset: str = "quickTesting",
    resume: bool = False,
    timeout_minutes: int = 35,
):
    job = await manager.create_job(
        DatasetJobData(name=name, preset=preset, resume=resume, timeout_minutes=timeout_minutes)
    )
    assert job is not None
    return job


def _daily_bar_row() -> dict[str, int | str]:
    return {
        "Date": "2026-01-01",
        "O": 1,
        "H": 2,
        "L": 1,
        "C": 2,
        "Vo": 100,
    }


def _master_row(code: str, name: str) -> dict[str, str]:
    return {"Code": code, "MktNm": "プライム", "CoName": name}


def test_convert_stocks_maps_jquants_fields():
    rows = _convert_stocks(
        [
            {
                "Code": "72030",
                "CoName": "Toyota",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "ScaleCat": "TOPIX Core30",
                "Date": "2020-01-01",
            }
        ]
    )
    assert len(rows) == 1
    assert rows[0]["code"] == "7203"
    assert rows[0]["company_name"] == "Toyota"
    assert rows[0]["market_name"] == "プライム"


@pytest.mark.asyncio
async def test_build_dataset_returns_error_for_unknown_preset(isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="unknown")
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/unknown.db"
    client = AsyncMock()

    result = await _build_dataset(job, resolver, client)
    assert result.success is False
    assert result.errors == ["Unknown preset: unknown"]


@pytest.mark.asyncio
async def test_build_dataset_returns_cancelled_before_master_fetch(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="quickTesting")
    job.cancelled.set()
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/cancelled.db"
    client = AsyncMock()

    monkeypatch.setattr(
        dataset_builder_service,
        "get_preset",
        lambda name: PresetConfig(markets=["prime"]),
    )

    result = await _build_dataset(job, resolver, client)
    assert result.success is False
    assert result.errors == ["Cancelled"]
    assert client.get_paginated.await_count == 0


@pytest.mark.asyncio
async def test_build_dataset_returns_error_when_no_stock_matches_preset(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="quickTesting")
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/empty.db"

    client = AsyncMock()
    client.get_paginated = AsyncMock(return_value=[{"Code": "99990", "MktNm": "グロース"}])

    monkeypatch.setattr(
        dataset_builder_service,
        "get_preset",
        lambda name: PresetConfig(markets=["prime"]),
    )

    result = await _build_dataset(job, resolver, client)
    assert result.success is False
    assert result.errors == ["No stocks matched the preset filters"]


@pytest.mark.asyncio
async def test_build_dataset_success_with_warnings(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="full")
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/full.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=True,
        include_statements=True,
        include_margin=True,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)
    monkeypatch.setattr(
        dataset_builder_service,
        "convert_fins_summary_rows",
        lambda data, default_code: [{"code": default_code, "disclosed_date": "2026-01-01"}] if data else [],
    )

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.db_path = db_path
            self.calls: list[str] = []
            self.closed = False
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            self.calls.append("stocks")
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            self.calls.append(f"info:{key}")

        def upsert_stock_data(self, rows):
            self.calls.append("stock_data")
            return len(rows)

        def upsert_topix_data(self, rows):
            self.calls.append("topix")
            return len(rows)

        def upsert_statements(self, rows):
            self.calls.append("statements")
            return len(rows)

        def upsert_margin_data(self, rows):
            self.calls.append("margin")
            return len(rows)

        def close(self):
            self.closed = True

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [
                _master_row("11110", "A"),
                _master_row("22220", "B"),
            ]
        if path == "/equities/bars/daily":
            if params and params.get("code") == "11110":
                return [_daily_bar_row()]
            raise RuntimeError("bars failed")
        if path == "/indices/bars/daily/topix":
            raise RuntimeError("topix failed")
        if path == "/fins/summary":
            if params and params.get("code") == "11110":
                return [{"Code": "11110", "DisclosedDate": "2026-01-01"}]
            raise RuntimeError("fins failed")
        if path == "/markets/margin-interest":
            if params and params.get("code") == "11110":
                return [{"Date": "2026-01-01", "LongVol": 10, "ShrtVol": 5}]
            raise RuntimeError("margin failed")
        return []

    client = AsyncMock()
    client.get_paginated.side_effect = fake_get_paginated

    result = await _build_dataset(job, resolver, client)

    assert result.success is True
    assert result.totalStocks == 2
    assert result.processedStocks == 1
    assert result.outputPath == "/tmp/full.db"
    assert result.warnings is not None
    assert any("Stock 2222" in warning for warning in result.warnings)
    assert any("TOPIX:" in warning for warning in result.warnings)
    assert any("Statements 2222" in warning for warning in result.warnings)
    assert any("Margin 2222" in warning for warning in result.warnings)

    writer = DummyWriter.instances[-1]
    assert "stocks" in writer.calls
    assert "stock_data" in writer.calls
    assert "statements" in writer.calls
    assert "margin" in writer.calls
    assert writer.closed is True


@pytest.mark.asyncio
async def test_build_dataset_returns_partial_result_when_cancelled_during_stock_loop(
    monkeypatch,
    isolated_dataset_manager,
):
    job = await _create_job(isolated_dataset_manager, preset="quick")
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/partial.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.closed = False
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def close(self):
            self.closed = True

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [
                _master_row("11110", "A"),
                _master_row("22220", "B"),
            ]
        if path == "/equities/bars/daily":
            job.cancelled.set()
            return [_daily_bar_row()]
        return []

    client = AsyncMock()
    client.get_paginated.side_effect = fake_get_paginated

    result = await _build_dataset(job, resolver, client)
    assert result.success is False
    assert result.processedStocks == 1
    assert result.errors == ["Cancelled"]
    assert DummyWriter.instances[-1].closed is True


@pytest.mark.asyncio
async def test_build_dataset_writes_manifest_v1(monkeypatch, isolated_dataset_manager, tmp_path):
    job = await _create_job(isolated_dataset_manager, preset="quick")
    resolver = MagicMock()
    db_path = tmp_path / "manifest.db"
    resolver.get_db_path.return_value = str(db_path)

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        return []

    client = AsyncMock()
    client.get_paginated.side_effect = fake_get_paginated

    result = await _build_dataset(job, resolver, client)
    assert result.success is True

    manifest_path = tmp_path / "manifest.manifest.v1.json"
    assert manifest_path.exists() is True
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["schemaVersion"] == 1
    assert manifest["dataset"]["name"] == "dataset"
    assert manifest["dataset"]["dbFile"] == "manifest.db"
    assert manifest["counts"]["stocks"] == 1
    assert manifest["coverage"]["stocksWithQuotes"] == 1
    assert manifest["checksums"]["datasetDbSha256"]
    assert manifest["checksums"]["logicalSha256"]
    assert manifest["checksums"]["datasetDbSha256"] == hashlib.sha256(db_path.read_bytes()).hexdigest()


@pytest.mark.asyncio
async def test_build_dataset_rerun_keeps_logical_checksum_reproducible(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    resolver = MagicMock()
    db_path = tmp_path / "repro.db"
    resolver.get_db_path.return_value = str(db_path)

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        return []

    client = AsyncMock()
    client.get_paginated.side_effect = fake_get_paginated

    job_first = await _create_job(isolated_dataset_manager, name="repro", preset="quick")
    first_result = await _build_dataset(job_first, resolver, client)
    assert first_result.success is True
    isolated_dataset_manager.complete_job(job_first.job_id, first_result)
    first_manifest_path = tmp_path / "repro.manifest.v1.json"
    first_manifest = json.loads(first_manifest_path.read_text(encoding="utf-8"))

    job_second = await _create_job(isolated_dataset_manager, name="repro", preset="quick")
    second_result = await _build_dataset(job_second, resolver, client)
    assert second_result.success is True
    isolated_dataset_manager.complete_job(job_second.job_id, second_result)
    second_manifest = json.loads(first_manifest_path.read_text(encoding="utf-8"))

    assert first_manifest["checksums"]["logicalSha256"] == second_manifest["checksums"]["logicalSha256"]
    assert first_manifest["counts"] == second_manifest["counts"]
    assert first_manifest["coverage"] == second_manifest["coverage"]


@pytest.mark.asyncio
async def test_build_dataset_raises_when_manifest_generation_fails(monkeypatch, isolated_dataset_manager, tmp_path):
    job = await _create_job(isolated_dataset_manager, preset="quick")
    resolver = MagicMock()
    resolver.get_db_path.return_value = str(tmp_path / "manifest-fail.db")

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    def _raise_manifest_error(*, db_path: str, dataset_name: str, preset_name: str, manifest_path=None) -> str:
        del db_path, dataset_name, preset_name, manifest_path
        raise RuntimeError("manifest failed")

    monkeypatch.setattr(dataset_builder_service, "_write_dataset_manifest", _raise_manifest_error)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        return []

    client = AsyncMock()
    client.get_paginated.side_effect = fake_get_paginated

    with pytest.raises(RuntimeError, match="manifest failed"):
        await _build_dataset(job, resolver, client)


@pytest.mark.asyncio
async def test_build_dataset_handles_empty_stock_rows_and_progress_mod10(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="quick")
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/no_rows.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    progress_messages: list[str] = []
    original_update_progress = isolated_dataset_manager.update_progress

    def _capture_progress(job_id, progress):
        progress_messages.append(progress.message)
        original_update_progress(job_id, progress)

    monkeypatch.setattr(dataset_builder_service.dataset_job_manager, "update_progress", _capture_progress)

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.stock_data_calls = 0
            self.closed = False
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            self.stock_data_calls += 1
            return len(rows)

        def close(self):
            self.closed = True

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [
                {"Code": f"{i:04d}0", "MktNm": "プライム", "CoName": f"S{i}"}
                for i in range(10)
            ]
        if path == "/equities/bars/daily":
            return []
        return []

    client = AsyncMock()
    client.get_paginated.side_effect = fake_get_paginated

    result = await _build_dataset(job, resolver, client)
    assert result.success is True
    assert result.processedStocks == 10
    assert any("Stock data: 10/10" in message for message in progress_messages)
    writer = DummyWriter.instances[-1]
    assert writer.stock_data_calls == 0
    assert writer.closed is True


@pytest.mark.asyncio
async def test_build_dataset_resume_skips_existing_stock_data_codes(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="quick", resume=True)
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/resume.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        def __init__(self, db_path: str):
            return None

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def get_existing_stock_data_codes(self):
            return {"1111"}

        def upsert_stock_data(self, rows):
            return len(rows)

        def close(self):
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    fetched_codes: list[str] = []

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [
                _master_row("11110", "A"),
                _master_row("22220", "B"),
            ]
        if path == "/equities/bars/daily":
            code = params.get("code") if params else None
            if code is not None:
                fetched_codes.append(str(code))
            return [_daily_bar_row()]
        return []

    client = AsyncMock()
    client.get_paginated.side_effect = fake_get_paginated

    result = await _build_dataset(job, resolver, client)
    assert result.success is True
    assert fetched_codes == ["22220"]
    assert result.processedStocks == 1


@pytest.mark.asyncio
async def test_build_dataset_topix_skips_fetch_when_cancelled_before_topix(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="topix")
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/topix_skip.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=True,
        include_statements=False,
        include_margin=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        def __init__(self, db_path: str):
            self.closed = False

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def close(self):
            self.closed = True

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            job.cancelled.set()
            return [_daily_bar_row()]
        if path == "/indices/bars/daily/topix":
            raise AssertionError("topix should not be fetched when cancelled")
        return []

    client = AsyncMock()
    client.get_paginated.side_effect = fake_get_paginated

    result = await _build_dataset(job, resolver, client)
    assert result.success is True
    assert result.processedStocks == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("topix_rows,expected_topix_calls", [([{"Date": "2026-01-01", "O": 1, "H": 2, "L": 1, "C": 2}], 1), ([], 0)])
async def test_build_dataset_topix_rows_true_and_false_branches(
    monkeypatch,
    isolated_dataset_manager,
    topix_rows,
    expected_topix_calls,
):
    job = await _create_job(isolated_dataset_manager, preset="topix")
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/topix_rows.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=True,
        include_statements=False,
        include_margin=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.topix_calls = 0
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def upsert_topix_data(self, rows):
            self.topix_calls += 1
            return len(rows)

        def close(self):
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        if path == "/indices/bars/daily/topix":
            return topix_rows
        return []

    client = AsyncMock()
    client.get_paginated.side_effect = fake_get_paginated

    result = await _build_dataset(job, resolver, client)
    assert result.success is True
    writer = DummyWriter.instances[-1]
    assert writer.topix_calls == expected_topix_calls


@pytest.mark.asyncio
async def test_build_dataset_fetches_sector_indices_from_catalog(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="indices")
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/indices.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=True,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)
    monkeypatch.setattr(dataset_builder_service, "get_index_catalog_codes", lambda: {"0040", "0050", "0500"})

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.indices_calls = 0
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def upsert_indices_data(self, rows):
            self.indices_calls += 1
            return len(rows)

        def close(self):
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        if path == "/indices/bars/daily":
            code = params.get("code") if params else None
            if code in {"0040", "0050"}:
                return [{"Date": "2026-01-01", "Code": code, "O": 1, "H": 2, "L": 1, "C": 2}]
            return []
        return []

    client = AsyncMock()
    client.get_paginated.side_effect = fake_get_paginated

    result = await _build_dataset(job, resolver, client)
    assert result.success is True
    writer = DummyWriter.instances[-1]
    assert writer.indices_calls == 2


@pytest.mark.asyncio
async def test_build_dataset_skips_incomplete_ohlcv_rows_without_failing_stock(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="ohlcv")
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/ohlcv.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.stock_data_rows = 0
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            self.stock_data_rows += len(rows)
            return len(rows)

        def close(self):
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [
                _daily_bar_row(),
                {"Date": "2026-01-02", "O": None, "H": 2, "L": 1, "C": 2, "Vo": 100},
            ]
        return []

    client = AsyncMock()
    client.get_paginated.side_effect = fake_get_paginated

    result = await _build_dataset(job, resolver, client)
    assert result.success is True
    assert result.processedStocks == 1
    assert result.warnings is not None
    assert any("Skipped incomplete OHLCV rows" in warning for warning in result.warnings)
    writer = DummyWriter.instances[-1]
    assert writer.stock_data_rows == 1


@pytest.mark.asyncio
async def test_build_dataset_statements_handles_empty_rows_and_cancel_break(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="statements")
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/statements_break.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=True,
        include_margin=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)
    monkeypatch.setattr(dataset_builder_service, "convert_fins_summary_rows", lambda data, default_code: [])

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.statement_calls = 0
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def upsert_statements(self, rows):
            self.statement_calls += 1
            return len(rows)

        def close(self):
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [
                _master_row("11110", "A"),
                _master_row("22220", "B"),
            ]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        if path == "/fins/summary":
            job.cancelled.set()
            return []
        return []

    client = AsyncMock()
    client.get_paginated.side_effect = fake_get_paginated

    result = await _build_dataset(job, resolver, client)
    assert result.success is True
    writer = DummyWriter.instances[-1]
    assert writer.statement_calls == 0


@pytest.mark.asyncio
async def test_build_dataset_margin_handles_empty_rows_and_cancel_break(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="margin")
    resolver = MagicMock()
    resolver.get_db_path.return_value = "/tmp/margin_break.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=True,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.margin_calls = 0
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def upsert_margin_data(self, rows):
            self.margin_calls += 1
            return len(rows)

        def close(self):
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [
                _master_row("11110", "A"),
                _master_row("22220", "B"),
            ]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        if path == "/markets/margin-interest":
            job.cancelled.set()
            return []
        return []

    client = AsyncMock()
    client.get_paginated.side_effect = fake_get_paginated

    result = await _build_dataset(job, resolver, client)
    assert result.success is True
    writer = DummyWriter.instances[-1]
    assert writer.margin_calls == 0


@pytest.mark.asyncio
async def test_start_dataset_build_marks_failed_on_timeout(monkeypatch, isolated_dataset_manager):
    data = DatasetJobData(name="timeout", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()

    async def fake_wait_for(coro, timeout):
        coro.close()
        raise asyncio.TimeoutError()

    monkeypatch.setattr(dataset_builder_service.asyncio, "wait_for", fake_wait_for)

    job = await start_dataset_build(data, resolver, client)
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.FAILED
    assert stored.error == "Dataset build timed out after 35 minutes"


@pytest.mark.asyncio
async def test_start_dataset_build_marks_failed_on_custom_timeout(monkeypatch, isolated_dataset_manager):
    data = DatasetJobData(name="timeout-custom", preset="quickTesting", timeout_minutes=90)
    resolver = MagicMock()
    client = AsyncMock()
    timeout_values: list[int] = []

    async def fake_wait_for(coro, timeout):
        coro.close()
        timeout_values.append(timeout)
        raise asyncio.TimeoutError()

    monkeypatch.setattr(dataset_builder_service.asyncio, "wait_for", fake_wait_for)

    job = await start_dataset_build(data, resolver, client)
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.FAILED
    assert stored.error == "Dataset build timed out after 90 minutes"
    assert timeout_values == [90 * 60]


@pytest.mark.asyncio
async def test_start_dataset_build_marks_failed_on_unexpected_error(monkeypatch, isolated_dataset_manager):
    data = DatasetJobData(name="error", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()

    async def fake_wait_for(coro, timeout):
        coro.close()
        raise RuntimeError("dataset exploded")

    monkeypatch.setattr(dataset_builder_service.asyncio, "wait_for", fake_wait_for)

    job = await start_dataset_build(data, resolver, client)
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.FAILED
    assert stored.error == "dataset exploded"


@pytest.mark.asyncio
async def test_start_dataset_build_handles_cancelled_error(monkeypatch, isolated_dataset_manager):
    data = DatasetJobData(name="cancelled", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()

    async def fake_wait_for(coro, timeout):
        coro.close()
        raise asyncio.CancelledError()

    monkeypatch.setattr(dataset_builder_service.asyncio, "wait_for", fake_wait_for)

    job = await start_dataset_build(data, resolver, client)
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.PENDING
    assert stored.error is None


@pytest.mark.asyncio
async def test_start_dataset_build_skips_complete_when_job_cancelled(monkeypatch, isolated_dataset_manager):
    data = DatasetJobData(name="skip-complete", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()

    async def fake_build(job, resolver, client):
        job.cancelled.set()
        return DatasetResult(success=True, totalStocks=1, processedStocks=1)

    monkeypatch.setattr(dataset_builder_service, "_build_dataset", fake_build)

    job = await start_dataset_build(data, resolver, client)
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.PENDING
    assert stored.result is None


@pytest.mark.asyncio
async def test_start_dataset_build_completes_when_not_cancelled(monkeypatch, isolated_dataset_manager):
    data = DatasetJobData(name="completed", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()

    async def fake_build(job, resolver, client):
        return DatasetResult(success=True, totalStocks=1, processedStocks=1, outputPath="/tmp/completed.db")

    monkeypatch.setattr(dataset_builder_service, "_build_dataset", fake_build)

    job = await start_dataset_build(data, resolver, client)
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.COMPLETED
    assert stored.result is not None
    assert stored.result.success is True
