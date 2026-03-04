from __future__ import annotations

import csv
import gzip
import io
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from src.application.services import jquants_bulk_service as bulk_module
from src.application.services.jquants_bulk_service import JQuantsBulkService, _parse_date


def _gzip_csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    if not rows:
        return b""
    buffer = io.BytesIO()
    with gzip.open(buffer, mode="wt", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return buffer.getvalue()


async def _noop_downloader(_url: str) -> bytes:
    return b""


class _BulkClient:
    def __init__(
        self,
        *,
        list_payload: list[dict[str, Any]],
        signed_urls: dict[str, str],
        list_response: dict[str, Any] | None = None,
        bulk_get_payloads: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        self._list_payload = list_payload
        self._signed_urls = signed_urls
        self._list_response = list_response
        self._bulk_get_payloads = bulk_get_payloads or {}

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self.calls.append((path, params))
        if path == "/bulk/list":
            if self._list_response is not None:
                return dict(self._list_response)
            return {"data": self._list_payload}
        if path == "/bulk/get":
            key = str((params or {}).get("key") or "")
            if key in self._bulk_get_payloads:
                return dict(self._bulk_get_payloads[key])
            return {"url": self._signed_urls[key]}
        raise RuntimeError(f"unexpected path: {path}")


@pytest.mark.asyncio
async def test_bulk_service_build_plan_and_cache_reuse(tmp_path: Path) -> None:
    rows = [{"Code": "72030", "Date": "2026-02-10", "O": "1", "H": "2", "L": "1", "C": "2", "Vo": "1000"}]
    payload = _gzip_csv_bytes(rows)
    key = "equities_bars_daily_20260210.csv.gz"
    client = _BulkClient(
        list_payload=[{"Key": key, "LastModified": "2026-02-11T00:00:00Z", "Size": len(payload)}],
        signed_urls={key: "https://signed.local/equities-20260210.csv.gz"},
    )

    download_calls: list[str] = []

    async def _downloader(url: str) -> bytes:
        download_calls.append(url)
        return payload

    service = JQuantsBulkService(
        client,  # type: ignore[arg-type]
        cache_dir=tmp_path / "bulk-cache",
        downloader=_downloader,
    )

    plan = await service.build_plan(
        endpoint="/equities/bars/daily",
        exact_dates=["2026-02-10"],
    )
    assert len(plan.files) == 1
    assert plan.estimated_api_calls == 3
    assert plan.estimated_cache_hits == 0
    assert plan.estimated_cache_misses == 1

    result_first = await service.fetch_with_plan(plan)
    assert result_first.api_calls == 2
    assert result_first.cache_hits == 0
    assert result_first.cache_misses == 1
    assert result_first.rows[0]["Code"] == "72030"
    assert result_first.rows[0]["Date"] == "2026-02-10"
    assert download_calls == ["https://signed.local/equities-20260210.csv.gz"]

    plan_second = await service.build_plan(
        endpoint="/equities/bars/daily",
        exact_dates=["2026-02-10"],
    )
    assert plan_second.estimated_cache_hits == 1
    assert plan_second.estimated_cache_misses == 0
    assert plan_second.estimated_api_calls == 1

    result_second = await service.fetch_with_plan(plan_second)
    assert result_second.api_calls == 0
    assert result_second.cache_hits == 1
    assert result_second.cache_misses == 0
    assert download_calls == ["https://signed.local/equities-20260210.csv.gz"]
    assert len([call for call in client.calls if call[0] == "/bulk/get"]) == 1


@pytest.mark.asyncio
async def test_bulk_service_selects_monthly_file_by_exact_date(tmp_path: Path) -> None:
    monthly_key = "fins_summary_historical_202602.csv.gz"
    daily_key = "fins_summary_live_20260301.csv.gz"
    client = _BulkClient(
        list_payload=[
            {"Key": monthly_key, "LastModified": "2026-03-01T00:00:00Z", "Size": 100},
            {"Key": daily_key, "LastModified": "2026-03-01T00:00:00Z", "Size": 100},
        ],
        signed_urls={monthly_key: "https://signed.local/monthly.csv.gz", daily_key: "https://signed.local/daily.csv.gz"},
    )
    async def _never_downloader(_url: str) -> bytes:
        raise AssertionError("download should not run in plan-only test")

    service = JQuantsBulkService(
        client,  # type: ignore[arg-type]
        cache_dir=tmp_path / "bulk-cache",
        downloader=_never_downloader,
    )

    plan = await service.build_plan(
        endpoint="/fins/summary",
        exact_dates=["2026-02-15"],
    )
    assert [f.key for f in plan.files] == [monthly_key]


@pytest.mark.asyncio
async def test_bulk_service_fetch_with_callback_without_accumulating_rows(tmp_path: Path) -> None:
    key = "equities_bars_daily_20260210.csv.gz"
    rows = [{"Code": "72030", "Date": "2026-02-10", "O": "1", "H": "2", "L": "1", "C": "2", "Vo": "1000"}]
    payload = _gzip_csv_bytes(rows)
    client = _BulkClient(
        list_payload=[{"Key": key, "LastModified": "2026-02-11T00:00:00Z", "Size": len(payload)}],
        signed_urls={key: "https://signed.local/equities-20260210.csv.gz"},
    )

    async def _downloader(_url: str) -> bytes:
        return payload

    service = JQuantsBulkService(
        client,  # type: ignore[arg-type]
        cache_dir=tmp_path / "bulk-cache",
        downloader=_downloader,
    )

    plan = await service.build_plan(endpoint="/equities/bars/daily")
    seen_batches: list[list[dict[str, Any]]] = []

    async def _on_rows_batch(batch_rows: list[dict[str, Any]], _file_info: Any) -> None:
        seen_batches.append(batch_rows)

    result = await service.fetch_with_plan(
        plan,
        on_rows_batch=_on_rows_batch,
        accumulate_rows=False,
    )

    assert len(seen_batches) == 1
    assert seen_batches[0][0]["Code"] == "72030"
    assert result.rows == []
    assert result.api_calls == 2


@pytest.mark.asyncio
async def test_bulk_service_wraps_signed_url_download_error(tmp_path: Path) -> None:
    key = "indices_bars_daily_20260210.csv.gz"
    payload_rows = [{"Date": "2026-02-10", "Code": "0000", "O": "1", "H": "2", "L": "1", "C": "2"}]
    payload = _gzip_csv_bytes(payload_rows)
    client = _BulkClient(
        list_payload=[{"Key": key, "LastModified": "2026-02-11T00:00:00Z", "Size": len(payload)}],
        signed_urls={key: "https://signed.local/indices-20260210.csv.gz"},
    )

    async def _failing_downloader(_url: str) -> bytes:
        raise RuntimeError("network down")

    service = JQuantsBulkService(
        client,  # type: ignore[arg-type]
        cache_dir=tmp_path / "bulk-cache",
        downloader=_failing_downloader,
    )

    plan = await service.build_plan(endpoint="/indices/bars/daily")
    with pytest.raises(RuntimeError, match="bulk signed-url download failed"):
        await service.fetch_with_plan(plan)


@pytest.mark.asyncio
async def test_bulk_service_raises_when_bulk_get_url_is_missing(tmp_path: Path) -> None:
    key = "indices_bars_daily_20260210.csv.gz"
    client = _BulkClient(
        list_payload=[{"Key": key, "LastModified": "2026-02-11T00:00:00Z", "Size": 10}],
        signed_urls={key: "https://signed.local/unused.csv.gz"},
        bulk_get_payloads={key: {}},
    )
    service = JQuantsBulkService(
        client,  # type: ignore[arg-type]
        cache_dir=tmp_path / "bulk-cache",
        downloader=_noop_downloader,
    )

    plan = await service.build_plan(endpoint="/indices/bars/daily")
    with pytest.raises(RuntimeError, match="did not return a valid url"):
        await service.fetch_with_plan(plan)


@pytest.mark.asyncio
async def test_bulk_service_plan_and_fetch_adds_list_call(tmp_path: Path) -> None:
    key = "equities_bars_daily_20260210.csv.gz"
    payload = _gzip_csv_bytes([{"Code": "72030", "Date": "2026-02-10", "O": "1", "H": "2", "L": "1", "C": "2", "Vo": "1000"}])
    client = _BulkClient(
        list_payload=[{"Key": key, "LastModified": "2026-02-11T00:00:00Z", "Size": len(payload)}],
        signed_urls={key: "https://signed.local/equities-20260210.csv.gz"},
    )

    async def _downloader(_url: str) -> bytes:
        return payload

    service = JQuantsBulkService(
        client,  # type: ignore[arg-type]
        cache_dir=tmp_path / "bulk-cache",
        downloader=_downloader,
    )

    _plan, result = await service.plan_and_fetch(endpoint="/equities/bars/daily")
    assert result.api_calls == 3  # /bulk/list + /bulk/get + signed-url


@pytest.mark.asyncio
async def test_bulk_service_build_plan_accepts_files_key_payload(tmp_path: Path) -> None:
    monthly_key = "fins_summary_historical_202602.csv.gz"
    client = _BulkClient(
        list_payload=[],
        signed_urls={},
        list_response={"files": [{"Key": monthly_key, "LastModified": "2026-03-01T00:00:00Z", "Size": "abc"}]},
    )
    service = JQuantsBulkService(
        client,  # type: ignore[arg-type]
        cache_dir=tmp_path / "bulk-cache",
        downloader=_noop_downloader,
    )

    plan = await service.build_plan(endpoint="/fins/summary", date_from="2026-02-01", date_to="2026-02-28")
    assert [f.key for f in plan.files] == [monthly_key]
    assert plan.files[0].size == 0


@pytest.mark.asyncio
async def test_bulk_service_select_files_includes_unknown_range_for_safety(tmp_path: Path) -> None:
    unknown_key = "fins_summary_live_latest.csv.gz"
    out_of_range_key = "fins_summary_historical_202601.csv.gz"
    client = _BulkClient(
        list_payload=[
            {"Key": unknown_key, "LastModified": "2026-03-01T00:00:00Z", "Size": 1},
            {"Key": out_of_range_key, "LastModified": "2026-03-01T00:00:00Z", "Size": 1},
        ],
        signed_urls={},
    )
    service = JQuantsBulkService(
        client,  # type: ignore[arg-type]
        cache_dir=tmp_path / "bulk-cache",
        downloader=lambda _url: b"",  # type: ignore[arg-type]
    )

    plan = await service.build_plan(endpoint="/fins/summary", date_from="2026-02-01", date_to="2026-02-28")
    assert [f.key for f in plan.files] == [unknown_key]


def test_bulk_service_cache_meta_invalid_json_and_non_dict(tmp_path: Path) -> None:
    client = _BulkClient(list_payload=[], signed_urls={})
    service = JQuantsBulkService(client, cache_dir=tmp_path / "bulk-cache", downloader=_noop_downloader)  # type: ignore[arg-type]
    key = "equities_bars_daily_20260210.csv.gz"
    meta_path = service._meta_cache_path(key)

    meta_path.write_text("{not-json", encoding="utf-8")
    assert service._read_cache_meta(key) is None

    meta_path.write_text("[]", encoding="utf-8")
    assert service._read_cache_meta(key) is None


def test_parse_date_helper_supports_both_formats() -> None:
    assert _parse_date("2026-02-10")
    assert _parse_date("20260210")
    assert _parse_date("") is None
    assert _parse_date("not-a-date") is None


@pytest.mark.asyncio
async def test_bulk_service_download_bytes_records_metrics(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class _Response:
        content = b"ok"

        def raise_for_status(self) -> None:
            return None

    class _AsyncClient:
        async def __aenter__(self) -> "_AsyncClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
            return None

        async def get(self, url: str) -> _Response:
            assert url == "https://signed.local/file.csv.gz"
            return _Response()

    client = _BulkClient(list_payload=[], signed_urls={})
    service = JQuantsBulkService(client, cache_dir=tmp_path / "bulk-cache", downloader=_noop_downloader)  # type: ignore[arg-type]

    recorder = MagicMock()
    monkeypatch.setattr(bulk_module, "metrics_recorder", recorder)
    monkeypatch.setattr(bulk_module.httpx, "AsyncClient", lambda timeout=60.0: _AsyncClient())

    data = await service._download_bytes("https://signed.local/file.csv.gz")
    assert data == b"ok"
    recorder.record_jquants_fetch.assert_called_once_with("/bulk/download")
