"""J-Quants bulk download service.

This service wraps `/bulk/list` + `/bulk/get` + signed-url download and provides
CSV(gzip) parsing with local cache.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx
from loguru import logger

from src.infrastructure.external_api.clients.jquants_client import JQuantsAsyncClient
from src.shared.observability.metrics import metrics_recorder
from src.shared.paths import get_cache_dir


@dataclass(frozen=True)
class BulkFileInfo:
    key: str
    last_modified: str
    size: int
    range_start: date | None
    range_end: date | None


@dataclass(frozen=True)
class BulkFetchPlan:
    endpoint: str
    files: list[BulkFileInfo]
    list_api_calls: int
    estimated_api_calls: int
    estimated_cache_hits: int
    estimated_cache_misses: int


@dataclass
class BulkFetchResult:
    rows: list[dict[str, Any]]
    api_calls: int
    cache_hits: int
    cache_misses: int
    selected_files: int


class JQuantsBulkService:
    """Bulk API helper used by sync strategies."""

    def __init__(
        self,
        client: JQuantsAsyncClient,
        *,
        cache_dir: Path | None = None,
        downloader: Callable[[str], Awaitable[bytes]] | None = None,
    ) -> None:
        self._client = client
        self._cache_dir = cache_dir or (get_cache_dir() / "jquants-bulk")
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._downloader = downloader or self._download_bytes

    async def build_plan(
        self,
        *,
        endpoint: str,
        date_from: str | None = None,
        date_to: str | None = None,
        exact_dates: list[str] | None = None,
    ) -> BulkFetchPlan:
        body = await self._client.get("/bulk/list", params={"endpoint": endpoint})
        list_api_calls = 1

        files = self._extract_files(body)
        selected_files = self._select_files(
            files,
            date_from=date_from,
            date_to=date_to,
            exact_dates=exact_dates,
        )

        estimated_cache_hits = 0
        estimated_cache_misses = 0
        for file_info in selected_files:
            if self._is_cache_fresh(file_info):
                estimated_cache_hits += 1
            else:
                estimated_cache_misses += 1

        estimated_api_calls = list_api_calls + (estimated_cache_misses * 2)

        return BulkFetchPlan(
            endpoint=endpoint,
            files=selected_files,
            list_api_calls=list_api_calls,
            estimated_api_calls=estimated_api_calls,
            estimated_cache_hits=estimated_cache_hits,
            estimated_cache_misses=estimated_cache_misses,
        )

    async def fetch_with_plan(self, plan: BulkFetchPlan) -> BulkFetchResult:
        rows: list[dict[str, Any]] = []
        api_calls = 0
        cache_hits = 0
        cache_misses = 0

        for file_info in plan.files:
            cache_path = self._data_cache_path(file_info.key)
            if self._is_cache_fresh(file_info):
                cache_hits += 1
            else:
                cache_misses += 1
                body = await self._client.get("/bulk/get", params={"key": file_info.key})
                api_calls += 1
                signed_url = body.get("url")
                if not isinstance(signed_url, str) or not signed_url:
                    raise RuntimeError(f"bulk/get did not return a valid url for key={file_info.key}")

                try:
                    payload = await self._downloader(signed_url)
                except Exception as exc:  # noqa: BLE001 - preserve original cause
                    raise RuntimeError(f"bulk signed-url download failed for key={file_info.key}") from exc
                api_calls += 1
                cache_path.write_bytes(payload)
                self._write_cache_meta(file_info)

            rows.extend(self._read_csv_gzip_rows(cache_path))

        return BulkFetchResult(
            rows=rows,
            api_calls=api_calls,
            cache_hits=cache_hits,
            cache_misses=cache_misses,
            selected_files=len(plan.files),
        )

    async def plan_and_fetch(
        self,
        *,
        endpoint: str,
        date_from: str | None = None,
        date_to: str | None = None,
        exact_dates: list[str] | None = None,
    ) -> tuple[BulkFetchPlan, BulkFetchResult]:
        plan = await self.build_plan(
            endpoint=endpoint,
            date_from=date_from,
            date_to=date_to,
            exact_dates=exact_dates,
        )
        result = await self.fetch_with_plan(plan)
        result.api_calls += plan.list_api_calls
        return plan, result

    async def _download_bytes(self, url: str) -> bytes:
        metrics_recorder.record_jquants_fetch("/bulk/download")
        logger.info(
            "JQuants bulk download",
            event="jquants_fetch",
            endpoint="/bulk/download",
        )
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.content

    def _extract_files(self, body: dict[str, Any]) -> list[BulkFileInfo]:
        payload: list[Any] = []
        data_payload = body.get("data")
        if isinstance(data_payload, list):
            payload = data_payload
        else:
            files_payload = body.get("files")
            if isinstance(files_payload, list):
                payload = files_payload
        files: list[BulkFileInfo] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            key = str(item.get("Key") or item.get("key") or "").strip()
            if not key:
                continue
            last_modified = str(item.get("LastModified") or item.get("last_modified") or "")
            raw_size = item.get("Size") or item.get("size") or 0
            try:
                size = int(raw_size)
            except (TypeError, ValueError):
                size = 0

            range_start, range_end = self._infer_file_range(key)
            files.append(
                BulkFileInfo(
                    key=key,
                    last_modified=last_modified,
                    size=size,
                    range_start=range_start,
                    range_end=range_end,
                )
            )

        return files

    def _select_files(
        self,
        files: list[BulkFileInfo],
        *,
        date_from: str | None,
        date_to: str | None,
        exact_dates: list[str] | None,
    ) -> list[BulkFileInfo]:
        from_date = _parse_date(date_from)
        to_date = _parse_date(date_to)
        target_dates = {
            parsed
            for parsed in (_parse_date(value) for value in (exact_dates or []))
            if parsed is not None
        }

        selected: list[BulkFileInfo] = []
        for file_info in files:
            if target_dates:
                if file_info.range_start is None or file_info.range_end is None:
                    selected.append(file_info)
                    continue
                if any(file_info.range_start <= value <= file_info.range_end for value in target_dates):
                    selected.append(file_info)
                continue

            if from_date is None and to_date is None:
                selected.append(file_info)
                continue

            if file_info.range_start is None or file_info.range_end is None:
                selected.append(file_info)
                continue

            if from_date is not None and file_info.range_end < from_date:
                continue
            if to_date is not None and file_info.range_start > to_date:
                continue
            selected.append(file_info)

        return selected

    def _is_cache_fresh(self, file_info: BulkFileInfo) -> bool:
        data_path = self._data_cache_path(file_info.key)
        if not data_path.exists():
            return False

        metadata = self._read_cache_meta(file_info.key)
        if metadata is None:
            return False

        return (
            metadata.get("key") == file_info.key
            and metadata.get("last_modified") == file_info.last_modified
            and int(metadata.get("size") or 0) == file_info.size
        )

    def _read_cache_meta(self, key: str) -> dict[str, Any] | None:
        meta_path = self._meta_cache_path(key)
        if not meta_path.exists():
            return None
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def _write_cache_meta(self, file_info: BulkFileInfo) -> None:
        payload = {
            "key": file_info.key,
            "last_modified": file_info.last_modified,
            "size": file_info.size,
            "cached_at": datetime.now(UTC).isoformat(),
        }
        self._meta_cache_path(file_info.key).write_text(
            json.dumps(payload, ensure_ascii=False),
            encoding="utf-8",
        )

    def _read_csv_gzip_rows(self, path: Path) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        with gzip.open(path, mode="rt", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if not isinstance(row, dict):
                    continue
                cleaned: dict[str, Any] = {}
                for raw_key, raw_value in row.items():
                    if raw_key is None:
                        continue
                    key = str(raw_key).strip()
                    if not key:
                        continue
                    if isinstance(raw_value, str):
                        cleaned[key] = raw_value.strip()
                    else:
                        cleaned[key] = raw_value
                if cleaned:
                    rows.append(cleaned)
        return rows

    def _data_cache_path(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self._cache_dir / f"{digest}.csv.gz"

    def _meta_cache_path(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self._cache_dir / f"{digest}.meta.json"

    def _infer_file_range(self, key: str) -> tuple[date | None, date | None]:
        file_name = Path(key).name

        daily_match = re.search(r"(\d{8})(?=\.csv(?:\.gz)?$)", file_name)
        if daily_match:
            day = _parse_date(daily_match.group(1))
            return day, day

        monthly_match = re.search(r"(\d{6})(?=\.csv(?:\.gz)?$)", file_name)
        if monthly_match:
            ym = monthly_match.group(1)
            year = int(ym[:4])
            month = int(ym[4:6])
            start = date(year, month, 1)
            if month == 12:
                end = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                end = date(year, month + 1, 1) - timedelta(days=1)
            return start, end

        return None, None


def _parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()  # noqa: DTZ007
        except ValueError:
            continue
    return None
