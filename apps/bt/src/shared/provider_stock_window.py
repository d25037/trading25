"""Shared provider-adjusted daily stock validation and fingerprinting."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

_PROVIDER_PLAN_KEY = "provider_plan"
_PROVIDER_AS_OF_KEY = "provider_as_of"
_PROVIDER_SOURCE_FINGERPRINT_KEY = "provider_source_fingerprint"
_PROVIDER_COVERAGE_START_KEY = "provider_coverage_start"
_PROVIDER_COVERAGE_END_KEY = "provider_coverage_end"


def _normalize_stock_code(code: str) -> str:
    if len(code) in {5, 6} and code.endswith("0"):
        return code[:-1]
    return code

PROVIDER_RAW_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover_value",
    "adjustment_factor",
)
PROVIDER_ADJUSTED_COLUMNS = (
    "adjusted_open",
    "adjusted_high",
    "adjusted_low",
    "adjusted_close",
    "adjusted_volume",
)
PROVIDER_NUMERIC_COLUMNS = (*PROVIDER_RAW_COLUMNS, *PROVIDER_ADJUSTED_COLUMNS)
PROVIDER_DRIFT_COLUMNS = ("adjustment_factor", *PROVIDER_ADJUSTED_COLUMNS)


@dataclass(frozen=True, slots=True)
class ProviderStockCoverage:
    start: str
    end: str


@dataclass(frozen=True, slots=True)
class ProviderStockMetadata:
    provider_plan: str
    provider_as_of: str
    provider_source_fingerprint: str


def _required_text(mapping: Mapping[str, Any], key: str) -> str:
    value = mapping.get(key)
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ValueError(f"Provider stock window requires {key}")
    return text


def _iso_date(value: str, *, field: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Provider stock window {field} must be an ISO date") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"Provider stock window {field} must be an ISO date")
    return value


def coerce_provider_stock_coverage(
    coverage: ProviderStockCoverage | Mapping[str, Any],
) -> ProviderStockCoverage:
    if isinstance(coverage, ProviderStockCoverage):
        normalized = coverage
    else:
        start = coverage.get(
            "start", coverage.get(_PROVIDER_COVERAGE_START_KEY)
        )
        end = coverage.get("end", coverage.get(_PROVIDER_COVERAGE_END_KEY))
        normalized = ProviderStockCoverage(
            start=str(start).strip() if start is not None else "",
            end=str(end).strip() if end is not None else "",
        )
    start = _iso_date(normalized.start, field="coverage start")
    end = _iso_date(normalized.end, field="coverage end")
    if start > end:
        raise ValueError("Provider stock window coverage start must not exceed end")
    return ProviderStockCoverage(start=start, end=end)


def coerce_provider_stock_metadata(
    metadata: ProviderStockMetadata | Mapping[str, Any],
) -> ProviderStockMetadata:
    if isinstance(metadata, ProviderStockMetadata):
        normalized = metadata
    else:
        normalized = ProviderStockMetadata(
            provider_plan=_required_text(metadata, _PROVIDER_PLAN_KEY),
            provider_as_of=_required_text(metadata, _PROVIDER_AS_OF_KEY),
            provider_source_fingerprint=_required_text(
                metadata, _PROVIDER_SOURCE_FINGERPRINT_KEY
            ),
        )
    _iso_date(normalized.provider_as_of, field="provider as-of")
    return normalized


def validate_provider_stock_window(
    code: str,
    rows: Sequence[Mapping[str, Any]],
    coverage: ProviderStockCoverage | Mapping[str, Any],
    metadata: ProviderStockMetadata | Mapping[str, Any],
) -> tuple[str, list[dict[str, Any]], ProviderStockCoverage, ProviderStockMetadata]:
    normalized_code = _normalize_stock_code(code)
    if not normalized_code:
        raise ValueError("Provider stock window requires a valid code")
    normalized_coverage = coerce_provider_stock_coverage(coverage)
    normalized_metadata = coerce_provider_stock_metadata(metadata)

    normalized_rows: list[dict[str, Any]] = []
    dates: set[str] = set()
    for source in rows:
        row = dict(source)
        row_code = _normalize_stock_code(str(row.get("code", "")))
        if row_code != normalized_code:
            raise ValueError(
                "Provider stock window row code does not match replacement code"
            )
        row_date = _iso_date(str(row.get("date", "")).strip(), field="row date")
        if row_date in dates:
            raise ValueError(
                f"Provider stock window contains duplicate date: {row_date}"
            )
        if not normalized_coverage.start <= row_date <= normalized_coverage.end:
            raise ValueError(
                f"Provider stock window row date is outside coverage: {row_date}"
            )
        dates.add(row_date)

        for column in PROVIDER_NUMERIC_COLUMNS:
            value = row.get(column)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"Provider stock window {column} must be finite")
            if not math.isfinite(float(value)):
                raise ValueError(f"Provider stock window {column} must be finite")
        if float(row["adjustment_factor"]) <= 0:
            raise ValueError("Provider stock window adjustment_factor must be positive")
        if (
            int(row["volume"]) != row["volume"]
            or int(row["adjusted_volume"]) != row["adjusted_volume"]
        ):
            raise ValueError("Provider stock window volume values must be integral")
        if int(row["volume"]) < 0 or int(row["adjusted_volume"]) < 0:
            raise ValueError("Provider stock window volume values must be non-negative")
        if float(row["turnover_value"]) < 0:
            raise ValueError(
                "Provider stock window turnover_value must be non-negative"
            )
        if float(row["high"]) < max(
            float(row["open"]), float(row["low"]), float(row["close"])
        ) or float(row["low"]) > min(
            float(row["open"]), float(row["high"]), float(row["close"])
        ):
            raise ValueError("Provider stock window raw OHLC is inconsistent")
        row["code"] = normalized_code
        row["date"] = row_date
        row["volume"] = int(row["volume"])
        row["adjusted_volume"] = int(row["adjusted_volume"])
        normalized_rows.append(row)

    if not normalized_rows:
        raise ValueError("Provider stock window requires at least one row")
    normalized_rows.sort(key=lambda row: str(row["date"]))
    observed_start = str(normalized_rows[0]["date"])
    observed_end = str(normalized_rows[-1]["date"])
    if (
        normalized_coverage.start != observed_start
        or normalized_coverage.end != observed_end
    ):
        raise ValueError(
            "Provider stock window coverage must equal observed row date bounds"
        )
    if normalized_metadata.provider_as_of < normalized_coverage.end:
        raise ValueError(
            "Provider stock window provider as-of must be on or after coverage end"
        )
    future_factor = 1.0
    for row in reversed(normalized_rows):
        for raw_column, adjusted_column in (
            ("open", "adjusted_open"),
            ("high", "adjusted_high"),
            ("low", "adjusted_low"),
            ("close", "adjusted_close"),
        ):
            expected = float(row[raw_column]) * future_factor
            actual = float(row[adjusted_column])
            if not math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-6):
                raise ValueError(
                    "Provider stock window provider-adjusted consistency failed: "
                    f"{adjusted_column} on {row['date']}"
                )
        expected_volume = round(int(row["volume"]) / future_factor)
        if int(row["adjusted_volume"]) != expected_volume:
            raise ValueError(
                "Provider stock window provider-adjusted consistency failed: "
                f"adjusted_volume on {row['date']}"
            )
        future_factor *= float(row["adjustment_factor"])
        if not math.isfinite(future_factor) or future_factor <= 0:
            raise ValueError(
                "Provider stock window cumulative adjustment factor must be positive and finite"
            )
    return normalized_code, normalized_rows, normalized_coverage, normalized_metadata


def provider_stock_source_fingerprint(rows: Sequence[Mapping[str, Any]]) -> str:
    """Return an order-independent fingerprint composable across append batches."""
    aggregate = bytearray(hashlib.sha256().digest_size)
    for row in rows:
        canonical_row = {
            "code": str(row.get("code", "")),
            "date": str(row.get("date", "")),
            **{column: row.get(column) for column in PROVIDER_NUMERIC_COLUMNS},
        }
        payload = json.dumps(
            canonical_row, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        )
        digest = hashlib.sha256(payload.encode("utf-8")).digest()
        for index, value in enumerate(digest):
            aggregate[index] ^= value
    return aggregate.hex()


def combine_provider_stock_source_fingerprints(*fingerprints: str) -> str:
    """Combine disjoint provider row-set fingerprints without rereading history."""
    aggregate = bytearray(hashlib.sha256().digest_size)
    for fingerprint in fingerprints:
        digest = bytes.fromhex(fingerprint)
        if len(digest) != len(aggregate):
            raise ValueError("Provider stock source fingerprint is invalid")
        for index, value in enumerate(digest):
            aggregate[index] ^= value
    return aggregate.hex()
