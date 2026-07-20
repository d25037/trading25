"""Pure row/date conversion helpers for market sync strategies."""

from __future__ import annotations

import math
import re
from datetime import UTC, date, datetime
from typing import Any

from loguru import logger

from src.application.services.options_225 import normalize_options_225_raw_row
from src.application.services.stock_data_row_builder import (
    build_stock_data_row,
    is_provider_no_trade_row,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code

_BULK_STOCK_KEY_ALIASES: dict[str, str] = {
    "code": "Code",
    "date": "Date",
    "o": "O",
    "open": "O",
    "h": "H",
    "high": "H",
    "l": "L",
    "low": "L",
    "c": "C",
    "close": "C",
    "vo": "Vo",
    "volume": "Vo",
    "va": "Va",
    "turnovervalue": "Va",
    "turnover_value": "Va",
    "adjo": "AdjO",
    "adjopen": "AdjO",
    "adjh": "AdjH",
    "adjhigh": "AdjH",
    "adjl": "AdjL",
    "adjlow": "AdjL",
    "adjc": "AdjC",
    "adjclose": "AdjC",
    "adjvo": "AdjVo",
    "adjvolume": "AdjVo",
    "adjfactor": "AdjFactor",
}

_BULK_INDEX_KEY_ALIASES: dict[str, str] = {
    **_BULK_STOCK_KEY_ALIASES,
    "sectorname": "SectorName",
    "sector_name": "SectorName",
    "indexcode": "Code",
    "index_code": "Code",
}

_BULK_OPTIONS_225_KEY_ALIASES: dict[str, str] = {
    "date": "Date",
    "code": "Code",
    "o": "O",
    "h": "H",
    "l": "L",
    "c": "C",
    "eo": "EO",
    "eh": "EH",
    "el": "EL",
    "ec": "EC",
    "ao": "AO",
    "ah": "AH",
    "al": "AL",
    "ac": "AC",
    "vo": "Vo",
    "oi": "OI",
    "va": "Va",
    "cm": "CM",
    "strike": "Strike",
    "vooa": "VoOA",
    "emmrgntrgdiv": "EmMrgnTrgDiv",
    "pcdiv": "PCDiv",
    "ltd": "LTD",
    "sqd": "SQD",
    "settle": "Settle",
    "theo": "Theo",
    "basevol": "BaseVol",
    "underpx": "UnderPx",
    "iv": "IV",
    "ir": "IR",
}

_BULK_MARGIN_KEY_ALIASES: dict[str, str] = {
    "code": "Code",
    "date": "Date",
    "longvol": "LongVol",
    "longvolume": "LongVol",
    "longmarginvolume": "LongVol",
    "longmargintradevolume": "LongVol",
    "longmarginoutstandingbalance": "LongVol",
    "long_margin_volume": "LongVol",
    "shrtvol": "ShrtVol",
    "shortvol": "ShrtVol",
    "shortvolume": "ShrtVol",
    "shortmarginvolume": "ShrtVol",
    "shortmargintradevolume": "ShrtVol",
    "shortmarginoutstandingbalance": "ShrtVol",
    "short_margin_volume": "ShrtVol",
}

_BULK_STOCK_MASTER_KEY_ALIASES: dict[str, str] = {
    "date": "Date",
    "code": "Code",
    "coname": "CoName",
    "companyname": "CoName",
    "companynm": "CoName",
    "conameen": "CoNameEn",
    "companynameenglish": "CoNameEn",
    "companynmen": "CoNameEn",
    "s17": "S17",
    "sector17code": "S17",
    "sector17": "S17",
    "s17nm": "S17Nm",
    "sector17codename": "S17Nm",
    "sector17name": "S17Nm",
    "s33": "S33",
    "sector33code": "S33",
    "sector33": "S33",
    "s33nm": "S33Nm",
    "sector33codename": "S33Nm",
    "sector33name": "S33Nm",
    "scalecat": "ScaleCat",
    "scalecategory": "ScaleCat",
    "mkt": "Mkt",
    "marketcode": "Mkt",
    "mktnm": "MktNm",
    "marketcodename": "MktNm",
    "marketname": "MktNm",
    "listeddate": "ListedDate",
    "listingdate": "ListedDate",
}

_BULK_FINS_KEY_ALIASES: dict[str, str] = {
    "code": "Code",
    "discdate": "DiscDate",
    "eps": "EPS",
    "np": "NP",
    "eq": "Eq",
    "curpertype": "CurPerType",
    "doctype": "DocType",
    "nxfeps": "NxFEPS",
    "fsales": "FSales",
    "forecastsales": "FSales",
    "nxfsales": "NxFSales",
    "nxsales": "NxFSales",
    "nextyearforecastsales": "NxFSales",
    "bps": "BPS",
    "sales": "Sales",
    "op": "OP",
    "odp": "OdP",
    "cfo": "CFO",
    "divann": "DivAnn",
    "divfy": "DivFY",
    "fdivann": "FDivAnn",
    "fdivfy": "FDivFY",
    "nxfdivann": "NxFDivAnn",
    "nxfdivfy": "NxFDivFY",
    "payoutratioann": "PayoutRatioAnn",
    "fpayoutratioann": "FPayoutRatioAnn",
    "nxfpayoutratioann": "NxFPayoutRatioAnn",
    "feps": "FEPS",
    "cfi": "CFI",
    "cff": "CFF",
    "casheq": "CashEq",
    "ta": "TA",
    "shoutfy": "ShOutFY",
    "trshfy": "TrShFY",
}


def _canonicalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.strip().lower())


def _normalize_bulk_row_keys(
    rows: list[dict[str, Any]],
    aliases: dict[str, str],
) -> list[dict[str, Any]]:
    if not rows:
        return rows

    first_row = rows[0]
    remap: dict[str, str] = {}
    for raw_key in first_row.keys():
        canonical = _canonicalize_key(str(raw_key))
        target = aliases.get(canonical)
        if target and target != raw_key:
            remap[str(raw_key)] = target

    if not remap:
        return rows

    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        normalized = dict(row)
        for raw_key, target in remap.items():
            if _has_non_empty_value(normalized.get(target)):
                continue
            raw_value = row.get(raw_key)
            if raw_value is None:
                continue
            normalized[target] = raw_value
        normalized_rows.append(normalized)
    return normalized_rows


def normalize_bulk_stock_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_STOCK_KEY_ALIASES)


def normalize_bulk_indices_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_INDEX_KEY_ALIASES)


def normalize_bulk_options_225_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_OPTIONS_225_KEY_ALIASES)


def normalize_bulk_margin_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_MARGIN_KEY_ALIASES)


def _normalize_bulk_stock_master_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_STOCK_MASTER_KEY_ALIASES)


def normalize_bulk_fins_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_FINS_KEY_ALIASES)


def _has_non_empty_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _parse_date(value: str) -> date | None:
    """YYYY-MM-DD / YYYYMMDD を date に正規化して返す。"""
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").date()  # noqa: DTZ007
        return datetime.strptime(text, "%Y-%m-%d").date()  # noqa: DTZ007
    except ValueError:
        return None


def _to_iso_date_text(value: str | None) -> str | None:
    parsed = _parse_date(value or "")
    if parsed is None:
        return None
    return parsed.isoformat()


def _normalize_iso_date_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None

    if (
        len(text) == 10
        and text[4] == "-"
        and text[7] == "-"
        and text[:4].isdigit()
        and text[5:7].isdigit()
        and text[8:10].isdigit()
    ):
        try:
            date(int(text[:4]), int(text[5:7]), int(text[8:10]))
        except ValueError:
            return None
        return text

    if len(text) == 8 and text.isdigit():
        try:
            date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        except ValueError:
            return None
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"

    return _to_iso_date_text(text)


def _coerce_float_fast(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = float(text)
        except ValueError:
            return None
        return parsed if math.isfinite(parsed) else None
    return None


def _collect_sample_code(sample_codes: list[str], code: str) -> None:
    if code in sample_codes:
        return
    if len(sample_codes) >= 5:
        return
    sample_codes.append(code)


def convert_stock_bulk_rows(
    data: list[dict[str, Any]],
    *,
    target_dates: set[str] | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    skipped = 0
    sample_codes: list[str] = []
    seen: set[tuple[str, str]] = set()
    date_cache: dict[str, str | None] = {}
    created_at = datetime.now(UTC).isoformat()

    for row in normalize_bulk_stock_rows(data):
        code = normalize_stock_code(row.get("Code", row.get("code", "")))
        if not code:
            continue

        raw_date_value = row.get("Date", row.get("date"))
        if isinstance(raw_date_value, date):
            cache_key = raw_date_value.isoformat()
        else:
            cache_key = str(raw_date_value)
        if cache_key in date_cache:
            date_text = date_cache[cache_key]
        else:
            date_text = _normalize_iso_date_text(raw_date_value)
            date_cache[cache_key] = date_text
        if date_text is None:
            skipped += 1
            _collect_sample_code(sample_codes, code)
            continue

        if target_dates is not None and date_text not in target_dates:
            continue

        normalized_input = dict(row)
        normalized_input["Date"] = date_text
        converted = build_stock_data_row(
            normalized_input,
            normalized_code=code,
            created_at=created_at,
        )
        if converted is None:
            if not is_provider_no_trade_row(normalized_input):
                raise ValueError(
                    "incomplete provider daily row requires retry or full refresh: "
                    f"{code} {date_text}"
                )
            skipped += 1
            _collect_sample_code(sample_codes, code)
            continue

        row_key = (code, date_text)
        if row_key in seen:
            continue
        seen.add(row_key)

        rows.append(converted)

    if skipped > 0:
        sample = ", ".join(sample_codes) if sample_codes else "unknown"
        logger.warning(
            "Skipped {} daily quotes with incomplete OHLCV data (sample codes: {})",
            skipped,
            sample,
        )
    return rows


def build_target_date_set(dates: list[str]) -> set[str] | None:
    normalized = {
        date_text
        for date_text in (_normalize_iso_date_text(value) for value in dates)
        if date_text is not None
    }
    return normalized or None


def convert_topix_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    created_at = datetime.now(UTC).isoformat()
    rows: list[dict[str, Any]] = []
    for d in data:
        rows.append(
            {
                "date": d.get("Date", d.get("date", "")),
                "open": d.get("O", d.get("open")),
                "high": d.get("H", d.get("high")),
                "low": d.get("L", d.get("low")),
                "close": d.get("C", d.get("close")),
                "created_at": created_at,
            }
        )
    return rows


def convert_stock_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """JQuants 銘柄マスタ -> DB 行"""
    rows = []
    for d in data:
        code = normalize_stock_code(str(d.get("Code", "") or ""))
        if not code:
            continue
        rows.append(
            {
                "code": code,
                "company_name": d.get("CoName", ""),
                "company_name_english": d.get("CoNameEn"),
                "market_code": d.get("Mkt", ""),
                "market_name": d.get("MktNm", ""),
                "sector_17_code": d.get("S17", ""),
                "sector_17_name": d.get("S17Nm", ""),
                "sector_33_code": d.get("S33", ""),
                "sector_33_name": d.get("S33Nm", ""),
                "scale_category": d.get("ScaleCat"),
                "listed_date": d.get("ListedDate")
                or d.get("ListingDate")
                or d.get("listed_date")
                or "",
                "created_at": datetime.now(UTC).isoformat(),
            }
        )
    return rows


def group_stock_master_bulk_rows_by_date(
    data: list[dict[str, Any]],
    *,
    target_dates: set[str] | None,
    default_snapshot_date: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    normalized_rows = _normalize_bulk_stock_master_rows(data)
    raw_rows_by_date: dict[str, list[dict[str, Any]]] = {}
    for row in normalized_rows:
        row_date = _normalize_iso_date_text(row.get("Date"))
        snapshot_date = row_date
        if row_date is None:
            snapshot_date = default_snapshot_date
        if snapshot_date is None:
            continue
        if target_dates is not None and snapshot_date not in target_dates:
            continue

        normalized_row = row
        if row_date is None:
            normalized_row = dict(row)
            normalized_row["Date"] = snapshot_date
        raw_rows_by_date.setdefault(snapshot_date, []).append(normalized_row)

    grouped: dict[str, list[dict[str, Any]]] = {}
    for snapshot_date, raw_rows in raw_rows_by_date.items():
        converted_rows = convert_stock_rows(raw_rows)
        if converted_rows:
            grouped[snapshot_date] = converted_rows
    return grouped


def convert_options_225_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    created_at = datetime.now(UTC).isoformat()
    return [normalize_options_225_raw_row(item, created_at=created_at) for item in data]


def convert_stock_data_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """JQuants 株価データ -> DB 行"""
    rows: list[dict[str, Any]] = []
    skipped = 0
    sample_codes: list[str] = []
    created_at = datetime.now(UTC).isoformat()

    for d in data:
        row = build_stock_data_row(d, created_at=created_at)
        if row is None:
            code = normalize_stock_code(d.get("Code", ""))
            date_text = _normalize_iso_date_text(d.get("Date"))
            if code and date_text and not is_provider_no_trade_row(d):
                raise ValueError(
                    "incomplete provider daily row requires retry or full refresh: "
                    f"{code} {date_text}"
                )
            skipped += 1
            if code and code not in sample_codes and len(sample_codes) < 5:
                sample_codes.append(code)
            continue
        rows.append(row)

    if skipped > 0:
        sample = ", ".join(sample_codes) if sample_codes else "unknown"
        logger.warning(
            "Skipped {} daily quotes with incomplete OHLCV data (sample codes: {})",
            skipped,
            sample,
        )
    return rows


def extract_list_items(
    body: dict[str, Any],
    *,
    preferred_keys: tuple[str, ...] = ("data",),
) -> list[dict[str, Any]]:
    """レスポンスの配列ペイロードをキー揺れ込みで取り出す。"""

    def _coerce_dict_items(value: Any) -> list[dict[str, Any]] | None:
        if not isinstance(value, list):
            return None
        return [item for item in value if isinstance(item, dict)]

    for key in preferred_keys:
        dict_items = _coerce_dict_items(body.get(key))
        if dict_items is not None:
            return dict_items

    for value in body.values():
        dict_items = _coerce_dict_items(value)
        if dict_items is not None:
            return dict_items

    return []


def _normalize_index_code(value: Any) -> str:
    """指数コードを文字列化し、数字コードは 4 桁に正規化する。"""
    text = str(value).strip() if value is not None else ""
    if not text:
        return ""
    if text.isdigit() and len(text) < 4:
        return text.zfill(4)
    return text.upper()


def _extract_index_code(index_info: dict[str, Any]) -> str:
    """指数コードをキー揺れを吸収して取得。"""
    code = (
        index_info.get("code")
        or index_info.get("Code")
        or index_info.get("index_code")
        or index_info.get("indexCode")
    )
    return _normalize_index_code(code)


def convert_index_master_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """JQuants 指数マスタ -> DB 行。"""
    rows: list[dict[str, Any]] = []
    created_at = datetime.now(UTC).isoformat()
    for idx in data:
        code = _extract_index_code(idx)
        if not code:
            continue
        rows.append(
            {
                "code": code,
                "name": idx.get("name") or idx.get("Name") or "",
                "name_english": idx.get("name_english") or idx.get("nameEnglish"),
                "category": idx.get("category") or idx.get("Category") or "",
                "data_start_date": idx.get("data_start_date")
                or idx.get("dataStartDate"),
                "created_at": created_at,
            }
        )
    return rows


def convert_indices_data_rows(
    data: list[dict[str, Any]], code: str | None
) -> list[dict[str, Any]]:
    """JQuants 指数データ -> DB 行。日付欠損行はスキップ。"""
    rows: list[dict[str, Any]] = []
    created_at = datetime.now(UTC).isoformat()
    skipped_missing_date = 0
    skipped_missing_code = 0

    for d in data:
        row_code = _extract_index_code(d) or _normalize_index_code(code)
        if not row_code:
            skipped_missing_code += 1
            continue

        row_date = d.get("Date") or d.get("date") or ""
        if not row_date:
            skipped_missing_date += 1
            continue

        rows.append(
            {
                "code": row_code,
                "date": row_date,
                "open": d.get("O", d.get("open")),
                "high": d.get("H", d.get("high")),
                "low": d.get("L", d.get("low")),
                "close": d.get("C", d.get("close")),
                "sector_name": d.get("SectorName", d.get("sector_name")),
                "created_at": created_at,
            }
        )

    if skipped_missing_date > 0:
        logger.warning(
            "Skipped {} index rows with missing date (code={})",
            skipped_missing_date,
            code,
        )
    if skipped_missing_code > 0:
        logger.warning("Skipped {} index rows with missing code", skipped_missing_code)
    return rows


def _resolve_margin_volume(
    row: dict[str, Any],
    *keys: str,
) -> float | None:
    for key in keys:
        if key in row:
            return _coerce_float_fast(row.get(key))
    return None


def convert_margin_rows(
    data: list[dict[str, Any]],
    *,
    default_code: str | None = None,
    target_codes: set[str] | None = None,
    min_date_exclusive: str | None = None,
) -> list[dict[str, Any]]:
    """JQuants 信用残データ -> DB 行。"""
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    skipped_missing_date = 0
    skipped_missing_code = 0

    for item in data:
        code = normalize_stock_code(
            item.get("Code") or item.get("code") or default_code or ""
        )
        if not code:
            skipped_missing_code += 1
            continue
        if target_codes is not None and code not in target_codes:
            continue

        row_date = _normalize_iso_date_text(item.get("Date", item.get("date")))
        if row_date is None:
            skipped_missing_date += 1
            continue
        if min_date_exclusive and not _is_date_after(row_date, min_date_exclusive):
            continue

        row_key = (code, row_date)
        if row_key in seen:
            continue
        seen.add(row_key)

        rows.append(
            {
                "code": code,
                "date": row_date,
                "long_margin_volume": _resolve_margin_volume(
                    item,
                    "LongVol",
                    "longVol",
                    "long_volume",
                    "longMarginVolume",
                    "longMarginTradeVolume",
                    "longMarginOutstandingBalance",
                    "long_margin_volume",
                ),
                "short_margin_volume": _resolve_margin_volume(
                    item,
                    "ShrtVol",
                    "shortVol",
                    "short_volume",
                    "shortMarginVolume",
                    "shortMarginTradeVolume",
                    "shortMarginOutstandingBalance",
                    "short_margin_volume",
                ),
            }
        )

    if skipped_missing_date > 0:
        logger.warning("Skipped {} margin rows with missing date", skipped_missing_date)
    if skipped_missing_code > 0:
        logger.warning("Skipped {} margin rows with missing code", skipped_missing_code)
    return rows


def latest_date(values: list[str]) -> str | None:
    """日付文字列配列から最新日を返す。"""
    latest: str | None = None
    for value in values:
        if not value:
            continue
        if latest is None or _is_date_after(value, latest):
            latest = value
    return latest


def extract_dates_after(
    rows: list[dict[str, Any]],
    anchor_date: str | None,
    *,
    include_anchor: bool = False,
) -> list[str]:
    """行配列から anchor_date 以降（またはより後）の日付を抽出して昇順化する。"""
    dates: set[str] = set()
    for row in rows:
        row_date = row.get("date")
        if not row_date:
            continue
        if anchor_date:
            if include_anchor:
                if _is_date_after(anchor_date, row_date):
                    continue
            elif not _is_date_after(row_date, anchor_date):
                continue
        dates.add(row_date)
    return sorted(dates, key=_date_sort_key)


def to_jquants_date_param(value: str) -> str:
    """J-Quants 向けの日付パラメータ（YYYYMMDD）に変換。"""
    parsed = _parse_date(value)
    if parsed is None:
        return value
    return parsed.strftime("%Y%m%d")


def _is_date_after(lhs: str, rhs: str) -> bool:
    """日付文字列（YYYY-MM-DD / YYYYMMDD）の大小比較。"""
    left = _parse_date(lhs)
    right = _parse_date(rhs)
    if left is None or right is None:
        return lhs > rhs
    return left > right


def _date_sort_key(value: str) -> tuple[int, str]:
    """日付ソート用キー（parse 失敗時は末尾に回す）。"""
    parsed = _parse_date(value)
    if parsed is None:
        return (1, value)
    return (0, parsed.isoformat())
