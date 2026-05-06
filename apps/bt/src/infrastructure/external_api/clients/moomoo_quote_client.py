"""Read-only moomoo OpenD quote client.

The integration intentionally uses only OpenQuoteContext. Trading contexts are
out of scope for this repository path until a separate live-order design exists.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import importlib
from typing import Any, Callable

from loguru import logger

from src.shared.observability.correlation import get_correlation_id


class MoomooOpenDError(Exception):
    """moomoo OpenD read-only quote error."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(message)


@dataclass(frozen=True)
class MoomooOpenDConfig:
    """OpenD connection settings."""

    host: str
    port: int
    is_encrypt: bool
    enabled: bool
    max_history_rows: int


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def normalize_us_code(symbol: str) -> str:
    """Normalize a US symbol to OpenD's `US.AAPL` style."""
    cleaned = symbol.strip().upper()
    if not cleaned:
        raise MoomooOpenDError(422, "symbol is required")
    if "." in cleaned:
        market, raw_symbol = cleaned.split(".", 1)
        if market != "US" or not raw_symbol:
            raise MoomooOpenDError(422, "Only US market symbols are supported")
        return f"US.{raw_symbol}"
    return f"US.{cleaned}"


def symbol_from_us_code(code: str) -> str:
    """Return the bare ticker from an OpenD US code."""
    cleaned = str(code).strip().upper()
    if cleaned.startswith("US."):
        return cleaned[3:]
    return cleaned


def _coerce_scalar(value: Any) -> Any:
    if value is None:
        return None
    try:
        if bool(value != value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _coerce_float(value: Any) -> float | None:
    value = _coerce_scalar(value)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    value = _coerce_scalar(value)
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any) -> bool | None:
    value = _coerce_scalar(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return bool(value)


def _coerce_str(value: Any) -> str | None:
    value = _coerce_scalar(value)
    if value in (None, ""):
        return None
    return str(value)


def _dataframe_records(data: Any) -> list[dict[str, Any]]:
    if hasattr(data, "to_dict"):
        records = data.to_dict(orient="records")
        if isinstance(records, list):
            return [dict(row) for row in records if isinstance(row, dict)]
    if isinstance(data, list):
        return [dict(row) for row in data if isinstance(row, dict)]
    return []


class MoomooQuoteClient:
    """Thin, read-only wrapper around moomoo OpenD quote APIs."""

    def __init__(self, config: MoomooOpenDConfig) -> None:
        self._config = config
        self._sdk: Any | None = None
        self._sdk_import_error: str | None = None

    @property
    def config(self) -> MoomooOpenDConfig:
        return self._config

    def _load_sdk(self) -> Any:
        if self._sdk is not None:
            return self._sdk
        if self._sdk_import_error is not None:
            raise MoomooOpenDError(503, self._sdk_import_error)
        try:
            self._sdk = importlib.import_module("moomoo")
        except ImportError as exc:
            self._sdk_import_error = "moomoo Python SDK is not installed"
            raise MoomooOpenDError(503, self._sdk_import_error) from exc
        except Exception as exc:
            self._sdk_import_error = f"moomoo Python SDK import failed: {exc}"
            raise MoomooOpenDError(503, self._sdk_import_error) from exc
        return self._sdk

    def _open_quote_context(self) -> Any:
        if not self._config.enabled:
            raise MoomooOpenDError(503, "moomoo OpenD integration is disabled")
        sdk = self._load_sdk()
        try:
            return sdk.OpenQuoteContext(
                host=self._config.host,
                port=self._config.port,
                is_encrypt=self._config.is_encrypt,
            )
        except Exception as exc:
            raise MoomooOpenDError(503, f"moomoo OpenD is not reachable: {exc}") from exc

    def _with_quote_context(self, operation: Callable[[Any, Any], Any]) -> Any:
        sdk = self._load_sdk()
        quote_ctx = self._open_quote_context()
        try:
            return operation(sdk, quote_ctx)
        finally:
            close = getattr(quote_ctx, "close", None)
            if callable(close):
                close()

    @staticmethod
    def _ensure_ret_ok(sdk: Any, ret: Any, data: Any, operation: str) -> None:
        if ret == getattr(sdk, "RET_OK", 0):
            return
        message = str(data) if data is not None else f"moomoo {operation} failed"
        raise MoomooOpenDError(502, message)

    def status(self) -> dict[str, Any]:
        sdk_installed = False
        open_d_reachable = False
        quote_context_ready = False
        message: str | None = None

        try:
            self._load_sdk()
            sdk_installed = True
        except MoomooOpenDError as exc:
            message = exc.message

        if sdk_installed and self._config.enabled:
            try:
                quote_ctx = self._open_quote_context()
                close = getattr(quote_ctx, "close", None)
                if callable(close):
                    close()
                open_d_reachable = True
                quote_context_ready = True
            except MoomooOpenDError as exc:
                message = exc.message

        return {
            "enabled": self._config.enabled,
            "sdkInstalled": sdk_installed,
            "openDReachable": open_d_reachable,
            "quoteContextReady": quote_context_ready,
            "host": self._config.host,
            "port": self._config.port,
            "message": message,
        }

    def search_us_stocks(self, query: str, limit: int) -> dict[str, Any]:
        normalized_query = query.strip().upper()
        if not normalized_query:
            raise MoomooOpenDError(422, "query is required")

        def operation(sdk: Any, quote_ctx: Any) -> list[dict[str, Any]]:
            ret, data = quote_ctx.get_stock_basicinfo(
                sdk.Market.US,
                sdk.SecurityType.STOCK,
            )
            self._ensure_ret_ok(sdk, ret, data, "get_stock_basicinfo")
            return _dataframe_records(data)

        logger.info(
            "moomoo US stock search",
            event="moomoo_opend_fetch",
            endpoint="get_stock_basicinfo",
            market="US",
            correlationId=get_correlation_id(),
        )
        rows = self._with_quote_context(operation)
        items: list[dict[str, Any]] = []
        for row in rows:
            code = _coerce_str(row.get("code")) or ""
            symbol = symbol_from_us_code(code)
            name = _coerce_str(row.get("name"))
            if normalized_query not in symbol and normalized_query not in code.upper():
                if name is None or normalized_query not in name.upper():
                    continue
            items.append(
                {
                    "code": code,
                    "symbol": symbol,
                    "name": name,
                    "lotSize": _coerce_int(row.get("lot_size")),
                    "stockType": _coerce_str(row.get("stock_type")),
                    "exchangeType": _coerce_str(row.get("exchange_type")),
                    "listingDate": _coerce_str(row.get("listing_date")),
                    "delisting": _coerce_bool(row.get("delisting")),
                }
            )
            if len(items) >= limit:
                break
        return {
            "query": query,
            "items": items,
            "count": len(items),
            "lastUpdated": _now_iso(),
        }

    def get_us_history(
        self,
        symbol: str,
        start: str | None,
        end: str | None,
        max_rows: int | None = None,
    ) -> dict[str, Any]:
        code = normalize_us_code(symbol)
        row_limit = min(max_rows or self._config.max_history_rows, self._config.max_history_rows)

        def operation(sdk: Any, quote_ctx: Any) -> tuple[list[dict[str, Any]], bool]:
            rows: list[dict[str, Any]] = []
            page_req_key = None
            has_more = False
            while len(rows) < row_limit:
                ret, data, page_req_key = quote_ctx.request_history_kline(
                    code,
                    start=start,
                    end=end,
                    ktype=sdk.KLType.K_DAY,
                    autype=sdk.AuType.QFQ,
                    max_count=min(1000, row_limit - len(rows)),
                    page_req_key=page_req_key,
                    extended_time=False,
                )
                self._ensure_ret_ok(sdk, ret, data, "request_history_kline")
                rows.extend(_dataframe_records(data))
                if page_req_key is None:
                    break
                if len(rows) >= row_limit:
                    has_more = True
                    break
            return rows[:row_limit], has_more

        logger.info(
            "moomoo US history fetch",
            event="moomoo_opend_fetch",
            endpoint="request_history_kline",
            symbol=code,
            correlationId=get_correlation_id(),
        )
        rows, has_more = self._with_quote_context(operation)
        normalized_rows = [
            {
                "code": _coerce_str(row.get("code")) or code,
                "timeKey": _coerce_str(row.get("time_key")) or "",
                "name": _coerce_str(row.get("name")),
                "open": _coerce_float(row.get("open")),
                "close": _coerce_float(row.get("close")),
                "high": _coerce_float(row.get("high")),
                "low": _coerce_float(row.get("low")),
                "volume": _coerce_float(row.get("volume")),
                "turnover": _coerce_float(row.get("turnover")),
                "peRatio": _coerce_float(row.get("pe_ratio")),
                "turnoverRate": _coerce_float(row.get("turnover_rate")),
                "changeRate": _coerce_float(row.get("change_rate")),
                "lastClose": _coerce_float(row.get("last_close")),
            }
            for row in rows
        ]
        return {
            "symbol": symbol_from_us_code(code),
            "code": code,
            "timeframe": "1d",
            "adjustment": "qfq",
            "rows": normalized_rows,
            "count": len(normalized_rows),
            "hasMore": has_more,
            "lastUpdated": _now_iso(),
        }

    def get_us_snapshot(self, symbols: list[str]) -> dict[str, Any]:
        codes = [normalize_us_code(symbol) for symbol in symbols]
        if not codes:
            raise MoomooOpenDError(422, "At least one symbol is required")

        def operation(sdk: Any, quote_ctx: Any) -> list[dict[str, Any]]:
            ret, data = quote_ctx.get_market_snapshot(codes)
            self._ensure_ret_ok(sdk, ret, data, "get_market_snapshot")
            return _dataframe_records(data)

        logger.info(
            "moomoo US snapshot fetch",
            event="moomoo_opend_fetch",
            endpoint="get_market_snapshot",
            symbols=codes,
            correlationId=get_correlation_id(),
        )
        rows = self._with_quote_context(operation)
        items = [
            {
                "code": _coerce_str(row.get("code")) or "",
                "symbol": symbol_from_us_code(_coerce_str(row.get("code")) or ""),
                "name": _coerce_str(row.get("name")),
                "updateTime": _coerce_str(row.get("update_time")),
                "lastPrice": _coerce_float(row.get("last_price")),
                "openPrice": _coerce_float(row.get("open_price")),
                "highPrice": _coerce_float(row.get("high_price")),
                "lowPrice": _coerce_float(row.get("low_price")),
                "prevClosePrice": _coerce_float(row.get("prev_close_price")),
                "volume": _coerce_float(row.get("volume")),
                "turnover": _coerce_float(row.get("turnover")),
                "turnoverRate": _coerce_float(row.get("turnover_rate")),
                "peRatio": _coerce_float(row.get("pe_ratio")),
                "pbRatio": _coerce_float(row.get("pb_ratio")),
                "totalMarketValue": _coerce_float(row.get("total_market_val")),
                "suspension": _coerce_bool(row.get("suspension")),
            }
            for row in rows
        ]
        return {
            "symbols": [symbol_from_us_code(code) for code in codes],
            "items": items,
            "count": len(items),
            "lastUpdated": _now_iso(),
        }
