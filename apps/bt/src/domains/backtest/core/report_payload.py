"""Backtest report payload serialization helpers."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.domains.backtest.core.simulation import BacktestSimulationResult
from src.shared.models.allocation import AllocationInfo


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    return coerced if math.isfinite(coerced) else None


def _coerce_series_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return value
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    return str(value)


def _serialize_series(series: Any) -> dict[str, Any] | None:
    if series is None:
        return None
    if isinstance(series, pd.DataFrame):
        if series.shape[1] == 1:
            series = series.iloc[:, 0]
        else:
            return None
    if not isinstance(series, pd.Series):
        try:
            series = pd.Series(series)
        except Exception:
            return None
    normalized = series.copy()
    index = [_coerce_series_value(v) for v in normalized.index.tolist()]
    values = [_coerce_series_value(v) for v in normalized.tolist()]
    return {"index": index, "values": values}


def _deserialize_series(payload: dict[str, Any] | None) -> pd.Series:
    if not isinstance(payload, dict):
        return pd.Series(dtype=float)
    index = payload.get("index", [])
    values = payload.get("values", [])
    return pd.Series(values, index=index)


def _serialize_dataframe(frame: Any) -> list[dict[str, Any]]:
    if frame is None:
        return []
    if isinstance(frame, pd.Series):
        frame = frame.to_frame(name="value")
    if not isinstance(frame, pd.DataFrame):
        try:
            frame = pd.DataFrame(frame)
        except Exception:
            return []
    normalized = frame.copy()
    for column in normalized.columns:
        normalized[column] = normalized[column].map(_coerce_series_value)
    return [
        {str(key): value for key, value in record.items()}
        for record in normalized.to_dict(orient="records")
    ]


def _deserialize_dataframe(records: Any) -> pd.DataFrame:
    if not isinstance(records, list):
        return pd.DataFrame()
    return pd.DataFrame(records)


def _serialize_metric_map(portfolio: Any) -> dict[str, float | None]:
    def _read_metric(method_name: str) -> float | None:
        if portfolio is None:
            return None
        try:
            method = getattr(portfolio, method_name)
        except Exception:
            return None
        try:
            value = method() if callable(method) else method
        except Exception:
            return None
        if hasattr(value, "mean"):
            try:
                value = value.mean()
            except Exception:
                return None
        return _coerce_float(value)

    return {
        "annualized_volatility": _read_metric("annualized_volatility"),
        "sharpe_ratio": _read_metric("sharpe_ratio"),
        "sortino_ratio": _read_metric("sortino_ratio"),
        "calmar_ratio": _read_metric("calmar_ratio"),
        "omega_ratio": _read_metric("omega_ratio"),
    }


def _serialize_stats(stats: Any) -> list[dict[str, Any]]:
    if stats is None:
        return []
    if isinstance(stats, pd.Series):
        frame = stats.reset_index()
        frame.columns = ["metric", "value"]
        return _serialize_dataframe(frame)
    if isinstance(stats, pd.DataFrame):
        return _serialize_dataframe(stats.reset_index())
    try:
        frame = pd.DataFrame(stats)
    except Exception:
        return []
    return _serialize_dataframe(frame.reset_index())


def _deserialize_stats(records: Any) -> pd.Series:
    frame = _deserialize_dataframe(records)
    if frame.empty:
        return pd.Series(dtype=object)
    if "metric" in frame.columns and "value" in frame.columns:
        return pd.Series(frame["value"].to_list(), index=frame["metric"].to_list())
    first = frame.columns[0]
    return pd.Series(frame.iloc[:, 1].to_list(), index=frame[first].to_list())


def _serialize_portfolio(portfolio: Any) -> dict[str, Any] | None:
    if portfolio is None:
        return None
    trades = getattr(portfolio, "trades", None)
    trade_records = getattr(trades, "records_readable", None) if trades is not None else None
    try:
        trade_stats = trades.stats() if trades is not None else None
    except Exception:
        trade_stats = None
    try:
        final_stats = portfolio.stats()
    except Exception:
        final_stats = None
    return {
        "value_series": _serialize_series(portfolio.value()),
        "drawdown_series": _serialize_series(portfolio.drawdown()),
        "returns_series": _serialize_series(portfolio.returns()),
        "trade_records": _serialize_dataframe(trade_records),
        "trade_stats": _serialize_stats(trade_stats),
        "final_stats": _serialize_stats(final_stats),
        "risk_metrics": _serialize_metric_map(portfolio),
    }


def _deserialize_portfolio(payload: dict[str, Any] | None) -> "SerializedPortfolioView | None":
    if not isinstance(payload, dict):
        return None
    return SerializedPortfolioView(
        value_series=_deserialize_series(payload.get("value_series")),
        drawdown_series=_deserialize_series(payload.get("drawdown_series")),
        returns_series=_deserialize_series(payload.get("returns_series")),
        trade_records=_deserialize_dataframe(payload.get("trade_records")),
        trade_stats=_deserialize_stats(payload.get("trade_stats")),
        final_stats=_deserialize_stats(payload.get("final_stats")),
        risk_metrics=payload.get("risk_metrics", {}),
    )


def _serialize_entry_counts(all_entries: Any) -> dict[str, Any] | None:
    if all_entries is None:
        return None
    try:
        entries_per_day = all_entries.sum(axis=1)
    except Exception:
        return None
    return _serialize_series(entries_per_day)


def _deserialize_entry_counts(payload: dict[str, Any] | None) -> pd.DataFrame | None:
    if not isinstance(payload, dict):
        return None
    series = _deserialize_series(payload)
    if series.empty:
        return pd.DataFrame(columns=["signal_count"])
    return pd.DataFrame({"signal_count": series})


def _serialize_allocation_info(allocation_info: Any) -> dict[str, Any] | None:
    if allocation_info is None:
        return None
    if isinstance(allocation_info, AllocationInfo):
        return {"kind": "allocation_info", "payload": allocation_info.model_dump()}
    if isinstance(allocation_info, (int, float)):
        return {"kind": "scalar", "payload": allocation_info}
    if hasattr(allocation_info, "model_dump"):
        try:
            return {"kind": "allocation_info", "payload": allocation_info.model_dump()}
        except Exception:
            pass
    return {"kind": "text", "payload": str(allocation_info)}


def _deserialize_allocation_info(payload: dict[str, Any] | None) -> Any:
    if not isinstance(payload, dict):
        return None
    kind = payload.get("kind")
    value = payload.get("payload")
    if kind == "allocation_info" and isinstance(value, dict):
        try:
            return AllocationInfo.model_validate(value)
        except Exception:
            return value
    return value


def build_backtest_report_payload(
    simulation_result: BacktestSimulationResult,
) -> dict[str, Any]:
    """Serialize the simulation stage output for presentation-only rendering."""

    return {
        "allocation_info": _serialize_allocation_info(simulation_result.allocation_info),
        "entry_signal_counts": _serialize_entry_counts(simulation_result.all_entries),
        "initial_portfolio": _serialize_portfolio(simulation_result.initial_portfolio),
        "kelly_portfolio": _serialize_portfolio(simulation_result.kelly_portfolio),
    }


def write_backtest_report_payload(
    *,
    path: Path,
    payload: dict[str, Any],
) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


@dataclass(slots=True)
class SerializedTradesView:
    records_readable: pd.DataFrame
    _stats: pd.Series

    def stats(self) -> pd.Series:
        return self._stats


@dataclass(slots=True)
class SerializedPortfolioView:
    value_series: pd.Series
    drawdown_series: pd.Series
    returns_series: pd.Series
    trade_records: pd.DataFrame
    trade_stats: pd.Series
    final_stats: pd.Series
    risk_metrics: dict[str, Any]

    @property
    def trades(self) -> SerializedTradesView:
        return SerializedTradesView(self.trade_records, self.trade_stats)

    def value(self) -> pd.Series:
        return self.value_series

    def drawdown(self) -> pd.Series:
        return self.drawdown_series

    def returns(self) -> pd.Series:
        return self.returns_series

    def annualized_volatility(self) -> Any:
        return self.risk_metrics.get("annualized_volatility")

    def sharpe_ratio(self) -> Any:
        return self.risk_metrics.get("sharpe_ratio")

    def sortino_ratio(self) -> Any:
        return self.risk_metrics.get("sortino_ratio")

    def calmar_ratio(self) -> Any:
        return self.risk_metrics.get("calmar_ratio")

    def omega_ratio(self) -> Any:
        return self.risk_metrics.get("omega_ratio")

    def stats(self) -> pd.Series:
        return self.final_stats


@dataclass(slots=True)
class BacktestReportRenderContext:
    initial_portfolio: SerializedPortfolioView | None
    kelly_portfolio: SerializedPortfolioView | None
    allocation_info: Any
    all_entries: pd.DataFrame | None


def load_backtest_report_payload(path: str | Path) -> BacktestReportRenderContext:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return BacktestReportRenderContext(
        initial_portfolio=_deserialize_portfolio(payload.get("initial_portfolio")),
        kelly_portfolio=_deserialize_portfolio(payload.get("kelly_portfolio")),
        allocation_info=_deserialize_allocation_info(payload.get("allocation_info")),
        all_entries=_deserialize_entry_counts(payload.get("entry_signal_counts")),
    )
