"""Signal overlay compute service backed by local market.duckdb."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

import pandas as pd
from loguru import logger

from src.application.services.analytics_provenance import build_market_provenance
from src.application.services.market_data_errors import MarketDataError
from src.application.services.market_ohlcv_loader import stock_exists_in_reader
from src.application.services.screening_market_loader import (
    load_market_multi_data,
    load_market_sector_indices,
    load_market_stock_sector_mapping,
    load_market_topix_data,
)
from src.domains.analytics.screening_requirements import build_strategy_data_requirements
from src.domains.strategy.runtime.compiler import compile_runtime_strategy
from src.domains.strategy.runtime.loader import ConfigLoader
from src.domains.strategy.runtime.screening_mode import load_strategy_screening_config
from src.domains.strategy.signals.processor import SignalProcessor
from src.domains.strategy.signals.registry import SIGNAL_REGISTRY, SignalDefinition
from src.entrypoints.http.schemas.analytics_common import ResponseDiagnostics
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams


def _build_signal_definition_map() -> dict[str, SignalDefinition]:
    mapping: dict[str, SignalDefinition] = {}
    for sig_def in SIGNAL_REGISTRY:
        signal_type = sig_def.param_key.split(".")[-1]
        if signal_type in mapping:
            logger.warning(
                f"重複するsignal_type '{signal_type}': "
                f"既存={mapping[signal_type].param_key}, 新規={sig_def.param_key}"
            )
            continue
        mapping[signal_type] = sig_def
    return mapping


_SIGNAL_DEFINITION_MAP = _build_signal_definition_map()


def _get_signal_definition(signal_type: str) -> SignalDefinition | None:
    return _SIGNAL_DEFINITION_MAP.get(signal_type)


def _format_date(idx: Any) -> str:
    return idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)


def _extract_trigger_dates(signal_series: pd.Series) -> list[str]:
    return [
        _format_date(idx)
        for idx, val in signal_series.items()
        if pd.notna(val) and bool(val)
    ]


@dataclass
class _LoadedSignalData:
    stock_code: str
    daily: pd.DataFrame
    margin_data: pd.DataFrame | None
    statements_data: pd.DataFrame | None
    benchmark_data: pd.DataFrame | None
    sector_data: dict[str, pd.DataFrame] | None
    stock_sector_name: str | None
    loaded_domains: list[str]
    warnings: list[str]


@dataclass
class _StrategyContext:
    strategy_name: str
    strategy_fingerprint: str
    shared_config: SharedConfig
    entry_params: SignalParams
    exit_params: SignalParams
    compiled_strategy: Any


class SignalService:
    """シグナル計算サービス"""

    def __init__(self, market_reader: MarketDbReader | None = None) -> None:
        self._market_reader = market_reader
        self._signal_processor = SignalProcessor()
        self._config_loader = ConfigLoader()

    def close(self) -> None:
        return None

    def _validate_date_range(
        self, start_date: date | None, end_date: date | None
    ) -> None:
        if start_date and end_date and start_date > end_date:
            raise ValueError(
                f"無効な日付範囲: start_date ({start_date}) は "
                f"end_date ({end_date}) より前である必要があります"
            )

    @staticmethod
    def _resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        if timeframe == "daily":
            return df
        freq = "W" if timeframe == "weekly" else "ME"
        return df.resample(freq).agg({
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }).dropna(subset=["Close"])

    @staticmethod
    def _resample_ohlc(df: pd.DataFrame | None, timeframe: str) -> pd.DataFrame | None:
        if df is None or df.empty or timeframe == "daily":
            return df
        freq = "W" if timeframe == "weekly" else "ME"
        return df.resample(freq).agg({
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
        }).dropna(subset=["Close"])

    @staticmethod
    def _resample_aligned_frame(
        df: pd.DataFrame | None,
        target_index: pd.DatetimeIndex,
    ) -> pd.DataFrame | None:
        if df is None or df.empty:
            return df
        return df.reindex(target_index).ffill()

    def _resolve_strategy_context(self, strategy_name: str) -> _StrategyContext:
        loaded = load_strategy_screening_config(self._config_loader, strategy_name)
        fingerprint = hashlib.sha256(
            json.dumps(
                loaded.config,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return _StrategyContext(
            strategy_name=strategy_name,
            strategy_fingerprint=fingerprint,
            shared_config=loaded.shared_config,
            entry_params=loaded.entry_params,
            exit_params=loaded.exit_params,
            compiled_strategy=loaded.compiled_strategy,
        )

    def _build_signal_params(
        self,
        signal_type: str,
        params: dict[str, Any],
        mode: Literal["entry", "exit"],
    ) -> SignalParams:
        _ = mode
        sig_def = _get_signal_definition(signal_type)
        if sig_def is None:
            raise ValueError(f"未対応のシグナル: {signal_type}")

        signal_params = SignalParams()
        param_key_parts = sig_def.param_key.split(".")

        if len(param_key_parts) == 1:
            self._update_top_level_field(signal_params, param_key_parts[0], params)
        elif len(param_key_parts) == 2:
            self._update_nested_field(signal_params, param_key_parts, params)

        return signal_params

    def _update_top_level_field(
        self,
        signal_params: SignalParams,
        field_name: str,
        params: dict[str, Any],
    ) -> None:
        if not hasattr(signal_params, field_name):
            return
        field_obj = getattr(signal_params, field_name)
        field_dict = field_obj.model_dump()
        field_dict["enabled"] = True
        for key, value in params.items():
            if key in field_dict:
                field_dict[key] = value
        setattr(signal_params, field_name, type(field_obj).model_validate(field_dict))

    def _update_nested_field(
        self,
        signal_params: SignalParams,
        param_key_parts: list[str],
        params: dict[str, Any],
    ) -> None:
        parent_name, child_name = param_key_parts
        if not hasattr(signal_params, parent_name):
            return
        parent_obj = getattr(signal_params, parent_name)
        parent_dict = parent_obj.model_dump()
        parent_dict["enabled"] = True
        if child_name in parent_dict:
            child_dict = parent_dict[child_name]
            child_dict["enabled"] = True
            for key, value in params.items():
                if key in child_dict:
                    child_dict[key] = value
            parent_dict[child_name] = child_dict
        setattr(signal_params, parent_name, type(parent_obj).model_validate(parent_dict))

    def _build_ad_hoc_param_sets(
        self,
        signals: list[dict[str, Any]],
    ) -> tuple[SignalParams, SignalParams]:
        entry_params = SignalParams()
        exit_params = SignalParams()
        for spec in signals:
            signal_type = spec["type"]
            params = spec.get("params", {})
            mode: Literal["entry", "exit"] = spec.get("mode", "entry")
            built = self._build_signal_params(signal_type, params, mode)
            target = entry_params if mode == "entry" else exit_params
            sig_def = _get_signal_definition(signal_type)
            if sig_def is None:
                continue
            parent_name = sig_def.param_key.split(".")[0]
            setattr(target, parent_name, getattr(built, parent_name))
        return entry_params, exit_params

    def _load_signal_data(
        self,
        stock_code: str,
        *,
        shared_config: SharedConfig,
        entry_params: SignalParams,
        exit_params: SignalParams,
        start_date: date | None,
        end_date: date | None,
    ) -> _LoadedSignalData:
        if self._market_reader is None:
            raise MarketDataError(
                "ローカル市場データが初期化されていません",
                reason="market_db_missing",
                recovery="market_db_sync",
            )

        start_str = start_date.isoformat() if start_date else None
        end_str = end_date.isoformat() if end_date else None
        requirements = build_strategy_data_requirements(
            shared_config=shared_config,
            entry_params=entry_params,
            exit_params=exit_params,
            stock_codes=(stock_code,),
            start_date=start_str,
            end_date=end_str,
            signal_registry=SIGNAL_REGISTRY,
        )

        warnings: list[str] = []
        multi_data, load_warnings = load_market_multi_data(
            self._market_reader,
            [stock_code],
            start_date=requirements.multi_data_key.start_date,
            end_date=requirements.multi_data_key.end_date,
            include_margin_data=requirements.multi_data_key.include_margin_data,
            include_statements_data=requirements.multi_data_key.include_statements_data,
            period_type=requirements.multi_data_key.period_type,
            include_forecast_revision=requirements.multi_data_key.include_forecast_revision,
        )
        warnings.extend(load_warnings)

        normalized_code = normalize_stock_code(stock_code)
        stock_payload = multi_data.get(normalized_code, {})
        daily = stock_payload.get("daily")
        if not isinstance(daily, pd.DataFrame) or daily.empty:
            if stock_exists_in_reader(self._market_reader, stock_code):
                raise MarketDataError(
                    f"銘柄 {stock_code} のローカルOHLCVデータがありません",
                    reason="local_stock_data_missing",
                    recovery="stock_refresh",
                )
            raise MarketDataError(
                f"銘柄 {stock_code} がローカル市場データに存在しません",
                reason="stock_not_found",
            )

        loaded_domains = ["stock_data"]
        margin_data = stock_payload.get("margin_daily")
        if isinstance(margin_data, pd.DataFrame) and not margin_data.empty:
            loaded_domains.append("margin_data")
        else:
            margin_data = None

        statements_data = stock_payload.get("statements_daily")
        if isinstance(statements_data, pd.DataFrame) and not statements_data.empty:
            loaded_domains.append("statements")
        else:
            statements_data = None

        benchmark_data: pd.DataFrame | None = None
        if requirements.needs_benchmark and requirements.benchmark_data_key is not None:
            benchmark_data = load_market_topix_data(
                self._market_reader,
                start_date=requirements.benchmark_data_key.start_date,
                end_date=requirements.benchmark_data_key.end_date,
            )
            if benchmark_data is not None and not benchmark_data.empty:
                loaded_domains.append("topix_data")

        sector_data: dict[str, pd.DataFrame] | None = None
        stock_sector_name: str | None = None
        if requirements.needs_sector and requirements.sector_data_key is not None:
            sector_data = load_market_sector_indices(
                self._market_reader,
                start_date=requirements.sector_data_key.start_date,
                end_date=requirements.sector_data_key.end_date,
            )
            stock_sector_mapping = load_market_stock_sector_mapping(self._market_reader)
            stock_sector_name = stock_sector_mapping.get(normalized_code)
            if sector_data:
                loaded_domains.append("indices_data")

        return _LoadedSignalData(
            stock_code=normalized_code or stock_code,
            daily=daily,
            margin_data=margin_data,
            statements_data=statements_data,
            benchmark_data=benchmark_data,
            sector_data=sector_data,
            stock_sector_name=stock_sector_name,
            loaded_domains=loaded_domains,
            warnings=warnings,
        )

    def _build_signal_data(
        self,
        loaded: _LoadedSignalData,
        timeframe: Literal["daily", "weekly", "monthly"],
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        ohlcv = self._resample_ohlcv(loaded.daily, timeframe)
        if ohlcv.empty or ohlcv["Close"].isna().all():
            raise ValueError(
                f"銘柄 {loaded.stock_code} のリサンプル後データが不足しています "
                f"(timeframe={timeframe})"
            )

        benchmark_data = self._resample_ohlc(loaded.benchmark_data, timeframe)
        sector_data = (
            {
                key: value
                for key, sector_df in (loaded.sector_data or {}).items()
                if (value := self._resample_ohlc(sector_df, timeframe)) is not None and not value.empty
            }
            if loaded.sector_data
            else None
        )
        margin_data = self._resample_aligned_frame(loaded.margin_data, pd.DatetimeIndex(ohlcv.index))
        statements_data = self._resample_aligned_frame(
            loaded.statements_data,
            pd.DatetimeIndex(ohlcv.index),
        )

        data = {
            "ohlc_data": ohlcv,
            "close": ohlcv["Close"],
            "execution_close": ohlcv["Close"],
            "volume": ohlcv["Volume"],
            "margin_data": margin_data,
            "statements_data": statements_data,
            "benchmark_data": benchmark_data,
            "sector_data": sector_data,
            "stock_sector_name": loaded.stock_sector_name,
            "execution_data": None,
            "is_relative_mode": False,
        }
        return ohlcv, data

    def _build_signal_diagnostics(
        self,
        signal_def: SignalDefinition,
        signal_params: SignalParams,
        data: dict[str, Any],
        error: str | None = None,
    ) -> ResponseDiagnostics:
        missing: list[str] = []
        if signal_def.data_checker and not signal_def.data_checker(data):
            description = self._signal_processor._describe_missing_requirements(signal_def, data)
            missing = [item.strip() for item in description.split(",") if item.strip()]

        effective_period_type: str | None = None
        if signal_def.param_key.startswith("fundamental"):
            effective_period_type = signal_params.fundamental.period_type

        warnings = [error] if error else []
        return ResponseDiagnostics(
            missing_required_data=missing,
            used_fields=list(signal_def.data_requirements),
            effective_period_type=effective_period_type,
            warnings=warnings,
        )

    def _slice_signal_params(
        self,
        signal_def: SignalDefinition,
        signal_params: SignalParams,
    ) -> SignalParams:
        isolated = SignalParams()
        parts = signal_def.param_key.split(".")
        parent_name = parts[0]
        parent_obj = getattr(signal_params, parent_name)
        parent_dict = parent_obj.model_dump()

        if len(parts) == 2:
            child_name = parts[1]
            for key, value in list(parent_dict.items()):
                if isinstance(value, dict) and "enabled" in value:
                    parent_dict[key] = {
                        **value,
                        "enabled": key == child_name and bool(value.get("enabled", False)),
                    }
            parent_dict["enabled"] = True

        setattr(
            isolated,
            parent_name,
            type(parent_obj).model_validate(parent_dict),
        )
        return isolated

    def _compute_signal_result_from_params(
        self,
        *,
        signal_type: str,
        mode: Literal["entry", "exit"],
        signal_params: SignalParams,
        data: dict[str, Any],
        compiled_strategy: Any,
    ) -> dict[str, Any]:
        sig_def = _get_signal_definition(signal_type)
        if sig_def is None:
            diagnostics = ResponseDiagnostics(
                missing_required_data=[],
                used_fields=[],
                warnings=[f"未対応のシグナル: {signal_type}"],
            )
            return {
                "label": signal_type,
                "mode": mode,
                "trigger_dates": [],
                "count": 0,
                "error": f"未対応のシグナル: {signal_type}",
                "diagnostics": diagnostics.model_dump(mode="json"),
            }

        isolated_params = self._slice_signal_params(sig_def, signal_params)
        diagnostics = self._build_signal_diagnostics(sig_def, isolated_params, data)

        if mode == "exit" and sig_def.exit_disabled:
            diagnostics.warnings.append(f"シグナル '{signal_type}' はExitモードでは使用できません")
            return {
                "label": sig_def.name,
                "mode": mode,
                "trigger_dates": [],
                "count": 0,
                "error": f"シグナル '{signal_type}' はExitモードでは使用できません",
                "diagnostics": diagnostics.model_dump(mode="json"),
            }

        try:
            base_signal = (
                pd.Series(True, index=data["ohlc_data"].index)
                if mode == "entry"
                else pd.Series(False, index=data["ohlc_data"].index)
            )
            if mode == "entry":
                signal_series = self._signal_processor.apply_entry_signals(
                    base_signal=base_signal,
                    ohlc_data=data["ohlc_data"],
                    signal_params=isolated_params,
                    margin_data=data.get("margin_data"),
                    statements_data=data.get("statements_data"),
                    benchmark_data=data.get("benchmark_data"),
                    execution_data=data.get("execution_data"),
                    relative_mode=bool(data.get("is_relative_mode", False)),
                    sector_data=data.get("sector_data"),
                    stock_sector_name=data.get("stock_sector_name"),
                    compiled_strategy=compiled_strategy,
                )
            else:
                signal_series = self._signal_processor.apply_exit_signals(
                    base_exits=base_signal,
                    data=data["ohlc_data"],
                    signal_params=isolated_params,
                    margin_data=data.get("margin_data"),
                    statements_data=data.get("statements_data"),
                    benchmark_data=data.get("benchmark_data"),
                    execution_data=data.get("execution_data"),
                    relative_mode=bool(data.get("is_relative_mode", False)),
                    sector_data=data.get("sector_data"),
                    stock_sector_name=data.get("stock_sector_name"),
                    compiled_strategy=compiled_strategy,
                )
            trigger_dates = _extract_trigger_dates(signal_series)
            return {
                "label": sig_def.name,
                "mode": mode,
                "trigger_dates": trigger_dates,
                "count": len(trigger_dates),
                "diagnostics": diagnostics.model_dump(mode="json"),
            }
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"シグナル '{signal_type}' 計算失敗")
            diagnostics.warnings.append(str(exc))
            return {
                "label": sig_def.name,
                "mode": mode,
                "trigger_dates": [],
                "count": 0,
                "error": str(exc),
                "diagnostics": diagnostics.model_dump(mode="json"),
            }

    def _compute_single_signal_result(
        self,
        *,
        signal_type: str,
        mode: Literal["entry", "exit"],
        params: dict[str, Any],
        data: dict[str, Any],
        compiled_strategy: Any,
    ) -> dict[str, Any]:
        signal_params = self._build_signal_params(signal_type, params, mode)
        return self._compute_signal_result_from_params(
            signal_type=signal_type,
            mode=mode,
            signal_params=signal_params,
            data=data,
            compiled_strategy=compiled_strategy,
        )

    def _extract_strategy_signal_specs(
        self,
        entry_params: SignalParams,
        exit_params: SignalParams,
    ) -> list[tuple[str, Literal["entry", "exit"], SignalParams]]:
        specs: list[tuple[str, Literal["entry", "exit"], SignalParams]] = []
        for sig_def in SIGNAL_REGISTRY:
            signal_type = sig_def.param_key.split(".")[-1]
            if sig_def.enabled_checker(entry_params):
                specs.append((signal_type, "entry", entry_params))
            if not sig_def.exit_disabled and sig_def.enabled_checker(exit_params):
                specs.append((signal_type, "exit", exit_params))
        return specs

    def compute_signals(
        self,
        stock_code: str,
        source: str,
        timeframe: Literal["daily", "weekly", "monthly"],
        signals: list[dict[str, Any]] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        strategy_name: str | None = None,
    ) -> dict[str, Any]:
        self._validate_date_range(start_date, end_date)
        if source != "market":
            raise ValueError("source='market' のみ対応しています")

        signals = signals or []
        if strategy_name is None and not signals:
            return {
                "stock_code": stock_code,
                "timeframe": timeframe,
                "strategy_name": None,
                "signals": {},
                "provenance": build_market_provenance(
                    loaded_domains=("stock_data",),
                ).model_dump(mode="json"),
                "diagnostics": ResponseDiagnostics().model_dump(mode="json"),
            }

        if strategy_name is not None:
            strategy_context = self._resolve_strategy_context(strategy_name)
            entry_params = strategy_context.entry_params
            exit_params = strategy_context.exit_params
            shared_config = strategy_context.shared_config
            compiled_strategy = strategy_context.compiled_strategy
        else:
            entry_params, exit_params = self._build_ad_hoc_param_sets(signals)
            shared_config = SharedConfig.model_validate(
                {"timeframe": "daily", "execution_policy": {"mode": "standard"}},
                context={"resolve_stock_codes": False},
            )
            compiled_strategy = compile_runtime_strategy(
                strategy_name="ad_hoc_signal_request",
                shared_config=shared_config,
                entry_signal_params=entry_params,
                exit_signal_params=exit_params,
            )
            strategy_context = None

        loaded = self._load_signal_data(
            stock_code,
            shared_config=shared_config,
            entry_params=entry_params,
            exit_params=exit_params,
            start_date=start_date,
            end_date=end_date,
        )
        ohlcv, data = self._build_signal_data(loaded, timeframe)

        if strategy_context is not None:
            signal_results: dict[str, dict[str, Any]] = {}
            for signal_type, mode, signal_params in self._extract_strategy_signal_specs(
                entry_params,
                exit_params,
            ):
                key = f"{mode}:{signal_type}"
                signal_results[key] = self._compute_signal_result_from_params(
                    signal_type=signal_type,
                    mode=mode,
                    signal_params=signal_params,
                    data=data,
                    compiled_strategy=compiled_strategy,
                )

            combined = self._signal_processor.generate_signals(
                strategy_entries=pd.Series(True, index=ohlcv.index),
                strategy_exits=pd.Series(False, index=ohlcv.index),
                ohlc_data=ohlcv,
                entry_signal_params=entry_params,
                exit_signal_params=exit_params,
                margin_data=data.get("margin_data"),
                statements_data=data.get("statements_data"),
                benchmark_data=data.get("benchmark_data"),
                execution_data=data.get("execution_data"),
                relative_mode=False,
                sector_data=data.get("sector_data"),
                stock_sector_name=data.get("stock_sector_name"),
                compiled_strategy=compiled_strategy,
            )
            combined_entry = {
                "label": f"{strategy_name} entry",
                "mode": "entry",
                "trigger_dates": _extract_trigger_dates(combined.entries),
                "count": int(combined.entries.fillna(False).astype(bool).sum()),
                "diagnostics": ResponseDiagnostics().model_dump(mode="json"),
            }
            combined_exit = {
                "label": f"{strategy_name} exit",
                "mode": "exit",
                "trigger_dates": _extract_trigger_dates(combined.exits),
                "count": int(combined.exits.fillna(False).astype(bool).sum()),
                "diagnostics": ResponseDiagnostics().model_dump(mode="json"),
            }
            all_missing = sorted({
                item
                for result in signal_results.values()
                for item in result.get("diagnostics", {}).get("missing_required_data", [])
            })
            return {
                "stock_code": stock_code,
                "timeframe": timeframe,
                "strategy_name": strategy_name,
                "signals": signal_results,
                "combined_entry": combined_entry,
                "combined_exit": combined_exit,
                "provenance": build_market_provenance(
                    reference_date=end_date.isoformat() if end_date else None,
                    loaded_domains=loaded.loaded_domains,
                    warnings=loaded.warnings,
                    strategy_name=strategy_context.strategy_name,
                    strategy_fingerprint=strategy_context.strategy_fingerprint,
                ).model_dump(mode="json"),
                "diagnostics": ResponseDiagnostics(
                    missing_required_data=all_missing,
                    used_fields=loaded.loaded_domains,
                    warnings=loaded.warnings,
                ).model_dump(mode="json"),
            }

        signal_results = {
            spec["type"]: self._compute_single_signal_result(
                signal_type=spec["type"],
                mode=spec.get("mode", "entry"),
                params=spec.get("params", {}),
                data=data,
                compiled_strategy=compiled_strategy,
            )
            for spec in signals
        }
        all_missing = sorted({
            item
            for result in signal_results.values()
            for item in result.get("diagnostics", {}).get("missing_required_data", [])
        })
        return {
            "stock_code": stock_code,
            "timeframe": timeframe,
            "strategy_name": None,
            "signals": signal_results,
            "combined_entry": None,
            "combined_exit": None,
            "provenance": build_market_provenance(
                reference_date=end_date.isoformat() if end_date else None,
                loaded_domains=loaded.loaded_domains,
                warnings=loaded.warnings,
            ).model_dump(mode="json"),
            "diagnostics": ResponseDiagnostics(
                missing_required_data=all_missing,
                used_fields=loaded.loaded_domains,
                warnings=loaded.warnings,
            ).model_dump(mode="json"),
        }


signal_service = SignalService()
