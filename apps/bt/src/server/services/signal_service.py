"""Signal Service: OHLCV系シグナル計算サービス

Phase 1: OHLCV系シグナルのみ対応（fundamental/margin/sector/benchmark除外）
"""

from __future__ import annotations

import threading
from datetime import date
from typing import Any, Literal

import pandas as pd
from loguru import logger

from src.lib.market_db.market_reader import MarketDbReader
from src.server.services.market_ohlcv_loader import load_stock_ohlcv_df
from src.strategies.signals.registry import SIGNAL_REGISTRY, SignalDefinition

# Phase 1対象シグナル（OHLCV系: ohlc/volume データのみ使用）
PHASE1_SIGNAL_NAMES: frozenset[str] = frozenset({
    # oscillator
    "rsi_threshold",
    "rsi_spread",
    # breakout
    "period_breakout",
    "ma_breakout",
    "atr_support_break",
    "retracement",
    "mean_reversion",
    "crossover",
    "buy_and_hold",
    # volatility
    "bollinger_bands",
    # volume
    "volume",
    "trading_value",
    "trading_value_range",
})


def _build_signal_definition_map() -> dict[str, SignalDefinition]:
    """SIGNAL_REGISTRYからsignal_type -> SignalDefinitionのマッピングを構築

    param_keyからsignal_typeを抽出:
      "volume" -> "volume", "fundamental.per" -> "per"
    """
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


# シグナル定義マッピング（モジュールロード時に構築）
_SIGNAL_DEFINITION_MAP: dict[str, SignalDefinition] = _build_signal_definition_map()


def _get_signal_definition(signal_type: str) -> SignalDefinition | None:
    """シグナル定義を取得"""
    return _SIGNAL_DEFINITION_MAP.get(signal_type)


def _format_date(idx: Any) -> str:
    """日付インデックスをISO形式文字列に変換"""
    if hasattr(idx, "strftime"):
        return idx.strftime("%Y-%m-%d")
    return str(idx)


def _extract_trigger_dates(signal_series: pd.Series[bool]) -> list[str]:
    """booleanシリーズからTrueの日付を抽出（NaN安全）"""
    return [
        _format_date(idx)
        for idx, val in signal_series.items()
        if pd.notna(val) and val is True
    ]


class SignalService:
    """シグナル計算サービス"""

    def __init__(self, market_reader: MarketDbReader | None = None) -> None:
        self._market_reader = market_reader
        self._market_client = None
        self._client_lock = threading.Lock()

    @property
    def market_client(self):
        """MarketAPIClientをスレッドセーフに遅延初期化（ダブルチェックロッキング）"""
        if self._market_client is None:
            with self._client_lock:
                if self._market_client is None:
                    from src.api.market_client import MarketAPIClient

                    self._market_client = MarketAPIClient()
        return self._market_client

    def close(self) -> None:
        if self._market_client is not None:
            self._market_client.close()
            self._market_client = None

    def load_ohlcv(
        self,
        stock_code: str,
        source: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """OHLCVデータをロード"""
        start_str = start_date.isoformat() if start_date else None
        end_str = end_date.isoformat() if end_date else None

        if source == "market":
            if self._market_reader is not None:
                df = load_stock_ohlcv_df(self._market_reader, stock_code, start_str, end_str)
            else:
                df = self.market_client.get_stock_ohlcv(stock_code, start_str, end_str)
        else:
            from src.api.dataset import DatasetAPIClient

            with DatasetAPIClient(source) as client:
                df = client.get_stock_ohlcv(stock_code, start_str, end_str)

        if df.empty:
            raise ValueError(f"銘柄 {stock_code} のOHLCVデータが取得できません")
        return df

    @staticmethod
    def resample_timeframe(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """時間枠でリサンプル（daily/weekly/monthly）"""
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

    def _build_signal_data(self, ohlcv: pd.DataFrame) -> dict[str, Any]:
        """シグナル計算用データ辞書を構築（OHLCV系専用）"""
        return {
            "ohlc_data": ohlcv,
            "close": ohlcv["Close"],
            "execution_close": ohlcv["Close"],
            "volume": ohlcv["Volume"],
        }

    def _build_signal_params(
        self,
        signal_type: str,
        params: dict[str, Any],
        mode: Literal["entry", "exit"],
    ) -> Any:
        """APIリクエストのparamsからSignalParamsオブジェクトを構築"""
        from src.models.signals import SignalParams

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
        signal_params: Any,
        field_name: str,
        params: dict[str, Any],
    ) -> None:
        """トップレベルフィールドを更新（例: volume, beta）"""
        if not hasattr(signal_params, field_name):
            return

        field_obj = getattr(signal_params, field_name)
        field_dict = field_obj.model_dump()
        field_dict["enabled"] = True

        for key, value in params.items():
            if key in field_dict:
                field_dict[key] = value

        new_field = type(field_obj).model_validate(field_dict)
        setattr(signal_params, field_name, new_field)

    def _update_nested_field(
        self,
        signal_params: Any,
        param_key_parts: list[str],
        params: dict[str, Any],
    ) -> None:
        """ネストフィールドを更新（例: fundamental.per）"""
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

        new_parent = type(parent_obj).model_validate(parent_dict)
        setattr(signal_params, parent_name, new_parent)

    def compute_signal(
        self,
        signal_type: str,
        params: dict[str, Any],
        mode: Literal["entry", "exit"],
        data: dict[str, Any],
    ) -> pd.Series[bool]:
        """単一シグナルを計算"""
        sig_def = _get_signal_definition(signal_type)
        if sig_def is None:
            raise ValueError(f"未対応のシグナル: {signal_type}")

        if signal_type not in PHASE1_SIGNAL_NAMES:
            raise ValueError(
                f"シグナル '{signal_type}' はPhase 1では未対応です "
                f"(OHLCV系のみ対応: {sorted(PHASE1_SIGNAL_NAMES)})"
            )

        if mode == "exit" and sig_def.exit_disabled:
            raise ValueError(f"シグナル '{signal_type}' はExitモードでは使用できません")

        signal_params = self._build_signal_params(signal_type, params, mode)
        func_params = sig_def.param_builder(signal_params, data)

        return sig_def.signal_func(**func_params)

    def compute_signals(
        self,
        stock_code: str,
        source: str,
        timeframe: Literal["daily", "weekly", "monthly"],
        signals: list[dict[str, Any]],
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """複数シグナルを一括計算し、発火日を返却"""
        self._validate_date_range(start_date, end_date)

        if not signals:
            logger.debug(f"空のシグナルリスト: stock_code={stock_code}")
            return {"stock_code": stock_code, "timeframe": timeframe, "signals": {}}

        ohlcv = self.load_ohlcv(stock_code, source, start_date, end_date)
        ohlcv = self.resample_timeframe(ohlcv, timeframe)
        self._validate_resampled_data(ohlcv, stock_code, timeframe)

        data = self._build_signal_data(ohlcv)
        results = self._compute_all_signals(signals, data)

        return {"stock_code": stock_code, "timeframe": timeframe, "signals": results}

    def _validate_date_range(
        self, start_date: date | None, end_date: date | None
    ) -> None:
        """日付範囲のバリデーション"""
        if start_date and end_date and start_date > end_date:
            raise ValueError(
                f"無効な日付範囲: start_date ({start_date}) は "
                f"end_date ({end_date}) より前である必要があります"
            )

    def _validate_resampled_data(
        self, ohlcv: pd.DataFrame, stock_code: str, timeframe: str
    ) -> None:
        """リサンプル後データの品質検証"""
        if ohlcv.empty or ohlcv["Close"].isna().all():
            raise ValueError(
                f"銘柄 {stock_code} のリサンプル後データが不足しています "
                f"(timeframe={timeframe})"
            )

    def _compute_all_signals(
        self,
        signals: list[dict[str, Any]],
        data: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        """全シグナルを計算して結果を返却"""
        results: dict[str, dict[str, Any]] = {}

        for spec in signals:
            signal_type = spec["type"]
            params = spec.get("params", {})
            mode: Literal["entry", "exit"] = spec.get("mode", "entry")

            try:
                signal_series = self.compute_signal(signal_type, params, mode, data)
                trigger_dates = _extract_trigger_dates(signal_series)
                results[signal_type] = {
                    "trigger_dates": trigger_dates,
                    "count": len(trigger_dates),
                }
            except Exception as e:
                logger.exception(f"シグナル '{signal_type}' 計算失敗")
                results[signal_type] = {
                    "trigger_dates": [],
                    "count": 0,
                    "error": str(e),
                }

        return results


# グローバルインスタンス
signal_service = SignalService()
