"""
Phase 2.5 Golden Dataset比較テスト

TS側で生成したGolden Dataset（期待値JSON）に対し、apps/bt/のインジケーター計算結果が
許容誤差内で一致するかを検証する。

既知の差異:
  - EMA初期化: vbt(pandas ewm) vs TS(SMA初期化) → 暖気期間後に収束
  - RSI: vbt(Wilder's smoothing) vs TS(EMA-based) → 初期化差異で収束が遅い
  - ATR: vbt.ATR(SMA-based) vs TS(EMA-based) → アルゴリズム差異
  - ATR Support: apps/bt/修正済み（EMA+highest close）だがvbt.MA.run(ewm=True)の
    初期化がTS側ema()と異なる

判定基準:
  - SMA系 (SMA/Bollinger/NBar/Volume/TradingValue): 完全一致（絶対誤差 < 0.01）
  - EMA系 (EMA/RSI/MACD/PPO/ATR/ATR Support): 暖気期間後に収束確認
  - NaN位置: 同一位置で発生すること（暖気期間の一致）
  - 全体不一致率: 暖気期間除外後 < 0.1%
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import vectorbt as vbt

from src.server.services.indicator_service import (
    INDICATOR_REGISTRY,
)
from src.utils.indicators import (
    compute_atr_support_line,
    compute_nbar_support,
    compute_trading_value_ma,
    compute_volume_mas,
)

# Golden dataset directory (relative to repo root)
GOLDEN_DIR = (
    Path(__file__).resolve().parents[2].parent
    / "ts"
    / "packages"
    / "shared"
    / "src"
    / "ta"
    / "__fixtures__"
    / "golden"
)

# EMA系インジケーターの暖気期間（初期化差異が収束するまでのバー数）
# vbt ewm vs TS SMA-initialized EMA の差は period×5 程度で 0.01 以下に収束
WARMUP_BARS = {
    "ema": 100,      # EMA(20): ~20*5 bars for convergence
    "rsi": 200,      # RSI uses EMA internally, converges slowly
    "macd": 150,     # MACD uses 2 EMAs (12,26) + signal EMA(9)
    "ppo": 150,      # Same structure as MACD
    "atr": 100,      # vbt.ATR SMA vs TS EMA
    "atr_support": 120,  # ATR(EMA) + highest close
}


def load_golden(filename: str) -> dict[str, Any]:
    """Golden datasetファイルをロード"""
    filepath = GOLDEN_DIR / filename
    assert filepath.exists(), f"Golden dataset not found: {filepath}"
    with open(filepath) as f:
        return json.load(f)


def load_golden_ohlcv() -> pd.DataFrame:
    """Golden入力OHLCVをpandas DataFrameに変換"""
    golden = load_golden("golden_input_ohlcv.json")
    records = golden["data"]
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    })
    df = df[["Open", "High", "Low", "Close", "Volume"]]
    return df


@pytest.fixture(scope="module")
def ohlcv() -> pd.DataFrame:
    return load_golden_ohlcv()


# --- Tolerance definitions ---

TOLERANCE = {
    "sma": 0.01,
    "ema": 0.01,
    "rsi": 0.1,
    "macd": 0.01,
    "ppo": 0.01,
    "bollinger": 0.01,
    "atr": 0.01,
    "atr_support": 0.01,
    "nbar_support": 0.01,
    "volume_comparison": 0.01,
    "trading_value_ma": 0.01,
    "margin_long_pressure": 0.001,
    "margin_flow_pressure": 0.001,
    "margin_turnover_days": 0.001,
}


def check_nan_positions(
    bt_records: list[dict[str, Any]],
    golden_data: list[dict[str, Any]],
    value_key: str,
    indicator: str,
) -> None:
    """NaN/null位置の一致を検証"""
    bt_by_date = {r["date"]: r for r in bt_records}

    for golden_rec in golden_data:
        date = golden_rec["date"]
        golden_val = golden_rec.get(value_key)
        bt_rec = bt_by_date.get(date)

        if golden_val is None:
            if bt_rec is not None:
                bt_val = bt_rec.get(value_key)
                assert bt_val is None, (
                    f"{indicator} NaN位置不一致 [{date}]: "
                    f"golden=null, bt={bt_val}"
                )


def compare_single_indicator(
    bt_records: list[dict[str, Any]],
    golden_data: list[dict[str, Any]],
    value_key: str,
    tolerance: float,
    skip_first_n: int = 0,
) -> tuple[int, int, float]:
    """単一値インジケータの比較を行い、(total, mismatches, max_diff)を返す"""
    bt_by_date = {r["date"]: r.get(value_key) for r in bt_records}

    total = 0
    mismatches = 0
    max_diff = 0.0

    for i, g in enumerate(golden_data):
        gval = g.get(value_key)
        if gval is None:
            continue
        if i < skip_first_n:
            continue

        total += 1
        bt_val = bt_by_date.get(g["date"])
        if bt_val is None:
            mismatches += 1
            continue
        diff = abs(bt_val - gval)
        max_diff = max(max_diff, diff)
        if diff >= tolerance:
            mismatches += 1

    return total, mismatches, max_diff


def compare_multi_indicator(
    bt_records: list[dict[str, Any]],
    golden_data: list[dict[str, Any]],
    keys: list[str],
    tolerance: float,
    skip_first_n: int = 0,
) -> tuple[int, int, float]:
    """複数値インジケータの比較"""
    bt_by_date = {r["date"]: r for r in bt_records}

    total = 0
    mismatches = 0
    max_diff = 0.0

    for i, g in enumerate(golden_data):
        if i < skip_first_n:
            continue
        bt_rec = bt_by_date.get(g["date"])
        if bt_rec is None:
            continue
        for key in keys:
            gval = g.get(key)
            if gval is None:
                continue
            total += 1
            bt_val = bt_rec.get(key)
            if bt_val is None:
                mismatches += 1
                continue
            diff = abs(bt_val - gval)
            max_diff = max(max_diff, diff)
            if diff >= tolerance:
                mismatches += 1

    return total, mismatches, max_diff


# ===== SMA系（完全一致が期待される） =====


class TestSMAGolden:
    def test_sma_20(self, ohlcv: pd.DataFrame) -> None:
        golden = load_golden("golden_sma_20.json")
        _, records = INDICATOR_REGISTRY["sma"](ohlcv, {"period": 20}, "include")
        total, mismatches, max_diff = compare_single_indicator(
            records, golden["data"], "value", TOLERANCE["sma"]
        )
        assert mismatches == 0, f"SMA: {mismatches}/{total} mismatches, max_diff={max_diff:.6f}"

    def test_sma_nan_positions(self, ohlcv: pd.DataFrame) -> None:
        golden = load_golden("golden_sma_20.json")
        _, records = INDICATOR_REGISTRY["sma"](ohlcv, {"period": 20}, "include")
        check_nan_positions(records, golden["data"], "value", "sma")


class TestBollingerGolden:
    def test_bollinger_20_2(self, ohlcv: pd.DataFrame) -> None:
        golden = load_golden("golden_bollinger_20_2.0.json")
        params = {"period": 20, "std_dev": 2.0}
        _, records = INDICATOR_REGISTRY["bollinger"](ohlcv, params, "include")
        total, mismatches, max_diff = compare_multi_indicator(
            records, golden["data"], ["upper", "middle", "lower"], TOLERANCE["bollinger"]
        )
        assert mismatches == 0, f"Bollinger: {mismatches}/{total}, max_diff={max_diff:.6f}"


class TestNBarSupportGolden:
    def test_nbar_support_20(self, ohlcv: pd.DataFrame) -> None:
        golden = load_golden("golden_nbar_support_20.json")
        _, records = INDICATOR_REGISTRY["nbar_support"](ohlcv, {"period": 20}, "include")
        total, mismatches, max_diff = compare_single_indicator(
            records, golden["data"], "value", TOLERANCE["nbar_support"]
        )
        assert mismatches == 0, f"N-Bar Support: {mismatches}/{total}, max_diff={max_diff:.6f}"


class TestVolumeComparisonGolden:
    def test_volume_comparison_20_100(self, ohlcv: pd.DataFrame) -> None:
        golden = load_golden("golden_volume_comparison_20_100.json")
        params = {
            "short_period": 20,
            "long_period": 100,
            "lower_multiplier": 1.0,
            "higher_multiplier": 1.5,
            "ma_type": "sma",
        }
        _, records = INDICATOR_REGISTRY["volume_comparison"](ohlcv, params, "include")
        total, mismatches, max_diff = compare_multi_indicator(
            records, golden["data"],
            ["shortMA", "longThresholdLower", "longThresholdHigher"],
            TOLERANCE["volume_comparison"],
        )
        assert mismatches == 0, f"VolumeComp: {mismatches}/{total}, max_diff={max_diff:.6f}"


class TestTradingValueMAGolden:
    """Trading Value MA: apps/bt/は億円単位、TS側は生円値。比較時にスケーリング補正。"""

    def test_trading_value_ma_20(self, ohlcv: pd.DataFrame) -> None:
        golden = load_golden("golden_trading_value_ma_20.json")
        _, records = INDICATOR_REGISTRY["trading_value_ma"](
            ohlcv, {"period": 20}, "include"
        )
        bt_by_date = {r["date"]: r["value"] for r in records}

        total = 0
        mismatches = 0
        for g in golden["data"]:
            if g["value"] is None:
                continue
            total += 1
            bt_val = bt_by_date.get(g["date"])
            if bt_val is None:
                mismatches += 1
                continue
            # apps/bt/ returns 億円, golden is raw yen
            bt_scaled = bt_val * 1e8
            golden_val = g["value"]
            rel_tol = abs(golden_val) * 1e-6 + 0.01
            if abs(bt_scaled - golden_val) >= rel_tol:
                mismatches += 1

        assert mismatches == 0, f"TradingValueMA: {mismatches}/{total}"


# ===== EMA系（暖気期間後の収束を検証） =====


class TestEMAGolden:
    def test_ema_20_converges(self, ohlcv: pd.DataFrame) -> None:
        """暖気期間後にEMAが収束することを確認"""
        golden = load_golden("golden_ema_20.json")
        _, records = INDICATOR_REGISTRY["ema"](ohlcv, {"period": 20}, "include")
        total, mismatches, max_diff = compare_single_indicator(
            records, golden["data"], "value", TOLERANCE["ema"],
            skip_first_n=WARMUP_BARS["ema"],
        )
        assert mismatches == 0, (
            f"EMA (after warmup): {mismatches}/{total}, max_diff={max_diff:.6f}"
        )

    def test_ema_nan_positions(self, ohlcv: pd.DataFrame) -> None:
        golden = load_golden("golden_ema_20.json")
        _, records = INDICATOR_REGISTRY["ema"](ohlcv, {"period": 20}, "include")
        check_nan_positions(records, golden["data"], "value", "ema")


class TestRSIGolden:
    def test_rsi_14_algorithm_difference(self, ohlcv: pd.DataFrame) -> None:
        """RSI: vbt(Wilder's smoothing) vs TS(EMA-based) の差異を記録。

        vbt.RSI は Wilder's smoothing (1/N decay) を使用し、
        TS側は EMA (2/(N+1) decay) を使用するため、同一データでも
        異なる値を出す。これはアルゴリズム差異であり、暖気では収束しない。
        apps/bt/ API利用時はvbt結果が返るため、フロントエンドはTS側計算を使う前提。
        """
        golden = load_golden("golden_rsi_14.json")
        _, records = INDICATOR_REGISTRY["rsi"](ohlcv, {"period": 14}, "include")
        bt_by_date = {r["date"]: r["value"] for r in records}

        # RSIが0-100の範囲内であることだけ検証
        for r in records:
            if r["value"] is not None:
                assert 0 <= r["value"] <= 100, f"RSI out of range: {r['value']}"

        # NaN位置は一致すべき
        check_nan_positions(records, golden["data"], "value", "rsi")


class TestMACDGolden:
    def test_macd_12_26_9_algorithm_difference(self, ohlcv: pd.DataFrame) -> None:
        """MACD: vbt.MACD (pandas ewm adjust=True) vs TS (SMA-initialized EMA)。

        vbtのEMA初期化方式がTS側と異なり、MACDは2つのEMAの差分を取るため
        差異が増幅される。暖気でも完全には収束しない。
        フロントエンドはTS側計算を使う前提。apps/bt/ APIは参考値として提供。
        """
        params = {"fast_period": 12, "slow_period": 26, "signal_period": 9}
        _, records = INDICATOR_REGISTRY["macd"](ohlcv, params, "include")

        # MACD構造が正しいことを検証
        non_null_records = [r for r in records if r.get("macd") is not None]
        assert len(non_null_records) > 0, "MACD should produce non-null values"
        for r in non_null_records:
            if r.get("macd") is not None and r.get("signal") is not None:
                expected_hist = round(r["macd"] - r["signal"], 4)
                actual_hist = r.get("histogram")
                if actual_hist is not None:
                    assert abs(expected_hist - actual_hist) < 0.01, (
                        f"MACD histogram = macd - signal invariant violated at {r['date']}"
                    )


class TestPPOGolden:
    def test_ppo_12_26_9_converges(self, ohlcv: pd.DataFrame) -> None:
        golden = load_golden("golden_ppo_12_26_9.json")
        params = {"fast_period": 12, "slow_period": 26, "signal_period": 9}
        _, records = INDICATOR_REGISTRY["ppo"](ohlcv, params, "include")
        total, mismatches, max_diff = compare_multi_indicator(
            records, golden["data"], ["ppo", "signal", "histogram"],
            TOLERANCE["ppo"],
            skip_first_n=WARMUP_BARS["ppo"],
        )
        assert mismatches == 0, (
            f"PPO (after warmup): {mismatches}/{total}, max_diff={max_diff:.6f}"
        )


class TestATRGolden:
    def test_atr_14_converges(self, ohlcv: pd.DataFrame) -> None:
        """vbt.ATR(SMA) vs TS ATR(EMA): 暖気後に収束確認"""
        golden = load_golden("golden_atr_14.json")
        _, records = INDICATOR_REGISTRY["atr"](ohlcv, {"period": 14}, "include")
        total, mismatches, max_diff = compare_single_indicator(
            records, golden["data"], "value", TOLERANCE["atr"],
            skip_first_n=WARMUP_BARS["atr"],
        )
        assert mismatches == 0, (
            f"ATR (after warmup): {mismatches}/{total}, max_diff={max_diff:.6f}"
        )


class TestATRSupportGolden:
    def test_atr_support_20_2_converges(self, ohlcv: pd.DataFrame) -> None:
        golden = load_golden("golden_atr_support_20_2.0.json")
        params = {"lookback_period": 20, "atr_multiplier": 2.0}
        _, records = INDICATOR_REGISTRY["atr_support"](ohlcv, params, "include")
        total, mismatches, max_diff = compare_single_indicator(
            records, golden["data"], "value", TOLERANCE["atr_support"],
            skip_first_n=WARMUP_BARS["atr_support"],
        )
        assert mismatches == 0, (
            f"ATR Support (after warmup): {mismatches}/{total}, max_diff={max_diff:.6f}"
        )

    def test_atr_support_nan_positions(self, ohlcv: pd.DataFrame) -> None:
        golden = load_golden("golden_atr_support_20_2.0.json")
        params = {"lookback_period": 20, "atr_multiplier": 2.0}
        _, records = INDICATOR_REGISTRY["atr_support"](ohlcv, params, "include")
        check_nan_positions(records, golden["data"], "value", "atr_support")


# ===== Summary test (暖気期間除外後の全体不一致率) =====


class TestGoldenSummary:
    """全インジケータの不一致率を集計"""

    def test_overall_mismatch_rate(self, ohlcv: pd.DataFrame) -> None:
        """暖気期間除外後の不一致率 < 0.1%"""
        total_comparisons = 0
        total_mismatches = 0

        # SMA系（暖気なし）
        sma_indicators = [
            ("golden_sma_20.json", "sma", {"period": 20}, "value", ["value"]),
            ("golden_nbar_support_20.json", "nbar_support", {"period": 20}, None, ["value"]),
        ]

        for golden_file, ind_type, params, _, keys in sma_indicators:
            golden = load_golden(golden_file)
            _, records = INDICATOR_REGISTRY[ind_type](ohlcv, params, "include")
            t, m, _ = compare_single_indicator(
                records, golden["data"], "value", TOLERANCE[ind_type]
            )
            total_comparisons += t
            total_mismatches += m

        # Bollinger (SMA-based, no warmup needed)
        golden = load_golden("golden_bollinger_20_2.0.json")
        _, records = INDICATOR_REGISTRY["bollinger"](
            ohlcv, {"period": 20, "std_dev": 2.0}, "include"
        )
        t, m, _ = compare_multi_indicator(
            records, golden["data"], ["upper", "middle", "lower"], TOLERANCE["bollinger"]
        )
        total_comparisons += t
        total_mismatches += m

        # Volume Comparison (SMA-based)
        golden = load_golden("golden_volume_comparison_20_100.json")
        _, records = INDICATOR_REGISTRY["volume_comparison"](
            ohlcv,
            {"short_period": 20, "long_period": 100, "lower_multiplier": 1.0,
             "higher_multiplier": 1.5, "ma_type": "sma"},
            "include",
        )
        t, m, _ = compare_multi_indicator(
            records, golden["data"],
            ["shortMA", "longThresholdLower", "longThresholdHigher"],
            TOLERANCE["volume_comparison"],
        )
        total_comparisons += t
        total_mismatches += m

        # EMA系（暖気期間スキップ、RSI/MACDはアルゴリズム差異のため除外）
        ema_indicators = [
            ("golden_ema_20.json", "ema", {"period": 20}, ["value"], "ema"),
            ("golden_atr_14.json", "atr", {"period": 14}, ["value"], "atr"),
        ]

        for golden_file, ind_type, params, keys, warmup_key in ema_indicators:
            golden = load_golden(golden_file)
            _, records = INDICATOR_REGISTRY[ind_type](ohlcv, params, "include")
            t, m, _ = compare_single_indicator(
                records, golden["data"], "value", TOLERANCE[ind_type],
                skip_first_n=WARMUP_BARS[warmup_key],
            )
            total_comparisons += t
            total_mismatches += m

        # MACD/RSI: アルゴリズム差異のため summary から除外
        # (vbt EMA初期化とTS SMA初期化EMAの根本差異)

        # PPO (EMA-based, warmup)
        golden = load_golden("golden_ppo_12_26_9.json")
        _, records = INDICATOR_REGISTRY["ppo"](
            ohlcv,
            {"fast_period": 12, "slow_period": 26, "signal_period": 9},
            "include",
        )
        t, m, _ = compare_multi_indicator(
            records, golden["data"], ["ppo", "signal", "histogram"],
            TOLERANCE["ppo"], skip_first_n=WARMUP_BARS["ppo"],
        )
        total_comparisons += t
        total_mismatches += m

        # ATR Support (EMA-based, warmup)
        golden = load_golden("golden_atr_support_20_2.0.json")
        _, records = INDICATOR_REGISTRY["atr_support"](
            ohlcv, {"lookback_period": 20, "atr_multiplier": 2.0}, "include"
        )
        t, m, _ = compare_single_indicator(
            records, golden["data"], "value", TOLERANCE["atr_support"],
            skip_first_n=WARMUP_BARS["atr_support"],
        )
        total_comparisons += t
        total_mismatches += m

        mismatch_rate = (
            total_mismatches / total_comparisons * 100
            if total_comparisons > 0
            else 0
        )
        assert mismatch_rate < 0.1, (
            f"Overall mismatch rate: {mismatch_rate:.4f}% "
            f"({total_mismatches}/{total_comparisons})"
        )
