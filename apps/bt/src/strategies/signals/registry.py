"""
シグナルレジストリ（データ駆動設計）

全シグナル定義を宣言的に管理し、processor.pyの冗長コードを削減
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from src.models.signals import SignalParams

# シグナル関数のimport
from .beta import beta_range_signal
from .breakout import (
    atr_support_break_signal,
    ma_breakout_signal,
    period_breakout_signal,
    retracement_signal,
)
from .buy_and_hold import generate_buy_and_hold_signals
from .crossover import indicator_crossover_signal
from .fundamental import (
    cfo_yield_threshold,
    is_expected_growth_eps,
    is_growing_eps,
    is_growing_profit,
    is_growing_sales,
    is_high_dividend_yield,
    is_high_operating_margin,
    is_high_roa,
    is_high_roe,
    market_cap_threshold,
    operating_cash_flow_threshold,
    simple_fcf_threshold,
    simple_fcf_yield_threshold,
    is_undervalued_by_pbr,
    is_undervalued_by_per,
    is_undervalued_growth_by_peg,
)
from .index_daily_change import index_daily_change_signal
from .index_macd_histogram import index_macd_histogram_signal
from .margin import margin_balance_percentile_signal
from .mean_reversion import mean_reversion_combined_signal
from .rsi_spread import rsi_spread_signal
from .risk_adjusted import risk_adjusted_return_signal
from .rsi_threshold import rsi_threshold_signal
from .sector_strength import (
    sector_rotation_phase_signal,
    sector_strength_ranking_signal,
    sector_volatility_regime_signal,
)
from .trading_value import trading_value_signal
from .trading_value_range import trading_value_range_signal
from .volatility import bollinger_bands_signal
from .volume import volume_signal

logger = logging.getLogger(__name__)


@dataclass
class SignalDefinition:
    """
    シグナル定義（宣言的設定）

    Attributes:
        name: シグナル名（ロギング用）
        signal_func: シグナル計算関数
        enabled_checker: 有効性チェック関数
        param_builder: パラメータ構築関数
        entry_purpose: エントリー時の目的（ロギング用）
        exit_purpose: エグジット時の目的（ロギング用）
        category: シグナルカテゴリ
        description: シグナル説明文
        param_key: SignalParams内のフィールドパス (例: 'volume', 'fundamental.per')
        data_checker: 必須データチェック関数（オプション）
        exit_disabled: Exitシグナルとして使用不可フラグ（Buy&Hold等）
    """

    name: str
    signal_func: Callable
    enabled_checker: Callable[[SignalParams], bool]
    param_builder: Callable[[SignalParams, dict], dict]
    entry_purpose: str
    exit_purpose: str
    category: str  # 'breakout'|'volume'|'oscillator'|'volatility'|'macro'|'fundamental'|'sector'
    description: str
    param_key: str  # SignalParams内のフィールドパス
    data_checker: Callable[[dict], bool] | None = None
    exit_disabled: bool = False  # デフォルトはExit可能
    data_requirements: list[str] = field(default_factory=list)


# ===== データチェックヘルパー関数 =====


def _has_statements_column(d: dict[str, Any], column: str) -> bool:
    """財務諸表データに指定カラムが存在し、有効な値があるかチェック"""
    return (
        "statements_data" in d
        and d["statements_data"] is not None
        and not d["statements_data"].empty
        and column in d["statements_data"].columns
        and d["statements_data"][column].notna().any()
    )


def _has_benchmark_data(d: dict[str, Any]) -> bool:
    """ベンチマークデータが存在し、有効な値があるかチェック"""
    return (
        "benchmark_data" in d
        and d["benchmark_data"] is not None
        and not d["benchmark_data"].empty
        and "Close" in d["benchmark_data"].columns
        and d["benchmark_data"]["Close"].notna().any()
    )


def _has_statements_columns(d: dict[str, Any], *columns: str) -> bool:
    """財務諸表データに複数の指定カラムが存在し、有効な値があるかチェック"""
    if (
        "statements_data" not in d
        or d["statements_data"] is None
        or d["statements_data"].empty
    ):
        return False
    return all(
        col in d["statements_data"].columns
        and d["statements_data"][col].notna().any()
        for col in columns
    )


def _select_fundamental_column(
    params: SignalParams, adjusted: str, raw: str
) -> str:
    """Select adjusted or raw column based on config."""
    return adjusted if params.fundamental.use_adjusted else raw


def _has_margin_data(d: dict[str, Any]) -> bool:
    """信用残高データが存在し、有効な値があるかチェック"""
    return (
        "margin_data" in d
        and d["margin_data"] is not None
        and not d["margin_data"].empty
        and "margin_balance" in d["margin_data"].columns
        and d["margin_data"]["margin_balance"].notna().any()
    )


def _has_sector_data(d: dict[str, Any]) -> bool:
    """セクターデータが存在し、銘柄セクター名も設定されているかチェック"""
    if not bool(d.get("sector_data")) or not bool(d.get("stock_sector_name")):
        return False
    stock_sector = d["stock_sector_name"]
    if stock_sector not in d["sector_data"]:
        logger.debug(
            f"セクター '{stock_sector}' がsector_dataに未含 "
            f"(利用可能: {list(d['sector_data'].keys())[:5]}...)"
        )
        return False
    return True


def _has_stock_sector_close(d: dict[str, Any]) -> bool:
    """当該銘柄のセクターインデックス終値データが取得可能かチェック"""
    if not _has_sector_data(d):
        return False
    sector_name = d["stock_sector_name"]
    sector_df = d["sector_data"].get(sector_name)
    return (
        sector_df is not None
        and "Close" in sector_df.columns
        and sector_df["Close"].notna().any()
    )


def _has_sector_data_and_benchmark(d: dict[str, Any]) -> bool:
    """セクターデータとベンチマークデータの両方が存在するかチェック"""
    return _has_sector_data(d) and _has_benchmark_data(d)


def _has_stock_sector_close_and_benchmark(d: dict[str, Any]) -> bool:
    """セクター終値とベンチマークデータの両方が利用可能かチェック"""
    return _has_stock_sector_close(d) and _has_benchmark_data(d)


# ===== シグナルレジストリ（全シグナル定義） =====

SIGNAL_REGISTRY: list[SignalDefinition] = [
    # 1. 出来高シグナル（direction統一版）
    SignalDefinition(
        name="出来高",
        signal_func=volume_signal,
        enabled_checker=lambda p: p.volume.enabled,
        param_builder=lambda p, d: {
            "volume": d["volume"],
            "direction": p.volume.direction,
            "threshold": p.volume.threshold,
            "short_period": p.volume.short_period,
            "long_period": p.volume.long_period,
            "ma_type": p.volume.ma_type,
        },
        entry_purpose="急増絞り込み/減少除外",
        exit_purpose="急増利確/減少損切り",
        category="volume",
        description="出来高の急増/急減を検出",
        param_key="volume",
        data_checker=lambda d: "volume" in d,
        data_requirements=["volume"],
    ),
    # 2. 売買代金シグナル（X日平均売買代金が閾値以上/以下を判定）
    SignalDefinition(
        name="売買代金",
        signal_func=trading_value_signal,
        enabled_checker=lambda p: p.trading_value.enabled,
        param_builder=lambda p, d: {
            "close": d["execution_close"],  # 相対価格モード対応: 実価格を使用
            "volume": d["volume"],
            "direction": p.trading_value.direction,
            "period": p.trading_value.period,
            "threshold_value": p.trading_value.threshold_value,
        },
        entry_purpose="流動性絞り込み（X日平均売買代金が閾値以上）",
        exit_purpose="流動性枯渇警告（X日平均売買代金が閾値以下）",
        category="volume",
        description="売買代金の閾値判定",
        param_key="trading_value",
        data_checker=lambda d: "execution_close" in d and "volume" in d,
        data_requirements=["ohlc", "volume"],
    ),
    # 2-2. 売買代金範囲シグナル
    SignalDefinition(
        name="売買代金範囲",
        signal_func=trading_value_range_signal,
        enabled_checker=lambda p: p.trading_value_range.enabled,
        param_builder=lambda p, d: {
            "close": d["execution_close"],  # 相対価格モード対応: 実価格を使用
            "volume": d["volume"],
            "period": p.trading_value_range.period,
            "min_threshold": p.trading_value_range.min_threshold,
            "max_threshold": p.trading_value_range.max_threshold,
        },
        entry_purpose="流動性範囲フィルター（X日平均売買代金が範囲内）",
        exit_purpose="流動性異常警告（X日平均売買代金が範囲外）",
        category="volume",
        description="売買代金の範囲判定",
        param_key="trading_value_range",
        data_checker=lambda d: "execution_close" in d and "volume" in d,
        data_requirements=["ohlc", "volume"],
    ),
    # 3. PERシグナル
    SignalDefinition(
        name="PER",
        signal_func=is_undervalued_by_per,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.per.enabled,
        param_builder=lambda p, d: {
            "close": d["execution_close"],  # 相対価格モード対応: 実価格を使用
            "eps": d["statements_data"][
                _select_fundamental_column(p, "AdjustedEPS", "EPS")
            ],
            "threshold": p.fundamental.per.threshold,
            "condition": p.fundamental.per.condition,
            "exclude_negative": p.fundamental.per.exclude_negative,
        },
        entry_purpose="PER（株価収益率）が閾値以下の割安株を選定",
        exit_purpose="PERが閾値を超えた割高株を除外",
        category="fundamental",
        description="PER（株価収益率）の閾値判定",
        param_key="fundamental.per",
        data_checker=lambda d: _has_statements_column(d, "EPS"),
        data_requirements=["statements:EPS"],
    ),
    # 5. ROEシグナル
    SignalDefinition(
        name="ROE",
        signal_func=is_high_roe,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.roe.enabled,
        param_builder=lambda p, d: {
            "roe": d["statements_data"]["ROE"],
            "threshold": p.fundamental.roe.threshold,
            "condition": p.fundamental.roe.condition,
        },
        entry_purpose="ROE（自己資本利益率）が閾値以上の高収益企業を選定",
        exit_purpose="ROEが閾値を下回った収益性低下企業を除外",
        category="fundamental",
        description="ROE（自己資本利益率）の閾値判定",
        param_key="fundamental.roe",
        data_checker=lambda d: _has_statements_column(d, "ROE"),
        data_requirements=["statements:ROE"],
    ),
    # 5.5. ROAシグナル
    SignalDefinition(
        name="ROA",
        signal_func=is_high_roa,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.roa.enabled,
        param_builder=lambda p, d: {
            "roa": d["statements_data"]["ROA"],
            "threshold": p.fundamental.roa.threshold,
            "condition": p.fundamental.roa.condition,
        },
        entry_purpose="ROA（総資産利益率）が閾値以上の高効率企業を選定",
        exit_purpose="ROAが閾値を下回った資産効率低下企業を除外",
        category="fundamental",
        description="ROA（総資産利益率）の閾値判定",
        param_key="fundamental.roa",
        data_checker=lambda d: _has_statements_column(d, "ROA"),
        data_requirements=["statements:ROA"],
    ),
    # 6. β値シグナル
    SignalDefinition(
        name="β値",
        signal_func=beta_range_signal,
        enabled_checker=lambda p: p.beta.enabled,
        param_builder=lambda p, d: {
            "stock_price": d["execution_close"],  # 相対価格モード対応: 実価格を使用
            "market_price": d["benchmark_data"]["Close"],
            "beta_min": p.beta.min_beta,
            "beta_max": p.beta.max_beta,
            "lookback_period": p.beta.lookback_period,
        },
        entry_purpose="市場感応度判定",
        exit_purpose="相関変化警告",
        category="volatility",
        description="ベンチマーク対比のベータ値判定",
        param_key="beta",
        data_checker=_has_benchmark_data,
        data_requirements=["benchmark"],
    ),
    # 7. 信用残高シグナル
    SignalDefinition(
        name="信用残高",
        signal_func=margin_balance_percentile_signal,
        enabled_checker=lambda p: p.margin.enabled,
        param_builder=lambda p, d: {
            "margin_balance": d["margin_data"]["margin_balance"],
            "params": p.margin,
        },
        entry_purpose="需給判定",
        exit_purpose="需給変化警告",
        category="macro",
        description="信用残高の買残/売残判定",
        param_key="margin",
        data_checker=_has_margin_data,
        data_requirements=["margin"],
    ),
    # 8. ATRサポートブレイクシグナル（direction統一設計）
    SignalDefinition(
        name="ATRサポートブレイク",
        signal_func=atr_support_break_signal,
        enabled_checker=lambda p: p.atr_support_break.enabled,
        param_builder=lambda p, d: {
            "high": d["ohlc_data"]["High"],
            "low": d["ohlc_data"]["Low"],
            "close": d["close"],
            "lookback_period": p.atr_support_break.lookback_period,
            "atr_multiplier": p.atr_support_break.atr_multiplier,
            "direction": p.atr_support_break.direction,
            "price_column": p.atr_support_break.price_column,
        },
        entry_purpose="損切り/ショートエントリー",
        exit_purpose="反発/ショートエグジット",
        category="breakout",
        description="ATRベースのサポートライン突破を検出",
        param_key="atr_support_break",
        data_checker=lambda d: (
            "ohlc_data" in d
            and all(col in d["ohlc_data"].columns for col in ["High", "Low"])
            and "close" in d
        ),
        data_requirements=["ohlc"],
    ),
    # 9. リトレースメントシグナル（フィボナッチ下落率ベース）
    SignalDefinition(
        name="リトレースメント",
        signal_func=retracement_signal,
        enabled_checker=lambda p: hasattr(p, "retracement") and p.retracement.enabled,
        param_builder=lambda p, d: {
            "high": d["ohlc_data"]["High"],
            "close": d["close"],
            "low": d["ohlc_data"]["Low"] if "Low" in d["ohlc_data"].columns else None,
            "lookback_period": p.retracement.lookback_period,
            "retracement_level": p.retracement.retracement_level,
            "direction": p.retracement.direction,
            "price_column": p.retracement.price_column,
        },
        entry_purpose="押し目買い（フィボナッチレベル下抜け）",
        exit_purpose="戻り売り（フィボナッチレベル上抜け）",
        category="breakout",
        description="高値からの押し目を検出",
        param_key="retracement",
        data_checker=lambda d: (
            "ohlc_data" in d and "High" in d["ohlc_data"].columns and "close" in d
        ),
        data_requirements=["ohlc"],
    ),
    # 10. 期間ブレイクアウトシグナル（direction統一設計）
    SignalDefinition(
        name="期間ブレイクアウト",
        signal_func=period_breakout_signal,
        enabled_checker=lambda p: hasattr(p, "period_breakout")
        and p.period_breakout.enabled,
        param_builder=lambda p, d: {
            "price": d["ohlc_data"]["High"]
            if p.period_breakout.direction == "high"
            else d["ohlc_data"]["Low"],
            "period": p.period_breakout.period,
            "direction": p.period_breakout.direction,
            "condition": p.period_breakout.condition,
            "lookback_days": p.period_breakout.lookback_days,
        },
        entry_purpose="高値/安値ブレイク",
        exit_purpose="高値/安値維持",
        category="breakout",
        description="指定期間の高値/安値をブレイクしたかを判定",
        param_key="period_breakout",
        data_checker=lambda d: "ohlc_data" in d,
        data_requirements=["ohlc"],
    ),
    # 11. クロスオーバーシグナル（SMA/RSI/MACD/EMA統一設計）
    SignalDefinition(
        name="クロスオーバー",
        signal_func=indicator_crossover_signal,
        enabled_checker=lambda p: hasattr(p, "crossover") and p.crossover.enabled,
        param_builder=lambda p, d: {
            "close": d["close"],
            "indicator_type": p.crossover.type,
            "fast_period": p.crossover.fast_period,
            "slow_period": p.crossover.slow_period,
            "direction": p.crossover.direction,
            "signal_period": p.crossover.signal_period,
            "lookback_days": p.crossover.lookback_days,
        },
        entry_purpose="ゴールデンクロス/デッドクロス",
        exit_purpose="ゴールデンクロス/デッドクロス",
        category="breakout",
        description="2つの指標のクロスオーバーを検出",
        param_key="crossover",
        data_checker=lambda d: "close" in d,
        data_requirements=["ohlc"],
    ),
    # 12. ボリンジャーバンドシグナル（エントリー・エグジット両用）
    SignalDefinition(
        name="ボリンジャーバンド",
        signal_func=bollinger_bands_signal,
        enabled_checker=lambda p: hasattr(p, "bollinger_bands")
        and p.bollinger_bands.enabled,
        param_builder=lambda p, d: {
            "ohlc_data": d["ohlc_data"],
            "window": p.bollinger_bands.window,
            "alpha": p.bollinger_bands.alpha,
            "position": p.bollinger_bands.position,
        },
        entry_purpose="過熱回避/売られすぎ回避/トレンド確認",
        exit_purpose="過熱利確/売られすぎ損切り",
        category="volatility",
        description="ボリンジャーバンドの位置判定",
        param_key="bollinger_bands",
        data_checker=lambda d: "ohlc_data" in d and "Close" in d["ohlc_data"].columns,
        data_requirements=["ohlc"],
    ),
    # 13. Buy&Holdシグナル（全日程エントリー可能）
    SignalDefinition(
        name="Buy&Hold",
        signal_func=generate_buy_and_hold_signals,
        enabled_checker=lambda p: hasattr(p, "buy_and_hold") and p.buy_and_hold.enabled,
        param_builder=lambda p, d: {
            "close": d["close"],
        },
        entry_purpose="全日程エントリー可能",
        exit_purpose="（エグジット非対応）",
        category="breakout",
        description="全日程エントリー可能（ベンチマーク用）",
        param_key="buy_and_hold",
        data_checker=lambda d: "close" in d,
        exit_disabled=True,  # Exit用途では使用不可（全日程Trueのため）
        data_requirements=["ohlc"],
    ),
    # 14. RSI閾値シグナル（買われすぎ・売られすぎ判定）
    SignalDefinition(
        name="RSI閾値",
        signal_func=rsi_threshold_signal,
        enabled_checker=lambda p: hasattr(p, "rsi_threshold")
        and p.rsi_threshold.enabled,
        param_builder=lambda p, d: {
            "close": d["close"],
            "period": p.rsi_threshold.period,
            "threshold": p.rsi_threshold.threshold,
            "condition": p.rsi_threshold.condition,
        },
        entry_purpose="売られすぎ判定/買われすぎ回避",
        exit_purpose="買われすぎ利確/売られすぎ損切り",
        category="oscillator",
        description="RSIの閾値判定",
        param_key="rsi_threshold",
        data_checker=lambda d: "close" in d,
        data_requirements=["ohlc"],
    ),
    # 15. RSIスプレッドシグナル（短期RSIと長期RSIの差分判定）
    SignalDefinition(
        name="RSIスプレッド",
        signal_func=rsi_spread_signal,
        enabled_checker=lambda p: hasattr(p, "rsi_spread") and p.rsi_spread.enabled,
        param_builder=lambda p, d: {
            "close": d["close"],
            "fast_period": p.rsi_spread.fast_period,
            "slow_period": p.rsi_spread.slow_period,
            "threshold": p.rsi_spread.threshold,
            "condition": p.rsi_spread.condition,
        },
        entry_purpose="上昇モメンタム強/下降モメンタム強判定",
        exit_purpose="モメンタム減衰/モメンタム反転検出",
        category="oscillator",
        description="短期RSIと長期RSIのスプレッド判定",
        param_key="rsi_spread",
        data_checker=lambda d: "close" in d,
        data_requirements=["ohlc"],
    ),
    # 16. 平均回帰シグナル（SMA/EMA基準線・乖離・回復統合）
    SignalDefinition(
        name="平均回帰",
        signal_func=mean_reversion_combined_signal,
        enabled_checker=lambda p: hasattr(p, "mean_reversion")
        and p.mean_reversion.enabled,
        param_builder=lambda p, d: {
            "ohlc_data": d["ohlc_data"],
            "baseline_type": p.mean_reversion.baseline_type,
            "baseline_period": p.mean_reversion.baseline_period,
            "deviation_threshold": p.mean_reversion.deviation_threshold,
            "deviation_direction": p.mean_reversion.deviation_direction,
            "recovery_price": p.mean_reversion.recovery_price,
            "recovery_direction": p.mean_reversion.recovery_direction,
        },
        entry_purpose="乖離エントリー（割安買い）",
        exit_purpose="回復エグジット（平均回帰利確）",
        category="breakout",
        description="移動平均からの乖離と回復を検出",
        param_key="mean_reversion",
        data_checker=lambda d: "ohlc_data" in d
        and all(col in d["ohlc_data"].columns for col in ["High", "Low", "Close"]),
        data_requirements=["ohlc"],
    ),
    # 17. MA線ブレイクアウトシグナル（クロス検出版）
    SignalDefinition(
        name="MA線ブレイクアウト",
        signal_func=ma_breakout_signal,
        enabled_checker=lambda p: hasattr(p, "ma_breakout") and p.ma_breakout.enabled,
        param_builder=lambda p, d: {
            "price": d["close"],
            "period": p.ma_breakout.period,
            "ma_type": p.ma_breakout.ma_type,
            "direction": p.ma_breakout.direction,
            "lookback_days": p.ma_breakout.lookback_days,
        },
        entry_purpose="MA線クロス検出（エントリー）",
        exit_purpose="MA線クロス検出（エグジット）",
        category="breakout",
        description="株価が移動平均線を上抜け/下抜けしたかを判定",
        param_key="ma_breakout",
        data_checker=lambda d: "close" in d,
        data_requirements=["ohlc"],
    ),
    # 18. 指数前日比シグナル（市場環境フィルター）
    SignalDefinition(
        name="指数前日比",
        signal_func=index_daily_change_signal,
        enabled_checker=lambda p: hasattr(p, "index_daily_change")
        and p.index_daily_change.enabled,
        param_builder=lambda p, d: {
            "index_data": d["benchmark_data"],
            "max_daily_change_pct": p.index_daily_change.max_daily_change_pct,
            "direction": p.index_daily_change.direction,
        },
        entry_purpose="市場過熱回避（前日比低位日にエントリー）",
        exit_purpose="市場急騰利確（前日比高位日にエグジット）",
        category="macro",
        description="ベンチマーク指数の前日比変化率判定",
        param_key="index_daily_change",
        data_checker=_has_benchmark_data,
        data_requirements=["benchmark"],
    ),
    # 19. INDEXヒストグラムシグナル（市場モメンタム強弱判定）
    SignalDefinition(
        name="INDEXヒストグラム",
        signal_func=index_macd_histogram_signal,
        enabled_checker=lambda p: hasattr(p, "index_macd_histogram")
        and p.index_macd_histogram.enabled,
        param_builder=lambda p, d: {
            "index_data": d["benchmark_data"],
            "fast_period": p.index_macd_histogram.fast_period,
            "slow_period": p.index_macd_histogram.slow_period,
            "signal_period": p.index_macd_histogram.signal_period,
            "direction": p.index_macd_histogram.direction,
        },
        entry_purpose="市場モメンタム強判定（histogram正）",
        exit_purpose="市場モメンタム弱判定（histogram負）",
        category="macro",
        description="指数MACDヒストグラムの正負判定",
        param_key="index_macd_histogram",
        data_checker=_has_benchmark_data,
        data_requirements=["benchmark"],
    ),
    # 20. リスク調整リターンシグナル（シャープ/ソルティノレシオベース）
    SignalDefinition(
        name="リスク調整リターン",
        signal_func=risk_adjusted_return_signal,
        enabled_checker=lambda p: hasattr(p, "risk_adjusted_return")
        and p.risk_adjusted_return.enabled,
        param_builder=lambda p, d: {
            "close": d["close"],
            "lookback_period": p.risk_adjusted_return.lookback_period,
            "threshold": p.risk_adjusted_return.threshold,
            "ratio_type": p.risk_adjusted_return.ratio_type,
            "condition": p.risk_adjusted_return.condition,
        },
        entry_purpose="高リスク調整リターン銘柄選別",
        exit_purpose="低リスク調整リターン警告",
        category="fundamental",
        description="シャープレシオ/ソルティノレシオの判定",
        param_key="risk_adjusted_return",
        data_checker=lambda d: "close" in d,
        data_requirements=["ohlc"],
    ),
    # =====================================================================
    # 新規財務シグナル（2026-01追加）
    # =====================================================================
    # 21. PBRシグナル（株価純資産倍率）
    SignalDefinition(
        name="PBR",
        signal_func=is_undervalued_by_pbr,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.pbr.enabled,
        param_builder=lambda p, d: {
            "close": d["execution_close"],
            "bps": d["statements_data"][
                _select_fundamental_column(p, "AdjustedBPS", "BPS")
            ],
            "threshold": p.fundamental.pbr.threshold,
            "condition": p.fundamental.pbr.condition,
            "exclude_negative": p.fundamental.pbr.exclude_negative,
        },
        entry_purpose="PBR（株価純資産倍率）が閾値以下の割安株を選定",
        exit_purpose="PBRが閾値を超えた割高株を除外",
        category="fundamental",
        description="PBR（株価純資産倍率）の閾値判定",
        param_key="fundamental.pbr",
        data_checker=lambda d: _has_statements_column(d, "BPS"),
        data_requirements=["statements:BPS"],
    ),
    # 22. PEG Ratioシグナル（PER / EPS成長率）
    SignalDefinition(
        name="PEG Ratio",
        signal_func=is_undervalued_growth_by_peg,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.peg_ratio.enabled,
        param_builder=lambda p, d: {
            "close": d["execution_close"],
            "eps": d["statements_data"][
                _select_fundamental_column(p, "AdjustedEPS", "EPS")
            ],
            "next_year_forecast_eps": d["statements_data"][
                _select_fundamental_column(
                    p, "AdjustedNextYearForecastEPS", "NextYearForecastEPS"
                )
            ],
            "threshold": p.fundamental.peg_ratio.threshold,
            "condition": p.fundamental.peg_ratio.condition,
        },
        entry_purpose="PEG Ratio（PER÷EPS成長率）が閾値以下の割安成長株を選定",
        exit_purpose="PEGが閾値を超えた割高成長株を除外",
        category="fundamental",
        description="PEG Ratio（PER÷EPS成長率）の閾値判定",
        param_key="fundamental.peg_ratio",
        data_checker=lambda d: _has_statements_columns(d, "EPS", "NextYearForecastEPS"),
        data_requirements=["statements:EPS", "statements:NextYearForecastEPS"],
    ),
    # 23. Forward EPS成長率シグナル（来期予想EPSベース）
    SignalDefinition(
        name="Forward EPS成長率",
        signal_func=is_expected_growth_eps,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.forward_eps_growth.enabled,
        param_builder=lambda p, d: {
            "eps": d["statements_data"][
                _select_fundamental_column(p, "AdjustedEPS", "EPS")
            ],
            "next_year_forecast_eps": d["statements_data"][
                _select_fundamental_column(
                    p, "AdjustedNextYearForecastEPS", "NextYearForecastEPS"
                )
            ],
            "growth_threshold": p.fundamental.forward_eps_growth.threshold,
            "condition": p.fundamental.forward_eps_growth.condition,
        },
        entry_purpose="来期EPS成長率が閾値以上の高成長予想株を選定",
        exit_purpose="EPS成長率予想が閾値を下回った株を除外",
        category="fundamental",
        description="来期EPS成長率の閾値判定",
        param_key="fundamental.forward_eps_growth",
        data_checker=lambda d: _has_statements_columns(d, "EPS", "NextYearForecastEPS"),
        data_requirements=["statements:EPS", "statements:NextYearForecastEPS"],
    ),
    # 23-2. EPS成長率シグナル（実績EPSベース）
    SignalDefinition(
        name="EPS成長率",
        signal_func=is_growing_eps,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.eps_growth.enabled,
        param_builder=lambda p, d: {
            "eps": d["statements_data"][
                _select_fundamental_column(p, "AdjustedEPS", "EPS")
            ],
            "growth_threshold": p.fundamental.eps_growth.threshold,
            "periods": p.fundamental.eps_growth.periods,
            "condition": p.fundamental.eps_growth.condition,
        },
        entry_purpose="EPS成長率が閾値以上の企業を選定",
        exit_purpose="EPS成長率が閾値を下回った企業を除外",
        category="fundamental",
        description="EPS成長率の閾値判定（実績ベース）",
        param_key="fundamental.eps_growth",
        data_checker=lambda d: _has_statements_column(d, "EPS"),
        data_requirements=["statements:EPS"],
    ),
    # 24. Profit成長率シグナル
    SignalDefinition(
        name="Profit成長率",
        signal_func=is_growing_profit,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.profit_growth.enabled,
        param_builder=lambda p, d: {
            "profit": d["statements_data"]["Profit"],
            "growth_threshold": p.fundamental.profit_growth.threshold,
            "periods": p.fundamental.profit_growth.periods,
            "condition": p.fundamental.profit_growth.condition,
        },
        entry_purpose="利益成長率が閾値以上の企業を選定",
        exit_purpose="利益成長率が閾値を下回った企業を除外",
        category="fundamental",
        description="利益成長率の閾値判定",
        param_key="fundamental.profit_growth",
        data_checker=lambda d: _has_statements_column(d, "Profit"),
        data_requirements=["statements:Profit"],
    ),
    # 25. Sales成長率シグナル
    SignalDefinition(
        name="Sales成長率",
        signal_func=is_growing_sales,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.sales_growth.enabled,
        param_builder=lambda p, d: {
            "sales": d["statements_data"]["Sales"],
            "growth_threshold": p.fundamental.sales_growth.threshold,
            "periods": p.fundamental.sales_growth.periods,
            "condition": p.fundamental.sales_growth.condition,
        },
        entry_purpose="売上成長率が閾値以上の企業を選定",
        exit_purpose="売上成長率が閾値を下回った企業を除外",
        category="fundamental",
        description="売上成長率の閾値判定",
        param_key="fundamental.sales_growth",
        data_checker=lambda d: _has_statements_column(d, "Sales"),
        data_requirements=["statements:Sales"],
    ),
    # 26. 営業利益率シグナル
    SignalDefinition(
        name="営業利益率",
        signal_func=is_high_operating_margin,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.operating_margin.enabled,
        param_builder=lambda p, d: {
            "operating_margin": d["statements_data"]["OperatingMargin"],
            "threshold": p.fundamental.operating_margin.threshold,
            "condition": p.fundamental.operating_margin.condition,
        },
        entry_purpose="営業利益率が閾値以上の高収益企業を選定",
        exit_purpose="営業利益率が閾値を下回った企業を除外",
        category="fundamental",
        description="営業利益率の閾値判定",
        param_key="fundamental.operating_margin",
        data_checker=lambda d: _has_statements_column(d, "OperatingMargin"),
        data_requirements=["statements:OperatingMargin"],
    ),
    # 27. 営業キャッシュフローシグナル
    SignalDefinition(
        name="営業CF",
        signal_func=operating_cash_flow_threshold,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.operating_cash_flow.enabled,
        param_builder=lambda p, d: {
            "operating_cash_flow": d["statements_data"]["OperatingCashFlow"],
            "threshold": p.fundamental.operating_cash_flow.threshold,
            "condition": p.fundamental.operating_cash_flow.condition,
            "consecutive_periods": p.fundamental.operating_cash_flow.consecutive_periods,
        },
        entry_purpose="営業CFが閾値以上（通常は正）の企業を選定",
        exit_purpose="営業CFが閾値を下回った企業を除外",
        category="fundamental",
        description="営業キャッシュフローの閾値判定",
        param_key="fundamental.operating_cash_flow",
        data_checker=lambda d: _has_statements_column(d, "OperatingCashFlow"),
        data_requirements=["statements:OperatingCashFlow"],
    ),
    # 28. 配当利回りシグナル
    SignalDefinition(
        name="配当利回り",
        signal_func=is_high_dividend_yield,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.dividend_yield.enabled,
        param_builder=lambda p, d: {
            "dividend_fy": d["statements_data"]["DividendFY"],
            "close": d["execution_close"],
            "threshold": p.fundamental.dividend_yield.threshold,
            "condition": p.fundamental.dividend_yield.condition,
        },
        entry_purpose="配当利回りが閾値以上の高配当株を選定",
        exit_purpose="配当利回りが閾値を下回った株を除外",
        category="fundamental",
        description="配当利回りの閾値判定",
        param_key="fundamental.dividend_yield",
        data_checker=lambda d: _has_statements_column(d, "DividendFY"),
        data_requirements=["statements:DividendFY"],
    ),
    # 29. 簡易FCFシグナル（CFO + CFI）
    SignalDefinition(
        name="簡易FCF",
        signal_func=simple_fcf_threshold,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.simple_fcf.enabled,
        param_builder=lambda p, d: {
            "operating_cash_flow": d["statements_data"]["OperatingCashFlow"],
            "investing_cash_flow": d["statements_data"]["InvestingCashFlow"],
            "threshold": p.fundamental.simple_fcf.threshold,
            "condition": p.fundamental.simple_fcf.condition,
            "consecutive_periods": p.fundamental.simple_fcf.consecutive_periods,
        },
        entry_purpose="簡易FCF（CFO+CFI）が閾値以上の企業を選定",
        exit_purpose="簡易FCFが閾値を下回った企業を除外",
        category="fundamental",
        description="簡易FCF（CFO+CFI）の閾値判定",
        param_key="fundamental.simple_fcf",
        data_checker=lambda d: _has_statements_columns(d, "OperatingCashFlow", "InvestingCashFlow"),
        data_requirements=["statements:OperatingCashFlow", "statements:InvestingCashFlow"],
    ),
    # 30. CFO利回りシグナル（営業CF / 時価総額）
    SignalDefinition(
        name="CFO利回り",
        signal_func=cfo_yield_threshold,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.cfo_yield.enabled,
        param_builder=lambda p, d: {
            "close": d["execution_close"],
            "operating_cash_flow": d["statements_data"]["OperatingCashFlow"],
            "shares_outstanding": d["statements_data"]["SharesOutstanding"],
            "treasury_shares": d["statements_data"]["TreasuryShares"],
            "threshold": p.fundamental.cfo_yield.threshold,
            "condition": p.fundamental.cfo_yield.condition,
            "use_floating_shares": p.fundamental.cfo_yield.use_floating_shares,
        },
        entry_purpose="CFO利回りが閾値以上の高CF利回り企業を選定",
        exit_purpose="CFO利回りが閾値を下回った企業を除外",
        category="fundamental",
        description="CFO利回り（営業CF÷時価総額）の閾値判定",
        param_key="fundamental.cfo_yield",
        data_checker=lambda d: _has_statements_columns(
            d, "OperatingCashFlow", "SharesOutstanding"
        ),
        data_requirements=["statements:OperatingCashFlow", "statements:SharesOutstanding"],
    ),
    # 31. 簡易FCF利回りシグナル（(CFO+CFI) / 時価総額）
    SignalDefinition(
        name="簡易FCF利回り",
        signal_func=simple_fcf_yield_threshold,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.simple_fcf_yield.enabled,
        param_builder=lambda p, d: {
            "close": d["execution_close"],
            "operating_cash_flow": d["statements_data"]["OperatingCashFlow"],
            "investing_cash_flow": d["statements_data"]["InvestingCashFlow"],
            "shares_outstanding": d["statements_data"]["SharesOutstanding"],
            "treasury_shares": d["statements_data"]["TreasuryShares"],
            "threshold": p.fundamental.simple_fcf_yield.threshold,
            "condition": p.fundamental.simple_fcf_yield.condition,
            "use_floating_shares": p.fundamental.simple_fcf_yield.use_floating_shares,
        },
        entry_purpose="簡易FCF利回りが閾値以上の高FCF利回り企業を選定",
        exit_purpose="簡易FCF利回りが閾値を下回った企業を除外",
        category="fundamental",
        description="簡易FCF利回り（(CFO+CFI)÷時価総額）の閾値判定",
        param_key="fundamental.simple_fcf_yield",
        data_checker=lambda d: _has_statements_columns(
            d, "OperatingCashFlow", "InvestingCashFlow", "SharesOutstanding"
        ),
        data_requirements=["statements:OperatingCashFlow", "statements:InvestingCashFlow", "statements:SharesOutstanding"],
    ),
    # 31-2. 時価総額シグナル（市場規模フィルター）
    SignalDefinition(
        name="時価総額",
        signal_func=market_cap_threshold,
        enabled_checker=lambda p: p.fundamental.enabled and p.fundamental.market_cap.enabled,
        param_builder=lambda p, d: {
            "close": d["execution_close"],
            "shares_outstanding": d["statements_data"]["SharesOutstanding"],
            "treasury_shares": d["statements_data"]["TreasuryShares"],
            "threshold": p.fundamental.market_cap.threshold,
            "condition": p.fundamental.market_cap.condition,
            "use_floating_shares": p.fundamental.market_cap.use_floating_shares,
        },
        entry_purpose="時価総額が閾値以上の銘柄を選定（流動性・規模フィルター）",
        exit_purpose="時価総額が閾値を下回った銘柄を除外",
        category="fundamental",
        description="時価総額（億円単位）の閾値判定",
        param_key="fundamental.market_cap",
        data_checker=lambda d: _has_statements_column(d, "SharesOutstanding"),
        data_requirements=["statements:SharesOutstanding"],
    ),
    # =====================================================================
    # セクターシグナル（2026-01追加）
    # =====================================================================
    # 32. セクター強度ランキングシグナル
    SignalDefinition(
        name="セクター強度ランキング",
        signal_func=sector_strength_ranking_signal,
        enabled_checker=lambda p: hasattr(p, "sector_strength_ranking")
        and p.sector_strength_ranking.enabled,
        param_builder=lambda p, d: {
            "sector_data": d["sector_data"],
            "stock_sector_name": d["stock_sector_name"],
            "benchmark_close": d["benchmark_data"]["Close"],
            "momentum_period": p.sector_strength_ranking.momentum_period,
            "sharpe_period": p.sector_strength_ranking.sharpe_period,
            "top_n": p.sector_strength_ranking.top_n,
            "momentum_weight": p.sector_strength_ranking.momentum_weight,
            "sharpe_weight": p.sector_strength_ranking.sharpe_weight,
            "relative_weight": p.sector_strength_ranking.relative_weight,
            "selection_mode": p.sector_strength_ranking.selection_mode,
        },
        entry_purpose="上位/下位セクター所属銘柄のフィルタリング",
        exit_purpose="選択範囲外セクター転落銘柄のエグジット",
        category="sector",
        description="セクター強度ランキングによるフィルタリング（上位/下位N選択）",
        param_key="sector_strength_ranking",
        data_checker=_has_sector_data_and_benchmark,
        data_requirements=["sector", "benchmark"],
    ),
    # 33. セクターローテーション位相シグナル
    SignalDefinition(
        name="セクターローテーション位相",
        signal_func=sector_rotation_phase_signal,
        enabled_checker=lambda p: hasattr(p, "sector_rotation_phase")
        and p.sector_rotation_phase.enabled,
        param_builder=lambda p, d: {
            "sector_close": d["sector_data"][d["stock_sector_name"]]["Close"],
            "benchmark_close": d["benchmark_data"]["Close"],
            "rs_period": p.sector_rotation_phase.rs_period,
            "direction": p.sector_rotation_phase.direction,
        },
        entry_purpose="先行局面セクター銘柄のエントリー許可",
        exit_purpose="衰退局面セクター銘柄のエグジット",
        category="sector",
        description="RRG的4象限分類によるセクターローテーション位相判定",
        param_key="sector_rotation_phase",
        data_checker=_has_stock_sector_close_and_benchmark,
        data_requirements=["sector", "benchmark"],
    ),
    # 34. セクターボラティリティレジームシグナル
    SignalDefinition(
        name="セクターボラティリティレジーム",
        signal_func=sector_volatility_regime_signal,
        enabled_checker=lambda p: hasattr(p, "sector_volatility_regime")
        and p.sector_volatility_regime.enabled,
        param_builder=lambda p, d: {
            "sector_close": d["sector_data"][d["stock_sector_name"]]["Close"],
            "vol_period": p.sector_volatility_regime.vol_period,
            "vol_ma_period": p.sector_volatility_regime.vol_ma_period,
            "direction": p.sector_volatility_regime.direction,
            "spike_multiplier": p.sector_volatility_regime.spike_multiplier,
        },
        entry_purpose="低ボラティリティ環境でのエントリー許可",
        exit_purpose="高ボラティリティ環境でのエグジット",
        category="sector",
        description="セクターボラティリティレジーム判定（低ボラ/高ボラ環境）",
        param_key="sector_volatility_regime",
        data_checker=_has_stock_sector_close,
        data_requirements=["sector"],
    ),
]


def _validate_registry() -> None:
    """Validate that all param_key values in SIGNAL_REGISTRY are unique."""
    seen: set[str] = set()
    for sig in SIGNAL_REGISTRY:
        if sig.param_key in seen:
            raise ValueError(f"Duplicate param_key in SIGNAL_REGISTRY: {sig.param_key}")
        seen.add(sig.param_key)


_validate_registry()
