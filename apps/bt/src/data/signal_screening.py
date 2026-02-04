"""
シグナルスクリーニングモジュール

銘柄シグナル計算・スクリーニング処理ロジックを提供します。

高速化実装（2026-01版）:
- ループ反転: 銘柄→戦略の順序でシグナルキャッシング
- β値二重計算排除: beta_range_signal_with_value()で1回計算
- 早期終了: AND条件で最初のFalse後スキップ
- 並列処理: ThreadPoolExecutorで銘柄レベル並列化
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any

import pandas as pd
import vectorbt as vbt
from loguru import logger
from pydantic import BaseModel, Field, ValidationError

# シグナル関数のimport
from src.strategies.signals.volume import volume_signal
from src.strategies.signals.breakout import period_breakout_signal
from src.strategies.signals.volatility import bollinger_bands_signal
from src.strategies.signals.crossover import indicator_crossover_signal
from src.strategies.signals.rsi_threshold import rsi_threshold_signal
from src.strategies.signals.beta import beta_range_signal_with_value
from src.strategies.signals.trading_value import trading_value_signal
from src.strategies.signals.trading_value_range import trading_value_range_signal

# データローダーのimport
from src.data.loaders.index_loaders import load_topix_data_from_market_db


# ===== Pydanticモデル: パラメータ検証 =====


class VolumeSignalParamsValidated(BaseModel):
    """出来高シグナルパラメータ検証モデル"""

    direction: str = Field(default="surge", pattern="^(surge|drop)$")
    threshold: float = Field(default=1.5, ge=1.0, le=10.0)
    short_period: int = Field(default=20, ge=1, le=200)
    long_period: int = Field(default=100, ge=1, le=500)
    ma_type: str = Field(default="sma", pattern="^(sma|ema)$")


class PeriodBreakoutParamsValidated(BaseModel):
    """期間ブレイクアウトパラメータ検証モデル"""

    period: int = Field(default=20, ge=1, le=500)
    direction: str = Field(default="high", pattern="^(high|low)$")
    condition: str = Field(default="break", pattern="^(break|near)$")
    lookback_days: int = Field(default=1, ge=1, le=20)


class BollingerBandsParamsValidated(BaseModel):
    """ボリンジャーバンドパラメータ検証モデル"""

    window: int = Field(default=20, ge=5, le=200)
    alpha: float = Field(default=2.0, ge=1.0, le=5.0)
    position: str = Field(
        default="below_upper",
        pattern="^(below_upper|above_lower|outside_bands|inside_bands)$",
    )


class CrossoverParamsValidated(BaseModel):
    """クロスオーバーパラメータ検証モデル"""

    type: str = Field(default="sma", pattern="^(sma|ema|rsi|macd)$")
    fast_period: int = Field(default=25, ge=1, le=200)
    slow_period: int = Field(default=75, ge=1, le=500)
    direction: str = Field(default="golden", pattern="^(golden|dead)$")
    signal_period: int = Field(default=9, ge=1, le=100)
    lookback_days: int = Field(default=1, ge=1, le=20)


class RSIThresholdParamsValidated(BaseModel):
    """RSI閾値シグナルパラメータ検証モデル"""

    period: int = Field(default=14, ge=1, le=100)
    threshold: float = Field(default=30.0, gt=0, lt=100)
    condition: str = Field(default="below", pattern="^(below|above)$")


class BetaSignalParamsValidated(BaseModel):
    """β値シグナルパラメータ検証モデル"""

    min_beta: float = Field(default=0.5, ge=-2.0, le=5.0)
    max_beta: float = Field(default=1.5, ge=-2.0, le=5.0)
    lookback_period: int = Field(default=200, ge=20, le=500)


class TradingValueSignalParamsValidated(BaseModel):
    """売買代金シグナルパラメータ検証モデル"""

    direction: str = Field(default="above", pattern="^(above|below)$")
    period: int = Field(default=20, ge=1, le=200)
    threshold_value: float = Field(default=1.0, ge=0.0, le=10000.0)


class TradingValueRangeSignalParamsValidated(BaseModel):
    """売買代金範囲シグナルパラメータ検証モデル"""

    period: int = Field(default=20, gt=0, le=200)
    min_threshold: float = Field(default=0.5, ge=0.0, le=10000.0)
    max_threshold: float = Field(default=100.0, ge=0.0, le=10000.0)


# ===== シグナル判定・計算関数 =====


def is_signal_available_in_market_db(signal_name: str) -> bool:
    """
    シグナルがmarket.dbで利用可能かチェック

    Args:
        signal_name: シグナル名（YAML キー名）

    Returns:
        bool: 利用可能な場合True、利用不可の場合False

    利用可能シグナル（OHLCV系 + benchmark_data）:
        - volume, trading_value, trading_value_range
        - period_breakout, ma_breakout
        - crossover, bollinger_bands
        - atr_support_break, retracement
        - rsi_threshold, rsi_spread
        - mean_reversion
        - buy_and_hold
        - beta (benchmark_data: topix_dataテーブルを使用)

    利用不可シグナル（外部データ必要）:
        - fundamental (statements_data必要)
        - margin (margin_data必要)
        - index_daily_change (benchmark_data必要)
        - index_macd_histogram (benchmark_data必要)
    """
    # OHLCV系シグナル + β値シグナル（市場DBで利用可能）
    available_signals = {
        "volume",
        "trading_value",
        "trading_value_range",  # 売買代金範囲シグナル
        "period_breakout",
        "ma_breakout",
        "crossover",
        "bollinger_bands",
        "atr_support_break",
        "retracement",
        "rsi_threshold",
        "rsi_spread",
        "mean_reversion",
        "buy_and_hold",
        "beta",  # topix_dataテーブルからベンチマークデータをロード
    }

    return signal_name in available_signals


def calculate_signal_for_stock(
    stock_data: pd.DataFrame,
    signal_name: str,
    signal_params: dict[str, Any],
    benchmark_data: pd.DataFrame | None = None,
) -> tuple[pd.Series, float | None]:  # type: ignore[type-arg]
    """
    特定銘柄のシグナル計算（型安全性・パラメータ検証強化版）

    Args:
        stock_data: 株価DataFrame（Open, High, Low, Close, Volume）
        signal_name: シグナル名（YAML キー名）
        signal_params: シグナルパラメータ（YAML設定値）
        benchmark_data: ベンチマークデータ（β値シグナル用、Optional）

    Returns:
        tuple[pd.Series[bool], float | None]: シグナル結果とβ値（β値シグナルの場合のみ）

    Raises:
        ValueError: サポートされていないシグナル名の場合
        ValidationError: パラメータ検証エラー

    Notes:
        - Pydanticモデルによるパラメータ検証を実施
        - エラー時は全Falseを返し、警告をロガーに記録
        - β値シグナルの場合、最新のβ値を第2返り値として返す
        - CLAUDE.md L14-16, L35-38 対応（型安全性・パラメータ検証）
    """
    try:
        # シグナルごとに個別処理（Pydanticモデルで検証）
        if signal_name == "volume":
            # Pydanticモデルでパラメータ検証
            validated_params = VolumeSignalParamsValidated(**signal_params)

            result: pd.Series = volume_signal(  # type: ignore[type-arg]
                volume=stock_data["Volume"],
                direction=validated_params.direction,
                threshold=validated_params.threshold,
                short_period=validated_params.short_period,
                long_period=validated_params.long_period,
                ma_type=validated_params.ma_type,
            )
            return (result, None)

        elif signal_name == "period_breakout":
            # Pydanticモデルでパラメータ検証
            validated_params_pb = PeriodBreakoutParamsValidated(**signal_params)

            # direction に応じて High or Low を選択
            price = (
                stock_data["High"]
                if validated_params_pb.direction == "high"
                else stock_data["Low"]
            )

            result_pb: pd.Series = period_breakout_signal(  # type: ignore[type-arg]
                price=price,
                period=validated_params_pb.period,
                direction=validated_params_pb.direction,
                condition=validated_params_pb.condition,
                lookback_days=validated_params_pb.lookback_days,
            )
            return (result_pb, None)

        elif signal_name == "bollinger_bands":
            # Pydanticモデルでパラメータ検証
            validated_params_bb = BollingerBandsParamsValidated(**signal_params)

            result_bb: pd.Series = bollinger_bands_signal(  # type: ignore[type-arg]
                ohlc_data=stock_data,
                window=validated_params_bb.window,
                alpha=validated_params_bb.alpha,  # YAMLキー'alpha'と一致
                position=validated_params_bb.position,
            )
            return (result_bb, None)

        elif signal_name == "crossover":
            # Pydanticモデルでパラメータ検証
            validated_params_co = CrossoverParamsValidated(**signal_params)

            result_co: pd.Series = indicator_crossover_signal(  # type: ignore[type-arg]
                close=stock_data["Close"],
                indicator_type=validated_params_co.type,
                fast_period=validated_params_co.fast_period,
                slow_period=validated_params_co.slow_period,
                direction=validated_params_co.direction,
                signal_period=validated_params_co.signal_period,
                lookback_days=validated_params_co.lookback_days,
            )
            return (result_co, None)

        elif signal_name == "rsi_threshold":
            # Pydanticモデルでパラメータ検証
            validated_params_rsi = RSIThresholdParamsValidated(**signal_params)

            result_rsi: pd.Series = rsi_threshold_signal(  # type: ignore[type-arg]
                close=stock_data["Close"],
                period=validated_params_rsi.period,
                threshold=validated_params_rsi.threshold,
                condition=validated_params_rsi.condition,
            )
            return (result_rsi, None)

        elif signal_name == "trading_value":
            # Pydanticモデルでパラメータ検証
            validated_params_tv = TradingValueSignalParamsValidated(**signal_params)

            # 売買代金シグナル計算
            result_tv: pd.Series = trading_value_signal(  # type: ignore[type-arg]
                close=stock_data["Close"],
                volume=stock_data["Volume"],
                direction=validated_params_tv.direction,
                period=validated_params_tv.period,
                threshold_value=validated_params_tv.threshold_value,
            )

            # 売買代金平均値を計算（億円単位）
            trading_value = stock_data["Close"] * stock_data["Volume"] / 1e8
            trading_value_ma = vbt.indicators.MA.run(
                trading_value, validated_params_tv.period, short_name="TradingValue_MA"
            ).ma

            # 最新の有効な売買代金平均を取得
            latest_trading_value_avg = None
            if not trading_value_ma.dropna().empty:
                latest_trading_value_avg = float(trading_value_ma.dropna().iloc[-1])

            return (result_tv, latest_trading_value_avg)

        elif signal_name == "trading_value_range":
            # Pydanticモデルでパラメータ検証
            validated_params_tvr = TradingValueRangeSignalParamsValidated(
                **signal_params
            )

            # 売買代金範囲シグナル計算
            result_tvr: pd.Series = trading_value_range_signal(  # type: ignore[type-arg]
                close=stock_data["Close"],
                volume=stock_data["Volume"],
                period=validated_params_tvr.period,
                min_threshold=validated_params_tvr.min_threshold,
                max_threshold=validated_params_tvr.max_threshold,
            )

            # 売買代金平均値を計算（億円単位）
            trading_value_tvr = stock_data["Close"] * stock_data["Volume"] / 1e8
            trading_value_ma_tvr = vbt.indicators.MA.run(
                trading_value_tvr,
                validated_params_tvr.period,
                short_name="TradingValue_MA",
            ).ma

            # 最新の有効な売買代金平均を取得
            latest_trading_value_avg_tvr = None
            if not trading_value_ma_tvr.dropna().empty:
                latest_trading_value_avg_tvr = float(
                    trading_value_ma_tvr.dropna().iloc[-1]
                )

            return (result_tvr, latest_trading_value_avg_tvr)

        elif signal_name == "beta":
            # ベンチマークデータチェック
            if benchmark_data is None:
                error_msg = "Beta signal requires benchmark_data"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Pydanticモデルでパラメータ検証
            validated_params_beta = BetaSignalParamsValidated(**signal_params)

            # β値シグナルとβ値を同時計算（二重計算排除）
            result_beta, latest_beta = beta_range_signal_with_value(
                stock_price=stock_data["Close"],
                market_price=benchmark_data["Close"],
                beta_min=validated_params_beta.min_beta,
                beta_max=validated_params_beta.max_beta,
                lookback_period=validated_params_beta.lookback_period,
            )

            return (result_beta, latest_beta)

        else:
            # サポート外シグナル
            error_msg = f"Unsupported signal: {signal_name}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    except ValidationError as e:
        # Pydanticバリデーションエラー（明確なエラーメッセージ）
        logger.error(
            f"Signal parameter validation failed for {signal_name}: {e.errors()}"
        )
        return (pd.Series(False, index=stock_data.index, dtype=bool), None)

    except ValueError as e:
        # サポート外シグナル等
        logger.error(f"Signal calculation failed for {signal_name}: {e}")
        return (pd.Series(False, index=stock_data.index, dtype=bool), None)

    except Exception as e:
        # その他のエラー（データ品質問題等）
        logger.warning(
            f"Unexpected error in signal calculation for {signal_name}: {e}"
        )
        return (pd.Series(False, index=stock_data.index, dtype=bool), None)


def _create_signal_cache_key(signal_name: str, signal_params: dict[str, Any]) -> str:
    """
    シグナルパラメータからキャッシュキーを生成（衝突回避版）

    Args:
        signal_name: シグナル名
        signal_params: シグナルパラメータ

    Returns:
        str: キャッシュキー（決定論的な文字列表現）

    Note:
        hash()は衝突リスクがあるため、JSON文字列による決定論的なキー生成を使用
    """
    import json

    # enabledキーを除外してパラメータをソート
    params_filtered = {
        k: v for k, v in sorted(signal_params.items()) if k != "enabled"
    }
    # JSON文字列で決定論的にシリアライズ
    params_str = json.dumps(params_filtered, sort_keys=True, ensure_ascii=False)
    return f"{signal_name}:{params_str}"


def _process_single_stock(
    code: str,
    stock_df: pd.DataFrame,
    company_name: str,
    strategies_signals: dict[str, dict[str, dict[str, Any]]],
    target_dates_str: list[str],
    target_dates_dt: pd.Index,  # type: ignore[type-arg]
    benchmark_data: pd.DataFrame | None,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """
    単一銘柄のスクリーニング処理（並列処理用）

    Args:
        code: 銘柄コード
        stock_df: 株価DataFrame
        company_name: 銘柄名
        strategies_signals: {戦略名: {シグナル名: パラメータ}}
        target_dates_str: 対象日付リスト（文字列形式、結果キー用）
        target_dates_dt: 対象日付リスト（DatetimeIndex形式、インデックス比較用）
        benchmark_data: ベンチマークデータ

    Returns:
        dict: {戦略名: {日付: [銘柄情報]}}
    """
    results: dict[str, dict[str, list[dict[str, Any]]]] = {}
    signal_cache: dict[str, tuple[pd.Series, float | None]] = {}  # type: ignore[type-arg]

    for strategy_name, signals in strategies_signals.items():
        results[strategy_name] = {date: [] for date in target_dates_str}

        if not signals:
            continue

        try:
            combined_signal = pd.Series(True, index=stock_df.index)
            beta_value: float | None = None
            trading_value_avg: float | None = None

            for signal_name, signal_params in signals.items():
                cache_key = _create_signal_cache_key(signal_name, signal_params)

                if cache_key in signal_cache:
                    signal, value = signal_cache[cache_key]
                else:
                    signal, value = calculate_signal_for_stock(
                        stock_df, signal_name, signal_params, benchmark_data
                    )
                    signal_cache[cache_key] = (signal, value)

                combined_signal &= signal

                if signal_name == "beta" and value is not None:
                    beta_value = value
                elif signal_name in ("trading_value", "trading_value_range"):
                    if value is not None:
                        trading_value_avg = value

                # 早期終了: 対象日付が全てFalseなら残りシグナルをスキップ
                has_any_signal = any(
                    dt in combined_signal.index and combined_signal[dt]
                    for dt in target_dates_dt
                )
                if not has_any_signal:
                    break
            else:
                # 全シグナル処理完了時のみ結果を登録
                for date_str, date_dt in zip(target_dates_str, target_dates_dt):
                    if date_dt not in combined_signal.index:
                        continue
                    if not combined_signal[date_dt]:
                        continue

                    result_item: dict[str, Any] = {
                        "code": code,
                        "company_name": company_name,
                        "close": float(stock_df.loc[date_dt, "Close"]),  # type: ignore[arg-type]
                        "volume": int(stock_df.loc[date_dt, "Volume"]),  # type: ignore[arg-type]
                    }
                    if beta_value is not None:
                        result_item["beta"] = beta_value
                    if trading_value_avg is not None:
                        result_item["trading_value_avg"] = trading_value_avg

                    results[strategy_name][date_str].append(result_item)

        except Exception as e:
            logger.warning(f"Screening failed for {code} in {strategy_name}: {e}")

    return results


def run_screening(
    stock_data_dict: dict[str, tuple[pd.DataFrame, str]],
    strategies_config: dict[str, dict[str, Any]],
    benchmark_data: pd.DataFrame | None,
    days: int = 10,
    max_workers: int | None = None,
) -> tuple[dict[str, dict[str, list[dict[str, Any]]]], list[str]]:
    """
    銘柄スクリーニング実行（高速化版）

    データロード済みの株価データに対してスクリーニングを実行します。
    データ取得は呼び出し元が担当します。

    Args:
        stock_data_dict: {銘柄コード: (株価DataFrame, 銘柄名)} の辞書
        strategies_config: 戦略設定辞書 {戦略名: {entry_filter_params: {...}}}
        benchmark_data: ベンチマークデータ（β値シグナル用、Optional）
        days: スクリーニング対象期間（営業日数）
        max_workers: 並列処理のワーカー数（None=自動）

    Returns:
        tuple[dict, list]:
            - 結果辞書: {戦略名: {日付文字列: [銘柄情報辞書]}}
            - 警告リスト: 利用不可シグナルの警告メッセージ
    """
    warnings: list[str] = []
    results: dict[str, dict[str, list[dict[str, Any]]]] = {}

    sample_stock = next(iter(stock_data_dict.values()))[0]
    target_dates_str = sample_stock.index[-days:].strftime("%Y-%m-%d").tolist()
    target_dates_dt = sample_stock.index[-days:]

    strategies_signals = _prepare_strategies_signals(
        strategies_config, target_dates_str, results, warnings
    )

    logger.info(
        f"Starting parallel screening for {len(stock_data_dict)} stocks "
        f"(workers: {max_workers or 'auto'})..."
    )

    results_lock = Lock()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_single_stock,
                code,
                stock_df,
                company_name,
                strategies_signals,
                target_dates_str,
                target_dates_dt,
                benchmark_data,
            ): code
            for code, (stock_df, company_name) in stock_data_dict.items()
        }

        completed = 0
        total = len(futures)
        for future in as_completed(futures):
            code = futures[future]
            try:
                stock_results = future.result()
                with results_lock:
                    for strategy_name, date_results in stock_results.items():
                        for date, items in date_results.items():
                            results[strategy_name][date].extend(items)
            except Exception as e:
                logger.warning(f"Screening failed for {code}: {e}")

            completed += 1
            if completed % 100 == 0:
                logger.info(f"Progress: {completed}/{total} stocks processed")

    logger.info(f"Screening completed: {total} stocks processed")
    return results, warnings


def load_benchmark_if_needed(
    strategies_config: dict[str, dict[str, Any]],
    warnings: list[str],
) -> pd.DataFrame | None:
    """
    β値シグナルが必要な場合にベンチマークデータをロード

    Args:
        strategies_config: 戦略設定辞書
        warnings: 警告リスト（ロード失敗時に追加）

    Returns:
        ベンチマークデータ（不要またはロード失敗時はNone）
    """

    def has_beta_signal(config: dict[str, Any]) -> bool:
        entry_params = config.get("entry_filter_params", {})
        beta_params = entry_params.get("beta", {})
        return "beta" in entry_params and beta_params.get("enabled", True)

    needs_benchmark = any(has_beta_signal(cfg) for cfg in strategies_config.values())

    if not needs_benchmark:
        return None

    try:
        logger.info("Loading TOPIX benchmark data for beta signal...")
        benchmark_data = load_topix_data_from_market_db()
        logger.info(
            f"TOPIX data loaded: {len(benchmark_data)} records "
            f"({benchmark_data.index[0]} to {benchmark_data.index[-1]})"
        )
        return benchmark_data
    except Exception as e:
        logger.error(f"Failed to load TOPIX data: {e}")
        warnings.append(f"[共通] beta (ベンチマークデータロード失敗: {e})")
        return None


def _prepare_strategies_signals(
    strategies_config: dict[str, dict[str, Any]],
    target_dates_str: list[str],
    results: dict[str, dict[str, list[dict[str, Any]]]],
    warnings: list[str],
) -> dict[str, dict[str, dict[str, Any]]]:
    """
    戦略ごとの利用可能シグナルを事前準備

    Args:
        strategies_config: 戦略設定辞書
        target_dates_str: 対象日付リスト（結果初期化用）
        results: 結果辞書（初期化される）
        warnings: 警告リスト（利用不可シグナル発見時に追加）

    Returns:
        {戦略名: {シグナル名: パラメータ}} の辞書
    """
    strategies_signals: dict[str, dict[str, dict[str, Any]]] = {}

    for strategy_name, strategy_config in strategies_config.items():
        logger.info(f"Preparing strategy: {strategy_name}")
        results[strategy_name] = {date: [] for date in target_dates_str}

        entry_filter_params = strategy_config.get("entry_filter_params", {})
        if not entry_filter_params:
            logger.warning(f"No entry_filter_params found for {strategy_name}")
            strategies_signals[strategy_name] = {}
            continue

        available_signals: dict[str, dict[str, Any]] = {}
        for signal_name, signal_params in entry_filter_params.items():
            if not signal_params.get("enabled", True):
                continue

            if not is_signal_available_in_market_db(signal_name):
                warnings.append(
                    f"[{strategy_name}] {signal_name} (データ不足のためスキップ)"
                )
                continue

            available_signals[signal_name] = signal_params

        if not available_signals:
            logger.warning(f"No available signals for {strategy_name}")

        strategies_signals[strategy_name] = available_signals

    return strategies_signals
