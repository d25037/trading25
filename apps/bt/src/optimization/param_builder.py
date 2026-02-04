"""
SignalParams動的構築モジュール

グリッドYAMLとベース戦略YAMLをマージしてSignalParamsを構築します。
"""

from typing import Any, TypeAlias

from loguru import logger
from pydantic import BaseModel

from src.models.signals import (
    ATRSupportBreakParams,
    BetaSignalParams,
    BollingerBandsSignalParams,
    BuyAndHoldSignalParams,
    CrossoverSignalParams,
    FundamentalSignalParams,
    IndexDailyChangeSignalParams,
    IndexMACDHistogramSignalParams,
    MABreakoutParams,
    MarginSignalParams,
    MeanReversionSignalParams,
    PeriodBreakoutParams,
    RetracementSignalParams,
    RiskAdjustedReturnSignalParams,
    RSISpreadSignalParams,
    RSIThresholdSignalParams,
    SectorRotationPhaseParams,
    SectorStrengthRankingParams,
    SectorVolatilityRegimeParams,
    SignalParams,
    TradingValueRangeSignalParams,
    TradingValueSignalParams,
    TrendSignalParams,
    VolatilitySignalParams,
    VolumeSignalParams,
)

# シグナルパラメータの値型（int, float, str, bool, または None）
SignalParamValue: TypeAlias = int | float | str | bool | None

# シグナル単位のパラメータ辞書（例: {"lookback_days": 10, "period": 100}）
SignalParamDict: TypeAlias = dict[str, SignalParamValue]

# グリッドパラメータ辞書（例: {"period_breakout": {"lookback_days": 10}}）
GridParamsDict: TypeAlias = dict[str, SignalParamDict]

# フラットなパラメータ辞書（例: {"entry_filter_params.period_breakout.lookback_days": 10}）
FlatParamsDict: TypeAlias = dict[str, SignalParamValue]

# マージ済みパラメータ辞書（シグナル名 → パラメータ辞書またはプリミティブ値）
MergedParamsDict: TypeAlias = dict[str, SignalParamDict | SignalParamValue]

def _unflatten_params(flat_params: dict[str, SignalParamValue]) -> dict[str, Any]:
    """
    フラットなパラメータを階層構造に戻す

    4階層以上のネスト（例: per.threshold）に対応

    Args:
        flat_params: フラットなパラメータ辞書
            例: {"per.threshold": 15.0, "pbr.threshold": 1.0}

    Returns:
        dict: 階層構造のパラメータ辞書
            例: {"per": {"threshold": 15.0}, "pbr": {"threshold": 1.0}}
    """
    result: dict[str, Any] = {}
    for key, value in flat_params.items():
        parts = key.split(".")
        current = result
        for _i, part in enumerate(parts[:-1]):
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value
    return result


def _deep_merge(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    """
    再帰的にネストされた辞書をマージ

    Args:
        base: ベース辞書（元の設定）
        updates: 更新辞書（グリッドから取得したパラメータ）

    Returns:
        dict: マージ結果（baseを変更せず新しい辞書を返す）

    Example:
        >>> base = {"enabled": True, "threshold": 15.0}
        >>> updates = {"threshold": 20.0}
        >>> _deep_merge(base, updates)
        {"enabled": True, "threshold": 20.0}
    """
    result = base.copy()
    for key, value in updates.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


# シグナル名からパラメータクラスへのマッピング
SIGNAL_PARAM_CLASSES: dict[str, type[BaseModel]] = {
    "period_breakout": PeriodBreakoutParams,
    "ma_breakout": MABreakoutParams,
    "bollinger_bands": BollingerBandsSignalParams,
    "volume": VolumeSignalParams,
    "trading_value": TradingValueSignalParams,
    "trading_value_range": TradingValueRangeSignalParams,
    "crossover": CrossoverSignalParams,
    "mean_reversion": MeanReversionSignalParams,
    "rsi_threshold": RSIThresholdSignalParams,
    "rsi_spread": RSISpreadSignalParams,
    "atr_support_break": ATRSupportBreakParams,
    "fundamental": FundamentalSignalParams,
    "beta": BetaSignalParams,
    "margin": MarginSignalParams,
    "volatility": VolatilitySignalParams,
    "trend": TrendSignalParams,
    "buy_and_hold": BuyAndHoldSignalParams,
    "index_daily_change": IndexDailyChangeSignalParams,
    "index_macd_histogram": IndexMACDHistogramSignalParams,
    "retracement": RetracementSignalParams,
    "risk_adjusted_return": RiskAdjustedReturnSignalParams,
    "sector_strength_ranking": SectorStrengthRankingParams,
    "sector_rotation_phase": SectorRotationPhaseParams,
    "sector_volatility_regime": SectorVolatilityRegimeParams,
}


def _validate_signal_names(
    grid_params_dict: GridParamsDict,
    base_params_dict: MergedParamsDict,
    section: str,
) -> None:
    """
    グリッドYAMLのシグナル名がベースYAMLに存在するか検証

    Args:
        grid_params_dict: グリッドYAMLから抽出したシグナルパラメータ
        base_params_dict: ベースYAMLのシグナルパラメータ
        section: セクション名（"entry_filter_params" or "exit_trigger_params"）

    Warning:
        グリッドYAMLに存在するがベースYAMLに存在しないシグナル名が
        見つかった場合、警告を出力する（エラーにはしない）

    Example:
        >>> # グリッドYAML内に "macd_cross" が存在するが、
        >>> # ベースYAMLには "crossover" しか存在しない場合
        >>> _validate_signal_names(
        ...     {"macd_cross": {"fast_period": 12}},
        ...     {"crossover": CrossoverSignalParams(...)},
        ...     "entry_filter_params"
        ... )
        >>> # WARNING: グリッドYAML内のシグナル名 "macd_cross" はベースYAMLに存在しません...
    """
    missing_signals = []

    for signal_name in grid_params_dict.keys():
        if signal_name not in base_params_dict:
            missing_signals.append(signal_name)

    if missing_signals:
        logger.warning(
            f"⚠️ グリッドYAML内のシグナル名がベースYAMLに存在しません:\n"
            f"   セクション: {section}\n"
            f"   存在しないシグナル: {', '.join(missing_signals)}\n"
            f"   ベースYAMLに存在するシグナル: {', '.join(base_params_dict.keys())}\n"
            f"   → これらのパラメータは無視され、最適化に反映されません。"
        )


def build_signal_params(
    params: FlatParamsDict,
    section: str,
    base_signal_params: SignalParams,
) -> SignalParams:
    """
    パラメータ辞書からSignalParamsを動的構築（ベース設定マージ）

    設計思想:
        - ベース戦略YAML（enabled, direction, condition等）を継承
        - グリッドYAMLで指定されたパラメータのみ上書き
        - その他の設定は全てベース戦略YAMLから引き継ぐ

    Args:
        params: {"entry_filter_params.period_breakout.lookback_days": 10, ...}
        section: "entry_filter_params" or "exit_trigger_params"
        base_signal_params: ベース戦略YAMLから読み込んだSignalParams

    Returns:
        SignalParams: ベース設定 + 最適化パラメータをマージしたSignalParams

    Example:
        # ベース戦略YAML (range_break_v6.yaml)
        entry_filter_params:
          period_breakout:
            enabled: true           # ← ベース設定から継承
            direction: "high"       # ← ベース設定から継承
            condition: "break"      # ← ベース設定から継承
            lookback_days: 10       # ← グリッドで上書き
            period: 100             # ← グリッドで上書き

        # グリッドYAML (range_break_v6_grid.yaml)
        parameter_ranges:
          entry_filter_params:
            period_breakout:
              lookback_days: [5, 10, 15, 20]  # 最適化対象
              period: [30, 50, 100, 200]      # 最適化対象

        # 結果: enabled=True, direction="high", condition="break"は継承
        #       lookback_days=10, period=100はグリッドから設定
    """
    # 1. ベース設定をdictに変換（継承用）
    base_params_dict: MergedParamsDict = base_signal_params.model_dump()

    # 2. セクション抽出（グリッドから最適化パラメータのみ）
    section_params: FlatParamsDict = {
        k.replace(f"{section}.", ""): v
        for k, v in params.items()
        if k.startswith(f"{section}.")
    }

    # 3. シグナル別にグルーピング
    # {"period_breakout.lookback_days": 10}
    # → {"period_breakout": {"lookback_days": 10}}
    # {"fundamental.per.threshold": 15.0}
    # → {"fundamental": {"per": {"threshold": 15.0}}}
    grid_params_dict: dict[str, Any] = {}
    for key, value in section_params.items():
        parts = key.split(".", 1)
        if len(parts) == 2:
            signal_name, param_path = parts
            if signal_name not in grid_params_dict:
                grid_params_dict[signal_name] = {}

            # ネストされたパスを階層構造に変換
            # "per.threshold" → {"per": {"threshold": value}}
            if "." in param_path:
                nested = _unflatten_params({param_path: value})
                grid_params_dict[signal_name] = _deep_merge(
                    grid_params_dict[signal_name], nested
                )
            else:
                grid_params_dict[signal_name][param_path] = value

    # 3.5. シグナル名検証（グリッドYAML vs ベースYAML）
    _validate_signal_names(grid_params_dict, base_params_dict, section)

    # 4. ベース設定とグリッド設定をマージ
    merged_params: MergedParamsDict = {}
    for signal_name, base_signal_config in base_params_dict.items():
        if base_signal_config is None:
            continue

        # ベース設定を辞書化
        base_signal_dict: SignalParamDict | SignalParamValue
        if isinstance(base_signal_config, dict):
            base_signal_dict = base_signal_config.copy()
        elif hasattr(base_signal_config, "model_dump"):
            base_signal_dict = base_signal_config.model_dump()
        elif hasattr(base_signal_config, "__dict__"):
            base_signal_dict = dict(vars(base_signal_config))
        else:
            # プリミティブ型の場合はそのまま
            base_signal_dict = base_signal_config

        # グリッド設定で深いマージ（ネストされたパラメータに対応）
        if signal_name in grid_params_dict and isinstance(base_signal_dict, dict):
            base_signal_dict = _deep_merge(base_signal_dict, grid_params_dict[signal_name])

        merged_params[signal_name] = base_signal_dict

    # 5. SignalParams構築（マージ結果から）
    kwargs: dict[str, Any] = {}
    for signal_name, param_class in SIGNAL_PARAM_CLASSES.items():
        if signal_name in merged_params:
            merged_value = merged_params[signal_name]
            if isinstance(merged_value, dict):
                kwargs[signal_name] = param_class(**merged_value)

    return SignalParams(**kwargs)
