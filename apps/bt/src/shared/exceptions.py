"""
カスタム例外モジュール

プロジェクト固有の例外クラスを定義
"""


class TradingBTError(Exception):
    """プロジェクトの基底例外クラス"""

    pass


# =============================================================================
# データローディング関連
# =============================================================================


class DataLoadError(TradingBTError):
    """データ読み込みエラーの基底クラス"""

    pass


class StockDataLoadError(DataLoadError):
    """株価データ読み込みエラー"""

    pass


class MarginDataLoadError(DataLoadError):
    """信用残高データ読み込みエラー"""

    pass


class StatementsDataLoadError(DataLoadError):
    """財務諸表データ読み込みエラー"""

    pass


class IndexDataLoadError(DataLoadError):
    """インデックスデータ読み込みエラー"""

    pass


class SectorDataLoadError(DataLoadError):
    """セクターデータ読み込みエラー"""

    pass


# =============================================================================
# データ準備関連
# =============================================================================


class DataPreparationError(TradingBTError):
    """データ準備・前処理エラー"""

    pass


class NoValidDataError(DataPreparationError):
    """有効なデータが存在しないエラー"""

    pass


class BatchAPIError(DataPreparationError):
    """バッチAPIエラー"""

    pass


# =============================================================================
# 最適化関連
# =============================================================================


class OptimizationError(TradingBTError):
    """最適化エラーの基底クラス"""

    pass


class OptimizationTimeoutError(OptimizationError):
    """最適化タイムアウトエラー"""

    pass


class NoOptimizationResultsError(OptimizationError):
    """最適化結果が空のエラー"""

    pass


# =============================================================================
# 戦略評価関連
# =============================================================================


class StrategyEvaluationError(TradingBTError):
    """戦略評価エラーの基底クラス"""

    pass


class CandidateEvaluationError(StrategyEvaluationError):
    """単一候補の評価エラー"""

    pass


class BatchEvaluationError(StrategyEvaluationError):
    """バッチ評価エラー"""

    pass


# =============================================================================
# 設定関連
# =============================================================================


class ConfigurationError(TradingBTError):
    """設定エラーの基底クラス"""

    pass


class StrategyConfigError(ConfigurationError):
    """戦略設定エラー"""

    pass


class InvalidParameterError(ConfigurationError):
    """無効なパラメータエラー"""

    pass


