"""
YAML設定駆動戦略クラス

YAML完全制御アーキテクチャの中核クラスを提供します：
- シングル/マルチ銘柄対応
- 統合ポートフォリオ分析
- 資金共有・配分最適化
- YAML設定による柔軟な戦略制御
"""

from typing import TYPE_CHECKING, Dict, Optional

import pandas as pd
import vectorbt as vbt

from src.models.signals import Signals
from src.strategies.signals.processor import SignalProcessor
from src.utils.logger_config import Logger

from .mixins import (
    BacktestExecutorMixin,
    DataManagerMixin,
    PortfolioAnalyzerKellyMixin,
)

if TYPE_CHECKING:
    from src.models.config import SharedConfig

from src.models.signals import SignalParams


class YamlConfigurableStrategy(
    DataManagerMixin,
    PortfolioAnalyzerKellyMixin,
    BacktestExecutorMixin,
):
    """
    YAML設定駆動戦略クラス

    YAML完全制御アーキテクチャの中核クラスとして以下の機能を提供します：
    - シングル/マルチ銘柄対応
    - 統合ポートフォリオと個別銘柄の分析
    - 資金共有・配分最適化対応
    - YAML設定による柔軟な戦略制御（entry_filter_params/exit_trigger_params）
    """

    def __init__(
        self,
        shared_config: "SharedConfig",
        entry_filter_params: Optional[SignalParams] = None,
        exit_trigger_params: Optional[SignalParams] = None,
    ):
        """
        YAML設定駆動戦略クラスの初期化

        Args:
            shared_config: 共通設定（SharedConfig）
            entry_filter_params: エントリーフィルターパラメータ（SignalParams）
            exit_trigger_params: エグジットトリガーパラメータ（SignalParams）
        """
        # SharedConfigから基本パラメータを設定
        self.dataset = shared_config.dataset
        self.stock_codes = shared_config.stock_codes
        self.stock_code = (
            self.stock_codes[0] if self.stock_codes else ""
        )  # 互換性のため
        self.initial_cash = shared_config.initial_cash
        self.fees = shared_config.fees
        self.slippage = shared_config.slippage
        self.spread = shared_config.spread
        self.borrow_fee = shared_config.borrow_fee
        self.max_concurrent_positions = shared_config.max_concurrent_positions
        self.max_exposure = shared_config.max_exposure
        self.start_date = shared_config.start_date
        self.end_date = shared_config.end_date
        self.printlog = shared_config.printlog

        # loguruベースのロガーを初期化
        self.logger = Logger(name=f"{self.stock_code}", printlog=shared_config.printlog)

        # VectorBT設定
        self.group_by = shared_config.group_by
        self.cash_sharing = shared_config.cash_sharing
        self.direction = shared_config.direction

        # Timeframe設定
        self.timeframe = shared_config.timeframe

        # Relative Mode関連属性
        self.relative_mode = shared_config.relative_mode
        self.benchmark_table = shared_config.benchmark_table
        self.benchmark_data: Optional[pd.DataFrame] = None

        # データ関連属性
        self.include_margin_data = shared_config.include_margin_data
        self.include_statements_data = shared_config.include_statements_data

        # マルチアセット用の追加属性
        self.multi_data_dict: Optional[Dict[str, Dict[str, pd.DataFrame]]] = None
        self.combined_portfolio: Optional[vbt.Portfolio] = None
        self.portfolio: Optional[vbt.Portfolio] = None

        # Relative Mode用の追加属性
        self.relative_data_dict: Optional[Dict[str, Dict[str, pd.DataFrame]]] = None
        self.execution_data_dict: Optional[Dict[str, Dict[str, pd.DataFrame]]] = None

        # エントリーフィルターパラメータを設定
        self.entry_filter_params: Optional[SignalParams] = entry_filter_params

        # エグジットトリガーパラメータを設定
        self.exit_trigger_params: Optional[SignalParams] = exit_trigger_params

        # 統合シグナルプロセッサー（Filter + Trigger統合）
        self.signal_processor = SignalProcessor()

        # Kelly criterion settings (Kelly基準のみ使用)
        self.kelly_fraction = shared_config.kelly_fraction
        self.min_allocation = shared_config.min_allocation
        self.max_allocation = shared_config.max_allocation

        # エントリーシグナルDataFrame（バックテスト実行後に設定される）
        self.all_entries: Optional[pd.DataFrame] = None

    def _log(self, message: str, level: str = "info") -> None:
        """
        ログ出力（loguruベース・printlog完全制御）

        Args:
            message: ログメッセージ
            level: ログレベル（info/debug/warning/error）
        """
        # printlog=Falseの場合、error/critical以外は出力しない
        if not self.printlog and level not in ["error", "critical"]:
            return

        if level == "debug":
            self.logger.debug(message)
        elif level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)
        elif level == "critical":
            self.logger.critical(message)
        else:
            self.logger.info(message)

    def generate_signals(
        self,
        data: pd.DataFrame,
        margin_data: Optional[pd.DataFrame] = None,
        statements_data: Optional[pd.DataFrame] = None,
        execution_data: Optional[pd.DataFrame] = None,
        sector_data: Optional[Dict[str, pd.DataFrame]] = None,
        stock_sector_name: Optional[str] = None,
    ) -> Signals:
        """
        売買シグナルを生成（YAML完全制御版）

        戦略固有シグナル生成を削除し、全Trueから開始。
        entry_filter_params/exit_trigger_paramsで条件絞り込み。

        Args:
            data: OHLCV データ（相対価格モードでは相対価格データ）
            margin_data: 信用残高データ（オプション）
            statements_data: 財務諸表データ（オプション）
            execution_data: 実行用OHLCVデータ（相対価格モードでの実価格データ、通常モードではNone）

        Returns:
            Signals: 買いシグナルと売りシグナルを含むSignalsオブジェクト
        """
        # 全Trueから開始（SignalProcessorで絞り込み）
        entries = pd.Series(True, index=data.index)
        exits = pd.Series(False, index=data.index)

        enhanced_signals = self.signal_processor.generate_signals(
            strategy_entries=entries,
            strategy_exits=exits,
            ohlc_data=data,
            entry_signal_params=self.entry_filter_params or SignalParams(),
            exit_signal_params=self.exit_trigger_params or SignalParams(),
            margin_data=margin_data,
            statements_data=statements_data,
            benchmark_data=self.benchmark_data,
            execution_data=execution_data,
            relative_mode=self.relative_mode,
            sector_data=sector_data,
            stock_sector_name=stock_sector_name,
        )
        entries, exits = enhanced_signals.entries, enhanced_signals.exits

        # 最終日売りシグナル
        last_valid_idx = data["Close"].last_valid_index()
        if last_valid_idx is not None:
            exits = exits.copy()
            exits.at[last_valid_idx] = True

        return Signals(entries=entries, exits=exits)

    def generate_multi_signals(
        self,
        stock_code: str,
        data: pd.DataFrame,
        margin_data: Optional[pd.DataFrame] = None,
        statements_data: Optional[pd.DataFrame] = None,
        execution_data: Optional[pd.DataFrame] = None,
        sector_data: Optional[Dict[str, pd.DataFrame]] = None,
        stock_sector_name: Optional[str] = None,
    ) -> Signals:
        """
        単一銘柄のシグナル生成（サブクラスで実装）

        Args:
            stock_code: 銘柄コード（ログ出力用）
            data: OHLCV データ（相対価格モードでは相対価格データ）
            margin_data: 信用残高データ（オプション）
            statements_data: 財務諸表データ（オプション）
            execution_data: 実行用OHLCVデータ（相対価格モードでの実価格データ、通常モードではNone）

        Returns:
            Signals: 買いシグナルと売りシグナルを含むSignalsオブジェクト
        """
        # 一時的に銘柄コードを設定してシグナル生成
        original_code = self.stock_code
        self.stock_code = stock_code
        try:
            signals = self.generate_signals(
                data,
                margin_data,
                statements_data,
                execution_data,
                sector_data=sector_data,
                stock_sector_name=stock_sector_name,
            )
        finally:
            self.stock_code = original_code
        return signals

    def _build_relative_status(self) -> str:
        """
        相対モード状態の表示文字列を構築

        注意: このメソッドは現在使用されていませんが、将来的な使用のために保持されています。

        Returns:
            str: 相対モード状態文字列
        """
        return (
            " (Relative Mode)"
            if hasattr(self, "relative_mode") and self.relative_mode
            else ""
        )
