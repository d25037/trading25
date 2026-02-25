"""
データ管理ミックスイン

YamlConfigurableStrategy用のデータ読み込み・変換・キャッシュ機能を提供します。
"""

from typing import TYPE_CHECKING

import pandas as pd

from src.domains.strategy.transforms import create_relative_ohlc_data
from src.infrastructure.data_access.loaders import load_topix_data, prepare_multi_data
from src.shared.models.signals.fundamental import FundamentalSignalParams
from src.shared.models.types import StatementsPeriodType

if TYPE_CHECKING:
    from .protocols import StrategyProtocol


class DataManagerMixin:
    """データ管理機能ミックスイン"""

    def _resolve_period_type(self: "StrategyProtocol") -> StatementsPeriodType:
        """entry_filter_params / exit_trigger_params から period_type を解決

        優先順位: entry側 → exit側 → デフォルト "FY"
        """
        for params in (self.entry_filter_params, self.exit_trigger_params):
            if params is not None:
                fundamental = params.fundamental
                if isinstance(fundamental, FundamentalSignalParams):
                    return fundamental.period_type
        return "FY"

    def _should_include_forecast_revision(self: "StrategyProtocol") -> bool:
        """四半期修正予想データの追加取得が必要か判定する。"""
        for params in (self.entry_filter_params, self.exit_trigger_params):
            if params is None:
                continue
            fundamental = params.fundamental
            if not isinstance(fundamental, FundamentalSignalParams):
                continue
            if not fundamental.enabled:
                continue
            if (
                fundamental.forward_eps_growth.enabled
                or fundamental.forecast_eps_above_all_actuals.enabled
                or fundamental.peg_ratio.enabled
                or fundamental.forward_dividend_growth.enabled
                or fundamental.forward_payout_ratio.enabled
            ):
                return True
        return False

    def load_multi_data(self: "StrategyProtocol") -> dict[str, dict[str, pd.DataFrame]]:
        """
        複数銘柄のデータを読み込み

        バッチAPIを使用して複数銘柄を一括取得します。
        これにより、50銘柄の場合 50 API calls → 1 API call に削減できます。

        Returns:
            Dict[str, Dict[str, pd.DataFrame]]: {銘柄コード: タイムフレーム別データ}
        """
        if self.multi_data_dict is None:
            self._log("複数銘柄データ読み込み開始（バッチAPI使用）", "info")

            # period_type をシグナルパラメータから解決
            period_type = self._resolve_period_type()
            include_forecast_revision = self._should_include_forecast_revision()

            include_margin_data = self.include_margin_data
            include_statements_data = self.include_statements_data

            should_load_margin = (
                self._should_load_margin_data()
                if hasattr(self, "_should_load_margin_data")
                else include_margin_data
            )
            should_load_statements = (
                self._should_load_statements_data()
                if hasattr(self, "_should_load_statements_data")
                else include_statements_data
            )

            if include_margin_data and not should_load_margin:
                self._log(
                    "信用残高データ読み込みをスキップ: 依存シグナルが無効です",
                    "debug",
                )
            if include_statements_data and not should_load_statements:
                self._log(
                    "財務諸表データ読み込みをスキップ: 依存シグナルが無効です",
                    "debug",
                )

            if not include_margin_data and should_load_margin:
                self._log(
                    "信用残高シグナルが有効ですが include_margin_data=false のためデータは読み込みません",
                    "warning",
                )
            if not include_statements_data and should_load_statements:
                self._log(
                    "財務諸表シグナルが有効ですが include_statements_data=false のためデータは読み込みません",
                    "warning",
                )

            # バッチAPIで一括取得
            self.multi_data_dict = prepare_multi_data(
                dataset=self.dataset,
                stock_codes=self.stock_codes,
                start_date=self.start_date,
                end_date=self.end_date,
                include_margin_data=include_margin_data and should_load_margin,
                include_statements_data=(
                    include_statements_data and should_load_statements
                ),
                timeframe=self.timeframe,
                period_type=period_type,
                include_forecast_revision=include_forecast_revision,
            )

            self._log(
                f"マルチデータ読み込み完了 - 銘柄数: {len(self.multi_data_dict)}", "info"
            )

        return self.multi_data_dict

    def load_benchmark_data(self: "StrategyProtocol") -> pd.DataFrame:
        """
        ベンチマークデータ（TOPIX等）を読み込み

        Returns:
            pd.DataFrame: ベンチマークのOHLCVデータ
        """
        if self.benchmark_data is None:
            self._log(f"ベンチマークデータ読み込み開始: {self.benchmark_table}", "info")
            try:
                self.benchmark_data = load_topix_data(
                    self.dataset, self.start_date, self.end_date
                )
                self._log(
                    f"ベンチマークデータ読み込み完了: {len(self.benchmark_data)}レコード",
                    "info",
                )
            except Exception as e:
                self._log(f"ベンチマークデータ読み込みエラー: {e}", "error")
                raise ValueError(
                    f"Failed to load benchmark data from {self.benchmark_table}: {e}"
                )

        return self.benchmark_data

    def load_relative_data(
        self: "StrategyProtocol",
    ) -> tuple[dict[str, dict[str, pd.DataFrame]], dict[str, dict[str, pd.DataFrame]]]:
        """
        Relative Mode用のデータを読み込み

        Returns:
            Tuple[Dict, Dict]: (相対価格データ, 実際の価格データ)
        """
        if self.relative_data_dict is None or self.execution_data_dict is None:
            self._log("Relative Modeデータ準備開始", "info")

            # ベンチマークデータ読み込み
            benchmark_data = self.load_benchmark_data()

            # 通常のデータを先に読み込み
            multi_data_dict = self.load_multi_data()

            self.relative_data_dict = {}
            self.execution_data_dict = {}

            for stock_code in self.stock_codes:
                self._log(f"Relative Data作成: {stock_code}", "debug")

                # 実際の価格データ（実行用）
                stock_data_dict = multi_data_dict[stock_code]
                self.execution_data_dict[stock_code] = stock_data_dict

                # 相対価格データ（シグナル用）
                relative_stock_data = {}
                for tf_name, stock_tf_data in stock_data_dict.items():
                    try:
                        relative_ohlc = create_relative_ohlc_data(
                            stock_tf_data, benchmark_data
                        )
                        relative_stock_data[tf_name] = relative_ohlc
                        self._log(
                            f"{stock_code} {tf_name}: {len(relative_ohlc)}レコードの相対データ作成完了",
                            "debug",
                        )
                    except Exception as e:
                        self._log(
                            f"{stock_code} {tf_name}の相対データ作成失敗: {e}",
                            "warning",
                        )
                        # 相対データ作成に失敗した場合は元データを使用
                        relative_stock_data[tf_name] = stock_tf_data

                self.relative_data_dict[stock_code] = relative_stock_data

            self._log(
                f"Relative Modeデータ準備完了 - 銘柄数: {len(self.stock_codes)}", "info"
            )

        return self.relative_data_dict, self.execution_data_dict
