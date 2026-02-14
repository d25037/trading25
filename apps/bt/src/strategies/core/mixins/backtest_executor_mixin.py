"""
バックテスト実行ミックスイン

YamlConfigurableStrategy用のバックテスト実行・結果生成機能を提供します。
"""

from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, cast

import pandas as pd
import vectorbt as vbt

from src.models.allocation import AllocationInfo

CostParams = Tuple[float, float]
GroupedPortfolioInputs = tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]

if TYPE_CHECKING:
    from .protocols import StrategyProtocol

# セクターデータに依存するシグナル名
_SECTOR_SIGNALS = ("sector_strength_ranking", "sector_rotation_phase", "sector_volatility_regime")

# ベンチマークデータに依存するシグナル名（セクター強度・ローテーションはTOPIX対比計算で必要）
_BENCHMARK_SIGNALS = (
    "beta", "index_daily_change", "index_macd_histogram",
    "sector_strength_ranking", "sector_rotation_phase",
)


def _is_signal_enabled(params: Any, signal_name: str) -> bool:
    """シグナルが有効かチェック"""
    if params is None:
        return False
    signal = getattr(params, signal_name, None)
    if signal is None:
        return False
    return getattr(signal, "enabled", False)


def _any_signal_enabled(
    entry_params: Any, exit_params: Any, signal_names: tuple[str, ...]
) -> str | None:
    """指定シグナル群のいずれかが有効なら、そのシグナル名を返す。無効ならNone。"""
    for name in signal_names:
        if _is_signal_enabled(entry_params, name):
            return f"entry_filter_params.{name}"
        if _is_signal_enabled(exit_params, name):
            return f"exit_trigger_params.{name}"
    return None


class BacktestExecutorMixin:
    """バックテスト実行機能ミックスイン"""

    def _find_signal_for_data_requirement(
        self: "StrategyProtocol",
        requirement: str,
    ) -> str | None:
        """指定データ要件が必要な有効シグナルを探索してパスを返す。"""
        from src.strategies.signals.registry import SIGNAL_REGISTRY

        entry_params = getattr(self, "entry_filter_params", None)
        exit_params = getattr(self, "exit_trigger_params", None)

        for signal_def in SIGNAL_REGISTRY:
            if not any(
                req == requirement or req.startswith(f"{requirement}:")
                for req in signal_def.data_requirements
            ):
                continue

            if entry_params is not None and signal_def.enabled_checker(entry_params):
                return f"entry_filter_params.{signal_def.param_key}"
            if exit_params is not None and signal_def.enabled_checker(exit_params):
                return f"exit_trigger_params.{signal_def.param_key}"

        return None

    def _should_load_sector_data(self: "StrategyProtocol") -> bool:
        """セクターデータのロードが必要かチェック"""
        entry_params = getattr(self, "entry_filter_params", None)
        exit_params = getattr(self, "exit_trigger_params", None)

        matched = _any_signal_enabled(entry_params, exit_params, _SECTOR_SIGNALS)
        if matched:
            self._log(f"セクターデータ必要: {matched}.enabled", "debug")
        return matched is not None

    def _should_load_benchmark(self: "StrategyProtocol") -> bool:
        """ベンチマークデータのロードが必要かチェック"""
        entry_params = getattr(self, "entry_filter_params", None)
        exit_params = getattr(self, "exit_trigger_params", None)

        matched = _any_signal_enabled(entry_params, exit_params, _BENCHMARK_SIGNALS)
        if matched:
            self._log(f"ベンチマーク必要: {matched}.enabled", "debug")
            return True

        self._log("ベンチマーク不要: 該当シグナルが有効化されていません", "debug")
        return False

    def _should_load_margin_data(self: "StrategyProtocol") -> bool:
        """信用残高データのロードが必要かチェック。"""
        matched = self._find_signal_for_data_requirement("margin")
        if matched:
            self._log(f"信用残高データ必要: {matched}", "debug")
            return True

        self._log("信用残高データ不要: 依存シグナルが有効化されていません", "debug")
        return False

    def _should_load_statements_data(self: "StrategyProtocol") -> bool:
        """財務諸表データのロードが必要かチェック。"""
        matched = self._find_signal_for_data_requirement("statements")
        if matched:
            self._log(f"財務諸表データ必要: {matched}", "debug")
            return True

        self._log("財務諸表データ不要: 依存シグナルが有効化されていません", "debug")
        return False

    def _calculate_cost_params(self: "StrategyProtocol") -> CostParams:
        """比例手数料とスリッページを計算する。

        Returns:
            (effective_fees, effective_slippage) のタプル。
            feesにはspread・借株費用を含み、slippageは分離して返す。
        """
        effective_fees = self.fees + self.spread
        if getattr(self, "direction", "longonly") in ["shortonly", "both"]:
            effective_fees += self.borrow_fee
        return effective_fees, self.slippage

    def _set_grouped_portfolio_inputs_cache(
        self: "StrategyProtocol",
        close_data: pd.DataFrame,
        all_entries: pd.DataFrame,
        all_exits: pd.DataFrame,
    ) -> None:
        """第2段階最適化用に統合ポートフォリオ入力を保持する。"""
        setattr(
            self,
            "_grouped_portfolio_inputs_cache",
            (close_data, all_entries, all_exits),
        )

    def _clear_grouped_portfolio_inputs_cache(self: "StrategyProtocol") -> None:
        """統合ポートフォリオ入力キャッシュをクリアする。"""
        setattr(self, "_grouped_portfolio_inputs_cache", None)

    def _get_grouped_portfolio_inputs_cache(
        self: "StrategyProtocol",
    ) -> GroupedPortfolioInputs | None:
        """保持済みの統合ポートフォリオ入力を取得する。"""
        cached = getattr(self, "_grouped_portfolio_inputs_cache", None)
        if cached is None:
            return None

        if not isinstance(cached, tuple) or len(cached) != 3:
            return None

        close_data, all_entries, all_exits = cached
        if not (
            isinstance(close_data, pd.DataFrame)
            and isinstance(all_entries, pd.DataFrame)
            and isinstance(all_exits, pd.DataFrame)
        ):
            return None

        return cast(GroupedPortfolioInputs, cached)

    def _create_grouped_portfolio(
        self: "StrategyProtocol",
        close_data: pd.DataFrame,
        all_entries: pd.DataFrame,
        all_exits: pd.DataFrame,
        allocation_pct: Optional[float] = None,
    ) -> vbt.Portfolio:
        """統合ポートフォリオを作成する。"""
        effective_fees, effective_slippage = self._calculate_cost_params()

        # ピラミッディング機能（現在未実装、常にFalse）
        pyramid_enabled = False

        if len(self.stock_codes) > 1:
            # マルチアセット戦略: 共有キャッシュプール + 適切なサイズ配分
            if allocation_pct is not None:
                # 2段階最適化: 最適化された配分率を使用
                allocation_per_asset = allocation_pct
                self._log(f"⚡ 最適化配分使用: {allocation_per_asset:.1%}", "info")
            else:
                # 通常実行: 均等配分率を使用
                allocation_per_asset = 1.0 / len(self.stock_codes)  # 均等配分率

            self._log(
                f"💰 資金配分: 総額{self.initial_cash:,}円（共有キャッシュプール）",
                "info",
            )
            self._log(
                f"📊 各銘柄配分率: {allocation_per_asset:.1%} ({allocation_per_asset * 100:.1f}%)",
                "info",
            )

            portfolio_kwargs = dict(
                close=close_data,
                entries=all_entries,
                exits=all_exits,
                direction=getattr(
                    self, "direction", "longonly"
                ),  # 🆕 追加: 取引方向設定
                init_cash=self.initial_cash,  # 🔧 修正: 共有キャッシュプール全体
                size=allocation_per_asset,  # 🆕 追加: 各銘柄への配分率
                size_type="percent",  # 🆕 追加: パーセント指定
                fees=effective_fees,
                slippage=effective_slippage,  # 約定価格シフト（ネイティブ対応）
                cash_sharing=True,  # 資金共有有効
                group_by=True,  # 統合ポートフォリオ
                accumulate=pyramid_enabled,  # 🆕 追加: ピラミッディング対応
                call_seq="auto",  # 🆕 追加: 実行順序最適化
                freq="D",
            )
            if self.max_exposure is not None:
                portfolio_kwargs["max_size"] = self.max_exposure

            return vbt.Portfolio.from_signals(**portfolio_kwargs)  # type: ignore[arg-type]

        # シングル銘柄戦略: 従来通り
        portfolio_kwargs = dict(
            close=close_data,
            entries=all_entries,
            exits=all_exits,
            direction=getattr(
                self, "direction", "longonly"
            ),  # 🆕 追加: 取引方向設定
            init_cash=self.initial_cash,
            fees=effective_fees,
            slippage=effective_slippage,  # 約定価格シフト（ネイティブ対応）
            cash_sharing=self.cash_sharing,
            group_by=True if self.cash_sharing else None,
            accumulate=pyramid_enabled,  # 🆕 追加: ピラミッディング対応
            freq="D",
        )
        if self.max_exposure is not None:
            portfolio_kwargs["max_size"] = self.max_exposure

        return vbt.Portfolio.from_signals(**portfolio_kwargs)  # type: ignore[arg-type]

    def run_multi_backtest_from_cached_signals(
        self: "StrategyProtocol",
        allocation_pct: float,
    ) -> vbt.Portfolio:
        """保持済みシグナルを再利用して配分のみ変更して再実行する。"""
        cached = self._get_grouped_portfolio_inputs_cache()
        if cached is None:
            raise ValueError("統合ポートフォリオ入力キャッシュが存在しません")

        close_data, all_entries, all_exits = cached
        self._log("⚡ キャッシュ済みシグナルを再利用して第2段階を実行", "info")
        portfolio = self._create_grouped_portfolio(
            close_data=close_data,
            all_entries=all_entries,
            all_exits=all_exits,
            allocation_pct=allocation_pct,
        )
        self.combined_portfolio = portfolio
        return portfolio

    def run_multi_backtest(
        self: "StrategyProtocol",
        allocation_pct: Optional[float] = None,
    ) -> Tuple[vbt.Portfolio, Optional[pd.DataFrame]]:
        """
        複数銘柄・Relative Modeのバックテストを実行

        Args:
            allocation_pct: 配分率上書き（Noneの場合は均等配分）

        Returns:
            Tuple[vbt.Portfolio, Optional[pd.DataFrame]]:
                - ポートフォリオオブジェクト
                - エントリーシグナルDataFrame（統合ポートフォリオの場合のみ、個別ポートフォリオの場合はNone）
        """
        if allocation_pct is None:
            # 新規の第1段階実行時は以前のキャッシュを無効化
            self._clear_grouped_portfolio_inputs_cache()

        # パラメータ設定
        use_group_by = self.group_by

        # データ読み込み（Relative Modeかどうかで分岐）
        multi_data_dict = None
        relative_data_dict = None
        execution_data_dict = None

        # Relative Mode判定とロギング
        mode_info = []
        if self.relative_mode:
            mode_info.append("Relative Mode")
        mode_str = " + ".join(mode_info) if mode_info else "Standard"

        self._log(f"{self.__class__.__name__} {mode_str} 実行開始", "info")
        self._log(
            f"銘柄数: {len(self.stock_codes)}, Group By: {use_group_by}",
            "debug",
        )

        # セクターデータ（シグナル用・一度だけロード）
        sector_data = None
        stock_sector_mapping = None

        if self._should_load_sector_data():
            self._log(
                "セクターデータ依存シグナル有効 - セクターデータロード開始",
                "info",
            )
            try:
                from src.data.loaders.sector_loaders import (
                    get_stock_sector_mapping,
                    load_all_sector_indices,
                )

                sector_data = load_all_sector_indices(
                    self.dataset, self.start_date, self.end_date
                )
                stock_sector_mapping = get_stock_sector_mapping(self.dataset)

                if sector_data:
                    self._log(
                        f"✅ セクターデータロード完了: {len(sector_data)}セクター",
                        "info",
                    )
                else:
                    self._log(
                        "⚠️  セクターデータが空 - セクターシグナルがスキップされます",
                        "warning",
                    )
            except Exception as e:
                self._log(
                    f"⚠️  セクターデータロード失敗: {e} - セクターシグナルがスキップされます",
                    "warning",
                )

        # ベンチマークデータ依存シグナルが有効な場合はベンチマークデータをロード
        if self._should_load_benchmark():
            self._log(
                "ベンチマークデータ依存シグナル有効 - ベンチマークデータロード開始",
                "info",
            )
            try:
                self.load_benchmark_data()
                if self.benchmark_data is not None and not self.benchmark_data.empty:
                    self._log(
                        f"✅ ベンチマークデータロード完了: {len(self.benchmark_data)}レコード",
                        "info",
                    )
                else:
                    self._log(
                        "⚠️  ベンチマークデータが空またはNone - 依存シグナルがスキップされます",
                        "warning",
                    )
            except Exception as e:
                self._log(
                    f"⚠️  ベンチマークデータロード失敗: {e} - 依存シグナルがスキップされます",
                    "warning",
                )

        # データ読み込み
        if self.relative_mode:
            # Relative Mode: 相対価格データ（シグナル用）と実際価格データ（実行用）を分離
            relative_data_dict, execution_data_dict = self.load_relative_data()
            self._log(
                "Relative Mode - シグナル用相対データと実行用実データを準備完了", "info"
            )
        else:
            # Standard Mode: 通常のマルチアセットデータ
            multi_data_dict = self.load_multi_data()

        # 🔧 データ同期: ロード成功した銘柄のみに絞り込み
        if self.relative_mode and execution_data_dict is not None:
            loaded_codes = set(execution_data_dict.keys())
        elif multi_data_dict is not None:
            loaded_codes = set(multi_data_dict.keys())
        else:
            loaded_codes = set()

        requested_codes = set(self.stock_codes)
        missing_codes = requested_codes - loaded_codes

        if missing_codes:
            self._log(
                f"⚠️ {len(missing_codes)}銘柄のデータが見つかりません: {sorted(missing_codes)[:10]}{'...' if len(missing_codes) > 10 else ''}",
                "warning",
            )
            # stock_codesを実際にロードできた銘柄に更新
            self.stock_codes = [code for code in self.stock_codes if code in loaded_codes]

        if not self.stock_codes:
            raise ValueError("有効な銘柄データがありません。データセットを確認してください。")

        # 各銘柄のデータとシグナルを統合
        data_dict = {}
        entries_dict = {}
        exits_dict = {}

        for stock_code in self.stock_codes:
            # セクターシグナル用: 当該銘柄のセクター名を取得
            stock_sector_name = None
            if stock_sector_mapping and stock_code in stock_sector_mapping:
                stock_sector_name = stock_sector_mapping[stock_code]

            if (
                self.relative_mode
                and relative_data_dict is not None
                and execution_data_dict is not None
            ):
                # Relative Mode: 相対価格データでシグナル生成、実際の価格データでポートフォリオ実行
                signal_data = cast(
                    pd.DataFrame,
                    relative_data_dict[stock_code]["daily"],  # type: ignore[index]
                )  # シグナル用相対データ
                execution_data = cast(
                    pd.DataFrame,
                    execution_data_dict[stock_code]["daily"],  # type: ignore[index]
                )  # 実行用実データ

                # margin_dataを取得（利用可能な場合）
                margin_data = None
                if (
                    self.include_margin_data
                    and "margin_daily" in execution_data_dict[stock_code]  # type: ignore[index]
                ):
                    margin_data = cast(
                        pd.DataFrame,
                        execution_data_dict[stock_code]["margin_daily"],  # type: ignore[index]
                    )

                # statements_dataの取得
                statements_data = None
                if (
                    self.include_statements_data
                    and "statements_daily" in execution_data_dict[stock_code]  # type: ignore[index]
                ):
                    statements_data = cast(
                        pd.DataFrame,
                        execution_data_dict[stock_code]["statements_daily"],  # type: ignore[index]
                    )

                # 相対価格データでシグナル生成（実価格データも渡す）
                signals = self.generate_multi_signals(
                    stock_code,
                    signal_data,
                    margin_data=margin_data,
                    statements_data=statements_data,
                    execution_data=execution_data,  # 実価格データを渡す（β値・売買代金シグナル用）
                    sector_data=sector_data,
                    stock_sector_name=stock_sector_name,
                )
                entries, exits = signals.entries, signals.exits

                # 実際の価格データをポートフォリオ実行用に設定
                stock_data = execution_data

                self._log(
                    f"{stock_code} (Relative): 相対データでシグナル生成, 実データで実行（β値・売買代金は実価格使用）",
                    "debug",
                )

            elif multi_data_dict is not None:
                # 通常のシングルTF処理
                stock_data = multi_data_dict[stock_code]["daily"]

                # margin_dataを取得（利用可能な場合）
                margin_data = None
                if (
                    self.include_margin_data
                    and "margin_daily" in multi_data_dict[stock_code]
                ):
                    margin_data = multi_data_dict[stock_code]["margin_daily"]

                # statements_dataの取得
                statements_data = None
                if (
                    self.include_statements_data
                    and "statements_daily" in multi_data_dict[stock_code]
                ):
                    statements_data = multi_data_dict[stock_code]["statements_daily"]

                signals = self.generate_multi_signals(
                    stock_code,
                    stock_data,
                    margin_data=margin_data,
                    statements_data=statements_data,
                    sector_data=sector_data,
                    stock_sector_name=stock_sector_name,
                )
                entries, exits = signals.entries, signals.exits
            else:
                raise ValueError("Data loading failed - no valid data source available")

            data_dict[stock_code] = stock_data
            entries_dict[stock_code] = entries
            exits_dict[stock_code] = exits

            # 🔍 DEBUG: 各銘柄のシグナル生成状況を詳細出力
            entries_count = entries.sum()
            exits_count = exits.sum()
            data_length = len(stock_data)

            self._log(
                f"{stock_code}: 買い{entries_count}件, 売り{exits_count}件 (データ{data_length}日分)",
                "info",
            )

            # さらに詳細なデバッグ情報
            if entries_count == 0:
                self._log(f"⚠️  {stock_code}: 買いシグナルが1件もありません", "warning")
            elif entries_count < 5:
                self._log(
                    f"📊 {stock_code}: 買いシグナルが少数({entries_count}件)です",
                    "info",
                )

        if use_group_by:
            # 統合ポートフォリオの場合
            # VectorBTネイティブ統合ポートフォリオ作成
            try:
                # データ統合（VectorBT対応のため終値のみ使用）
                close_data = pd.DataFrame(
                    {
                        stock_code: data["Close"]
                        for stock_code, data in data_dict.items()
                    }
                )

                # エントリー・エグジットシグナル統合
                # pandas 2.2.0+ FutureWarning回避のため、オプション設定を使用
                with pd.option_context("future.no_silent_downcasting", True):
                    all_entries = (
                        pd.DataFrame(entries_dict)
                        .fillna(False)
                        .infer_objects(copy=False)
                        .astype(bool)
                    )
                    all_exits = (
                        pd.DataFrame(exits_dict)
                        .fillna(False)
                        .infer_objects(copy=False)
                        .astype(bool)
                    )

                # データ型確認とクリーニング
                close_data = close_data.astype(float)

                # 🔍 DEBUG: 統合後のシグナル統計
                total_entries = all_entries.sum().sum()
                total_exits = all_exits.sum().sum()

                self._log(
                    f"データ統合完了 - Close: {close_data.shape}, Entries: {all_entries.shape}, Exits: {all_exits.shape}",
                    "debug",
                )
                self._log(
                    f"🚀 統合シグナル統計 - 全買いシグナル: {total_entries}件, 全売りシグナル: {total_exits}件",
                    "info",
                )

                # 各銘柄ごとのシグナル数もチェック
                entries_per_stock = all_entries.sum()
                active_stocks = (entries_per_stock > 0).sum()
                self._log(
                    f"📈 アクティブ銘柄数: {active_stocks}/{len(self.stock_codes)}銘柄",
                    "info",
                )

                # 同時保有ポジション数の上限（簡易: 日次のエントリー数を制限）
                if self.max_concurrent_positions:
                    all_entries = self._limit_entries_per_day(
                        all_entries, self.max_concurrent_positions
                    )

                self._set_grouped_portfolio_inputs_cache(
                    close_data=close_data,
                    all_entries=all_entries,
                    all_exits=all_exits,
                )

                portfolio = self._create_grouped_portfolio(
                    close_data=close_data,
                    all_entries=all_entries,
                    all_exits=all_exits,
                    allocation_pct=allocation_pct,
                )

                self.combined_portfolio = portfolio
                self._log("統合ポートフォリオ作成完了", "info")

                # ポートフォリオとエントリーシグナルDataFrameを返却
                return portfolio, all_entries
            except Exception as e:
                self._log(f"ポートフォリオ作成エラー: {e}", "error")
                raise RuntimeError(f"Failed to create portfolio: {e}")
        else:
            self._clear_grouped_portfolio_inputs_cache()
            # 個別ポートフォリオの場合（ピラミッディングは未実装）
            pyramid_enabled = False
            portfolio = self._create_individual_portfolios(
                data_dict, entries_dict, exits_dict, pyramid_enabled
            )
            # 個別ポートフォリオの場合はall_entriesはNone
            return portfolio, None

    def _create_individual_portfolios(
        self,
        data_dict: Dict[str, pd.DataFrame],
        entries_dict: Dict[str, pd.Series],
        exits_dict: Dict[str, pd.Series],
        pyramid_enabled: bool = False,
    ) -> vbt.Portfolio:
        """
        個別ポートフォリオを作成

        Args:
            data_dict: 銘柄別価格データ
            entries_dict: 銘柄別エントリシグナル
            exits_dict: 銘柄別エグジットシグナル

        Returns:
            vbt.Portfolio: 個別ポートフォリオ
        """
        close_data = pd.DataFrame({k: v["Close"] for k, v in data_dict.items()})
        entries_data = pd.DataFrame(entries_dict)
        exits_data = pd.DataFrame(exits_dict)

        # DataFrameの型を適切に設定
        entries_data = entries_data.fillna(False).infer_objects(copy=False).astype(bool)
        exits_data = exits_data.fillna(False).infer_objects(copy=False).astype(bool)

        if self.max_concurrent_positions:
            entries_data = self._limit_entries_per_day(
                entries_data, self.max_concurrent_positions
            )

        effective_fees, effective_slippage = self._calculate_cost_params()

        portfolio_kwargs = dict(
            close=close_data,
            entries=entries_data,
            exits=exits_data,
            direction=getattr(self, "direction", "longonly"),  # 🆕 追加: 取引方向設定
            init_cash=self.initial_cash,
            fees=effective_fees,
            slippage=effective_slippage,  # 約定価格シフト（ネイティブ対応）
            group_by=None,  # 個別ポートフォリオ
            accumulate=pyramid_enabled,  # 🆕 追加: ピラミッディング対応
            freq="D",
        )
        if self.max_exposure is not None:
            portfolio_kwargs["max_size"] = self.max_exposure

        portfolio = vbt.Portfolio.from_signals(**portfolio_kwargs)  # type: ignore[arg-type]

        self.portfolio = portfolio
        self._log("個別ポートフォリオ作成完了", "info")
        return portfolio

    @staticmethod
    def _limit_entries_per_day(
        entries: pd.DataFrame, max_positions: int
    ) -> pd.DataFrame:
        """日次のエントリー数を上限で制限（簡易版）"""
        if max_positions <= 0:
            return entries

        limited = entries.copy()
        for idx, row in entries.iterrows():
            if row.sum() <= max_positions:
                continue
            true_cols = row[row].index.tolist()
            drop_cols = true_cols[max_positions:]
            if drop_cols:
                limited.loc[idx, drop_cols] = False  # type: ignore[index]
        return limited

    def run_optimized_backtest(
        self, group_by: Optional[bool] = None
    ) -> Tuple[vbt.Portfolio, vbt.Portfolio, AllocationInfo]:
        """
        2段階Kelly基準最適化バックテストを実行

        Args:
            group_by: 統合ポートフォリオとして扱うか（Noneの場合はself.group_byを使用）

        Returns:
            Tuple[vbt.Portfolio, vbt.Portfolio, AllocationInfo]:
                - 第1段階結果
                - 第2段階最適化結果
                - Kelly基準統計情報
        """
        self._log("🎯 Kelly基準2段階最適化バックテスト開始", "info")

        try:
            # Kelly基準による2段階最適化
            (
                initial_portfolio,
                final_portfolio,
                optimized_allocation,
                stats,
                all_entries,
            ) = self.run_optimized_backtest_kelly(
                kelly_fraction=self.kelly_fraction,
                min_allocation=self.min_allocation,
                max_allocation=self.max_allocation,
            )

            # Kelly基準の詳細統計情報を返す
            allocation_info = AllocationInfo(
                method="kelly",
                allocation=optimized_allocation,
                win_rate=stats.get("win_rate", 0.0),
                avg_win=stats.get("avg_win", 0.0),
                avg_loss=stats.get("avg_loss", 0.0),
                total_trades=stats.get("total_trades", 0),
                full_kelly=stats.get("kelly", 0.0),
                kelly_fraction=self.kelly_fraction,
            )

            # all_entriesをインスタンス変数として保存（StrategyFactoryでアクセス可能にする）
            self.all_entries = all_entries

            return initial_portfolio, final_portfolio, allocation_info

        except Exception as e:
            self._log(f"Kelly基準2段階最適化バックテストエラー: {e}", "error")
            raise RuntimeError(f"Kelly基準2段階最適化バックテスト実行失敗: {e}")
