"""
統合シグナルプロセッサ

エントリー・エグジットシグナル処理を統一管理するSignalProcessorクラス
UnifiedFilterProcessor + UnifiedTriggerProcessor の統合版
"""

from typing import Callable, Literal, Optional

import pandas as pd
from loguru import logger

from src.models.signals import SignalParams, Signals

# データ駆動設計: シグナルレジストリからの動的処理
from .registry import SIGNAL_REGISTRY, SignalDefinition


class SignalProcessor:
    """
    統合シグナル処理クラス

    エントリー・エグジットシグナル処理を統一管理し、
    旧UnifiedFilterProcessor + UnifiedTriggerProcessorの機能を統合

    責任分離:
    - SignalProcessor: エントリー・エグジットシグナル統合処理
    - base_strategy: Signals オブジェクト生成・戦略全体制御
    """

    def __init__(self):
        """
        統合シグナルプロセッサーの初期化

        外部依存なしの純粋な関数型シグナルプロセッサー
        """
        pass

    def apply_entry_signals(
        self,
        base_signal: pd.Series,
        ohlc_data: pd.DataFrame,
        signal_params: SignalParams,
        margin_data: Optional[pd.DataFrame] = None,
        statements_data: Optional[pd.DataFrame] = None,
        benchmark_data: Optional[pd.DataFrame] = None,
        execution_data: Optional[pd.DataFrame] = None,
        load_benchmark_data: Optional[Callable[[], pd.DataFrame]] = None,
        relative_mode: bool = False,
        sector_data: Optional[dict] = None,
        stock_sector_name: Optional[str] = None,
    ) -> pd.Series:
        """
        エントリーシグナル適用（統一シグナル処理システム使用）

        Args:
            base_signal: 基本シグナル（戦略固有のエントリー条件）
            ohlc_data: OHLCV データ（相対価格モードでは相対価格データ）
            signal_params: 統合シグナルパラメータ
            margin_data: 信用残高データ（オプション）
            statements_data: 財務諸表データ（オプション）
            benchmark_data: ベンチマークデータ（オプション）
            execution_data: 実行用OHLCVデータ（相対価格モードでの実価格データ、通常モードではNone）
            load_benchmark_data: ベンチマークデータローダー関数（オプション）

        Returns:
            pd.Series: シグナル適用後のエントリーシグナル（boolean）
        """
        # 統一シグナル処理システムを使用（AND条件）
        return self.apply_signals(
            base_signal=base_signal,
            signal_type="entry",
            ohlc_data=ohlc_data,
            signal_params=signal_params,
            margin_data=margin_data,
            statements_data=statements_data,
            benchmark_data=benchmark_data,
            execution_data=execution_data,
            relative_mode=relative_mode,
            sector_data=sector_data,
            stock_sector_name=stock_sector_name,
        )

    def apply_exit_signals(
        self,
        base_exits: pd.Series,
        data: pd.DataFrame,
        signal_params: SignalParams,
        execution_data: Optional[pd.DataFrame] = None,
        relative_mode: bool = False,
        sector_data: Optional[dict] = None,
        stock_sector_name: Optional[str] = None,
        **optional_data,
    ) -> pd.Series:
        """
        エグジットシグナル適用（統一シグナル処理システム使用）

        Args:
            base_exits: 基本exitシグナル（戦略固有のexit条件）
            data: OHLCV データ（相対価格モードでは相対価格データ）
            signal_params: 統合シグナルパラメータ
            execution_data: 実行用OHLCVデータ（相対価格モードでの実価格データ、通常モードではNone）
            **optional_data: その他のデータ（margin_data, statements_data, benchmark_data等）

        Returns:
            pd.Series[bool]: シグナル適用後のexitシグナル（boolean）
        """
        # 統一シグナル処理システムを使用（OR条件）
        return self.apply_signals(
            base_signal=base_exits,
            signal_type="exit",
            ohlc_data=data,
            signal_params=signal_params,
            execution_data=execution_data,
            relative_mode=relative_mode,
            sector_data=sector_data,
            stock_sector_name=stock_sector_name,
            **optional_data,  # オプショナルデータを渡す
        )

    def generate_signals(
        self,
        strategy_entries: pd.Series,
        strategy_exits: pd.Series,
        ohlc_data: pd.DataFrame,
        entry_signal_params: SignalParams,
        exit_signal_params: SignalParams,
        execution_data: Optional[pd.DataFrame] = None,
        relative_mode: bool = False,
        sector_data: Optional[dict] = None,
        stock_sector_name: Optional[str] = None,
        **optional_data,
    ) -> Signals:
        """
        統合シグナル生成（エントリー + エグジット分離パラメータ）

        Entry/Exit で異なるパラメータを使用する正しい設計:
        - entry_signal_params: 厳格な絞り込み条件
        - exit_signal_params: 柔軟な発火条件

        Args:
            strategy_entries: 戦略固有エントリーシグナル
            strategy_exits: 戦略固有エグジットシグナル
            ohlc_data: OHLCV データ（相対価格モードでは相対価格データ）
            entry_signal_params: エントリー専用シグナルパラメータ
            exit_signal_params: エグジット専用シグナルパラメータ
            execution_data: 実行用OHLCVデータ（相対価格モードでの実価格データ、通常モードではNone）
            **optional_data: その他のデータ（margin_data, statements_data, benchmark_data等）

        Returns:
            Signals: 統合後のエントリー・エグジットシグナル
        """
        # エントリーシグナル適用（AND条件・厳格な絞り込み）
        filtered_entries = self.apply_entry_signals(
            base_signal=strategy_entries,
            ohlc_data=ohlc_data,
            signal_params=entry_signal_params,
            execution_data=execution_data,
            relative_mode=relative_mode,
            sector_data=sector_data,
            stock_sector_name=stock_sector_name,
            **optional_data,
        )

        # エグジットシグナル適用（OR条件・柔軟な発火）
        expanded_exits = self.apply_exit_signals(
            base_exits=strategy_exits,
            data=ohlc_data,
            signal_params=exit_signal_params,
            execution_data=execution_data,
            relative_mode=relative_mode,
            sector_data=sector_data,
            stock_sector_name=stock_sector_name,
            **optional_data,  # オプショナルデータ（benchmark_data等）を渡す
        )

        result = Signals(entries=filtered_entries, exits=expanded_exits)

        # 統合シグナル生成結果をログ出力
        logger.info(
            f"統合シグナル生成完了: "
            f"エントリー {result.entries.sum()}/{len(result.entries)}, "
            f"エグジット {result.exits.sum()}/{len(result.exits)}"
        )

        return result

    # ===== ヘルパーメソッド =====

    def _log_signal_start(self, signal_name: str, enabled: bool):
        """シグナル開始ログ"""
        logger.debug(f"{signal_name}: {'有効' if enabled else '無効'}")

    def _log_signal_effect(
        self, signal_name: str, base_signal: pd.Series, applied_signal: pd.Series
    ):
        """シグナル効果ログ"""
        before_count = base_signal.sum()
        combined = base_signal & applied_signal
        after_count = combined.sum()

        logger.debug(
            f"{signal_name}: {before_count} → {after_count} "
            f"({after_count / before_count * 100:.1f}% 残存)"
            if before_count > 0
            else f"{signal_name}: 0 → 0 (効果なし)"
        )

    # ===== レガシーヘルパーメソッド削除 =====
    # 旧: _apply_fundamental_signals, _apply_beta_signals, _apply_margin_signals, _apply_price_action_signals
    # 新: 統一ヘルパーメソッド _apply_*_signals_unified を使用

    # ===== 統一シグナル処理システム =====

    def apply_signals(
        self,
        base_signal: pd.Series,
        signal_type: Literal["entry", "exit"],
        ohlc_data: pd.DataFrame,
        signal_params: SignalParams,
        margin_data: Optional[pd.DataFrame] = None,
        statements_data: Optional[pd.DataFrame] = None,
        benchmark_data: Optional[pd.DataFrame] = None,
        execution_data: Optional[pd.DataFrame] = None,
        relative_mode: bool = False,
        sector_data: Optional[dict] = None,
        stock_sector_name: Optional[str] = None,
    ) -> pd.Series:
        """
        統一シグナル処理システム

        Entry/Exit で同じシグナル種類を使用し、結合方法のみを変える統一設計:
        - Entry: AND条件で絞り込み (厳格な条件)
        - Exit: OR条件で発火 (柔軟な条件)

        Args:
            base_signal: 基本シグナル（戦略固有の条件）
            signal_type: "entry" (AND結合) または "exit" (OR結合)
            ohlc_data: OHLCV データ（相対価格モードでは相対価格データ）
            signal_params: 統合シグナルパラメータ
            margin_data: 信用残高データ（オプション）
            statements_data: 財務諸表データ（オプション）
            benchmark_data: ベンチマークデータ（オプション）
            execution_data: 実行用OHLCVデータ（相対価格モードでの実価格データ、通常モードではNone）

        Returns:
            pd.Series: シグナル適用後のboolean Series
        """
        # 基本的な入力データチェック
        if ohlc_data is None or ohlc_data.empty:
            raise ValueError("OHLCデータが提供されていません")

        required_columns = ["Close", "Volume"]
        missing_columns = [
            col for col in required_columns if col not in ohlc_data.columns
        ]
        if missing_columns:
            raise ValueError(f"必須カラムが不足しています: {missing_columns}")

        # 基本データの取得と検証
        # β値・売買代金シグナルには execution_data の Close を使用（相対価格モード対応）
        close = ohlc_data["Close"].astype(float)
        volume = ohlc_data["Volume"].astype(float)

        # execution_data が提供されている場合は、β値・売買代金用の実価格を取得
        execution_close = (
            execution_data["Close"].astype(float)
            if execution_data is not None
            else close
        )

        # データ品質チェック: 全てNaNまたは空でないことを確認
        if not close.notna().any():
            raise ValueError(
                "Close価格データが全てNaNです。データの品質を確認してください。"
            )
        if not volume.notna().any():
            logger.warning(
                "Volumeデータが全てNaNです。出来高シグナルが正しく機能しない可能性があります。"
            )

        # シグナル条件のリスト（基本シグナルから開始）
        signal_conditions = [base_signal]

        logger.debug(f"{signal_type.capitalize()} signal処理開始")

        # 統一シグナル適用（Entry/Exit両対応）
        self._apply_signal_set(
            signal_conditions=signal_conditions,
            signal_type=signal_type,
            signal_params=signal_params,
            base_signal=base_signal,
            close=close,
            volume=volume,
            ohlc_data=ohlc_data,
            margin_data=margin_data,
            statements_data=statements_data,
            benchmark_data=benchmark_data,
            execution_close=execution_close,
            relative_mode=relative_mode,
            sector_data=sector_data,
            stock_sector_name=stock_sector_name,
        )

        # Entry: AND結合 / Exit: OR結合
        # NaN処理: シグナルがデータ不足（初期lookback期間等）でNaNの場合の安全な処理
        # - Entry (AND): NaNはFalse扱い（判定不能な期間は取引を許可しない = 安全側）
        # - Exit (OR): NaNはFalse扱い（データ不足は発火条件にならない）
        final_signal = signal_conditions[0]
        if signal_type == "entry":
            # AND条件: 全ての条件を満たす場合のみエントリー
            for condition in signal_conditions[1:]:
                # NaNをFalse扱い（判定不能な期間は取引を許可しない = 安全側）
                # pandas 2.2+: fillna downcasting警告回避のため infer_objects 使用
                filled = condition.fillna(False).infer_objects(copy=False)
                final_signal = final_signal & filled
        else:  # signal_type == "exit"
            # OR条件: いずれかの条件を満たせばエグジット
            for condition in signal_conditions[1:]:
                # NaNをFalse扱い（データ不足の期間は発火しない）
                # pandas 2.2+: fillna downcasting警告回避のため infer_objects 使用
                filled = condition.fillna(False).infer_objects(copy=False)
                final_signal = final_signal | filled

        # シグナル統計ログ
        final_count = final_signal.sum()
        total_count = len(final_signal)
        base_count = base_signal.sum()

        logger.debug(
            f"{signal_type.capitalize()} signal処理完了: "
            f"基本シグナル {base_count} → 最終シグナル {final_count}/{total_count} "
            f"({final_count / total_count * 100:.1f}% total, "
            f"{final_count / base_count * 100:.1f}% of base)"
            if base_count > 0
            else f"{signal_type.capitalize()} signal処理完了: {final_count}/{total_count} signals"
        )
        return final_signal.fillna(False)

    def _apply_signal_set(
        self,
        signal_conditions: list,
        signal_type: Literal["entry", "exit"],
        signal_params: SignalParams,
        base_signal: pd.Series,
        close: pd.Series,
        volume: pd.Series,
        ohlc_data: pd.DataFrame,
        margin_data: Optional[pd.DataFrame] = None,
        statements_data: Optional[pd.DataFrame] = None,
        benchmark_data: Optional[pd.DataFrame] = None,
        execution_close: Optional[pd.Series] = None,
        relative_mode: bool = False,
        sector_data: Optional[dict] = None,
        stock_sector_name: Optional[str] = None,
    ):
        """
        データ駆動型シグナル適用処理（統一レジストリベース）

        全シグナル種類をレジストリから動的に処理し、冗長コードを削減
        """
        # 相対価格モードは呼び出し元で明示的に指定
        # β値・売買代金シグナルは実価格が必要なため、相対価格モードではスキップが必要
        is_relative_mode = relative_mode

        # データソース辞書の構築
        data_sources = {
            "close": close,
            "volume": volume,
            "ohlc_data": ohlc_data,
            "margin_data": margin_data,
            "statements_data": statements_data,
            "benchmark_data": benchmark_data,
            "execution_close": execution_close if execution_close is not None else close,
            "is_relative_mode": is_relative_mode,  # 相対価格モードフラグ
            "sector_data": sector_data,  # セクターインデックスOHLCデータ
            "stock_sector_name": stock_sector_name,  # 当該銘柄のセクター名
        }

        # 統一シグナル処理（SIGNAL_REGISTRY）
        for signal_def in SIGNAL_REGISTRY:
            self._apply_unified_signal(
                signal_def=signal_def,
                signal_conditions=signal_conditions,
                signal_type=signal_type,
                signal_params=signal_params,
                base_signal=base_signal,
                data_sources=data_sources,
            )

    # 相対価格モードで使用不可のシグナル（実価格が必須）
    _REQUIRES_EXECUTION_DATA = {"β値", "売買代金", "売買代金範囲"}

    @staticmethod
    def _has_non_empty_dataframe(value: object) -> bool:
        return isinstance(value, pd.DataFrame) and (not value.empty)

    def _is_requirement_satisfied(self, requirement: str, data_sources: dict) -> bool:
        base, _, detail = requirement.partition(":")

        if base == "benchmark":
            benchmark = data_sources.get("benchmark_data")
            if not isinstance(benchmark, pd.DataFrame) or benchmark.empty:
                return False
            if "Close" not in benchmark.columns:
                return False
            return bool(benchmark["Close"].notna().any())

        if base == "statements":
            statements = data_sources.get("statements_data")
            if not isinstance(statements, pd.DataFrame) or statements.empty:
                return False
            if not detail:
                return True
            if detail not in statements.columns:
                return False
            return bool(statements[detail].notna().any())

        if base == "margin":
            margin = data_sources.get("margin_data")
            return self._has_non_empty_dataframe(margin)

        if base == "sector":
            sector_data = data_sources.get("sector_data")
            stock_sector_name = data_sources.get("stock_sector_name")
            if not isinstance(sector_data, dict) or not stock_sector_name:
                return False
            sector_df = sector_data.get(stock_sector_name)
            return self._has_non_empty_dataframe(sector_df)

        if base == "ohlc":
            ohlc_data = data_sources.get("ohlc_data")
            return (
                self._has_non_empty_dataframe(ohlc_data)
                and {"Open", "High", "Low", "Close"}.issubset(ohlc_data.columns)
            )

        if base == "volume":
            volume = data_sources.get("volume")
            return isinstance(volume, pd.Series) and bool(volume.notna().any())

        return True

    def _describe_missing_requirements(
        self,
        signal_def: SignalDefinition,
        data_sources: dict,
    ) -> str:
        if not signal_def.data_requirements:
            return "data checker returned False"

        missing = [
            requirement
            for requirement in signal_def.data_requirements
            if not self._is_requirement_satisfied(requirement, data_sources)
        ]
        if missing:
            return ", ".join(missing)
        return "data checker returned False"

    def _apply_unified_signal(
        self,
        signal_def: SignalDefinition,
        signal_conditions: list,
        signal_type: Literal["entry", "exit"],
        signal_params: SignalParams,
        base_signal: pd.Series,
        data_sources: dict,
    ):
        """統一シグナル適用（Entry/Exit両用）"""
        try:
            # 1. 有効性チェック
            if not signal_def.enabled_checker(signal_params):
                return

            # 1.5. 相対価格モードチェック（β値・売買代金は実価格が必須）
            if data_sources.get("is_relative_mode", False):
                if signal_def.name in self._REQUIRES_EXECUTION_DATA:
                    logger.warning(
                        f"⚠️  {signal_def.name}シグナル: 相対価格モードでは使用不可、スキップ "
                        "(execution_dataが必要)"
                    )
                    return

            # 1.6. Exit無効シグナルチェック（Buy&Hold等はExit用途では使用不可）
            if signal_type == "exit" and getattr(signal_def, "exit_disabled", False):
                logger.warning(
                    f"⚠️  {signal_def.name}シグナル: Exit用途では使用不可、スキップ"
                )
                return

            # 2. 必須データチェック
            if signal_def.data_checker and not signal_def.data_checker(data_sources):
                # データ不足でスキップ - デバッグログ出力
                missing = self._describe_missing_requirements(signal_def, data_sources)
                logger.warning(
                    f"⚠️  {signal_def.name}シグナル: 必須データ不足によりスキップ "
                    f"(不足: {missing})"
                )
                return

            # 3. 目的設定
            purpose = (
                signal_def.entry_purpose
                if signal_type == "entry"
                else signal_def.exit_purpose
            )
            self._log_signal_start(f"{signal_def.name}シグナル({purpose})", True)

            # 4. パラメータ構築
            params = signal_def.param_builder(signal_params, data_sources)

            # 5. シグナル計算
            result = signal_def.signal_func(**params)

            # 6. インデックス統一（重要！）
            # ベンチマークデータ等を使用するシグナルは異なるインデックスを持つ可能性があるため、
            # すべてのシグナル結果を base_signal のインデックスに統一する
            if not result.index.equals(base_signal.index):
                # 日付の共通部分のみを使用（交差）
                common_dates = base_signal.index.intersection(result.index)
                if len(common_dates) < len(base_signal.index):
                    missing_count = len(base_signal.index) - len(common_dates)
                    logger.warning(
                        f"{signal_def.name}シグナル: 日付不一致 "
                        f"({len(result)}件ベンチマーク vs {len(base_signal)}件株価, "
                        f"{missing_count}件欠損)"
                    )

                # 共通日付のシグナルのみ使用、それ以外はNaN
                # NaNはAND/OR結合時に安全に処理される:
                # - Entry (AND): NaNはFalse扱い（判定不能期間は取引禁止 = 安全側）
                # - Exit (OR): NaNはFalse扱い（発火しない）
                result = result.reindex(base_signal.index)  # fill_value省略でNaN

            # 7. ロギング＋条件追加
            self._log_signal_effect(
                f"{signal_def.name}({purpose})", base_signal, result
            )
            signal_conditions.append(result)

        except KeyError as e:
            # データキーエラー - 設定ミスの可能性が高いため早期終了
            logger.error(
                f"❌ {signal_def.name}シグナル: データキーエラー - {e}。"
                "シグナル設定またはデータソースを確認してください。"
            )
            raise
        except (ValueError, TypeError) as e:
            # 値エラー・型エラー - データ品質問題の可能性、スキップして続行
            logger.warning(
                f"⚠️  {signal_def.name}シグナル: {type(e).__name__} - {e}、スキップ"
            )
        except Exception as e:
            # その他の予期しないエラー - スキップして続行
            logger.warning(
                f"⚠️  {signal_def.name}シグナル: 予期しないエラー - {e}、スキップ"
            )
