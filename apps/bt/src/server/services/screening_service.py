"""
Screening Service

production戦略YAML駆動の動的スクリーニングサービス。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd
from loguru import logger

from src.data.access.mode import data_access_mode_context
from src.data.loaders import (
    get_stock_sector_mapping,
    load_all_sector_indices,
    load_topix_data,
    prepare_multi_data,
)
from src.lib.market_db.market_reader import MarketDbReader
from src.lib.market_db.query_helpers import normalize_stock_code
from src.lib.strategy_runtime.loader import ConfigLoader
from src.models.config import SharedConfig
from src.models.signals import SignalParams, Signals
from src.paths import get_backtest_results_dir
from src.server.schemas.screening import (
    BacktestMetric,
    MarketScreeningResponse,
    MatchedStrategyItem,
    ScreeningResultItem,
    ScreeningSortBy,
    ScreeningSummary,
    SortOrder,
)
from src.server.services.market_code_alias import resolve_market_codes
from src.strategies.signals.processor import SignalProcessor
from src.strategies.signals.registry import SIGNAL_REGISTRY


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _format_date(value: Any) -> str:
    """Datetime/文字列をYYYY-MM-DDへ正規化する。"""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    text = str(value)
    return text.split("T", 1)[0]


@dataclass(frozen=True)
class StockUniverseItem:
    code: str
    company_name: str
    scale_category: str | None
    sector_33_name: str | None


@dataclass(frozen=True)
class StrategyRuntime:
    name: str
    response_name: str
    basename: str
    entry_params: SignalParams
    exit_params: SignalParams
    shared_config: SharedConfig


class ScreeningService:
    """戦略YAML駆動のスクリーニングサービス"""

    _WARNING_LIMIT = 50

    def __init__(self, reader: MarketDbReader) -> None:
        self._reader = reader
        self._config_loader = ConfigLoader()
        self._signal_processor = SignalProcessor()

    def run_screening(
        self,
        markets: str = "prime",
        strategies: str | None = None,
        recent_days: int = 10,
        reference_date: str | None = None,
        backtest_metric: BacktestMetric = "sharpe_ratio",
        sort_by: ScreeningSortBy = "bestStrategyScore",
        order: SortOrder = "desc",
        limit: int | None = None,
    ) -> MarketScreeningResponse:
        """スクリーニングを実行"""
        requested_market_codes, query_market_codes = resolve_market_codes(markets)

        stock_universe = self._load_stock_universe(query_market_codes)
        strategy_runtimes = self._resolve_strategies(strategies)
        strategy_scores, missing_metric_strategies, metric_warnings = self._load_strategy_scores(
            strategy_runtimes,
            backtest_metric,
        )

        warnings: list[str] = list(metric_warnings)
        by_strategy = {s.response_name: 0 for s in strategy_runtimes}
        processed_codes: set[str] = set()

        aggregated: dict[str, dict[str, Any]] = {}

        for strategy in strategy_runtimes:
            try:
                matched_rows, processed, strategy_warnings = self._evaluate_strategy(
                    strategy,
                    stock_universe,
                    recent_days,
                    reference_date,
                )
            except Exception as exc:
                logger.exception(f"Strategy screening failed: {strategy.name}")
                warnings.append(f"{strategy.response_name}: evaluation failed ({exc})")
                continue

            processed_codes |= processed
            warnings.extend(strategy_warnings)

            strategy_score = strategy_scores.get(strategy.response_name)
            for stock, matched_date in matched_rows:
                by_strategy[strategy.response_name] += 1

                existing = aggregated.get(stock.code)
                if existing is None:
                    existing = {
                        "stock": stock,
                        "matchedDate": matched_date,
                        "matchedStrategies": [],
                    }
                    aggregated[stock.code] = existing
                elif matched_date > existing["matchedDate"]:
                    existing["matchedDate"] = matched_date

                existing["matchedStrategies"].append(
                    MatchedStrategyItem(
                        strategyName=strategy.response_name,
                        matchedDate=matched_date,
                        strategyScore=strategy_score,
                    )
                )

        all_results = [self._build_result_item(item) for item in aggregated.values()]
        sorted_results = self._sort_results(all_results, sort_by=sort_by, order=order)

        match_count = len(sorted_results)
        if limit is not None and limit > 0:
            sorted_results = sorted_results[:limit]

        summary = ScreeningSummary(
            totalStocksScreened=len(stock_universe),
            matchCount=match_count,
            skippedCount=max(0, len(stock_universe) - len(processed_codes)),
            byStrategy=by_strategy,
            strategiesEvaluated=[s.response_name for s in strategy_runtimes],
            strategiesWithoutBacktestMetrics=missing_metric_strategies,
            warnings=self._dedupe_warnings(warnings),
        )

        return MarketScreeningResponse(
            results=sorted_results,
            summary=summary,
            markets=requested_market_codes,
            recentDays=recent_days,
            referenceDate=reference_date,
            backtestMetric=backtest_metric,
            sortBy=sort_by,
            order=order,
            lastUpdated=_now_iso(),
        )

    def _load_stock_universe(self, market_codes: list[str]) -> list[StockUniverseItem]:
        """市場フィルタ済み銘柄母集団を読み込む。"""
        if not market_codes:
            return []

        placeholders = ",".join("?" for _ in market_codes)
        rows = self._reader.query(
            f"""
            SELECT code, company_name, scale_category, sector_33_name
            FROM stocks
            WHERE market_code IN ({placeholders})
            ORDER BY code
            """,
            tuple(market_codes),
        )

        deduped: dict[str, StockUniverseItem] = {}
        for row in rows:
            code = normalize_stock_code(str(row["code"]))
            if code in deduped:
                continue
            deduped[code] = StockUniverseItem(
                code=code,
                company_name=row["company_name"],
                scale_category=row["scale_category"],
                sector_33_name=row["sector_33_name"],
            )

        return list(deduped.values())

    def _resolve_strategies(self, strategies: str | None) -> list[StrategyRuntime]:
        """対象戦略をproductionカテゴリから解決する。"""
        metadata = [m for m in self._config_loader.get_strategy_metadata() if m.category == "production"]
        if not metadata:
            raise ValueError("No production strategies found")

        metadata_by_name = {m.name: m for m in metadata}
        basename_map: dict[str, list[str]] = {}
        for m in metadata:
            basename_map.setdefault(m.path.stem, []).append(m.name)

        selected_names: list[str]
        if strategies is None or not strategies.strip():
            selected_names = sorted(metadata_by_name.keys())
        else:
            requested = [s.strip() for s in strategies.split(",") if s.strip()]
            selected_names = []
            invalid: list[str] = []

            for token in requested:
                resolved = self._resolve_strategy_token(token, metadata_by_name, basename_map)
                if resolved is None:
                    invalid.append(token)
                    continue
                if resolved not in selected_names:
                    selected_names.append(resolved)

            if invalid:
                raise ValueError(
                    "Invalid strategies (production only): " + ", ".join(sorted(set(invalid)))
                )

        if not selected_names:
            raise ValueError("No valid production strategies selected")

        selected_metadata = [metadata_by_name[name] for name in selected_names]

        # basename重複時はフルネームをレスポンス名に使用
        basename_counts: dict[str, int] = {}
        for m in selected_metadata:
            basename_counts[m.path.stem] = basename_counts.get(m.path.stem, 0) + 1

        runtimes: list[StrategyRuntime] = []
        for m in selected_metadata:
            config = self._config_loader.load_strategy_config(m.name)
            shared_config_dict = self._config_loader.merge_shared_config(config)

            response_name = m.path.stem
            if basename_counts[response_name] > 1:
                response_name = m.name

            runtimes.append(
                StrategyRuntime(
                    name=m.name,
                    response_name=response_name,
                    basename=m.path.stem,
                    entry_params=SignalParams(**config.get("entry_filter_params", {})),
                    exit_params=SignalParams(**config.get("exit_trigger_params", {})),
                    shared_config=SharedConfig.model_validate(
                        shared_config_dict,
                        context={"resolve_stock_codes": False},
                    ),
                )
            )

        return runtimes

    def _resolve_strategy_token(
        self,
        token: str,
        metadata_by_name: dict[str, Any],
        basename_map: dict[str, list[str]],
    ) -> str | None:
        """クエリ指定戦略名をproduction戦略へ解決する。"""
        if token in metadata_by_name:
            return token

        if token.startswith("production/"):
            return token if token in metadata_by_name else None

        production_prefixed = f"production/{token}"
        if production_prefixed in metadata_by_name:
            return production_prefixed

        candidates = basename_map.get(token, [])
        if len(candidates) == 1:
            return candidates[0]

        return None

    def _load_strategy_scores(
        self,
        strategies: list[StrategyRuntime],
        metric: BacktestMetric,
    ) -> tuple[dict[str, float | None], list[str], list[str]]:
        """各戦略の最新バックテスト指標を取得する。"""
        scores: dict[str, float | None] = {}
        missing: list[str] = []
        warnings: list[str] = []

        for strategy in strategies:
            score, warning = self._load_latest_metric(strategy.basename, metric)
            scores[strategy.response_name] = score
            if score is None:
                missing.append(strategy.response_name)
            if warning:
                warnings.append(f"{strategy.response_name}: {warning}")

        return scores, missing, warnings

    def _load_latest_metric(
        self,
        strategy_basename: str,
        metric: BacktestMetric,
    ) -> tuple[float | None, str | None]:
        """戦略ディレクトリ内の最新*.metrics.jsonから指標を取得する。"""
        strategy_dir = get_backtest_results_dir(strategy_basename)
        if not strategy_dir.exists():
            return None, None

        metric_files = sorted(
            strategy_dir.glob("*.metrics.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not metric_files:
            return None, None

        latest = metric_files[0]
        try:
            payload = json.loads(latest.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return None, f"failed to read metrics ({latest.name}: {exc})"

        value = payload.get(metric)
        if value is None:
            return None, None

        if isinstance(value, (int, float)):
            return float(value), None

        try:
            return float(value), None
        except (TypeError, ValueError):
            return None, f"metric {metric} is not numeric in {latest.name}"

    def _evaluate_strategy(
        self,
        strategy: StrategyRuntime,
        stock_universe: list[StockUniverseItem],
        recent_days: int,
        reference_date: str | None,
    ) -> tuple[list[tuple[StockUniverseItem, str]], set[str], list[str]]:
        """1戦略分のスクリーニング評価を実行する。"""
        if not stock_universe:
            return [], set(), []

        stock_codes = [s.code for s in stock_universe]
        start_date, end_date = self._resolve_date_range(strategy.shared_config, reference_date)

        include_margin = (
            strategy.shared_config.include_margin_data
            and self._needs_data_requirement(strategy.entry_params, strategy.exit_params, "margin")
        )
        include_statements = (
            strategy.shared_config.include_statements_data
            and self._needs_data_requirement(strategy.entry_params, strategy.exit_params, "statements")
        )
        include_forecast_revision = self._should_include_forecast_revision(
            strategy.entry_params,
            strategy.exit_params,
        )
        needs_benchmark = self._needs_data_requirement(strategy.entry_params, strategy.exit_params, "benchmark")
        needs_sector = self._needs_data_requirement(strategy.entry_params, strategy.exit_params, "sector")

        warnings: list[str] = []

        with data_access_mode_context("direct"):
            multi_data = prepare_multi_data(
                dataset=strategy.shared_config.dataset,
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date,
                include_margin_data=include_margin,
                include_statements_data=include_statements,
                timeframe=strategy.shared_config.timeframe,
                period_type=self._resolve_period_type(strategy.entry_params, strategy.exit_params),
                include_forecast_revision=include_forecast_revision,
            )

            benchmark_data = None
            if needs_benchmark:
                try:
                    benchmark_data = load_topix_data(
                        strategy.shared_config.dataset,
                        start_date=start_date,
                        end_date=end_date,
                    )
                except Exception as exc:
                    warnings.append(f"benchmark load failed ({exc})")

            sector_data = None
            stock_sector_mapping: dict[str, str] = {}
            if needs_sector:
                try:
                    sector_data = load_all_sector_indices(
                        strategy.shared_config.dataset,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    stock_sector_mapping = get_stock_sector_mapping(strategy.shared_config.dataset)
                except Exception as exc:
                    warnings.append(f"sector data load failed ({exc})")

        matches: list[tuple[StockUniverseItem, str]] = []
        processed: set[str] = set()

        for stock in stock_universe:
            stock_data = multi_data.get(stock.code)
            if not stock_data:
                continue

            daily = stock_data.get("daily")
            if not isinstance(daily, pd.DataFrame) or daily.empty:
                continue

            processed.add(stock.code)

            margin_data = stock_data.get("margin_daily") if include_margin else None
            statements_data = stock_data.get("statements_daily") if include_statements else None

            try:
                signals = self._signal_processor.generate_signals(
                    strategy_entries=pd.Series(True, index=daily.index),
                    strategy_exits=pd.Series(False, index=daily.index),
                    ohlc_data=daily,
                    entry_signal_params=strategy.entry_params,
                    exit_signal_params=strategy.exit_params,
                    margin_data=margin_data,
                    statements_data=statements_data,
                    benchmark_data=benchmark_data,
                    sector_data=sector_data,
                    stock_sector_name=stock_sector_mapping.get(stock.code),
                )
            except Exception as exc:
                warnings.append(f"{stock.code} signal generation failed ({exc})")
                continue

            matched_date = self._find_recent_match_date(signals, recent_days)
            if matched_date is None:
                continue

            matches.append((stock, matched_date))

        return matches, processed, warnings

    def _resolve_date_range(
        self,
        shared_config: SharedConfig,
        reference_date: str | None,
    ) -> tuple[str | None, str | None]:
        """shared_config とクエリ日付からロード対象期間を解決する。"""
        start_date = shared_config.start_date or None
        end_date = shared_config.end_date or None

        if reference_date:
            if end_date is None or reference_date < end_date:
                end_date = reference_date

        return start_date, end_date

    def _needs_data_requirement(
        self,
        entry_params: SignalParams,
        exit_params: SignalParams,
        requirement: str,
    ) -> bool:
        """指定データ要件に依存する有効シグナルがあるか判定する。"""
        for signal_def in SIGNAL_REGISTRY:
            if not any(
                req == requirement or req.startswith(f"{requirement}:")
                for req in signal_def.data_requirements
            ):
                continue

            if signal_def.enabled_checker(entry_params):
                return True
            if signal_def.enabled_checker(exit_params):
                return True

        return False

    def _resolve_period_type(
        self,
        entry_params: SignalParams,
        exit_params: SignalParams,
    ) -> str:
        """fundamental設定から period_type を解決する。"""
        for params in (entry_params, exit_params):
            fundamental = params.fundamental
            period_type = getattr(fundamental, "period_type", None)
            if isinstance(period_type, str) and period_type:
                return period_type

        return "FY"

    def _should_include_forecast_revision(
        self,
        entry_params: SignalParams,
        exit_params: SignalParams,
    ) -> bool:
        """forward_eps_growth/peg_ratio 有効時に四半期修正取得を有効化。"""

        def _enabled(params: SignalParams) -> bool:
            fundamental = params.fundamental
            if not fundamental.enabled:
                return False
            return bool(
                fundamental.forward_eps_growth.enabled
                or fundamental.peg_ratio.enabled
            )

        return _enabled(entry_params) or _enabled(exit_params)

    def _find_recent_match_date(self, signals: Signals, recent_days: int) -> str | None:
        """entries=True かつ exits=False の直近一致日を返す。"""
        entries = signals.entries.fillna(False).astype(bool)
        exits = signals.exits.fillna(False).astype(bool)
        candidates = entries & (~exits)

        recent = candidates.tail(recent_days)
        if not recent.any():
            return None

        matched_index = recent[recent].index[-1]
        return _format_date(matched_index)

    def _build_result_item(self, aggregated_item: dict[str, Any]) -> ScreeningResultItem:
        """銘柄集約データをレスポンス項目へ変換する。"""
        stock: StockUniverseItem = aggregated_item["stock"]
        matched_date: str = aggregated_item["matchedDate"]
        matched_strategies: list[MatchedStrategyItem] = aggregated_item["matchedStrategies"]

        # 戦略一覧はスコア優先（nullは常に最後）で整列
        matched_strategies.sort(
            key=lambda s: (
                s.strategyScore is None,
                -(s.strategyScore or 0.0),
                s.strategyName,
            )
        )

        best = self._pick_best_strategy(matched_strategies)

        return ScreeningResultItem(
            stockCode=stock.code,
            companyName=stock.company_name,
            scaleCategory=stock.scale_category,
            sector33Name=stock.sector_33_name,
            matchedDate=matched_date,
            bestStrategyName=best.strategyName,
            bestStrategyScore=best.strategyScore,
            matchStrategyCount=len(matched_strategies),
            matchedStrategies=matched_strategies,
        )

    def _pick_best_strategy(
        self,
        matched_strategies: list[MatchedStrategyItem],
    ) -> MatchedStrategyItem:
        """最適戦略を決定する（score優先、nullは最後）。"""
        if not matched_strategies:
            raise ValueError("matched_strategies is empty")

        non_null = [s for s in matched_strategies if s.strategyScore is not None]
        if non_null:
            return max(non_null, key=lambda s: (float(s.strategyScore), s.strategyName))

        # 全てnullの場合は最新一致日を優先
        return max(matched_strategies, key=lambda s: (s.matchedDate, s.strategyName))

    def _sort_results(
        self,
        results: list[ScreeningResultItem],
        sort_by: ScreeningSortBy,
        order: SortOrder,
    ) -> list[ScreeningResultItem]:
        """結果ソート。bestStrategyScoreではnullを常に末尾へ配置。"""
        if sort_by == "bestStrategyScore":
            if order == "asc":
                return sorted(
                    results,
                    key=lambda r: (
                        r.bestStrategyScore is None,
                        r.bestStrategyScore if r.bestStrategyScore is not None else float("inf"),
                        r.stockCode,
                    ),
                )

            return sorted(
                results,
                key=lambda r: (
                    r.bestStrategyScore is None,
                    -(r.bestStrategyScore or 0.0),
                    r.stockCode,
                ),
            )

        reverse = order == "desc"

        if sort_by == "matchedDate":
            return sorted(results, key=lambda r: (r.matchedDate, r.stockCode), reverse=reverse)

        if sort_by == "stockCode":
            return sorted(results, key=lambda r: r.stockCode, reverse=reverse)

        if sort_by == "matchStrategyCount":
            return sorted(
                results,
                key=lambda r: (r.matchStrategyCount, r.stockCode),
                reverse=reverse,
            )

        return results

    def _dedupe_warnings(self, warnings: list[str]) -> list[str]:
        """警告を順序保持で重複排除し、件数を制限する。"""
        deduped: list[str] = []
        seen: set[str] = set()

        for warning in warnings:
            if warning in seen:
                continue
            seen.add(warning)
            deduped.append(warning)
            if len(deduped) >= self._WARNING_LIMIT:
                break

        if len(warnings) > len(deduped):
            deduped.append("additional warnings were truncated")

        return deduped
