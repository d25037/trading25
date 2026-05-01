"""Dynamic-PIT pruning study for the current range-break production backtest."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from src.domains.analytics.range_break_trade_archetype_decomposition import (
    _build_enriched_trade_df,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.shared.paths import get_data_dir

RANGE_BREAK_DYNAMIC_PIT_PRUNING_EXPERIMENT_ID = (
    "strategy-audit/range-break-dynamic-pit-pruning"
)
DEFAULT_STRATEGY_NAME = "range_break_v15"
DEFAULT_RESULT_STEM = "unknown_20260501_100647"
DEFAULT_COMPARISON_RESULT_STEM = "primeExTopix500_20260429_173512"
DEFAULT_TARGET_MIN_TRADES = 350
DEFAULT_TARGET_MAX_TRADES = 450
DEFAULT_DISCOVERY_END_DATE = "2021-12-31"
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_ALLOCATION_PCT = 20.0


@dataclass(frozen=True)
class _ThresholdCandidate:
    name: str
    feature_name: str
    direction: Literal["high", "low"]
    threshold: float
    mask: pd.Series


@dataclass(frozen=True)
class RangeBreakDynamicPitPruningResult:
    db_path: str
    strategy_name: str
    result_stem: str
    comparison_result_stem: str | None
    target_min_trades: int
    target_max_trades: int
    discovery_end_date: str
    severe_loss_threshold_pct: float
    allocation_pct: float
    analysis_start_date: str
    analysis_end_date: str
    baseline_summary_df: pd.DataFrame
    enriched_trade_df: pd.DataFrame
    candidate_summary_df: pd.DataFrame
    yearly_candidate_summary_df: pd.DataFrame
    threshold_summary_df: pd.DataFrame
    new_only_summary_df: pd.DataFrame


def run_range_break_dynamic_pit_pruning(
    *,
    strategy_name: str = DEFAULT_STRATEGY_NAME,
    result_stem: str = DEFAULT_RESULT_STEM,
    comparison_result_stem: str | None = DEFAULT_COMPARISON_RESULT_STEM,
    target_min_trades: int = DEFAULT_TARGET_MIN_TRADES,
    target_max_trades: int = DEFAULT_TARGET_MAX_TRADES,
    discovery_end_date: str = DEFAULT_DISCOVERY_END_DATE,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    allocation_pct: float = DEFAULT_ALLOCATION_PCT,
    enrich_features: bool = True,
) -> RangeBreakDynamicPitPruningResult:
    if target_min_trades <= 0 or target_max_trades < target_min_trades:
        raise ValueError("target trade range must be positive and ordered")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if allocation_pct <= 0:
        raise ValueError("allocation_pct must be positive")

    paths = _resolve_backtest_artifact_paths(strategy_name, result_stem)
    report = _load_json(paths["report"])
    metrics = _load_json(paths["metrics"])
    manifest = _load_json(paths["manifest"])
    shared_config = dict(manifest.get("parameters", {}).get("shared_config", {}))
    dataset_name = str(shared_config.get("universe_preset") or manifest.get("dataset_name"))
    trade_df = _normalize_trade_records(
        report.get("initial_portfolio", {}).get("trade_records", []),
        strategy_name=str(manifest.get("strategy_name") or strategy_name),
        dataset_name=dataset_name,
    )
    if trade_df.empty:
        raise RuntimeError(f"backtest report has no trade records: {paths['report']}")

    old_universe_codes = _load_comparison_universe_codes(
        strategy_name=strategy_name,
        comparison_result_stem=comparison_result_stem,
    )
    trade_df["dynamic_new_code"] = (
        ~trade_df["symbol"].astype(str).isin(old_universe_codes)
        if old_universe_codes
        else False
    )

    enriched_trade_df = (
        _build_enriched_trade_df(
            trade_ledger_df=trade_df,
            dataset_name=dataset_name,
            parameters=dict(manifest.get("parameters", {})),
        )
        if enrich_features
        else trade_df.copy()
    )
    enriched_trade_df["entry_date"] = pd.to_datetime(enriched_trade_df["entry_date"])
    enriched_trade_df["entry_year"] = enriched_trade_df["entry_date"].dt.year.astype(str)

    trading_day_count = _resolve_trading_day_count(report, enriched_trade_df)
    candidates, threshold_summary_df = _build_candidate_masks(
        enriched_trade_df,
        target_min_trades=target_min_trades,
        target_max_trades=target_max_trades,
        discovery_end_date=discovery_end_date,
    )
    candidate_summary_df = _build_candidate_summary_df(
        enriched_trade_df,
        candidates=candidates,
        target_min_trades=target_min_trades,
        target_max_trades=target_max_trades,
        trading_day_count=trading_day_count,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        allocation_pct=allocation_pct,
    )
    yearly_candidate_summary_df = _build_yearly_candidate_summary_df(
        enriched_trade_df,
        candidates=candidates,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    new_only_summary_df = _build_new_only_summary_df(
        enriched_trade_df,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    baseline_summary_df = _build_baseline_summary_df(
        metrics=metrics,
        manifest=manifest,
        report=report,
        enriched_trade_df=enriched_trade_df,
        old_universe_codes=old_universe_codes,
        trading_day_count=trading_day_count,
        target_min_trades=target_min_trades,
        target_max_trades=target_max_trades,
        allocation_pct=allocation_pct,
    )

    return RangeBreakDynamicPitPruningResult(
        db_path=str(paths["report"]),
        strategy_name=str(manifest.get("strategy_name") or strategy_name),
        result_stem=result_stem,
        comparison_result_stem=comparison_result_stem,
        target_min_trades=target_min_trades,
        target_max_trades=target_max_trades,
        discovery_end_date=discovery_end_date,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        allocation_pct=allocation_pct,
        analysis_start_date=str(enriched_trade_df["entry_date"].min().date()),
        analysis_end_date=str(enriched_trade_df["entry_date"].max().date()),
        baseline_summary_df=baseline_summary_df,
        enriched_trade_df=enriched_trade_df,
        candidate_summary_df=candidate_summary_df,
        yearly_candidate_summary_df=yearly_candidate_summary_df,
        threshold_summary_df=threshold_summary_df,
        new_only_summary_df=new_only_summary_df,
    )


def write_range_break_dynamic_pit_pruning_bundle(
    result: RangeBreakDynamicPitPruningResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANGE_BREAK_DYNAMIC_PIT_PRUNING_EXPERIMENT_ID,
        module=__name__,
        function="run_range_break_dynamic_pit_pruning",
        params={
            "strategy_name": result.strategy_name,
            "result_stem": result.result_stem,
            "comparison_result_stem": result.comparison_result_stem,
            "target_min_trades": result.target_min_trades,
            "target_max_trades": result.target_max_trades,
            "discovery_end_date": result.discovery_end_date,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "allocation_pct": result.allocation_pct,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "db_path": result.db_path,
            "strategy_name": result.strategy_name,
            "result_stem": result.result_stem,
            "comparison_result_stem": result.comparison_result_stem,
            "target_min_trades": result.target_min_trades,
            "target_max_trades": result.target_max_trades,
            "discovery_end_date": result.discovery_end_date,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "allocation_pct": result.allocation_pct,
        },
        result_tables={
            "baseline_summary_df": result.baseline_summary_df,
            "enriched_trade_df": result.enriched_trade_df,
            "candidate_summary_df": result.candidate_summary_df,
            "yearly_candidate_summary_df": result.yearly_candidate_summary_df,
            "threshold_summary_df": result.threshold_summary_df,
            "new_only_summary_df": result.new_only_summary_df,
        },
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_range_break_dynamic_pit_pruning_bundle(
    bundle_path: str | Path,
) -> RangeBreakDynamicPitPruningResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return RangeBreakDynamicPitPruningResult(
        db_path=str(metadata["db_path"]),
        strategy_name=str(metadata["strategy_name"]),
        result_stem=str(metadata["result_stem"]),
        comparison_result_stem=metadata.get("comparison_result_stem"),
        target_min_trades=int(metadata["target_min_trades"]),
        target_max_trades=int(metadata["target_max_trades"]),
        discovery_end_date=str(metadata["discovery_end_date"]),
        severe_loss_threshold_pct=float(metadata["severe_loss_threshold_pct"]),
        allocation_pct=float(metadata["allocation_pct"]),
        analysis_start_date=str(info.analysis_start_date),
        analysis_end_date=str(info.analysis_end_date),
        baseline_summary_df=tables["baseline_summary_df"],
        enriched_trade_df=tables["enriched_trade_df"],
        candidate_summary_df=tables["candidate_summary_df"],
        yearly_candidate_summary_df=tables["yearly_candidate_summary_df"],
        threshold_summary_df=tables["threshold_summary_df"],
        new_only_summary_df=tables["new_only_summary_df"],
    )


def get_range_break_dynamic_pit_pruning_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        RANGE_BREAK_DYNAMIC_PIT_PRUNING_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_range_break_dynamic_pit_pruning_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        RANGE_BREAK_DYNAMIC_PIT_PRUNING_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _resolve_backtest_artifact_paths(strategy_name: str, result_stem: str) -> dict[str, Path]:
    result_dir = get_data_dir() / "backtest" / "results" / strategy_name
    paths = {
        "report": result_dir / f"{result_stem}.report.json",
        "metrics": result_dir / f"{result_stem}.metrics.json",
        "manifest": result_dir / f"{result_stem}.manifest.json",
    }
    missing = [str(path) for path in paths.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"missing backtest artifact(s): {missing}")
    return paths


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_trade_records(
    records: list[dict[str, Any]],
    *,
    strategy_name: str,
    dataset_name: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in records:
        entry_raw = record.get("Entry Timestamp")
        if entry_raw is None:
            continue
        entry_ts = pd.Timestamp(entry_raw)
        exit_raw = record.get("Exit Timestamp")
        exit_ts = pd.Timestamp(exit_raw) if exit_raw else pd.NaT
        holding_days = (
            int((exit_ts.normalize() - entry_ts.normalize()).days)
            if pd.notna(exit_ts)
            else np.nan
        )
        rows.append(
            {
                "strategy_name": strategy_name,
                "strategy_basename": strategy_name.split("/")[-1],
                "dataset_name": dataset_name,
                "dataset_preset": dataset_name,
                "window_label": "full_dynamic_pit",
                "window_start_date": "",
                "window_end_date": "",
                "symbol": str(record.get("Column")),
                "entry_timestamp": entry_ts.isoformat(),
                "exit_timestamp": exit_ts.isoformat() if pd.notna(exit_ts) else None,
                "holding_days": holding_days,
                "size": _coerce_float(record.get("Size")),
                "avg_entry_price": _coerce_float(record.get("Avg Entry Price")),
                "avg_exit_price": _coerce_float(record.get("Avg Exit Price")),
                "pnl": _coerce_float(record.get("PnL")),
                "trade_return_pct": _coerce_float(record.get("Return")) * 100.0,
                "direction": str(record.get("Direction") or ""),
                "status": str(record.get("Status") or ""),
                "position_id": record.get("Position Id"),
            }
        )
    return pd.DataFrame(rows)


def _load_comparison_universe_codes(
    *,
    strategy_name: str,
    comparison_result_stem: str | None,
) -> set[str]:
    if not comparison_result_stem:
        return set()
    manifest_path = (
        get_data_dir()
        / "backtest"
        / "results"
        / strategy_name
        / f"{comparison_result_stem}.manifest.json"
    )
    if not manifest_path.is_file():
        return set()
    manifest = _load_json(manifest_path)
    codes = manifest.get("parameters", {}).get("shared_config", {}).get("stock_codes", [])
    return {str(code) for code in codes if code}


def _build_candidate_masks(
    frame: pd.DataFrame,
    *,
    target_min_trades: int,
    target_max_trades: int,
    discovery_end_date: str,
) -> tuple[dict[str, pd.Series], pd.DataFrame]:
    target_mid = (target_min_trades + target_max_trades) / 2.0
    keep_ratio = min(max(target_mid / max(len(frame), 1), 0.05), 0.95)
    candidates: dict[str, pd.Series] = {
        "baseline_all": pd.Series(True, index=frame.index, dtype=bool),
        "dynamic_new_codes_only": frame.get(
            "dynamic_new_code",
            pd.Series(False, index=frame.index),
        ).fillna(False).astype(bool),
    }
    threshold_rows: list[dict[str, Any]] = []
    specs: tuple[tuple[str, str, Literal["high", "low"]], ...] = (
        ("target_high_risk_adjusted_return_60", "risk_adjusted_return_60", "high"),
        ("target_low_forward_per", "forward_per", "low"),
        ("target_low_pbr", "pbr", "low"),
        ("target_small_market_cap", "market_cap_bil_jpy", "low"),
        ("target_low_rsi10", "rsi10", "low"),
        ("target_low_breakout_60d_runup", "breakout_60d_runup_pct", "low"),
        ("target_low_volume_ratio", "volume_ratio_value", "low"),
        ("target_high_topix_risk_adjusted_return_60", "topix_risk_adjusted_return_60", "high"),
    )
    for name, feature_name, direction in specs:
        if feature_name not in frame.columns:
            continue
        candidate = _build_target_threshold_candidate(
            frame,
            feature_name=feature_name,
            direction=direction,
            keep_ratio=keep_ratio,
            discovery_end_date=discovery_end_date,
        )
        candidates[name] = candidate.mask
        threshold_rows.append(
            {
                "candidate_name": name,
                "feature_name": feature_name,
                "direction": direction,
                "threshold": candidate.threshold,
                "keep_ratio": keep_ratio,
                "discovery_end_date": discovery_end_date,
            }
        )

    if {"forward_per", "risk_adjusted_return_60"}.issubset(frame.columns):
        low_fper = candidates.get("target_low_forward_per")
        high_rar = candidates.get("target_high_risk_adjusted_return_60")
        if low_fper is not None and high_rar is not None:
            candidates["combo_low_forward_per_high_rar"] = low_fper & high_rar
    if {"topix_return_60d_pct", "topix_close_vs_sma200_pct"}.issubset(frame.columns):
        candidates["topix_supportive"] = (
            pd.to_numeric(frame["topix_return_60d_pct"], errors="coerce") > 0
        ) & (pd.to_numeric(frame["topix_close_vs_sma200_pct"], errors="coerce") > 0)
    if {"rsi10", "breakout_60d_runup_pct", "volume_ratio_value"}.issubset(frame.columns):
        ranks = pd.DataFrame(
            {
                "rsi": pd.to_numeric(frame["rsi10"], errors="coerce").rank(pct=True),
                "runup": pd.to_numeric(
                    frame["breakout_60d_runup_pct"],
                    errors="coerce",
                ).rank(pct=True),
                "volume": pd.to_numeric(frame["volume_ratio_value"], errors="coerce").rank(
                    pct=True
                ),
            },
            index=frame.index,
        )
        candidates["not_overheat_overlap"] = (ranks >= 0.80).sum(axis=1) < 2
    return candidates, pd.DataFrame(threshold_rows)


def _build_target_threshold_candidate(
    frame: pd.DataFrame,
    *,
    feature_name: str,
    direction: Literal["high", "low"],
    keep_ratio: float,
    discovery_end_date: str,
) -> _ThresholdCandidate:
    feature = pd.to_numeric(frame[feature_name], errors="coerce")
    discovery_mask = pd.to_datetime(frame["entry_date"]) <= pd.Timestamp(discovery_end_date)
    discovery_feature = feature[discovery_mask].dropna()
    if discovery_feature.empty:
        threshold = math.nan
        mask = pd.Series(False, index=frame.index, dtype=bool)
    elif direction == "high":
        threshold = float(discovery_feature.quantile(1.0 - keep_ratio))
        mask = feature >= threshold
    else:
        threshold = float(discovery_feature.quantile(keep_ratio))
        mask = feature <= threshold
    return _ThresholdCandidate(
        name=f"target_{direction}_{feature_name}",
        feature_name=feature_name,
        direction=direction,
        threshold=threshold,
        mask=mask.fillna(False).astype(bool),
    )


def _build_candidate_summary_df(
    frame: pd.DataFrame,
    *,
    candidates: dict[str, pd.Series],
    target_min_trades: int,
    target_max_trades: int,
    trading_day_count: int,
    severe_loss_threshold_pct: float,
    allocation_pct: float = DEFAULT_ALLOCATION_PCT,
) -> pd.DataFrame:
    target_mid = (target_min_trades + target_max_trades) / 2.0
    rows = []
    for candidate_name, mask in candidates.items():
        selected = frame[mask.reindex(frame.index).fillna(False).astype(bool)].copy()
        metrics = _trade_metrics(
            selected,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        approx_concurrent = (
            metrics["trade_count"] * metrics["avg_holding_days"] / trading_day_count
            if trading_day_count > 0 and pd.notna(metrics["avg_holding_days"])
            else np.nan
        )
        rows.append(
            {
                "candidate_name": candidate_name,
                **metrics,
                "keep_pct": (
                    (metrics["trade_count"] / len(frame)) * 100.0 if len(frame) else np.nan
                ),
                "target_band_hit": target_min_trades
                <= metrics["trade_count"]
                <= target_max_trades,
                "target_distance": abs(metrics["trade_count"] - target_mid),
                "approx_concurrent_positions": approx_concurrent,
                "approx_gross_exposure_pct_at_allocation": approx_concurrent
                * allocation_pct
                if pd.notna(approx_concurrent)
                else np.nan,
            }
        )
    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary
    return summary.sort_values(
        ["target_band_hit", "target_distance", "avg_trade_return_pct", "profit_factor"],
        ascending=[False, True, False, False],
    ).reset_index(drop=True)


def _build_yearly_candidate_summary_df(
    frame: pd.DataFrame,
    *,
    candidates: dict[str, pd.Series],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows = []
    for candidate_name, mask in candidates.items():
        selected = frame[mask.reindex(frame.index).fillna(False).astype(bool)].copy()
        for entry_year, year_frame in selected.groupby("entry_year", dropna=False):
            rows.append(
                {
                    "candidate_name": candidate_name,
                    "entry_year": str(entry_year),
                    **_trade_metrics(
                        year_frame,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    ),
                }
            )
    return pd.DataFrame(rows)


def _build_new_only_summary_df(
    frame: pd.DataFrame,
    *,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    if "dynamic_new_code" not in frame.columns:
        return pd.DataFrame()
    rows = []
    for is_new, group in frame.groupby("dynamic_new_code", dropna=False):
        rows.append(
            {
                "dynamic_new_code": bool(is_new),
                **_trade_metrics(group, severe_loss_threshold_pct=severe_loss_threshold_pct),
            }
        )
    return pd.DataFrame(rows)


def _build_baseline_summary_df(
    *,
    metrics: dict[str, Any],
    manifest: dict[str, Any],
    report: dict[str, Any],
    enriched_trade_df: pd.DataFrame,
    old_universe_codes: set[str],
    trading_day_count: int,
    target_min_trades: int,
    target_max_trades: int,
    allocation_pct: float,
) -> pd.DataFrame:
    shared_config = dict(manifest.get("parameters", {}).get("shared_config", {}))
    provenance = dict(shared_config.get("universe_provenance", {}))
    entry_counts = report.get("entry_signal_counts", {}).get("values", [])
    baseline_metrics = _trade_metrics(
        enriched_trade_df,
        severe_loss_threshold_pct=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    )
    approx_concurrent = (
        baseline_metrics["trade_count"] * baseline_metrics["avg_holding_days"]
    ) / trading_day_count
    return pd.DataFrame(
        [
            {
                "result_stem": manifest.get("html_path", ""),
                "git_commit": manifest.get("git_commit"),
                "universe_preset": shared_config.get("universe_preset"),
                "universe_as_of_date": provenance.get("asOfDate"),
                "universe_resolved_count": provenance.get("resolvedCount"),
                "comparison_universe_count": len(old_universe_codes),
                "new_universe_code_count": max(
                    int(provenance.get("resolvedCount") or 0) - len(old_universe_codes),
                    0,
                ),
                "entry_signal_sum": int(sum(entry_counts)) if entry_counts else np.nan,
                "entry_signal_nonzero_days": int(sum(1 for value in entry_counts if value))
                if entry_counts
                else np.nan,
                "entry_signal_max_per_day": int(max(entry_counts)) if entry_counts else np.nan,
                "metric_total_return_pct": metrics.get("total_return"),
                "metric_sharpe_ratio": metrics.get("sharpe_ratio"),
                "metric_max_drawdown_pct": metrics.get("max_drawdown"),
                "metric_trade_count": metrics.get("trade_count") or metrics.get("total_trades"),
                "target_min_trades": target_min_trades,
                "target_max_trades": target_max_trades,
                "trading_day_count": trading_day_count,
                "approx_concurrent_positions": approx_concurrent,
                "approx_gross_exposure_pct_at_allocation": approx_concurrent * allocation_pct,
                **baseline_metrics,
            }
        ]
    )


def _trade_metrics(
    frame: pd.DataFrame,
    *,
    severe_loss_threshold_pct: float,
) -> dict[str, Any]:
    if frame.empty:
        return {
            "trade_count": 0,
            "win_rate_pct": np.nan,
            "avg_trade_return_pct": np.nan,
            "median_trade_return_pct": np.nan,
            "profit_factor": np.nan,
            "expectancy_pct": np.nan,
            "severe_loss_rate_pct": np.nan,
            "avg_holding_days": np.nan,
            "median_holding_days": np.nan,
            "worst_trade_return_pct": np.nan,
            "best_trade_return_pct": np.nan,
            "dynamic_new_trade_count": 0,
            "dynamic_new_trade_pct": np.nan,
        }
    returns = pd.to_numeric(frame["trade_return_pct"], errors="coerce")
    wins = returns[returns > 0]
    losses = returns[returns < 0]
    profit_factor = (
        float(wins.sum() / abs(losses.sum()))
        if not losses.empty and abs(float(losses.sum())) > 0
        else np.nan
    )
    dynamic_new_count = (
        int(frame["dynamic_new_code"].fillna(False).astype(bool).sum())
        if "dynamic_new_code" in frame.columns
        else 0
    )
    return {
        "trade_count": int(len(frame)),
        "win_rate_pct": float((returns > 0).mean() * 100.0),
        "avg_trade_return_pct": float(returns.mean()),
        "median_trade_return_pct": float(returns.median()),
        "profit_factor": profit_factor,
        "expectancy_pct": float(returns.mean()),
        "severe_loss_rate_pct": float((returns <= severe_loss_threshold_pct).mean() * 100.0),
        "avg_holding_days": float(pd.to_numeric(frame["holding_days"], errors="coerce").mean()),
        "median_holding_days": float(
            pd.to_numeric(frame["holding_days"], errors="coerce").median()
        ),
        "worst_trade_return_pct": float(returns.min()),
        "best_trade_return_pct": float(returns.max()),
        "dynamic_new_trade_count": dynamic_new_count,
        "dynamic_new_trade_pct": (dynamic_new_count / len(frame)) * 100.0,
    }


def _resolve_trading_day_count(report: dict[str, Any], frame: pd.DataFrame) -> int:
    values = report.get("entry_signal_counts", {}).get("values", [])
    if values:
        return len(values)
    return int(frame["entry_date"].nunique())


def _coerce_float(value: Any) -> float:
    if value is None:
        return np.nan
    try:
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def _build_summary_markdown(result: RangeBreakDynamicPitPruningResult) -> str:
    top_candidates = result.candidate_summary_df.head(12)
    return "\n".join(
        [
            "# Range Break Dynamic PIT Pruning",
            "",
            "## Scope",
            "",
            f"- Strategy: `{result.strategy_name}`",
            f"- Source result stem: `{result.result_stem}`",
            f"- Comparison result stem: `{result.comparison_result_stem}`",
            f"- Target trades: `{result.target_min_trades}` to `{result.target_max_trades}`",
            f"- Discovery end date for thresholds: `{result.discovery_end_date}`",
            "- Candidate thresholds are learned from the discovery segment and applied to the full trade ledger.",
            "- Metrics are trade-level pruning diagnostics, not a rebalanced portfolio backtest.",
            "",
            "## Baseline",
            "",
            _format_markdown_table(result.baseline_summary_df),
            "",
            "## Top Candidate Diagnostics",
            "",
            _format_markdown_table(
                top_candidates[
                    [
                        "candidate_name",
                        "trade_count",
                        "target_band_hit",
                        "avg_trade_return_pct",
                        "win_rate_pct",
                        "profit_factor",
                        "severe_loss_rate_pct",
                        "avg_holding_days",
                        "approx_concurrent_positions",
                        "approx_gross_exposure_pct_at_allocation",
                    ]
                ]
            ),
            "",
            "## Artifact Tables",
            "",
            "- `baseline_summary_df`",
            "- `enriched_trade_df`",
            "- `candidate_summary_df`",
            "- `yearly_candidate_summary_df`",
            "- `threshold_summary_df`",
            "- `new_only_summary_df`",
        ]
    )


def _build_published_summary(result: RangeBreakDynamicPitPruningResult) -> dict[str, Any]:
    best = (
        result.candidate_summary_df.iloc[0].to_dict()
        if not result.candidate_summary_df.empty
        else {}
    )
    baseline = (
        result.baseline_summary_df.iloc[0].to_dict()
        if not result.baseline_summary_df.empty
        else {}
    )
    return {
        "strategyName": result.strategy_name,
        "resultStem": result.result_stem,
        "targetMinTrades": result.target_min_trades,
        "targetMaxTrades": result.target_max_trades,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "baselineTradeCount": baseline.get("trade_count"),
        "bestCandidateName": best.get("candidate_name"),
        "bestCandidateTradeCount": best.get("trade_count"),
        "bestCandidateAvgTradeReturnPct": best.get("avg_trade_return_pct"),
        "bestCandidateProfitFactor": best.get("profit_factor"),
    }


def _format_markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows._"
    columns = list(frame.columns)
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in frame.to_dict(orient="records"):
        values = [_format_markdown_cell(row.get(column)) for column in columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _format_markdown_cell(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


__all__ = [
    "DEFAULT_ALLOCATION_PCT",
    "DEFAULT_COMPARISON_RESULT_STEM",
    "DEFAULT_DISCOVERY_END_DATE",
    "DEFAULT_RESULT_STEM",
    "DEFAULT_SEVERE_LOSS_THRESHOLD_PCT",
    "DEFAULT_STRATEGY_NAME",
    "DEFAULT_TARGET_MAX_TRADES",
    "DEFAULT_TARGET_MIN_TRADES",
    "RANGE_BREAK_DYNAMIC_PIT_PRUNING_EXPERIMENT_ID",
    "RangeBreakDynamicPitPruningResult",
    "get_range_break_dynamic_pit_pruning_bundle_path_for_run_id",
    "get_range_break_dynamic_pit_pruning_latest_bundle_path",
    "load_range_break_dynamic_pit_pruning_bundle",
    "run_range_break_dynamic_pit_pruning",
    "write_range_break_dynamic_pit_pruning_bundle",
]
