"""Daily Ranking triage lens for discretionary short-list generation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence, cast

import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    compose_daily_ranking_signal_features,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    AtrFeaturesRequest,
    LongLeadershipFeaturesRequest,
    SectorStrengthFeaturesRequest,
    build_atr_features,
    build_long_leadership_features,
    build_sector_strength_features,
)
from src.domains.analytics.daily_ranking_research_base import (
    DailyRankingPanelRequest,
    MarketScope,
    attach_daily_ranking_outcomes,
    build_daily_ranking_research_base,
    materialize_daily_ranking_signal_cohort,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.ranking_color_evidence import (
    DEFAULT_MARKET_SCOPES,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
)
from src.domains.analytics.ranking_research_selection_contract import (
    evaluate_frozen_selection,
    freeze_signal_topk,
)
from src.domains.analytics.ranking_sector_strength_evidence import DEFAULT_HORIZONS
from src.domains.analytics.readonly_duckdb_support import open_readonly_analysis_connection
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

RANKING_DAILY_TRIAGE_LENS_EXPERIMENT_ID = "market-behavior/ranking-daily-triage-lens"
DEFAULT_TOP_KS: tuple[int, ...] = (5, 10, 15)
DEFAULT_START_DATE = "2023-01-01"
DEFAULT_STRONG_GAIN_THRESHOLD_PCT = 10.0
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)


@dataclass(frozen=True)
class RankingDailyTriageLensResult:
    db_path: str
    source_mode: str
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    market_scopes: tuple[str, ...]
    top_ks: tuple[int, ...]
    severe_loss_threshold_pct: float
    strong_gain_threshold_pct: float
    observation_count: int
    coverage_diagnostics_df: pd.DataFrame
    daily_triage_candidates_df: pd.DataFrame
    attention_efficiency_df: pd.DataFrame
    kill_leakage_df: pd.DataFrame
    crowded_vs_neutral_triage_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_daily_triage_lens_research(
    db_path: str | Path,
    *,
    start_date: str | None = DEFAULT_START_DATE,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    top_ks: Iterable[int] = DEFAULT_TOP_KS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    strong_gain_threshold_pct: float = DEFAULT_STRONG_GAIN_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingDailyTriageLensResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_top_ks = tuple(sorted({int(top_k) for top_k in top_ks}))
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    _validate_params(
        horizons=resolved_horizons,
        top_ks=resolved_top_ks,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        strong_gain_threshold_pct=strong_gain_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-daily-triage-lens-",
    ) as ctx:
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="daily_triage",
                analysis_start_date=_parse_optional_date(start_date),
                analysis_end_date=_parse_optional_date(end_date),
                horizons=resolved_horizons,
                market_scopes=cast(
                    tuple[MarketScope, ...],
                    resolved_market_scopes,
                ),
                include_liquidity=True,
                percentile_features=(),
            ),
        )
        signal_source = relations.liquidity_ranked_signals
        if signal_source is None:
            raise RuntimeError("daily triage requires liquidity-ranked signals")
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="daily_triage_sector",
            ),
        )
        leadership_features = build_long_leadership_features(
            ctx.connection,
            LongLeadershipFeaturesRequest(
                source=signal_source,
                sector_features=sector_features,
                namespace="daily_triage_leadership",
                leadership_windows=_LEADERSHIP_WINDOWS,
            ),
        )
        atr_features = build_atr_features(
            ctx.connection,
            AtrFeaturesRequest(source=signal_source, namespace="daily_triage_atr"),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(leadership_features, atr_features),
            namespace="daily_triage",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="daily_triage_signals",
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="daily_triage_outcomes",
        )
        panel_df = _query_triage_panel(
            ctx.connection,
            source_name=evaluated.name,
            horizons=resolved_horizons,
        )
        return run_ranking_daily_triage_lens_from_panel(
            panel_df,
            db_path=str(db_path_obj),
            source_mode=str(ctx.source_mode),
            source_detail=ctx.source_detail,
            market_source=market_source,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_scopes=resolved_market_scopes,
            top_ks=resolved_top_ks,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
            strong_gain_threshold_pct=strong_gain_threshold_pct,
            observation_sample_limit=observation_sample_limit,
        )


def run_ranking_daily_triage_lens_from_panel(
    panel_df: pd.DataFrame,
    *,
    db_path: str = "<panel-input>",
    source_mode: str = "panel",
    source_detail: str = "in-memory panel",
    market_source: str = "panel",
    analysis_start_date: str | None = None,
    analysis_end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    top_ks: Iterable[int] = DEFAULT_TOP_KS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    strong_gain_threshold_pct: float = DEFAULT_STRONG_GAIN_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingDailyTriageLensResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_top_ks = tuple(sorted({int(top_k) for top_k in top_ks}))
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    _validate_params(
        horizons=resolved_horizons,
        top_ks=resolved_top_ks,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        strong_gain_threshold_pct=strong_gain_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )
    panel_df = _with_optional_panel_columns(panel_df)
    _assert_required_panel_columns(panel_df, horizons=resolved_horizons)
    panel_df = panel_df.loc[
        panel_df["market_scope"].astype("string").isin(resolved_market_scopes)
    ].copy()

    candidates = _build_triage_candidates_df(panel_df)
    return RankingDailyTriageLensResult(
        db_path=db_path,
        source_mode=str(source_mode),
        source_detail=source_detail,
        market_source=market_source,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        horizons=resolved_horizons,
        market_scopes=resolved_market_scopes,
        top_ks=resolved_top_ks,
        severe_loss_threshold_pct=float(severe_loss_threshold_pct),
        strong_gain_threshold_pct=float(strong_gain_threshold_pct),
        observation_count=int(len(candidates)),
        coverage_diagnostics_df=_build_coverage_diagnostics_df(candidates),
        daily_triage_candidates_df=candidates,
        attention_efficiency_df=_build_attention_efficiency_df(
            candidates,
            horizons=resolved_horizons,
            top_ks=resolved_top_ks,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
            strong_gain_threshold_pct=strong_gain_threshold_pct,
        ),
        kill_leakage_df=_build_kill_leakage_df(
            candidates,
            horizons=resolved_horizons,
            strong_gain_threshold_pct=strong_gain_threshold_pct,
        ),
        crowded_vs_neutral_triage_df=_build_crowded_vs_neutral_triage_df(
            candidates,
            horizons=resolved_horizons,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
            strong_gain_threshold_pct=strong_gain_threshold_pct,
        ),
        observation_sample_df=candidates.head(int(observation_sample_limit)).copy(),
    )


def write_ranking_daily_triage_lens_bundle(
    result: RankingDailyTriageLensResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_DAILY_TRIAGE_LENS_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_daily_triage_lens",
        function="run_ranking_daily_triage_lens_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "top_ks": list(result.top_ks),
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "strong_gain_threshold_pct": result.strong_gain_threshold_pct,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
            "primary_outcome": "attention_efficiency_df",
            "triage_buckets": ["inspect", "watch", "ignore", "kill"],
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "daily_triage_candidates_df": result.daily_triage_candidates_df,
            "attention_efficiency_df": result.attention_efficiency_df,
            "kill_leakage_df": result.kill_leakage_df,
            "crowded_vs_neutral_triage_df": result.crowded_vs_neutral_triage_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingDailyTriageLensResult) -> str:
    parts = [
        "# Ranking Daily Triage Lens",
        "",
        "## Metadata",
        "",
        f"- db_path: `{result.db_path}`",
        f"- source_mode: `{result.source_mode}`",
        f"- source_detail: `{result.source_detail}`",
        f"- market_source: `{result.market_source}`",
        f"- analysis_start_date: `{result.analysis_start_date}`",
        f"- analysis_end_date: `{result.analysis_end_date}`",
        f"- horizons: `{', '.join(str(item) for item in result.horizons)}`",
        f"- market_scopes: `{', '.join(result.market_scopes)}`",
        f"- top_ks: `{', '.join(str(item) for item in result.top_ks)}`",
        f"- observation_count: `{result.observation_count}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=50),
        "",
        "## Attention Efficiency",
        "",
        _top_rows_for_markdown(result.attention_efficiency_df, limit=120),
        "",
        "## Kill Leakage",
        "",
        _top_rows_for_markdown(result.kill_leakage_df, limit=80),
        "",
        "## Crowded vs Neutral Triage",
        "",
        _top_rows_for_markdown(result.crowded_vs_neutral_triage_df, limit=160),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _query_triage_panel(
    conn: Any,
    *,
    source_name: str,
    horizons: Sequence[int],
) -> pd.DataFrame:
    return_columns = ",\n                ".join(
        [f"r.forward_close_excess_return_{int(horizon)}d_pct" for horizon in horizons]
    )
    return conn.execute(
        f"""
        SELECT
            r.market_scope,
            r.date,
            r.code,
            r.company_name,
            r.sector_33_code,
            r.sector_33_name,
            r.liquidity_regime,
            r.valuation_signal,
            r.strong_value_confirmation,
            r.medium_value_confirmation,
            r.overvalued_warning,
            r.very_overvalued_warning,
            r.no_value_confirmation,
            r.pbr_percentile,
            r.forecast_per_percentile AS forward_per_percentile,
            CAST(NULL AS DOUBLE) AS psr_percentile,
            CAST(NULL AS DOUBLE) AS forward_psr_percentile,
            r.recent_return_20d_pct,
            r.recent_return_60d_pct,
            r.sector_strength_bucket,
            r.sector_strength_score,
            r.long_hybrid_leadership_score,
            coalesce(
                r.atr20_change_20d_pct >= 25.0
                AND r.atr20_to_atr60 < 1.25
                AND coalesce(r.recent_return_20d_pct, 0.0) < 30.0,
                FALSE
            ) AS atr20_acceleration_ex_overheat_flag,
            coalesce(
                r.atr20_change_20d_pct >= 25.0
                AND r.atr20_to_atr60 >= 1.25,
                FALSE
            ) AS atr20_to_atr60_overheat_flag,
            {return_columns}
        FROM {source_name} r
        ORDER BY date, market_scope, code
        """
    ).fetchdf()


def _build_triage_candidates_df(panel_df: pd.DataFrame) -> pd.DataFrame:
    df = panel_df.copy()
    for column in (
        "strong_value_confirmation",
        "medium_value_confirmation",
        "overvalued_warning",
        "very_overvalued_warning",
        "no_value_confirmation",
        "atr20_acceleration_ex_overheat_flag",
        "atr20_to_atr60_overheat_flag",
    ):
        df[column] = df[column].fillna(False).astype(bool)

    records: list[dict[str, object]] = []
    for raw_row in df.to_dict(orient="records"):
        row = {str(key): value for key, value in raw_row.items()}
        triage_bucket, triage_score, triage_reason = _classify_row(row)
        enriched = dict(row)
        enriched["triage_bucket"] = triage_bucket
        enriched["triage_score"] = triage_score
        enriched["triage_reason"] = triage_reason
        records.append(enriched)

    result = pd.DataFrame(records)
    if result.empty:
        return result
    return result.sort_values(
        ["date", "market_scope", "triage_score", "code"],
        ascending=[True, True, False, True],
        kind="mergesort",
    ).reset_index(drop=True)


def _with_optional_panel_columns(panel_df: pd.DataFrame) -> pd.DataFrame:
    df = panel_df.copy()
    for column in ("psr_percentile", "forward_psr_percentile"):
        if column not in df.columns:
            df[column] = pd.NA
    return df


def _classify_row(row: dict[str, object]) -> tuple[str, float, str]:
    strong_value = bool(row.get("strong_value_confirmation"))
    medium_value = bool(row.get("medium_value_confirmation"))
    overvalued = bool(row.get("overvalued_warning")) or bool(row.get("very_overvalued_warning"))
    no_value = bool(row.get("no_value_confirmation"))
    pbr = _float_or_none(row.get("pbr_percentile"))
    fwd_per = _float_or_none(row.get("forward_per_percentile"))
    psr = _float_or_none(row.get("psr_percentile"))
    fwd_psr = _float_or_none(row.get("forward_psr_percentile"))
    sector_score = _float_or_none(row.get("sector_strength_score"))
    long_hybrid = _float_or_none(row.get("long_hybrid_leadership_score"))
    liquidity_regime = str(row.get("liquidity_regime") or "")
    atr_accel = bool(row.get("atr20_acceleration_ex_overheat_flag"))
    atr_overheat = bool(row.get("atr20_to_atr60_overheat_flag"))

    high_bad_valuation = any(
        value is not None and value >= 0.8 for value in (pbr, fwd_per, psr, fwd_psr)
    )
    low_pbr = pbr is not None and pbr <= 0.1
    low_fwd_per = fwd_per is not None and fwd_per <= 0.1
    sector_strong = sector_score is not None and sector_score >= 0.8
    long_leadership = long_hybrid is not None and long_hybrid >= 0.8

    if overvalued or (no_value and high_bad_valuation):
        return "kill", -100.0, "overvalued_or_no_value_high_valuation"

    if strong_value and low_pbr and low_fwd_per:
        score = 100.0
        reasons = ["deep_value_low10_pbr_low10_fwd_per"]
        if liquidity_regime == "crowded_rerating":
            score += 15.0
            reasons.append("crowded_right_tail_candidate")
        if long_leadership:
            score += 10.0
            reasons.append("long_hybrid_leadership")
        if atr_accel:
            score += 8.0
            reasons.append("atr20_accel_ex_overheat")
        if sector_strong:
            score += 6.0
            reasons.append("sector_strong")
        if atr_overheat:
            score -= 12.0
            reasons.append("atr_overheat_caution")
        return "inspect", score, "+".join(reasons)

    if strong_value and (sector_strong or atr_accel or long_leadership):
        score = 82.0
        reasons = ["deep_value_confirmed"]
        if sector_strong:
            score += 6.0
            reasons.append("sector_strong")
        if atr_accel:
            score += 8.0
            reasons.append("atr20_accel_ex_overheat")
        if long_leadership:
            score += 6.0
            reasons.append("long_hybrid_leadership")
        if liquidity_regime == "crowded_rerating":
            score += 4.0
            reasons.append("crowded_candidate")
        return "inspect", score, "+".join(reasons)

    if strong_value or medium_value:
        return "watch", 50.0, "value_material_but_not_focused"

    return "ignore", 0.0, "not_enough_for_discretionary_shortlist"


def _build_coverage_diagnostics_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = []
    for (market_scope, liquidity_regime), group in df.groupby(
        ["market_scope", "liquidity_regime"],
        dropna=False,
    ):
        rows.append(
            {
                "market_scope": market_scope,
                "liquidity_regime": liquidity_regime,
                "observation_count": int(len(group)),
                "code_count": int(group["code"].nunique()),
                "date_count": int(group["date"].nunique()),
                "inspect_rate_pct": _pct((group["triage_bucket"] == "inspect").mean()),
                "watch_rate_pct": _pct((group["triage_bucket"] == "watch").mean()),
                "kill_rate_pct": _pct((group["triage_bucket"] == "kill").mean()),
            }
        )
    return pd.DataFrame(rows).sort_values(["market_scope", "liquidity_regime"]).reset_index(
        drop=True
    )


def _build_attention_efficiency_df(
    df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    top_ks: Sequence[int],
    severe_loss_threshold_pct: float,
    strong_gain_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for horizon in horizons:
        return_col = f"forward_close_excess_return_{int(horizon)}d_pct"
        if df.empty:
            continue
        for top_k in top_ks:
            evaluated_dates = []
            for _, date_frame in df.groupby("date", dropna=False, sort=True):
                signal_candidates = date_frame.loc[
                    date_frame["triage_bucket"].ne("kill"),
                    ["market_scope", "date", "code", "triage_score"],
                ].copy()
                if signal_candidates.empty:
                    continue
                frozen = freeze_signal_topk(
                    signal_candidates,
                    group_columns=("market_scope", "date"),
                    score_columns=("triage_score",),
                    k=int(top_k),
                    ascending=(False,),
                )
                evaluated = evaluate_frozen_selection(
                    frozen,
                    date_frame.loc[
                        :, ["market_scope", "date", "code", return_col]
                    ],
                    outcome_column=return_col,
                )
                evaluated_dates.append(evaluated)

            evaluated_date_count = len(evaluated_dates)
            complete_date_count = sum(
                item.outcome_status == "complete" for item in evaluated_dates
            )
            incomplete_date_count = evaluated_date_count - complete_date_count
            outcome_complete = bool(
                evaluated_date_count and incomplete_date_count == 0
            )
            candidate_count = sum(item.candidate_count for item in evaluated_dates)
            candidate_outcome_count = sum(
                item.candidate_outcome_count for item in evaluated_dates
            )
            selected_count = sum(len(item.selected) for item in evaluated_dates)
            selected_outcome_count = sum(
                item.selected_outcome_count for item in evaluated_dates
            )
            selected = (
                pd.concat(
                    [item.selected for item in evaluated_dates],
                    ignore_index=True,
                )
                if outcome_complete
                else pd.DataFrame(
                    columns=["market_scope", "date", "code", return_col]
                )
            )
            horizon_df = (
                df.loc[df[return_col].notna()].copy()
                if outcome_complete
                else df.iloc[0:0].copy()
            )
            effect_metrics = _attention_effect_metrics(
                horizon_df,
                selected,
                return_col=return_col,
                top_k=int(top_k),
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                strong_gain_threshold_pct=strong_gain_threshold_pct,
                outcome_complete=outcome_complete,
            )
            universe_count = len(df)
            universe_outcome_count = int(df[return_col].notna().sum())
            rows.append(
                {
                    "horizon": int(horizon),
                    "top_k": int(top_k),
                    "date_count": int(df["date"].nunique()),
                    "evaluated_date_count": evaluated_date_count,
                    "complete_date_count": complete_date_count,
                    "incomplete_date_count": incomplete_date_count,
                    "effect_date_count": (
                        evaluated_date_count if outcome_complete else 0
                    ),
                    "universe_count": universe_count,
                    "universe_outcome_count": universe_outcome_count,
                    "universe_outcome_coverage_pct": _pct(
                        universe_outcome_count / universe_count
                    ),
                    "candidate_count": candidate_count,
                    "candidate_outcome_count": candidate_outcome_count,
                    "candidate_outcome_coverage_pct": _pct(
                        candidate_outcome_count / candidate_count
                    )
                    if candidate_count
                    else float("nan"),
                    "selected_count": selected_count,
                    "selected_outcome_count": int(selected_outcome_count),
                    "selected_outcome_coverage_pct": _pct(
                        selected_outcome_count / selected_count
                    )
                    if selected_count
                    else float("nan"),
                    "outcome_status": "complete" if outcome_complete else "incomplete",
                    "attention_reduction_pct": _pct(
                        1.0 - selected_count / universe_count
                    ),
                    **effect_metrics,
                }
            )
    return pd.DataFrame(rows)


def _attention_effect_metrics(
    horizon_df: pd.DataFrame,
    selected_df: pd.DataFrame,
    *,
    return_col: str,
    top_k: int,
    severe_loss_threshold_pct: float,
    strong_gain_threshold_pct: float,
    outcome_complete: bool,
) -> dict[str, float]:
    columns = (
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "precision_positive_pct",
        "precision_strong_gain_pct",
        "severe_loss_rate_pct",
        "right_tail_capture_pct",
        "future_winner_capture_pct",
    )
    if not outcome_complete:
        return dict.fromkeys(columns, float("nan"))

    strong_total = int(
        (horizon_df[return_col] >= strong_gain_threshold_pct).sum()
    )
    return {
        "mean_forward_excess_return_pct": _mean(selected_df[return_col]),
        "median_forward_excess_return_pct": _median(selected_df[return_col]),
        "precision_positive_pct": _pct((selected_df[return_col] > 0.0).mean()),
        "precision_strong_gain_pct": _pct(
            (selected_df[return_col] >= strong_gain_threshold_pct).mean()
        ),
        "severe_loss_rate_pct": _pct(
            (selected_df[return_col] <= severe_loss_threshold_pct).mean()
        ),
        "right_tail_capture_pct": _pct(
            (
                int(
                    (
                        selected_df[return_col] >= strong_gain_threshold_pct
                    ).sum()
                )
                / strong_total
            )
            if strong_total
            else float("nan")
        ),
        "future_winner_capture_pct": _future_winner_capture_pct(
            horizon_df,
            selected_df,
            return_col=return_col,
            top_k=top_k,
        ),
    }


def _future_winner_capture_pct(
    horizon_df: pd.DataFrame,
    selected_df: pd.DataFrame,
    *,
    return_col: str,
    top_k: int,
) -> float:
    group_columns = ["market_scope", "date"]
    selected_by_scope_date = {
        scope_date: set(group["code"].astype(str))
        for scope_date, group in selected_df.groupby(group_columns, dropna=False)
    }
    captures: list[float] = []
    for scope_date, group in horizon_df.groupby(group_columns, dropna=False):
        future_winners = group.sort_values(
            [return_col, "code"],
            ascending=[False, True],
        ).head(top_k)
        denominator = len(future_winners)
        if denominator == 0:
            continue
        selected_codes = selected_by_scope_date.get(scope_date, set())
        winner_codes = set(future_winners["code"].astype(str))
        captures.append(len(selected_codes.intersection(winner_codes)) / denominator)
    if not captures:
        return float("nan")
    return _pct(float(pd.Series(captures).mean()))


def _build_kill_leakage_df(
    df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    strong_gain_threshold_pct: float,
) -> pd.DataFrame:
    rows = []
    for horizon in horizons:
        return_col = f"forward_close_excess_return_{int(horizon)}d_pct"
        horizon_df = df[df[return_col].notna()]
        killed = horizon_df[horizon_df["triage_bucket"] == "kill"]
        strong_total = int((horizon_df[return_col] >= strong_gain_threshold_pct).sum())
        killed_strong = int((killed[return_col] >= strong_gain_threshold_pct).sum())
        rows.append(
            {
                "horizon": int(horizon),
                "candidate_count": int(len(horizon_df)),
                "killed_count": int(len(killed)),
                "strong_gain_count": strong_total,
                "killed_strong_gain_count": killed_strong,
                "kill_leakage_pct": _pct(killed_strong / strong_total) if strong_total else 0.0,
                "killed_mean_forward_excess_return_pct": _mean(killed[return_col]),
                "killed_median_forward_excess_return_pct": _median(killed[return_col]),
            }
        )
    return pd.DataFrame(rows)


def _build_crowded_vs_neutral_triage_df(
    df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
    strong_gain_threshold_pct: float,
) -> pd.DataFrame:
    rows = []
    for horizon in horizons:
        return_col = f"forward_close_excess_return_{int(horizon)}d_pct"
        horizon_df = df[df[return_col].notna()]
        for (liquidity_regime, triage_bucket), group in horizon_df.groupby(
            ["liquidity_regime", "triage_bucket"],
            dropna=False,
        ):
            rows.append(
                {
                    "horizon": int(horizon),
                    "liquidity_regime": liquidity_regime,
                    "triage_bucket": triage_bucket,
                    "observation_count": int(len(group)),
                    "code_count": int(group["code"].nunique()),
                    "date_count": int(group["date"].nunique()),
                    "mean_forward_excess_return_pct": _mean(group[return_col]),
                    "median_forward_excess_return_pct": _median(group[return_col]),
                    "positive_rate_pct": _pct((group[return_col] > 0.0).mean()),
                    "strong_gain_rate_pct": _pct(
                        (group[return_col] >= strong_gain_threshold_pct).mean()
                    ),
                    "severe_loss_rate_pct": _pct(
                        (group[return_col] <= severe_loss_threshold_pct).mean()
                    ),
                }
            )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(
        ["horizon", "liquidity_regime", "triage_bucket"]
    ).reset_index(drop=True)


def _validate_params(
    *,
    horizons: Sequence[int],
    top_ks: Sequence[int],
    severe_loss_threshold_pct: float,
    strong_gain_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(int(horizon) <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive integers")
    if not top_ks or any(int(top_k) <= 0 for top_k in top_ks):
        raise ValueError("top_ks must contain positive integers")
    if severe_loss_threshold_pct >= 0.0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if strong_gain_threshold_pct <= 0.0:
        raise ValueError("strong_gain_threshold_pct must be positive")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _assert_required_panel_columns(df: pd.DataFrame, *, horizons: Sequence[int]) -> None:
    required = {
        "market_scope",
        "date",
        "code",
        "company_name",
        "liquidity_regime",
        "valuation_signal",
        "strong_value_confirmation",
        "medium_value_confirmation",
        "overvalued_warning",
        "very_overvalued_warning",
        "no_value_confirmation",
        "pbr_percentile",
        "forward_per_percentile",
        "sector_strength_score",
        "long_hybrid_leadership_score",
        "atr20_acceleration_ex_overheat_flag",
        "atr20_to_atr60_overheat_flag",
    }
    required.update(f"forward_close_excess_return_{int(horizon)}d_pct" for horizon in horizons)
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"triage panel is missing required columns: {', '.join(missing)}")


def _parse_optional_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        numeric_value = float(cast(Any, value))
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric_value):
        return None
    return numeric_value


def _pct(value: float) -> float:
    if pd.isna(value):
        return float("nan")
    return round(float(value) * 100.0, 4)


def _mean(series: pd.Series) -> float:
    return round(float(series.mean()), 4) if len(series) else float("nan")


def _median(series: pd.Series) -> float:
    return round(float(series.median()), 4) if len(series) else float("nan")


__all__ = [
    "DEFAULT_START_DATE",
    "DEFAULT_STRONG_GAIN_THRESHOLD_PCT",
    "DEFAULT_TOP_KS",
    "RANKING_DAILY_TRIAGE_LENS_EXPERIMENT_ID",
    "RankingDailyTriageLensResult",
    "build_summary_markdown",
    "run_ranking_daily_triage_lens_from_panel",
    "run_ranking_daily_triage_lens_research",
    "write_ranking_daily_triage_lens_bundle",
]
