"""ATR expansion forward-response research."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast, Iterable, Sequence

import numpy as np
import pandas as pd

from src.domains.analytics.daily_ranking_feature_builders import (
    build_atr_features,
)
from src.domains.analytics.daily_ranking_research_base import (
    DAILY_RANKING_BASE_REQUIRED_VALID_SESSIONS,
    DailyRankingPanelRequest,
    MarketScope,
    attach_daily_ranking_outcomes,
    build_daily_ranking_research_base,
    materialize_daily_ranking_signal_cohort,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)

PUBLIC_FEATURE_BUILDER = build_atr_features
ATR_EXPANSION_FORWARD_RESPONSE_EXPERIMENT_ID = (
    "market-behavior/atr-expansion-forward-response"
)
DEFAULT_ATR_WINDOWS: tuple[int, ...] = (20, 60)
DEFAULT_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 500
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
OVERHEAT_RETURN_20D_THRESHOLD_PCT = 30.0
OUTCOME_COVERAGE_POLICY = "complete_selected_membership_required_per_entry_mode"
_MARKET_SCOPE_ORDER: tuple[str, ...] = ("all", "prime", "standard", "growth", "unknown")
_ENTRY_MODES: tuple[str, ...] = ("close_to_close", "next_open_to_close")
_EXPANSION_BUCKET_ORDER: tuple[str, ...] = (
    "top_10pct",
    "top_20pct",
    "middle_60pct",
    "bottom_20pct",
    "bottom_10pct",
)
_RETURN_REGIME_ORDER: tuple[str, ...] = (
    "persistent_runup",
    "short_pullback_in_uptrend",
    "short_bounce",
    "downtrend_decline",
)
_ATR_EXPANSION_STATE_ORDER: tuple[str, ...] = (
    "dual_expansion",
    "short_atr_expansion",
    "atr20_acceleration",
    "no_expansion",
)
_LIQUIDITY_COLOR_ATR_STATE_ORDER: tuple[str, ...] = (
    "all_atr",
    "overheat_excluded",
    "overheat_only",
    "atr20_acceleration_ex_overheat",
    *_ATR_EXPANSION_STATE_ORDER,
)
_ATR_PAIR_BUCKET_ORDER: tuple[str, ...] = ("high_20pct", "middle_60pct", "low_20pct")


@dataclass(frozen=True)
class AtrExpansionForwardResponseResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    atr_windows: tuple[int, ...]
    return_windows: tuple[int, ...]
    horizons: tuple[int, ...]
    market_scopes: tuple[str, ...]
    min_observations: int
    severe_loss_threshold_pct: float
    outcome_coverage_policy: str
    observation_count: int
    observation_sample_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    atr_expansion_response_df: pd.DataFrame
    return_regime_interaction_df: pd.DataFrame
    atr_pair_interaction_df: pd.DataFrame
    liquidity_color_atr_interaction_df: pd.DataFrame


def run_atr_expansion_forward_response_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    atr_windows: Iterable[int] = DEFAULT_ATR_WINDOWS,
    return_windows: Iterable[int] = DEFAULT_RETURN_WINDOWS,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> AtrExpansionForwardResponseResult:
    resolved_atr_windows = tuple(sorted({int(window) for window in atr_windows}))
    resolved_return_windows = tuple(sorted({int(window) for window in return_windows}))
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    _validate_params(
        atr_windows=resolved_atr_windows,
        return_windows=resolved_return_windows,
        horizons=resolved_horizons,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="atr-expansion-forward-response-",
    ) as ctx:
        market_source = "stock_master_daily_exact_date"
        request = DailyRankingPanelRequest(
            namespace="atr_expansion",
            analysis_start_date=_parse_optional_date(start_date),
            analysis_end_date=_parse_optional_date(end_date),
            horizons=resolved_horizons,
            market_scopes=cast(tuple[MarketScope, ...], resolved_market_scopes),
            include_liquidity=True,
            percentile_features=(),
            required_valid_sessions=max(
                DAILY_RANKING_BASE_REQUIRED_VALID_SESSIONS,
                max(resolved_atr_windows) + 20,
                max(resolved_return_windows) + 1,
            ),
        )
        relations = build_daily_ranking_research_base(ctx.connection, request)
        signal_cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=relations.ranked_signals,
            name="atr_expansion_signals",
        )
        _create_signal_observation_panel(
            ctx.connection,
            signal_name=signal_cohort.name,
            include_all="all" in resolved_market_scopes,
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            signal_cohort,
            relations,
            name="atr_expansion_outcomes",
        )
        _create_observation_panel(
            ctx.connection,
            evaluated_name=evaluated.name,
            include_all="all" in resolved_market_scopes,
        )
        _create_liquidity_color_atr_work(ctx.connection)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM atr_expansion_panel"
            ).fetchone()[0]
        )
        coverage_diagnostics_df = _build_coverage_diagnostics_df(
            ctx.connection,
            horizons=resolved_horizons,
        )
        atr_expansion_response_df = _build_atr_expansion_response_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        return_regime_interaction_df = _build_return_regime_interaction_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        atr_pair_interaction_df = _build_atr_pair_interaction_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        liquidity_color_atr_interaction_df = _build_liquidity_color_atr_interaction_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        observation_sample_df = _query_observation_sample_df(
            ctx.connection,
            limit=observation_sample_limit,
            horizons=resolved_horizons,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    return AtrExpansionForwardResponseResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        market_source=market_source,
        analysis_start_date=_str_or_none(observation_sample_df["date"].min())
        if "date" in observation_sample_df and not observation_sample_df.empty
        else start_date,
        analysis_end_date=end_date,
        atr_windows=resolved_atr_windows,
        return_windows=resolved_return_windows,
        horizons=resolved_horizons,
        market_scopes=resolved_market_scopes,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        outcome_coverage_policy=OUTCOME_COVERAGE_POLICY,
        observation_count=observation_count,
        observation_sample_df=observation_sample_df,
        coverage_diagnostics_df=coverage_diagnostics_df,
        atr_expansion_response_df=atr_expansion_response_df,
        return_regime_interaction_df=return_regime_interaction_df,
        atr_pair_interaction_df=atr_pair_interaction_df,
        liquidity_color_atr_interaction_df=liquidity_color_atr_interaction_df,
    )


def write_atr_expansion_forward_response_bundle(
    result: AtrExpansionForwardResponseResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=ATR_EXPANSION_FORWARD_RESPONSE_EXPERIMENT_ID,
        module=__name__,
        function="run_atr_expansion_forward_response_research",
        params={
            "atr_windows": list(result.atr_windows),
            "return_windows": list(result.return_windows),
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "min_observations": result.min_observations,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "outcome_coverage_policy": result.outcome_coverage_policy,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
            "observation_sample_count": int(len(result.observation_sample_df)),
            "outcome_coverage_policy": result.outcome_coverage_policy,
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "atr_expansion_response_df": result.atr_expansion_response_df,
            "return_regime_interaction_df": result.return_regime_interaction_df,
            "atr_pair_interaction_df": result.atr_pair_interaction_df,
            "liquidity_color_atr_interaction_df": (
                result.liquidity_color_atr_interaction_df
            ),
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: AtrExpansionForwardResponseResult) -> str:
    coverage = _top_rows_for_markdown(result.coverage_diagnostics_df, limit=24)
    atr_response = _top_rows_for_markdown(
        result.atr_expansion_response_df,
        sort_columns=[
            "market_scope",
            "atr_feature",
            "expansion_bucket_order",
            "entry_mode",
            "horizon",
        ],
        limit=80,
    )
    return_regime = _top_rows_for_markdown(
        result.return_regime_interaction_df,
        sort_columns=[
            "market_scope",
            "return_regime_order",
            "atr_expansion_state_order",
            "entry_mode",
            "horizon",
        ],
        limit=80,
    )
    atr_pair = _top_rows_for_markdown(
        result.atr_pair_interaction_df,
        sort_columns=[
            "market_scope",
            "atr20_bucket_order",
            "atr60_bucket_order",
            "entry_mode",
            "horizon",
        ],
        limit=80,
    )
    liquidity_color_atr = _top_rows_for_markdown(
        result.liquidity_color_atr_interaction_df,
        sort_columns=[
            "market_scope",
            "liquidity_regime_order",
            "ui_color_order",
            "atr_expansion_state_order",
            "entry_mode",
            "horizon",
        ],
        limit=100,
    )
    return "\n".join(
        [
            "# ATR Expansion Forward Response",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Market source: `{result.market_source}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- Observation count: `{result.observation_count}`",
            f"- ATR windows: `{list(result.atr_windows)}`",
            f"- Return windows: `{list(result.return_windows)}`",
            f"- Forward horizons: `{list(result.horizons)}`",
            f"- Market scopes: `{list(result.market_scopes)}`",
            f"- Min observations: `{result.min_observations}`",
            "",
            "## Coverage Diagnostics",
            "",
            coverage,
            "",
            "## ATR Expansion Response",
            "",
            atr_response,
            "",
            "## Return Regime Interaction",
            "",
            return_regime,
            "",
            "## ATR Pair Interaction",
            "",
            atr_pair,
            "",
            "## Liquidity Color ATR Interaction",
            "",
            liquidity_color_atr,
            "",
        ]
    )


def _validate_params(
    *,
    atr_windows: Sequence[int],
    return_windows: Sequence[int],
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not atr_windows or any(window <= 1 for window in atr_windows):
        raise ValueError("atr_windows must be greater than 1")
    if set(atr_windows) != {20, 60}:
        raise ValueError("atr_windows must include 20 and 60")
    if not return_windows or any(window <= 0 for window in return_windows):
        raise ValueError("return_windows must be positive")
    if not {20, 60}.issubset(set(return_windows)):
        raise ValueError("return_windows must include 20 and 60")
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if severe_loss_threshold_pct >= 0.0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _create_signal_observation_panel(
    conn: Any,
    *,
    signal_name: str,
    include_all: bool,
) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE atr_expansion_signal_panel AS
        SELECT
            signal.*,
            signal.forecast_per AS forward_per,
            signal.forecast_per_percentile AS forward_per_percentile,
            signal.forecast_per_to_per_ratio AS forward_per_to_per_ratio
        FROM {signal_name} AS signal
        """
    )
    _create_scoped_view(
        conn,
        panel_name="atr_expansion_signal_panel",
        view_name="atr_expansion_signal_scoped",
        include_all=include_all,
    )


def _create_observation_panel(
    conn: Any,
    *,
    evaluated_name: str,
    include_all: bool,
) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE atr_expansion_panel AS
        SELECT
            evaluated.*,
            evaluated.forecast_per AS forward_per,
            evaluated.forecast_per_percentile AS forward_per_percentile,
            evaluated.forecast_per_to_per_ratio AS forward_per_to_per_ratio
        FROM {evaluated_name}
        AS evaluated
        """
    )
    _create_scoped_view(
        conn,
        panel_name="atr_expansion_panel",
        view_name="atr_expansion_scoped",
        include_all=include_all,
    )


def _create_scoped_view(
    conn: Any,
    *,
    panel_name: str,
    view_name: str,
    include_all: bool,
) -> None:
    all_union = (
        """
        UNION ALL
        SELECT * REPLACE ('all' AS market_scope)
        FROM base
        """
        if include_all
        else ""
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        WITH base AS (
            SELECT
                p.*,
                strftime(CAST(p.date AS DATE), '%Y') AS anchor_year,
                strftime(CAST(p.date AS DATE), '%Y-%m') AS anchor_month
            FROM {panel_name} p
        )
        SELECT *
        FROM base
        {all_union}
        """
    )


def _create_liquidity_color_atr_work(conn: Any) -> None:
    color_selects: list[str] = []
    for regime_order, (regime, ui_colors) in enumerate(_liquidity_color_sql().items()):
        for color_order, (ui_color, color_sql) in enumerate(ui_colors.items()):
            local_color_sql = color_sql.replace("r.", "a.")
            color_selects.append(
                f"""
                SELECT
                    a.*,
                    '{regime}' AS liquidity_regime,
                    {regime_order} AS liquidity_regime_order,
                    '{ui_color}' AS ui_color,
                    {color_order} AS ui_color_order
                FROM atr_expansion_signal_scoped a
                WHERE a.liquidity_regime = '{regime}'
                  AND {local_color_sql}
                """
            )
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE atr_liquidity_color_work AS\n"
        + "\nUNION ALL\n".join(color_selects)
    )


def _build_coverage_diagnostics_df(
    conn: Any,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    queries = [
        f"""
        SELECT
            market AS market_scope,
            '{entry_mode}' AS entry_mode,
            {int(horizon)} AS horizon,
            count(*) AS selected_observation_count,
            count({return_column}) AS complete_outcome_count,
            count(*) - count({return_column}) AS incomplete_outcome_count,
            CASE WHEN count(*) = count({return_column})
                 THEN 'complete' ELSE 'incomplete' END AS outcome_coverage_status,
            '{OUTCOME_COVERAGE_POLICY}' AS outcome_coverage_policy,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            min(date) AS min_date,
            max(date) AS max_date,
            avg(CASE WHEN atr20_pct IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_coverage_pct,
            avg(CASE WHEN atr60_pct IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr60_coverage_pct,
            avg(CASE WHEN atr20_to_atr60 IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_to_atr60_coverage_pct,
            avg(CASE WHEN atr20_change_20d_pct IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_change_20d_coverage_pct
        FROM atr_expansion_panel
        GROUP BY market
        """
        for horizon in horizons
        for entry_mode, return_column in (
            ("close_to_close", f"forward_close_excess_return_{int(horizon)}d_pct"),
            (
                "next_open_to_close",
                f"forward_next_open_excess_return_{int(horizon)}d_pct",
            ),
        )
    ]
    frame = conn.execute("\nUNION ALL\n".join(queries)).fetchdf()
    return _sort_summary_df(
        frame,
        columns=[
            "market_scope",
            "entry_mode",
            "horizon",
            "selected_observation_count",
            "complete_outcome_count",
            "incomplete_outcome_count",
            "outcome_coverage_status",
            "outcome_coverage_policy",
            "observation_count",
            "code_count",
            "date_count",
            "min_date",
            "max_date",
            "atr20_coverage_pct",
            "atr60_coverage_pct",
            "atr20_to_atr60_coverage_pct",
            "atr20_change_20d_coverage_pct",
        ],
    )


def _build_atr_expansion_response_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for feature in ("atr20_pct", "atr60_pct", "atr20_to_atr60", "atr20_change_20d_pct"):
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW atr_expansion_percentile_work AS
            SELECT
                *,
                percent_rank() OVER (
                    PARTITION BY market_scope, anchor_year
                    ORDER BY {feature}
                ) AS atr_feature_rank_pct
            FROM atr_expansion_signal_scoped
            WHERE {feature} IS NOT NULL
            """
        )
        for bucket in _EXPANSION_BUCKET_ORDER:
            for entry_mode in _ENTRY_MODES:
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name="atr_expansion_percentile_work",
                            condition=_percentile_condition(bucket),
                            condition_fields={
                                "condition_family": "annual_atr_percentile_bucket",
                                "atr_feature": feature,
                                "expansion_bucket": bucket,
                                "expansion_bucket_order": _EXPANSION_BUCKET_ORDER.index(
                                    bucket
                                ),
                                "entry_mode": entry_mode,
                                "horizon": int(horizon),
                            },
                            return_column=_return_column(entry_mode, horizon),
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_atr_expansion_response_columns())


def _build_return_regime_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for return_regime in _RETURN_REGIME_ORDER:
        for atr_state in _ATR_EXPANSION_STATE_ORDER:
            for entry_mode in _ENTRY_MODES:
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            condition=(
                                f"({_return_regime_condition(return_regime)}) "
                                f"AND ({_atr_expansion_state_condition(atr_state)})"
                            ),
                            condition_fields={
                                "condition_family": "return_regime_atr_expansion",
                                "return_regime": return_regime,
                                "return_regime_order": _RETURN_REGIME_ORDER.index(
                                    return_regime
                                ),
                                "atr_expansion_state": atr_state,
                                "atr_expansion_state_order": _ATR_EXPANSION_STATE_ORDER.index(
                                    atr_state
                                ),
                                "entry_mode": entry_mode,
                                "horizon": int(horizon),
                            },
                            return_column=_return_column(entry_mode, horizon),
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_return_regime_interaction_columns())


def _build_atr_pair_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    conn.execute(
        """
        CREATE OR REPLACE TEMP VIEW atr_pair_percentile_work AS
        SELECT
            *,
            percent_rank() OVER (
                PARTITION BY market_scope, anchor_year
                ORDER BY atr20_pct
            ) AS atr20_rank_pct,
            percent_rank() OVER (
                PARTITION BY market_scope, anchor_year
                ORDER BY atr60_pct
            ) AS atr60_rank_pct
        FROM atr_expansion_signal_scoped
        WHERE atr20_pct IS NOT NULL
          AND atr60_pct IS NOT NULL
        """
    )
    frames: list[pd.DataFrame] = []
    for atr20_bucket in _ATR_PAIR_BUCKET_ORDER:
        for atr60_bucket in _ATR_PAIR_BUCKET_ORDER:
            condition = (
                f"{_pair_bucket_condition('atr20_rank_pct', atr20_bucket)} "
                f"AND {_pair_bucket_condition('atr60_rank_pct', atr60_bucket)}"
            )
            for entry_mode in _ENTRY_MODES:
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name="atr_pair_percentile_work",
                            condition=condition,
                            condition_fields={
                                "condition_family": "atr20_atr60_pair",
                                "atr20_bucket": atr20_bucket,
                                "atr20_bucket_order": _ATR_PAIR_BUCKET_ORDER.index(
                                    atr20_bucket
                                ),
                                "atr60_bucket": atr60_bucket,
                                "atr60_bucket_order": _ATR_PAIR_BUCKET_ORDER.index(
                                    atr60_bucket
                                ),
                                "entry_mode": entry_mode,
                                "horizon": int(horizon),
                            },
                            return_column=_return_column(entry_mode, horizon),
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_atr_pair_interaction_columns())


def _build_liquidity_color_atr_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for state in _LIQUIDITY_COLOR_ATR_STATE_ORDER:
        for entry_mode in _ENTRY_MODES:
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        source_name="atr_liquidity_color_work",
                        condition=_liquidity_color_atr_state_condition(state),
                        condition_fields={
                            "condition_family": "liquidity_color_atr_expansion",
                            "atr_expansion_state": state,
                            "atr_expansion_state_order": _LIQUIDITY_COLOR_ATR_STATE_ORDER.index(
                                state
                            ),
                            "entry_mode": entry_mode,
                            "horizon": int(horizon),
                        },
                        return_column=_return_column(entry_mode, horizon),
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                        group_columns=[
                            "market_scope",
                            "liquidity_regime",
                            "liquidity_regime_order",
                            "ui_color",
                            "ui_color_order",
                        ],
                    )
                )
    return _concat_sorted(frames, columns=_liquidity_color_atr_interaction_columns())


def _aggregate_condition(
    conn: Any,
    *,
    condition: str,
    condition_fields: dict[str, Any],
    return_column: str,
    min_observations: int,
    severe_loss_threshold_pct: float,
    source_name: str = "atr_expansion_signal_scoped",
    group_columns: Sequence[str] = ("market_scope",),
) -> pd.DataFrame:
    group_select = ",\n                ".join(
        f"selected.{column} AS {column}" for column in group_columns
    )
    group_by = ", ".join(f"selected.{column}" for column in group_columns)
    coverage_gate = (
        "selected_observation_count = complete_outcome_count "
        "AND selected_observation_count >= ?"
    )
    frame = conn.execute(
        f"""
        WITH selected_membership AS (
            SELECT *
            FROM {source_name}
            WHERE {condition}
        ),
        aggregated AS (
            SELECT
                {group_select},
                count(*) AS selected_observation_count,
                count(evaluated.{return_column}) AS complete_outcome_count,
                count(*) - count(evaluated.{return_column})
                    AS incomplete_outcome_count,
                count(evaluated.{return_column}) AS observation_count,
                count(DISTINCT CASE WHEN evaluated.{return_column} IS NOT NULL
                                    THEN selected.code END) AS code_count,
                count(DISTINCT CASE WHEN evaluated.{return_column} IS NOT NULL
                                    THEN selected.date END) AS date_count,
                avg(evaluated.{return_column}) AS raw_mean_forward_excess_return_pct,
                median(evaluated.{return_column})
                    AS raw_median_forward_excess_return_pct,
                quantile_cont(evaluated.{return_column}, 0.10)
                    AS raw_p10_forward_excess_return_pct,
                quantile_cont(evaluated.{return_column}, 0.25)
                    AS raw_p25_forward_excess_return_pct,
                quantile_cont(evaluated.{return_column}, 0.75)
                    AS raw_p75_forward_excess_return_pct,
                quantile_cont(evaluated.{return_column}, 0.90)
                    AS raw_p90_forward_excess_return_pct,
                avg(CASE WHEN evaluated.{return_column} > 0 THEN 1.0
                         WHEN evaluated.{return_column} IS NOT NULL THEN 0.0 END) * 100.0
                    AS raw_win_rate_pct,
                avg(CASE WHEN evaluated.{return_column} <= ? THEN 1.0
                         WHEN evaluated.{return_column} IS NOT NULL THEN 0.0 END) * 100.0
                    AS raw_severe_loss_rate_pct,
                median(selected.recent_return_20d_pct)
                    AS median_recent_return_20d_pct,
                median(selected.recent_return_60d_pct)
                    AS median_recent_return_60d_pct,
                median(selected.atr20_pct) AS median_atr20_pct,
                median(selected.atr60_pct) AS median_atr60_pct,
                median(selected.atr20_to_atr60) AS median_atr20_to_atr60,
                median(selected.atr20_change_20d_pct)
                    AS median_atr20_change_20d_pct,
                median(selected.med_adv60_jpy) / 1000000.0
                    AS median_med_adv60_mil_jpy
            FROM selected_membership selected
            LEFT JOIN atr_expansion_scoped evaluated
              ON evaluated.code = selected.code
             AND evaluated.date = selected.date
             AND evaluated.market_scope = selected.market_scope
            GROUP BY {group_by}
        )
        SELECT
            * EXCLUDE (
                raw_mean_forward_excess_return_pct,
                raw_median_forward_excess_return_pct,
                raw_p10_forward_excess_return_pct,
                raw_p25_forward_excess_return_pct,
                raw_p75_forward_excess_return_pct,
                raw_p90_forward_excess_return_pct,
                raw_win_rate_pct,
                raw_severe_loss_rate_pct
            ),
            CASE
                WHEN selected_observation_count <> complete_outcome_count
                    THEN 'incomplete'
                WHEN selected_observation_count < ? THEN 'insufficient_observations'
                ELSE 'complete'
            END AS outcome_coverage_status,
            '{OUTCOME_COVERAGE_POLICY}' AS outcome_coverage_policy,
            CASE WHEN {coverage_gate}
                 THEN raw_mean_forward_excess_return_pct END
                AS mean_forward_excess_return_pct,
            CASE WHEN {coverage_gate}
                 THEN raw_median_forward_excess_return_pct END
                AS median_forward_excess_return_pct,
            CASE WHEN {coverage_gate}
                 THEN raw_p10_forward_excess_return_pct END
                AS p10_forward_excess_return_pct,
            CASE WHEN {coverage_gate}
                 THEN raw_p25_forward_excess_return_pct END
                AS p25_forward_excess_return_pct,
            CASE WHEN {coverage_gate}
                 THEN raw_p75_forward_excess_return_pct END
                AS p75_forward_excess_return_pct,
            CASE WHEN {coverage_gate}
                 THEN raw_p90_forward_excess_return_pct END
                AS p90_forward_excess_return_pct,
            CASE WHEN {coverage_gate} THEN raw_win_rate_pct END AS win_rate_pct,
            CASE WHEN {coverage_gate}
                 THEN raw_severe_loss_rate_pct END AS severe_loss_rate_pct
        FROM aggregated
        """,
        [
            float(severe_loss_threshold_pct),
            int(min_observations),
            *[int(min_observations)] * 8,
        ],
    ).fetchdf()
    if frame.empty:
        return frame
    for column, value in condition_fields.items():
        frame[column] = value
    ordered = [*condition_fields.keys(), *group_columns]
    ordered.extend(
        column for column in _base_response_columns() if column not in group_columns
    )
    return frame.reindex(columns=ordered)


def _query_observation_sample_df(
    conn: Any,
    *,
    limit: int,
    horizons: Sequence[int],
) -> pd.DataFrame:
    return_columns = ",\n            ".join(
        [
            *[f"forward_close_excess_return_{horizon}d_pct" for horizon in horizons],
            *[
                f"forward_next_open_excess_return_{horizon}d_pct"
                for horizon in horizons
            ],
        ]
    )
    return conn.execute(
        f"""
        SELECT
            date,
            code,
            company_name,
            market,
            market_code,
            scale_category,
            close,
            med_adv60_jpy / 1000000.0 AS med_adv60_mil_jpy,
            atr20_pct,
            atr60_pct,
            atr20_to_atr60,
            atr20_change_20d_pct,
            recent_return_20d_pct,
            recent_return_60d_pct,
            {return_columns}
        FROM atr_expansion_panel
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _percentile_condition(bucket: str) -> str:
    if bucket == "top_10pct":
        return "atr_feature_rank_pct >= 0.9"
    if bucket == "top_20pct":
        return "atr_feature_rank_pct >= 0.8 AND atr_feature_rank_pct < 0.9"
    if bucket == "middle_60pct":
        return "atr_feature_rank_pct > 0.2 AND atr_feature_rank_pct < 0.8"
    if bucket == "bottom_20pct":
        return "atr_feature_rank_pct > 0.1 AND atr_feature_rank_pct <= 0.2"
    if bucket == "bottom_10pct":
        return "atr_feature_rank_pct <= 0.1"
    raise ValueError(f"unsupported expansion bucket: {bucket}")


def _return_regime_condition(regime: str) -> str:
    if regime == "persistent_runup":
        return "recent_return_20d_pct > 0 AND recent_return_60d_pct > 0"
    if regime == "short_pullback_in_uptrend":
        return "recent_return_20d_pct <= 0 AND recent_return_60d_pct > 0"
    if regime == "short_bounce":
        return "recent_return_20d_pct > 0 AND recent_return_60d_pct <= 0"
    if regime == "downtrend_decline":
        return "recent_return_20d_pct <= 0 AND recent_return_60d_pct <= 0"
    raise ValueError(f"unsupported return_regime: {regime}")


def _atr_expansion_state_condition(state: str) -> str:
    ratio_expansion = "atr20_to_atr60 >= 1.25"
    acceleration = "atr20_change_20d_pct >= 25.0"
    if state == "dual_expansion":
        return f"{ratio_expansion} AND {acceleration}"
    if state == "short_atr_expansion":
        return f"{ratio_expansion} AND NOT ({acceleration})"
    if state == "atr20_acceleration":
        return f"NOT ({ratio_expansion}) AND {acceleration}"
    if state == "no_expansion":
        return f"NOT ({ratio_expansion}) AND NOT ({acceleration})"
    raise ValueError(f"unsupported atr_expansion_state: {state}")


def _liquidity_color_atr_state_condition(state: str) -> str:
    if state == "all_atr":
        return "TRUE"
    if state == "overheat_excluded":
        return f"recent_return_20d_pct < {OVERHEAT_RETURN_20D_THRESHOLD_PCT}"
    if state == "overheat_only":
        return f"recent_return_20d_pct >= {OVERHEAT_RETURN_20D_THRESHOLD_PCT}"
    if state == "atr20_acceleration_ex_overheat":
        return (
            f"recent_return_20d_pct < {OVERHEAT_RETURN_20D_THRESHOLD_PCT} "
            f"AND ({_atr_expansion_state_condition('atr20_acceleration')})"
        )
    return _atr_expansion_state_condition(state)


def _liquidity_color_sql() -> dict[str, dict[str, str]]:
    strong_value = (
        "(r.pbr_percentile <= 0.2 AND r.forward_per_percentile <= 0.2) "
        "OR (r.per_percentile <= 0.2 AND r.forward_per_to_per_ratio <= 0.8)"
    )
    neutral_green = "r.per_percentile <= 0.2 AND r.forward_per_to_per_ratio <= 0.8"
    medium_value = (
        "r.pbr_percentile <= 0.2 "
        "OR (r.per_percentile <= 0.2 AND r.forward_per_to_per_ratio <= 1.0)"
    )
    return {
        "crowded_rerating": {
            "green": f"({strong_value})",
            "blue": f"({medium_value}) AND NOT ({strong_value})",
            "yellow": f"NOT ({medium_value})",
        },
        "neutral_rerating": {
            "green": f"({neutral_green})",
            "blue": f"NOT ({neutral_green})",
        },
    }


def _pair_bucket_condition(rank_column: str, bucket: str) -> str:
    if bucket == "high_20pct":
        return f"{rank_column} >= 0.8"
    if bucket == "middle_60pct":
        return f"{rank_column} > 0.2 AND {rank_column} < 0.8"
    if bucket == "low_20pct":
        return f"{rank_column} <= 0.2"
    raise ValueError(f"unsupported pair bucket: {bucket}")


def _return_column(entry_mode: str, horizon: int) -> str:
    if entry_mode == "close_to_close":
        return f"forward_close_excess_return_{horizon}d_pct"
    if entry_mode == "next_open_to_close":
        return f"forward_next_open_excess_return_{horizon}d_pct"
    raise ValueError(f"unsupported entry_mode: {entry_mode}")


def _parse_optional_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _str_or_none(value: Any) -> str | None:
    return None if value is None or pd.isna(value) else str(value)


def _sort_summary_df(frame: pd.DataFrame, *, columns: Sequence[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=list(columns))
    sort_columns = [
        column
        for column in ("market_scope", "min_date", "atr_feature", "horizon")
        if column in frame.columns
    ]
    result = frame.reindex(columns=list(columns))
    return result.sort_values(sort_columns, kind="stable") if sort_columns else result


def _top_rows_for_markdown(
    frame: pd.DataFrame,
    *,
    limit: int,
    sort_columns: Sequence[str] = (),
) -> str:
    if frame.empty:
        return "_No rows._"
    available = [column for column in sort_columns if column in frame.columns]
    ordered = frame.sort_values(available, kind="stable") if available else frame
    return "```text\n" + ordered.head(int(limit)).to_string(index=False) + "\n```"


def _concat_sorted(
    frames: Sequence[pd.DataFrame],
    *,
    columns: Sequence[str],
) -> pd.DataFrame:
    non_empty = [frame for frame in frames if frame is not None and not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    result = pd.concat(non_empty, ignore_index=True, sort=False)
    for column in columns:
        if column not in result.columns:
            result[column] = np.nan
    return _sort_summary_df(result, columns=list(columns))


def _base_response_columns() -> list[str]:
    return [
        "market_scope",
        "selected_observation_count",
        "complete_outcome_count",
        "incomplete_outcome_count",
        "outcome_coverage_status",
        "outcome_coverage_policy",
        "observation_count",
        "code_count",
        "date_count",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "p10_forward_excess_return_pct",
        "p25_forward_excess_return_pct",
        "p75_forward_excess_return_pct",
        "p90_forward_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_atr20_pct",
        "median_atr60_pct",
        "median_atr20_to_atr60",
        "median_atr20_change_20d_pct",
        "median_med_adv60_mil_jpy",
    ]


def _atr_expansion_response_columns() -> list[str]:
    return [
        "condition_family",
        "atr_feature",
        "expansion_bucket",
        "expansion_bucket_order",
        "entry_mode",
        "horizon",
        *_base_response_columns(),
    ]


def _return_regime_interaction_columns() -> list[str]:
    return [
        "condition_family",
        "return_regime",
        "return_regime_order",
        "atr_expansion_state",
        "atr_expansion_state_order",
        "entry_mode",
        "horizon",
        *_base_response_columns(),
    ]


def _atr_pair_interaction_columns() -> list[str]:
    return [
        "condition_family",
        "atr20_bucket",
        "atr20_bucket_order",
        "atr60_bucket",
        "atr60_bucket_order",
        "entry_mode",
        "horizon",
        *_base_response_columns(),
    ]


def _liquidity_color_atr_interaction_columns() -> list[str]:
    return [
        "condition_family",
        "atr_expansion_state",
        "atr_expansion_state_order",
        "entry_mode",
        "horizon",
        "market_scope",
        "liquidity_regime",
        "liquidity_regime_order",
        "ui_color",
        "ui_color_order",
        *[
            column
            for column in _base_response_columns()
            if column
            not in {
                "market_scope",
                "liquidity_regime",
                "liquidity_regime_order",
                "ui_color",
                "ui_color_order",
            }
        ],
    ]
