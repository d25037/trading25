"""Position-state excess evidence for Daily Ranking SMA5 diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast, Sequence

import numpy as np
import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    LONG_SCAFFOLDS,
    compose_daily_ranking_signal_features,
    table_exists,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    AtrFeaturesRequest,
    LongLeadershipFeaturesRequest,
    LongScaffoldFeaturesRequest,
    SectorStrengthFeaturesRequest,
    ShortScaffoldFeaturesRequest,
    SmaFeaturesRequest,
    build_atr_features,
    build_long_leadership_features,
    build_long_scaffold_features,
    build_sector_strength_features,
    build_short_scaffold_features,
    build_sma_features,
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
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)

PUBLIC_FEATURE_BUILDER = build_sma_features
RANKING_SMA5_POSITION_STATE_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-sma5-position-state-evidence"
)
DEFAULT_MIN_POSITION_DAYS = 30
DEFAULT_MIN_TRADES = 10
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_LONG_SCAFFOLDS = LONG_SCAFFOLDS
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data_raw",
    "stock_adjustment_bases",
    "stock_adjustment_basis_segments",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "indices_data",
    "index_master",
)
_DEFAULT_POSITION_LONG_SCAFFOLDS: tuple[str, ...] = (
    "deep_value_long_hybrid_atr20_accel",
    "neutral_deep_value_long_hybrid_atr20_accel",
    "neutral_deep_value_sector_strong_atr20_accel",
    "crowded_low10_pbr_forward_per_atr20_accel",
)
_ENTRY_RULES: tuple[str, ...] = (
    "no_entry_filter",
    "avoid_atr20_above_ge1",
)
_EXIT_RULES: tuple[str, ...] = (
    "count_0_1",
    "below_sma5_streak_ge3",
    "atr20_below_le_neg1",
    "combined_count_streak_atr",
)


@dataclass(frozen=True)
class RankingSma5PositionStateEvidenceResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    market_scopes: tuple[str, ...]
    long_scaffolds: tuple[str, ...]
    entry_rules: tuple[str, ...]
    exit_rules: tuple[str, ...]
    min_position_days: int
    min_trades: int
    severe_loss_threshold_pct: float
    required_tables: tuple[str, ...]
    observation_count: int
    position_day_count: int
    trade_count: int
    observation_sample_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    entry_rule_evidence_df: pd.DataFrame
    position_state_daily_evidence_df: pd.DataFrame
    position_state_trade_evidence_df: pd.DataFrame
    exit_reason_evidence_df: pd.DataFrame
    rotation_evidence_df: pd.DataFrame


def run_ranking_sma5_position_state_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    long_scaffolds: Sequence[str] = _DEFAULT_POSITION_LONG_SCAFFOLDS,
    entry_rules: Sequence[str] = _ENTRY_RULES,
    exit_rules: Sequence[str] = _EXIT_RULES,
    min_position_days: int = DEFAULT_MIN_POSITION_DAYS,
    min_trades: int = DEFAULT_MIN_TRADES,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingSma5PositionStateEvidenceResult:
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    resolved_long_scaffolds = tuple(dict.fromkeys(str(item) for item in long_scaffolds))
    resolved_entry_rules = tuple(dict.fromkeys(str(item) for item in entry_rules))
    resolved_exit_rules = tuple(dict.fromkeys(str(item) for item in exit_rules))
    _validate_params(
        long_scaffolds=resolved_long_scaffolds,
        entry_rules=resolved_entry_rules,
        exit_rules=resolved_exit_rules,
        min_position_days=min_position_days,
        min_trades=min_trades,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    market_source = "stock_master_daily_exact_date"

    source_mode: SourceMode
    source_detail: str
    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-sma5-position-state-evidence-",
    ) as ctx:
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail
        _assert_required_tables(ctx.connection)
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="sma5_position_state",
                analysis_start_date=None
                if start_date is None
                else date.fromisoformat(start_date),
                analysis_end_date=None
                if end_date is None
                else date.fromisoformat(end_date),
                horizons=(1,),
                market_scopes=cast(tuple[MarketScope, ...], resolved_market_scopes),
                include_liquidity=True,
                percentile_features=(),
            ),
        )
        signal_source = relations.ranked_signals
        atr_features = build_atr_features(
            ctx.connection,
            AtrFeaturesRequest(source=signal_source, namespace="position_atr"),
        )
        short_features = build_short_scaffold_features(
            ctx.connection,
            ShortScaffoldFeaturesRequest(
                source=signal_source,
                atr_features=atr_features,
                namespace="position_short",
            ),
        )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="position_sector",
            ),
        )
        leadership_features = build_long_leadership_features(
            ctx.connection,
            LongLeadershipFeaturesRequest(
                source=signal_source,
                sector_features=sector_features,
                namespace="position_leadership",
                leadership_windows=_LEADERSHIP_WINDOWS,
            ),
        )
        long_features = build_long_scaffold_features(
            ctx.connection,
            LongScaffoldFeaturesRequest(
                source=signal_source,
                leadership_features=leadership_features,
                short_scaffold_features=short_features,
                namespace="position_long",
            ),
        )
        sma_features = build_sma_features(
            ctx.connection,
            SmaFeaturesRequest(
                source=signal_source,
                price_history=relations.price_history,
                namespace="position_sma",
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(long_features, sma_features),
            namespace="sma5_position_state",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="position_state_timeline",
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="position_state_outcomes",
        )
        _create_position_feature_panel(ctx.connection, source_name=evaluated.name)
        feature_df = _query_position_feature_df(
            ctx.connection,
            long_scaffolds=resolved_long_scaffolds,
        )

    position_observation_df, position_daily_df, trade_df, exit_event_df = (
        _simulate_position_states(
            feature_df,
            long_scaffolds=resolved_long_scaffolds,
            entry_rules=resolved_entry_rules,
            exit_rules=resolved_exit_rules,
            observation_sample_limit=observation_sample_limit,
        )
    )
    coverage_df = _build_coverage_diagnostics_df(
        feature_df,
        long_scaffolds=resolved_long_scaffolds,
    )
    entry_rule_df = _build_entry_rule_evidence_df(
        feature_df,
        long_scaffolds=resolved_long_scaffolds,
        entry_rules=resolved_entry_rules,
        min_position_days=min_position_days,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    daily_evidence_df = _build_position_state_daily_evidence_df(
        position_daily_df,
        min_position_days=min_position_days,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    trade_evidence_df = _build_position_state_trade_evidence_df(
        trade_df,
        min_trades=min_trades,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    exit_reason_df = _build_exit_reason_evidence_df(
        exit_event_df,
        min_trades=min_trades,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    rotation_evidence_df = _build_rotation_evidence_df(
        feature_df,
        exit_event_df,
        long_scaffolds=resolved_long_scaffolds,
        entry_rules=resolved_entry_rules,
        min_trades=min_trades,
    )

    return RankingSma5PositionStateEvidenceResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        market_source=market_source,
        analysis_start_date=start_date,
        analysis_end_date=end_date,
        market_scopes=resolved_market_scopes,
        long_scaffolds=resolved_long_scaffolds,
        entry_rules=resolved_entry_rules,
        exit_rules=resolved_exit_rules,
        min_position_days=int(min_position_days),
        min_trades=int(min_trades),
        severe_loss_threshold_pct=float(severe_loss_threshold_pct),
        required_tables=_REQUIRED_TABLES,
        observation_count=int(len(feature_df)),
        position_day_count=int(len(position_daily_df)),
        trade_count=int(len(trade_df)),
        observation_sample_df=position_observation_df.head(
            int(observation_sample_limit)
        ).reset_index(drop=True),
        coverage_diagnostics_df=coverage_df,
        entry_rule_evidence_df=entry_rule_df,
        position_state_daily_evidence_df=daily_evidence_df,
        position_state_trade_evidence_df=trade_evidence_df,
        exit_reason_evidence_df=exit_reason_df,
        rotation_evidence_df=rotation_evidence_df,
    )


def write_ranking_sma5_position_state_evidence_bundle(
    result: RankingSma5PositionStateEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_SMA5_POSITION_STATE_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_sma5_position_state_evidence",
        function="run_ranking_sma5_position_state_evidence_research",
        params={
            "market_scopes": list(result.market_scopes),
            "long_scaffolds": list(result.long_scaffolds),
            "entry_rules": list(result.entry_rules),
            "exit_rules": list(result.exit_rules),
            "min_position_days": result.min_position_days,
            "min_trades": result.min_trades,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "required_tables": list(result.required_tables),
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": str(result.source_mode),
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
            "position_day_count": result.position_day_count,
            "trade_count": result.trade_count,
            "primary_outcome": "next_session close-to-close TOPIX excess while held_state=true",
            "entry_timing": "close signal enters the next close-to-close interval",
            "exit_timing": "close signal exits before the next close-to-close interval",
        },
        result_tables={
            "observation_sample_df": result.observation_sample_df,
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "entry_rule_evidence_df": result.entry_rule_evidence_df,
            "position_state_daily_evidence_df": result.position_state_daily_evidence_df,
            "position_state_trade_evidence_df": result.position_state_trade_evidence_df,
            "exit_reason_evidence_df": result.exit_reason_evidence_df,
            "rotation_evidence_df": result.rotation_evidence_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingSma5PositionStateEvidenceResult) -> str:
    parts = [
        "# Ranking SMA5 Position State Evidence",
        "",
        "## Metadata",
        "",
        f"- db_path: `{result.db_path}`",
        f"- source_mode: `{result.source_mode}`",
        f"- source_detail: `{result.source_detail}`",
        f"- market_source: `{result.market_source}`",
        f"- analysis_start_date: `{result.analysis_start_date}`",
        f"- analysis_end_date: `{result.analysis_end_date}`",
        f"- market_scopes: `{', '.join(result.market_scopes)}`",
        f"- long_scaffolds: `{', '.join(result.long_scaffolds)}`",
        f"- entry_rules: `{', '.join(result.entry_rules)}`",
        f"- exit_rules: `{', '.join(result.exit_rules)}`",
        f"- observation_count: `{result.observation_count}`",
        f"- position_day_count: `{result.position_day_count}`",
        f"- trade_count: `{result.trade_count}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=120),
        "",
        "## Entry Rule Evidence",
        "",
        _top_rows_for_markdown(result.entry_rule_evidence_df, limit=160),
        "",
        "## Position State Daily Evidence",
        "",
        _top_rows_for_markdown(result.position_state_daily_evidence_df, limit=220),
        "",
        "## Position State Trade Evidence",
        "",
        _top_rows_for_markdown(result.position_state_trade_evidence_df, limit=220),
        "",
        "## Exit Reason Evidence",
        "",
        _top_rows_for_markdown(result.exit_reason_evidence_df, limit=160),
        "",
        "## Rotation Evidence",
        "",
        _top_rows_for_markdown(result.rotation_evidence_df, limit=160),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not table_exists(conn, table)]
    if missing:
        raise ValueError(
            "market.duckdb is missing required tables: " + ", ".join(missing)
        )


def _create_position_feature_panel(conn: Any, *, source_name: str) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_sma5_position_feature_panel AS
        SELECT
            source.*,
            source.forecast_per AS forward_per,
            source.forecast_per_percentile AS forward_per_percentile,
            CASE
                WHEN source.below_sma5_streak_ge3_flag THEN 3
                WHEN source.close_below_sma5_flag = 1 THEN 1
                ELSE 0
            END AS below_sma5_streak,
            CASE
                WHEN source.atr20 > 0
                THEN (source.close - source.sma5) / source.atr20
            END AS sma5_atr20_deviation,
            source.forward_close_return_1d_pct AS next_session_return_pct,
            (
                source.forward_close_return_1d_pct
                - source.forward_close_excess_return_1d_pct
            ) AS next_session_topix_return_pct,
            source.forward_close_excess_return_1d_pct
                AS next_session_excess_return_pct
        FROM {source_name} source
        WHERE source.forward_close_excess_return_1d_pct IS NOT NULL
        """
    )


def _query_position_feature_df(
    conn: Any,
    *,
    long_scaffolds: Sequence[str],
) -> pd.DataFrame:
    scaffold_exprs = ",\n            ".join(
        f"CASE WHEN {condition} THEN TRUE ELSE FALSE END AS scaffold__{label}"
        for label, condition in _selected_scaffold_conditions(long_scaffolds)
    )
    return conn.execute(
        f"""
        SELECT
            date,
            code,
            company_name,
            market,
            market_code,
            market_scope,
            liquidity_regime,
            close,
            sma5,
            atr20,
            recent_return_20d_pct,
            recent_return_60d_pct,
            liquidity_residual_z,
            valuation_signal,
            pbr,
            pbr_percentile,
            forward_per,
            forward_per_percentile,
            sector_strength_bucket,
            sector_strength_score,
            long_hybrid_leadership_score,
            atr20_change_20d_pct,
            atr20_acceleration_ex_overheat_flag,
            sma5_above_count_5d,
            sma5_count_group,
            below_sma5_streak,
            sma5_atr20_deviation,
            next_session_return_pct,
            next_session_topix_return_pct,
            next_session_excess_return_pct,
            {scaffold_exprs}
        FROM ranking_sma5_position_feature_panel
        ORDER BY code, date
        """
    ).fetchdf()


def _simulate_position_states(
    feature_df: pd.DataFrame,
    *,
    long_scaffolds: Sequence[str],
    entry_rules: Sequence[str],
    exit_rules: Sequence[str],
    observation_sample_limit: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if feature_df.empty:
        return (
            pd.DataFrame(columns=_position_observation_columns()),
            pd.DataFrame(columns=_position_daily_columns()),
            pd.DataFrame(columns=_trade_columns()),
            pd.DataFrame(columns=_exit_event_columns()),
        )
    working = feature_df.sort_values(["code", "date"]).reset_index(drop=True)
    observation_rows: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    exit_rows: list[dict[str, Any]] = []

    for long_scaffold in long_scaffolds:
        scaffold_column = f"scaffold__{long_scaffold}"
        if scaffold_column not in working.columns:
            continue
        relevant_codes = set(
            working.loc[working[scaffold_column].fillna(False), "code"].astype(str)
        )
        if not relevant_codes:
            continue
        scaffold_working = working.loc[working["code"].astype(str).isin(relevant_codes)]
        code_groups = [
            code_df for _, code_df in scaffold_working.groupby("code", sort=False)
        ]
        for entry_rule in entry_rules:
            for exit_rule in exit_rules:
                for code_df in code_groups:
                    held = False
                    trade_id = 0
                    trade_returns: list[float] = []
                    trade_start_date: str | None = None
                    trade_entry_date: str | None = None
                    last_held_date: str | None = None
                    for row in code_df.itertuples(index=False):
                        row_dict = row._asdict()
                        entry_signal = bool(
                            row_dict.get(scaffold_column)
                        ) and _entry_allowed(
                            row_dict,
                            entry_rule,
                        )
                        exit_signal, exit_reason = _exit_triggered(row_dict, exit_rule)
                        if held and exit_signal:
                            trade_rows.append(
                                _build_trade_row(
                                    row_dict,
                                    long_scaffold=long_scaffold,
                                    entry_rule=entry_rule,
                                    exit_rule=exit_rule,
                                    trade_id=trade_id,
                                    trade_start_date=trade_start_date,
                                    trade_entry_date=trade_entry_date,
                                    trade_exit_date=str(row_dict["date"]),
                                    last_held_date=last_held_date,
                                    exit_reason=exit_reason or "exit_signal",
                                    trade_returns=trade_returns,
                                )
                            )
                            exit_rows.append(
                                _build_exit_event_row(
                                    row_dict,
                                    long_scaffold=long_scaffold,
                                    entry_rule=entry_rule,
                                    exit_rule=exit_rule,
                                    trade_id=trade_id,
                                    exit_reason=exit_reason,
                                )
                            )
                            held = False
                            trade_returns = []
                            trade_start_date = None
                            trade_entry_date = None
                            last_held_date = None
                            entry_signal = False
                        if not held and entry_signal:
                            held = True
                            trade_id += 1
                            trade_returns = []
                            trade_start_date = str(row_dict["date"])
                            trade_entry_date = str(row_dict["date"])
                        held_state = bool(held)
                        if (held_state or entry_signal or exit_signal) and len(
                            observation_rows
                        ) < observation_sample_limit:
                            observation_rows.append(
                                _build_position_observation_row(
                                    row_dict,
                                    long_scaffold=long_scaffold,
                                    entry_rule=entry_rule,
                                    exit_rule=exit_rule,
                                    held_state=held_state,
                                    entry_signal=entry_signal,
                                    exit_signal=exit_signal,
                                    exit_reason=exit_reason,
                                    trade_id=trade_id if held_state else None,
                                )
                            )
                        if held_state and pd.notna(
                            row_dict["next_session_excess_return_pct"]
                        ):
                            daily_return = float(
                                row_dict["next_session_excess_return_pct"]
                            )
                            trade_returns.append(daily_return)
                            last_held_date = str(row_dict["date"])
                            position_rows.append(
                                _build_position_daily_row(
                                    row_dict,
                                    long_scaffold=long_scaffold,
                                    entry_rule=entry_rule,
                                    exit_rule=exit_rule,
                                    trade_id=trade_id,
                                )
                            )
                    if held:
                        last_row = {
                            str(key): value
                            for key, value in code_df.iloc[-1].to_dict().items()
                        }
                        trade_rows.append(
                            _build_trade_row(
                                last_row,
                                long_scaffold=long_scaffold,
                                entry_rule=entry_rule,
                                exit_rule=exit_rule,
                                trade_id=trade_id,
                                trade_start_date=trade_start_date,
                                trade_entry_date=trade_entry_date,
                                trade_exit_date=None,
                                last_held_date=last_held_date,
                                exit_reason="open_at_end",
                                trade_returns=trade_returns,
                            )
                        )

    return (
        pd.DataFrame(observation_rows, columns=_position_observation_columns()),
        pd.DataFrame(position_rows, columns=_position_daily_columns()),
        pd.DataFrame(trade_rows, columns=_trade_columns()),
        pd.DataFrame(exit_rows, columns=_exit_event_columns()),
    )


def _build_coverage_diagnostics_df(
    feature_df: pd.DataFrame,
    *,
    long_scaffolds: Sequence[str],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for long_scaffold in long_scaffolds:
        column = f"scaffold__{long_scaffold}"
        if column not in feature_df.columns:
            continue
        selected = feature_df.loc[feature_df[column].fillna(False)]
        if selected.empty:
            continue
        rows.append(
            {
                "long_scaffold": long_scaffold,
                "observation_count": int(len(selected)),
                "code_count": int(selected["code"].nunique()),
                "date_count": int(selected["date"].nunique()),
                "median_sma5_above_count_5d": _median(selected["sma5_above_count_5d"]),
                "count_0_1_rate_pct": _rate(selected["sma5_above_count_5d"].le(1)),
                "below_sma5_streak_ge3_rate_pct": _rate(
                    selected["below_sma5_streak"].ge(3)
                ),
                "atr20_below_le_neg1_rate_pct": _rate(
                    selected["sma5_atr20_deviation"].le(-1.0)
                ),
                "atr20_above_ge1_rate_pct": _rate(
                    selected["sma5_atr20_deviation"].ge(1.0)
                ),
                "median_next_session_excess_return_pct": _median(
                    selected["next_session_excess_return_pct"]
                ),
            }
        )
    return pd.DataFrame(rows)


def _build_entry_rule_evidence_df(
    feature_df: pd.DataFrame,
    *,
    long_scaffolds: Sequence[str],
    entry_rules: Sequence[str],
    min_position_days: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for long_scaffold in long_scaffolds:
        column = f"scaffold__{long_scaffold}"
        if column not in feature_df.columns:
            continue
        for entry_rule in entry_rules:
            mask = feature_df[column].fillna(False) & _entry_allowed_mask(
                feature_df,
                entry_rule,
            )
            selected = feature_df.loc[mask]
            if len(selected) < min_position_days:
                continue
            returns = selected["next_session_excess_return_pct"].dropna()
            rows.append(
                {
                    "long_scaffold": long_scaffold,
                    "entry_rule": entry_rule,
                    "entry_signal_count": int(len(selected)),
                    "code_count": int(selected["code"].nunique()),
                    "date_count": int(selected["date"].nunique()),
                    "mean_next_session_excess_return_pct": _mean(returns),
                    "median_next_session_excess_return_pct": _median(returns),
                    "p10_next_session_excess_return_pct": _quantile(returns, 0.10),
                    "win_rate_pct": _rate(returns.gt(0)),
                    "severe_loss_day_rate_pct": _rate(
                        returns.le(severe_loss_threshold_pct)
                    ),
                    "median_sma5_atr20_deviation": _median(
                        selected["sma5_atr20_deviation"]
                    ),
                    "median_sma5_above_count_5d": _median(
                        selected["sma5_above_count_5d"]
                    ),
                }
            )
    return pd.DataFrame(rows)


def _build_position_state_daily_evidence_df(
    position_daily_df: pd.DataFrame,
    *,
    min_position_days: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    if position_daily_df.empty:
        return pd.DataFrame(columns=_position_daily_evidence_columns())
    daily = (
        position_daily_df.groupby(
            ["long_scaffold", "entry_rule", "exit_rule", "market_scope", "date"],
            dropna=False,
        )
        .agg(
            daily_position_count=("code", "nunique"),
            daily_equal_weight_excess_return_pct=(
                "next_session_excess_return_pct",
                "mean",
            ),
        )
        .reset_index()
    )
    rows: list[dict[str, Any]] = []
    for keys, group in daily.groupby(
        ["long_scaffold", "entry_rule", "exit_rule", "market_scope"],
        dropna=False,
    ):
        returns = group["daily_equal_weight_excess_return_pct"].dropna()
        position_day_count = int(group["daily_position_count"].sum())
        if position_day_count < min_position_days:
            continue
        cumulative = float(
            (np.prod(1.0 + returns.to_numpy(dtype=float) / 100.0) - 1.0) * 100.0
        )
        std = float(returns.std(ddof=0)) if len(returns) > 0 else np.nan
        mean = _mean(returns)
        rows.append(
            {
                "long_scaffold": keys[0],
                "entry_rule": keys[1],
                "exit_rule": keys[2],
                "market_scope": keys[3],
                "date_count": int(group["date"].nunique()),
                "position_day_count": position_day_count,
                "mean_positions_per_date": _mean(group["daily_position_count"]),
                "mean_daily_excess_return_pct": mean,
                "median_daily_excess_return_pct": _median(returns),
                "p10_daily_excess_return_pct": _quantile(returns, 0.10),
                "p25_daily_excess_return_pct": _quantile(returns, 0.25),
                "p75_daily_excess_return_pct": _quantile(returns, 0.75),
                "p90_daily_excess_return_pct": _quantile(returns, 0.90),
                "daily_win_rate_pct": _rate(returns.gt(0)),
                "severe_loss_day_rate_pct": _rate(
                    returns.le(severe_loss_threshold_pct)
                ),
                "cumulative_excess_return_pct": cumulative,
                "date_level_ir": (mean / std * np.sqrt(252.0))
                if std and not np.isnan(std)
                else np.nan,
            }
        )
    return pd.DataFrame(rows, columns=_position_daily_evidence_columns())


def _build_position_state_trade_evidence_df(
    trade_df: pd.DataFrame,
    *,
    min_trades: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    if trade_df.empty:
        return pd.DataFrame(columns=_trade_evidence_columns())
    rows: list[dict[str, Any]] = []
    for keys, group in trade_df.groupby(
        ["long_scaffold", "entry_rule", "exit_rule", "market_scope"],
        dropna=False,
    ):
        if len(group) < min_trades:
            continue
        returns = group["trade_excess_return_pct"].dropna()
        rows.append(
            {
                "long_scaffold": keys[0],
                "entry_rule": keys[1],
                "exit_rule": keys[2],
                "market_scope": keys[3],
                "trade_count": int(len(group)),
                "closed_trade_count": int(group["trade_exit_date"].notna().sum()),
                "code_count": int(group["code"].nunique()),
                "median_holding_days": _median(group["holding_days"]),
                "mean_holding_days": _mean(group["holding_days"]),
                "mean_trade_excess_return_pct": _mean(returns),
                "median_trade_excess_return_pct": _median(returns),
                "p10_trade_excess_return_pct": _quantile(returns, 0.10),
                "p90_trade_excess_return_pct": _quantile(returns, 0.90),
                "win_trade_rate_pct": _rate(returns.gt(0)),
                "severe_loss_trade_rate_pct": _rate(
                    returns.le(severe_loss_threshold_pct)
                ),
            }
        )
    return pd.DataFrame(rows, columns=_trade_evidence_columns())


def _build_exit_reason_evidence_df(
    exit_event_df: pd.DataFrame,
    *,
    min_trades: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    if exit_event_df.empty:
        return pd.DataFrame(columns=_exit_reason_evidence_columns())
    rows: list[dict[str, Any]] = []
    for keys, group in exit_event_df.groupby(
        ["long_scaffold", "entry_rule", "exit_rule", "exit_reason", "market_scope"],
        dropna=False,
    ):
        if len(group) < min_trades:
            continue
        returns = group["next_session_excess_return_pct"].dropna()
        rows.append(
            {
                "long_scaffold": keys[0],
                "entry_rule": keys[1],
                "exit_rule": keys[2],
                "exit_reason": keys[3],
                "market_scope": keys[4],
                "exit_event_count": int(len(group)),
                "code_count": int(group["code"].nunique()),
                "date_count": int(group["date"].nunique()),
                "mean_next_session_excess_return_pct": _mean(returns),
                "median_next_session_excess_return_pct": _median(returns),
                "p10_next_session_excess_return_pct": _quantile(returns, 0.10),
                "win_rate_pct": _rate(returns.gt(0)),
                "severe_loss_day_rate_pct": _rate(
                    returns.le(severe_loss_threshold_pct)
                ),
            }
        )
    return pd.DataFrame(rows, columns=_exit_reason_evidence_columns())


def _build_rotation_evidence_df(
    feature_df: pd.DataFrame,
    exit_event_df: pd.DataFrame,
    *,
    long_scaffolds: Sequence[str],
    entry_rules: Sequence[str],
    min_trades: int,
) -> pd.DataFrame:
    if feature_df.empty or exit_event_df.empty:
        return pd.DataFrame(columns=_rotation_evidence_columns())
    combined_exits = exit_event_df.loc[
        exit_event_df["exit_rule"].astype(str) == "combined_count_streak_atr"
    ].copy()
    if combined_exits.empty:
        return pd.DataFrame(columns=_rotation_evidence_columns())

    event_frames: list[pd.DataFrame] = []
    for long_scaffold in long_scaffolds:
        scaffold_column = f"scaffold__{long_scaffold}"
        if scaffold_column not in feature_df.columns:
            continue
        scaffold_mask = feature_df[scaffold_column].fillna(False)
        for entry_rule in entry_rules:
            entry_mask = _entry_allowed_mask(feature_df, entry_rule)
            combined_exit_mask = _combined_exit_mask(feature_df)
            valid_mask = scaffold_mask & entry_mask & ~combined_exit_mask
            healthy_mask = (
                valid_mask
                & pd.to_numeric(
                    feature_df["sma5_above_count_5d"],
                    errors="coerce",
                ).ge(2.0)
                & pd.to_numeric(
                    feature_df["below_sma5_streak"],
                    errors="coerce",
                ).lt(3.0)
                & pd.to_numeric(
                    feature_df["sma5_atr20_deviation"],
                    errors="coerce",
                ).gt(-1.0)
                & pd.to_numeric(
                    feature_df["sma5_atr20_deviation"],
                    errors="coerce",
                ).lt(1.0)
            )
            for rotation_rule, candidate_mask in (
                ("valid_same_scaffold_basket", valid_mask),
                ("healthy_same_scaffold_basket", healthy_mask),
            ):
                basket = _build_rotation_basket_df(
                    feature_df.loc[candidate_mask],
                    long_scaffold=long_scaffold,
                    entry_rule=entry_rule,
                    rotation_rule=rotation_rule,
                )
                events = combined_exits.loc[
                    (combined_exits["long_scaffold"].astype(str) == long_scaffold)
                    & (combined_exits["entry_rule"].astype(str) == entry_rule)
                ].copy()
                if events.empty:
                    continue
                joined = events.merge(
                    basket,
                    on=["date", "market_scope"],
                    how="left",
                )
                joined["rotation_rule"] = rotation_rule
                joined["source_next_session_excess_return_pct"] = pd.to_numeric(
                    joined["next_session_excess_return_pct"],
                    errors="coerce",
                )
                joined["rotation_minus_source_pct"] = (
                    joined["rotation_basket_excess_return_pct"]
                    - joined["source_next_session_excess_return_pct"]
                )
                event_frames.append(joined)

    if not event_frames:
        return pd.DataFrame(columns=_rotation_evidence_columns())
    rotation_events = pd.concat(event_frames, ignore_index=True)
    rows: list[dict[str, Any]] = []
    for keys, group in rotation_events.groupby(
        [
            "long_scaffold",
            "entry_rule",
            "exit_reason",
            "rotation_rule",
            "market_scope",
        ],
        dropna=False,
    ):
        if len(group) < min_trades:
            continue
        available = group.loc[group["target_candidate_count"].fillna(0).gt(0)]
        deltas = available["rotation_minus_source_pct"].dropna()
        rows.append(
            {
                "long_scaffold": keys[0],
                "entry_rule": keys[1],
                "exit_reason": keys[2],
                "rotation_rule": keys[3],
                "market_scope": keys[4],
                "exit_event_count": int(len(group)),
                "target_available_event_count": int(len(available)),
                "target_available_rate_pct": float(len(available) / len(group) * 100.0)
                if len(group) > 0
                else np.nan,
                "median_target_candidate_count": _median(
                    available["target_candidate_count"]
                ),
                "mean_target_candidate_count": _mean(
                    available["target_candidate_count"]
                ),
                "median_source_next_session_excess_return_pct": _median(
                    group["source_next_session_excess_return_pct"]
                ),
                "median_rotation_basket_excess_return_pct": _median(
                    available["rotation_basket_excess_return_pct"]
                ),
                "mean_rotation_basket_excess_return_pct": _mean(
                    available["rotation_basket_excess_return_pct"]
                ),
                "median_rotation_minus_source_pct": _median(deltas),
                "mean_rotation_minus_source_pct": _mean(deltas),
                "p10_rotation_minus_source_pct": _quantile(deltas, 0.10),
                "p90_rotation_minus_source_pct": _quantile(deltas, 0.90),
                "rotation_outperform_rate_pct": _rate(deltas.gt(0)),
            }
        )
    return pd.DataFrame(rows, columns=_rotation_evidence_columns())


def _build_rotation_basket_df(
    candidate_df: pd.DataFrame,
    *,
    long_scaffold: str,
    entry_rule: str,
    rotation_rule: str,
) -> pd.DataFrame:
    if candidate_df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "market_scope",
                "target_candidate_count",
                "rotation_basket_excess_return_pct",
            ]
        )
    grouped = (
        candidate_df.groupby(["date", "market_scope"], dropna=False)
        .agg(
            target_candidate_count=("code", "nunique"),
            rotation_basket_excess_return_pct=(
                "next_session_excess_return_pct",
                "mean",
            ),
        )
        .reset_index()
    )
    grouped["long_scaffold"] = long_scaffold
    grouped["entry_rule"] = entry_rule
    grouped["rotation_rule"] = rotation_rule
    return grouped[
        [
            "date",
            "market_scope",
            "target_candidate_count",
            "rotation_basket_excess_return_pct",
        ]
    ]


def _selected_scaffold_conditions(
    long_scaffolds: Sequence[str],
) -> tuple[tuple[str, str], ...]:
    available = dict(_LONG_SCAFFOLDS)
    missing = [item for item in long_scaffolds if item not in available]
    if missing:
        raise ValueError(f"unknown long_scaffolds: {', '.join(missing)}")
    return tuple((label, available[label]) for label in long_scaffolds)


def _entry_allowed(row: dict[str, Any], entry_rule: str) -> bool:
    if entry_rule == "no_entry_filter":
        return True
    if entry_rule == "avoid_atr20_above_ge1":
        value = row.get("sma5_atr20_deviation")
        return pd.notna(value) and float(value) < 1.0
    raise ValueError(f"unknown entry_rule: {entry_rule}")


def _entry_allowed_mask(frame: pd.DataFrame, entry_rule: str) -> pd.Series:
    if entry_rule == "no_entry_filter":
        return pd.Series(True, index=frame.index)
    if entry_rule == "avoid_atr20_above_ge1":
        return pd.to_numeric(
            frame["sma5_atr20_deviation"],
            errors="coerce",
        ).lt(1.0)
    raise ValueError(f"unknown entry_rule: {entry_rule}")


def _combined_exit_mask(frame: pd.DataFrame) -> pd.Series:
    count_exit = pd.to_numeric(
        frame["sma5_above_count_5d"],
        errors="coerce",
    ).le(1.0)
    streak_exit = pd.to_numeric(
        frame["below_sma5_streak"],
        errors="coerce",
    ).ge(3.0)
    atr_exit = pd.to_numeric(
        frame["sma5_atr20_deviation"],
        errors="coerce",
    ).le(-1.0)
    return count_exit | streak_exit | atr_exit


def _exit_triggered(row: dict[str, Any], exit_rule: str) -> tuple[bool, str | None]:
    count_exit = (
        pd.notna(row.get("sma5_above_count_5d"))
        and float(row["sma5_above_count_5d"]) <= 1.0
    )
    streak_exit = (
        pd.notna(row.get("below_sma5_streak"))
        and float(row["below_sma5_streak"]) >= 3.0
    )
    atr_exit = (
        pd.notna(row.get("sma5_atr20_deviation"))
        and float(row["sma5_atr20_deviation"]) <= -1.0
    )
    if exit_rule == "count_0_1":
        return count_exit, "count_0_1" if count_exit else None
    if exit_rule == "below_sma5_streak_ge3":
        return streak_exit, "below_sma5_streak_ge3" if streak_exit else None
    if exit_rule == "atr20_below_le_neg1":
        return atr_exit, "atr20_below_le_neg1" if atr_exit else None
    if exit_rule == "combined_count_streak_atr":
        if atr_exit:
            return True, "atr20_below_le_neg1"
        if streak_exit:
            return True, "below_sma5_streak_ge3"
        if count_exit:
            return True, "count_0_1"
        return False, None
    raise ValueError(f"unknown exit_rule: {exit_rule}")


def _build_position_observation_row(
    row: dict[str, Any],
    *,
    long_scaffold: str,
    entry_rule: str,
    exit_rule: str,
    held_state: bool,
    entry_signal: bool,
    exit_signal: bool,
    exit_reason: str | None,
    trade_id: int | None,
) -> dict[str, Any]:
    payload = _base_row_payload(row)
    payload.update(
        {
            "long_scaffold": long_scaffold,
            "entry_rule": entry_rule,
            "exit_rule": exit_rule,
            "held_state": bool(held_state),
            "entry_signal": bool(entry_signal),
            "exit_signal": bool(exit_signal),
            "exit_reason": exit_reason,
            "trade_id": trade_id,
        }
    )
    return payload


def _build_position_daily_row(
    row: dict[str, Any],
    *,
    long_scaffold: str,
    entry_rule: str,
    exit_rule: str,
    trade_id: int,
) -> dict[str, Any]:
    payload = _base_row_payload(row)
    payload.update(
        {
            "long_scaffold": long_scaffold,
            "entry_rule": entry_rule,
            "exit_rule": exit_rule,
            "trade_id": trade_id,
        }
    )
    return payload


def _build_trade_row(
    row: dict[str, Any],
    *,
    long_scaffold: str,
    entry_rule: str,
    exit_rule: str,
    trade_id: int,
    trade_start_date: str | None,
    trade_entry_date: str | None,
    trade_exit_date: str | None,
    last_held_date: str | None,
    exit_reason: str,
    trade_returns: Sequence[float],
) -> dict[str, Any]:
    returns = np.asarray(list(trade_returns), dtype=float)
    return {
        "long_scaffold": long_scaffold,
        "entry_rule": entry_rule,
        "exit_rule": exit_rule,
        "market_scope": row.get("market_scope"),
        "code": row.get("code"),
        "company_name": row.get("company_name"),
        "trade_id": trade_id,
        "trade_start_date": trade_start_date,
        "trade_entry_date": trade_entry_date,
        "trade_exit_date": trade_exit_date,
        "last_held_date": last_held_date,
        "exit_reason": exit_reason,
        "holding_days": int(len(returns)),
        "trade_excess_return_pct": float((np.prod(1.0 + returns / 100.0) - 1.0) * 100.0)
        if len(returns) > 0
        else np.nan,
        "mean_daily_excess_return_pct": float(np.mean(returns))
        if len(returns) > 0
        else np.nan,
        "min_daily_excess_return_pct": float(np.min(returns))
        if len(returns) > 0
        else np.nan,
        "max_daily_excess_return_pct": float(np.max(returns))
        if len(returns) > 0
        else np.nan,
    }


def _build_exit_event_row(
    row: dict[str, Any],
    *,
    long_scaffold: str,
    entry_rule: str,
    exit_rule: str,
    trade_id: int,
    exit_reason: str | None,
) -> dict[str, Any]:
    payload = _base_row_payload(row)
    payload.update(
        {
            "long_scaffold": long_scaffold,
            "entry_rule": entry_rule,
            "exit_rule": exit_rule,
            "trade_id": trade_id,
            "exit_reason": exit_reason,
        }
    )
    return payload


def _base_row_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "date": row.get("date"),
        "code": row.get("code"),
        "company_name": row.get("company_name"),
        "market": row.get("market"),
        "market_code": row.get("market_code"),
        "market_scope": row.get("market_scope"),
        "liquidity_regime": row.get("liquidity_regime"),
        "close": row.get("close"),
        "sma5": row.get("sma5"),
        "atr20": row.get("atr20"),
        "recent_return_20d_pct": row.get("recent_return_20d_pct"),
        "recent_return_60d_pct": row.get("recent_return_60d_pct"),
        "liquidity_residual_z": row.get("liquidity_residual_z"),
        "valuation_signal": row.get("valuation_signal"),
        "pbr": row.get("pbr"),
        "pbr_percentile": row.get("pbr_percentile"),
        "forward_per": row.get("forward_per"),
        "forward_per_percentile": row.get("forward_per_percentile"),
        "sector_strength_bucket": row.get("sector_strength_bucket"),
        "sector_strength_score": row.get("sector_strength_score"),
        "long_hybrid_leadership_score": row.get("long_hybrid_leadership_score"),
        "atr20_change_20d_pct": row.get("atr20_change_20d_pct"),
        "atr20_acceleration_ex_overheat_flag": row.get(
            "atr20_acceleration_ex_overheat_flag"
        ),
        "sma5_above_count_5d": row.get("sma5_above_count_5d"),
        "sma5_count_group": row.get("sma5_count_group"),
        "below_sma5_streak": row.get("below_sma5_streak"),
        "sma5_atr20_deviation": row.get("sma5_atr20_deviation"),
        "next_session_return_pct": row.get("next_session_return_pct"),
        "next_session_topix_return_pct": row.get("next_session_topix_return_pct"),
        "next_session_excess_return_pct": row.get("next_session_excess_return_pct"),
    }


def _validate_params(
    *,
    long_scaffolds: Sequence[str],
    entry_rules: Sequence[str],
    exit_rules: Sequence[str],
    min_position_days: int,
    min_trades: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    _selected_scaffold_conditions(long_scaffolds)
    unknown_entry = [rule for rule in entry_rules if rule not in _ENTRY_RULES]
    if unknown_entry:
        raise ValueError(f"unknown entry_rules: {', '.join(unknown_entry)}")
    unknown_exit = [rule for rule in exit_rules if rule not in _EXIT_RULES]
    if unknown_exit:
        raise ValueError(f"unknown exit_rules: {', '.join(unknown_exit)}")
    if min_position_days <= 0:
        raise ValueError("min_position_days must be positive")
    if min_trades <= 0:
        raise ValueError("min_trades must be positive")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _mean(values: Any) -> float:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    return float(series.mean()) if not series.empty else np.nan


def _median(values: Any) -> float:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    return float(series.median()) if not series.empty else np.nan


def _quantile(values: Any, q: float) -> float:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    return float(series.quantile(q)) if not series.empty else np.nan


def _rate(mask: Any) -> float:
    series = pd.Series(mask).dropna()
    return float(series.mean() * 100.0) if not series.empty else np.nan


def _position_observation_columns() -> list[str]:
    return [
        *_position_daily_columns(),
        "held_state",
        "entry_signal",
        "exit_signal",
        "exit_reason",
    ]


def _position_daily_columns() -> list[str]:
    return [
        "date",
        "code",
        "company_name",
        "market",
        "market_code",
        "market_scope",
        "liquidity_regime",
        "close",
        "sma5",
        "atr20",
        "recent_return_20d_pct",
        "recent_return_60d_pct",
        "liquidity_residual_z",
        "valuation_signal",
        "pbr",
        "pbr_percentile",
        "forward_per",
        "forward_per_percentile",
        "sector_strength_bucket",
        "sector_strength_score",
        "long_hybrid_leadership_score",
        "atr20_change_20d_pct",
        "atr20_acceleration_ex_overheat_flag",
        "sma5_above_count_5d",
        "sma5_count_group",
        "below_sma5_streak",
        "sma5_atr20_deviation",
        "next_session_return_pct",
        "next_session_topix_return_pct",
        "next_session_excess_return_pct",
        "long_scaffold",
        "entry_rule",
        "exit_rule",
        "trade_id",
    ]


def _trade_columns() -> list[str]:
    return [
        "long_scaffold",
        "entry_rule",
        "exit_rule",
        "market_scope",
        "code",
        "company_name",
        "trade_id",
        "trade_start_date",
        "trade_entry_date",
        "trade_exit_date",
        "last_held_date",
        "exit_reason",
        "holding_days",
        "trade_excess_return_pct",
        "mean_daily_excess_return_pct",
        "min_daily_excess_return_pct",
        "max_daily_excess_return_pct",
    ]


def _exit_event_columns() -> list[str]:
    return [
        *_position_daily_columns(),
        "exit_reason",
    ]


def _position_daily_evidence_columns() -> list[str]:
    return [
        "long_scaffold",
        "entry_rule",
        "exit_rule",
        "market_scope",
        "date_count",
        "position_day_count",
        "mean_positions_per_date",
        "mean_daily_excess_return_pct",
        "median_daily_excess_return_pct",
        "p10_daily_excess_return_pct",
        "p25_daily_excess_return_pct",
        "p75_daily_excess_return_pct",
        "p90_daily_excess_return_pct",
        "daily_win_rate_pct",
        "severe_loss_day_rate_pct",
        "cumulative_excess_return_pct",
        "date_level_ir",
    ]


def _trade_evidence_columns() -> list[str]:
    return [
        "long_scaffold",
        "entry_rule",
        "exit_rule",
        "market_scope",
        "trade_count",
        "closed_trade_count",
        "code_count",
        "median_holding_days",
        "mean_holding_days",
        "mean_trade_excess_return_pct",
        "median_trade_excess_return_pct",
        "p10_trade_excess_return_pct",
        "p90_trade_excess_return_pct",
        "win_trade_rate_pct",
        "severe_loss_trade_rate_pct",
    ]


def _exit_reason_evidence_columns() -> list[str]:
    return [
        "long_scaffold",
        "entry_rule",
        "exit_rule",
        "exit_reason",
        "market_scope",
        "exit_event_count",
        "code_count",
        "date_count",
        "mean_next_session_excess_return_pct",
        "median_next_session_excess_return_pct",
        "p10_next_session_excess_return_pct",
        "win_rate_pct",
        "severe_loss_day_rate_pct",
    ]


def _rotation_evidence_columns() -> list[str]:
    return [
        "long_scaffold",
        "entry_rule",
        "exit_reason",
        "rotation_rule",
        "market_scope",
        "exit_event_count",
        "target_available_event_count",
        "target_available_rate_pct",
        "median_target_candidate_count",
        "mean_target_candidate_count",
        "median_source_next_session_excess_return_pct",
        "median_rotation_basket_excess_return_pct",
        "mean_rotation_basket_excess_return_pct",
        "median_rotation_minus_source_pct",
        "mean_rotation_minus_source_pct",
        "p10_rotation_minus_source_pct",
        "p90_rotation_minus_source_pct",
        "rotation_outperform_rate_pct",
    ]
