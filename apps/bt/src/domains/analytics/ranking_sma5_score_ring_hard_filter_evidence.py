"""Pure score-ring membership and same-Close position-state semantics."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    compose_daily_ranking_signal_features,
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
    DailyRankingResearchRelations,
    build_daily_ranking_research_base,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
    require_market_v5_compatibility,
)
from src.domains.backtest.vectorbt_adapter import VectorbtAdapter
from src.shared.utils.pandas_type_guards import finite_float_or_none


SCORE_RING_THRESHOLDS: Mapping[str, float] = MappingProxyType(
    {
        "core_high_high": 0.80,
        "near_high_high_1": 0.70,
        "near_high_high_2": 0.60,
    }
)
ENTRY_RULE_IDS = (
    "E0_no_sma5_filter",
    "E1_close_above_sma5",
    "E2_count_ge_2",
    "E3_avoid_atr20_chase",
    "E4_count_ge_2_and_avoid_chase",
)
EXIT_RULE_IDS = (
    "X0_no_sma5_exit",
    "X1_close_below_sma5",
    "X2_count_le_1",
    "X3_below_streak_ge_3",
    "X4_atr20_below_le_neg1",
)
EXIT_PRECEDENCE = ("ring_exit", "sma5_exit", "time_exit", "terminal_exit")

_VALUE_SCORE_COLUMN = "value_composite_equal_score"
_LEADERSHIP_SCORE_COLUMN = "long_hybrid_leadership_score"
_REQUIRED_FEATURE_COLUMNS = frozenset(
    {
        "date",
        "code",
        "close",
        _VALUE_SCORE_COLUMN,
        _LEADERSHIP_SCORE_COLUMN,
    }
)
_REQUIRED_MARKET_TABLES = frozenset(
    {
        "stock_data_raw",
        "stock_data",
        "topix_data",
        "daily_valuation",
        "stock_master_daily",
        "indices_data",
        "index_master",
        "stock_provider_windows",
        "stock_adjustment_events",
        "current_basis_fundamentals_state",
        "current_basis_recompute_pending",
        "statements",
        "statement_metrics_adjusted",
    }
)
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000


@dataclass(frozen=True)
class PositionSignalFrames:
    close: pd.DataFrame
    entries: pd.DataFrame
    exits: pd.DataFrame
    held_intervals: pd.DataFrame
    state_events: pd.DataFrame


@dataclass(frozen=True)
class ResearchVariant:
    """One frozen score-ring and SMA5 position-state configuration."""

    ring_id: str
    entry_rule_id: str
    exit_rule_id: str
    max_holding_sessions: int
    name: str | None = None


@dataclass(frozen=True)
class VariantExecution:
    """VectorBT-authoritative fills paired with the independent state timeline."""

    variant: ResearchVariant
    portfolio: Any
    signal_frames: PositionSignalFrames
    trade_records_df: pd.DataFrame
    daily_portfolio_returns: pd.Series
    state_events: pd.DataFrame

    @property
    def trade_records(self) -> pd.DataFrame:
        """Compatibility-friendly name for the normalized VectorBT ledger."""

        return self.trade_records_df

    @property
    def daily_returns(self) -> pd.Series:
        """The approved date-level equal-weight active-portfolio return series."""

        return self.daily_portfolio_returns


@dataclass(frozen=True)
class MarketV5PitLineage:
    """Minimal, explicit provenance for this Market v5-only study."""

    data_plane_schema_version: int
    stock_price_adjustment_mode: str
    price_projection_verification_status: str
    no_stock_data_fallback: bool


@dataclass(frozen=True)
class RankingSma5ScoreRingHardFilterResearchResult:
    """Market v5 feature panel and provenance for score-ring execution research."""

    db_path: str
    source_mode: SourceMode
    source_detail: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    bootstrap_resamples: int
    min_trades: int
    min_signal_dates: int
    pit_lineage: MarketV5PitLineage
    feature_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def classify_score_ring(value_score: object, leadership_score: object) -> str:
    """Return the most selective score-ring label satisfied by two scores."""
    value = _safe_finite_float_or_none(value_score)
    leadership = _safe_finite_float_or_none(leadership_score)
    if value is None or leadership is None:
        return "missing"
    for ring_id, threshold in SCORE_RING_THRESHOLDS.items():
        if value >= threshold and leadership >= threshold:
            return ring_id
    return "outside"


def entry_rule_matches(row: Mapping[str, object], rule_id: str) -> bool:
    """Evaluate a frozen entry rule, failing closed for missing numeric inputs."""
    if rule_id not in ENTRY_RULE_IDS:
        raise ValueError(f"unknown entry rule: {rule_id}")
    if rule_id == "E0_no_sma5_filter":
        return True
    if rule_id == "E1_close_above_sma5":
        close = _numeric_value(row, "close")
        sma5 = _numeric_value(row, "sma5")
        return close is not None and sma5 is not None and close >= sma5
    if rule_id == "E2_count_ge_2":
        count = _numeric_value(row, "sma5_above_count_5d")
        return count is not None and count >= 2.0
    if rule_id == "E3_avoid_atr20_chase":
        deviation = _numeric_value(row, "sma5_atr20_deviation")
        return deviation is not None and deviation < 1.0
    count = _numeric_value(row, "sma5_above_count_5d")
    deviation = _numeric_value(row, "sma5_atr20_deviation")
    return count is not None and count >= 2.0 and deviation is not None and deviation < 1.0


def exit_rule_matches(row: Mapping[str, object], rule_id: str) -> bool:
    """Evaluate a frozen exit rule, failing closed for missing numeric inputs."""
    if rule_id not in EXIT_RULE_IDS:
        raise ValueError(f"unknown exit rule: {rule_id}")
    if rule_id == "X0_no_sma5_exit":
        return False
    if rule_id == "X1_close_below_sma5":
        close = _numeric_value(row, "close")
        sma5 = _numeric_value(row, "sma5")
        return close is not None and sma5 is not None and close < sma5
    if rule_id == "X2_count_le_1":
        count = _numeric_value(row, "sma5_above_count_5d")
        return count is not None and count <= 1.0
    if rule_id == "X3_below_streak_ge_3":
        streak = _numeric_value(row, "sma5_below_streak")
        return streak is not None and streak >= 3.0
    deviation = _numeric_value(row, "sma5_atr20_deviation")
    return deviation is not None and deviation <= -1.0


def build_position_signal_frames(
    feature_df: pd.DataFrame,
    *,
    ring_id: str,
    entry_rule_id: str,
    exit_rule_id: str,
    max_holding_sessions: int,
) -> PositionSignalFrames:
    """Build aligned price, signal, exposure, and event frames for one variant.

    Membership is threshold based, so a wider ring contains every qualifying row,
    including those classified into a more selective label.
    """
    _validate_arguments(
        feature_df,
        ring_id=ring_id,
        entry_rule_id=entry_rule_id,
        exit_rule_id=exit_rule_id,
        max_holding_sessions=max_holding_sessions,
    )
    prepared = feature_df.copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="raise")
    prepared["code"] = prepared["code"].astype(str)
    if prepared.duplicated(["date", "code"]).any():
        raise ValueError("feature_df must contain at most one row per date and code")
    prepared = prepared.sort_values(["code", "date"], kind="stable")

    dates = pd.DatetimeIndex(sorted(prepared["date"].unique()), name="date")
    codes = pd.Index(sorted(prepared["code"].unique()), name="code")
    close = (
        prepared.assign(close=pd.to_numeric(prepared["close"], errors="coerce"))
        .pivot(index="date", columns="code", values="close")
        .reindex(index=dates, columns=codes)
    )
    entries = pd.DataFrame(False, index=dates, columns=codes, dtype=bool)
    exits = pd.DataFrame(False, index=dates, columns=codes, dtype=bool)
    held_intervals = pd.DataFrame(False, index=dates, columns=codes, dtype=bool)
    events: list[dict[str, object]] = []

    for code, code_frame in prepared.groupby("code", sort=False):
        _build_code_position_state(
            code_frame,
            code=str(code),
            ring_id=ring_id,
            entry_rule_id=entry_rule_id,
            exit_rule_id=exit_rule_id,
            max_holding_sessions=max_holding_sessions,
            entries=entries,
            exits=exits,
            held_intervals=held_intervals,
            events=events,
        )

    state_events = pd.DataFrame(
        events,
        columns=["date", "code", "event_type", "exit_reason"],
    )
    if not state_events.empty:
        event_order = {"exit": 0, "entry": 1}
        state_events["_event_order"] = state_events["event_type"].map(event_order)
        state_events = (
            state_events.sort_values(["date", "code", "_event_order"], kind="stable")
            .drop(columns="_event_order")
            .reset_index(drop=True)
        )
    return PositionSignalFrames(
        close=close,
        entries=entries,
        exits=exits,
        held_intervals=held_intervals,
        state_events=state_events,
    )


def _build_code_position_state(
    code_frame: pd.DataFrame,
    *,
    code: str,
    ring_id: str,
    entry_rule_id: str,
    exit_rule_id: str,
    max_holding_sessions: int,
    entries: pd.DataFrame,
    exits: pd.DataFrame,
    held_intervals: pd.DataFrame,
    events: list[dict[str, object]],
) -> None:
    rows = cast(list[dict[str, Any]], code_frame.to_dict(orient="records"))
    finite_close_dates = [
        pd.Timestamp(row["date"])
        for row in rows
        if _numeric_value(row, "close") is not None
    ]
    if not finite_close_dates:
        return
    last_finite_close_date = finite_close_dates[-1]
    active = False
    held_sessions = 0
    previous_entry_eligibility = False

    for row in rows:
        date = pd.Timestamp(row["date"])
        has_close = _numeric_value(row, "close") is not None
        ring_member = _row_is_in_ring(row, ring_id)
        entry_eligible = (
            has_close and ring_member and entry_rule_matches(row, entry_rule_id)
        )

        if active:
            exit_reason = _exit_reason(
                row,
                ring_member=ring_member,
                exit_rule_id=exit_rule_id,
                held_sessions=held_sessions,
                max_holding_sessions=max_holding_sessions,
            )
            if has_close and exit_reason is not None:
                _emit_exit(
                    date,
                    code,
                    exit_reason,
                    exits=exits,
                    held_intervals=held_intervals,
                    events=events,
                )
                active = False
                held_sessions = 0
            elif has_close:
                held_intervals.loc[date, code] = True
                held_sessions += 1
        elif entry_eligible and not previous_entry_eligibility:
            if date != last_finite_close_date:
                entries.loc[date, code] = True
                events.append(
                    {
                        "date": date,
                        "code": code,
                        "event_type": "entry",
                        "exit_reason": None,
                    }
                )
                active = True
                held_sessions = 0

        previous_entry_eligibility = entry_eligible

    if active:
        _emit_exit(
            last_finite_close_date,
            code,
            "terminal_exit",
            exits=exits,
            held_intervals=held_intervals,
            events=events,
        )


def _exit_reason(
    row: Mapping[str, object],
    *,
    ring_member: bool,
    exit_rule_id: str,
    held_sessions: int,
    max_holding_sessions: int,
) -> str | None:
    if not ring_member:
        return "ring_exit"
    if exit_rule_matches(row, exit_rule_id):
        return "sma5_exit"
    if held_sessions >= max_holding_sessions - 1:
        return "time_exit"
    return None


def _emit_exit(
    date: pd.Timestamp,
    code: str,
    exit_reason: str,
    *,
    exits: pd.DataFrame,
    held_intervals: pd.DataFrame,
    events: list[dict[str, object]],
) -> None:
    exits.loc[date, code] = True
    held_intervals.loc[date, code] = True
    events.append(
        {
            "date": date,
            "code": code,
            "event_type": "exit",
            "exit_reason": exit_reason,
        }
    )


def _row_is_in_ring(row: Mapping[str, object], ring_id: str) -> bool:
    threshold = SCORE_RING_THRESHOLDS[ring_id]
    value = _numeric_value(row, _VALUE_SCORE_COLUMN)
    leadership = _numeric_value(row, _LEADERSHIP_SCORE_COLUMN)
    return value is not None and leadership is not None and value >= threshold and leadership >= threshold


def _numeric_value(row: Mapping[str, object], column: str) -> float | None:
    return _safe_finite_float_or_none(row.get(column))


def _safe_finite_float_or_none(value: object) -> float | None:
    try:
        return finite_float_or_none(value)
    except (TypeError, ValueError):
        return None


def _validate_arguments(
    feature_df: pd.DataFrame,
    *,
    ring_id: str,
    entry_rule_id: str,
    exit_rule_id: str,
    max_holding_sessions: int,
) -> None:
    if ring_id not in SCORE_RING_THRESHOLDS:
        raise ValueError(f"unknown score ring: {ring_id}")
    if entry_rule_id not in ENTRY_RULE_IDS:
        raise ValueError(f"unknown entry rule: {entry_rule_id}")
    if exit_rule_id not in EXIT_RULE_IDS:
        raise ValueError(f"unknown exit rule: {exit_rule_id}")
    if (
        isinstance(max_holding_sessions, bool)
        or not isinstance(max_holding_sessions, int)
        or max_holding_sessions <= 0
    ):
        raise ValueError("max_holding_sessions must be a positive integer")
    missing_columns = sorted(_REQUIRED_FEATURE_COLUMNS.difference(feature_df.columns))
    if missing_columns:
        raise ValueError(f"feature_df missing required columns: {', '.join(missing_columns)}")


def build_score_ring_feature_panel(
    conn: Any,
    relations: DailyRankingResearchRelations,
) -> pd.DataFrame:
    """Build the frozen score/SMA panel from canonical Daily Ranking builders.

    Scores are produced exclusively by the shared scaffold builders.  The two
    execution aliases below only express the SMA exit primitives in the units
    consumed by the frozen Task 1 state machine.
    """

    signal_source = relations.ranked_signals
    atr_features = build_atr_features(
        conn,
        AtrFeaturesRequest(source=signal_source, namespace="hard_filter_atr"),
    )
    short_features = build_short_scaffold_features(
        conn,
        ShortScaffoldFeaturesRequest(
            source=signal_source,
            atr_features=atr_features,
            namespace="hard_filter_short",
        ),
    )
    sector_features = build_sector_strength_features(
        conn,
        SectorStrengthFeaturesRequest(
            source=signal_source,
            population_source=signal_source,
            namespace="hard_filter_sector",
        ),
    )
    leadership_features = build_long_leadership_features(
        conn,
        LongLeadershipFeaturesRequest(
            source=signal_source,
            sector_features=sector_features,
            namespace="hard_filter_leadership",
            leadership_windows=_LEADERSHIP_WINDOWS,
        ),
    )
    sma_features = build_sma_features(
        conn,
        SmaFeaturesRequest(
            source=signal_source,
            price_history=relations.price_history,
            namespace="hard_filter_sma",
        ),
    )
    long_scaffold = build_long_scaffold_features(
        conn,
        LongScaffoldFeaturesRequest(
            source=signal_source,
            leadership_features=leadership_features,
            short_scaffold_features=short_features,
            namespace="hard_filter_long",
        ),
    )
    composed = compose_daily_ranking_signal_features(
        conn,
        source=signal_source,
        features=(long_scaffold, sma_features),
        namespace="sma5_score_ring_hard_filter",
    )
    panel_name = "ranking_sma5_score_ring_hard_filter_feature_panel"
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {panel_name} AS
        SELECT
            composed.*,
            CAST(
                CASE
                    WHEN composed.below_sma5_streak_ge3_flag THEN 3
                    WHEN composed.close_below_sma5_flag = 1 THEN 1
                    ELSE 0
                END AS INTEGER
            ) AS sma5_below_streak,
            CAST(
                CASE
                    WHEN composed.sma5 IS NOT NULL AND composed.atr20 > 0.0
                    THEN (composed.close - composed.sma5) / composed.atr20
                END AS DOUBLE
            ) AS sma5_atr20_deviation
        FROM {composed.name} AS composed
        """
    )
    panel = conn.execute(
        f"SELECT * FROM {panel_name} ORDER BY date, code"
    ).fetchdf()
    if "date" in panel.columns:
        panel["date"] = pd.to_datetime(panel["date"], errors="raise")
    return panel


def run_ranking_sma5_score_ring_hard_filter_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    bootstrap_resamples: int = 2_000,
    min_trades: int = 10,
    min_signal_dates: int = 10,
    observation_sample_limit: int = _DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingSma5ScoreRingHardFilterResearchResult:
    """Materialize the Market v5 score-ring feature panel with strict lineage."""

    _validate_research_parameters(
        bootstrap_resamples=bootstrap_resamples,
        min_trades=min_trades,
        min_signal_dates=min_signal_dates,
        observation_sample_limit=observation_sample_limit,
    )
    analysis_start = None if start_date is None else date.fromisoformat(start_date)
    analysis_end = None if end_date is None else date.fromisoformat(end_date)
    if analysis_start is not None and analysis_end is not None and analysis_start > analysis_end:
        raise ValueError("start_date must be on or before end_date")

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-sma5-score-ring-hard-filter-",
    ) as ctx:
        schema_version = require_market_v5_compatibility(
            ctx.connection,
            required_tables=_REQUIRED_MARKET_TABLES,
        )
        _assert_unambiguous_provider_adjusted_provenance(ctx.connection)
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="sma5_score_ring_hard_filter",
                analysis_start_date=analysis_start,
                analysis_end_date=analysis_end,
                horizons=(1,),
                market_scopes=("prime",),
                include_liquidity=True,
                percentile_features=(),
            ),
        )
        price_lineage = relations.lineage.price
        if (
            relations.lineage.verification_status != "verified"
            or price_lineage.verification_status != "verified"
            or not price_lineage.no_stock_data_fallback
        ):
            raise RuntimeError(
                "Market v5 price provenance is not verified; no stock_data fallback is allowed"
            )
        feature_df = build_score_ring_feature_panel(ctx.connection, relations)
        pit_lineage = MarketV5PitLineage(
            data_plane_schema_version=schema_version,
            stock_price_adjustment_mode="provider_adjusted_v1",
            price_projection_verification_status=price_lineage.verification_status,
            no_stock_data_fallback=price_lineage.no_stock_data_fallback,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    return RankingSma5ScoreRingHardFilterResearchResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        analysis_start_date=start_date,
        analysis_end_date=end_date,
        bootstrap_resamples=int(bootstrap_resamples),
        min_trades=int(min_trades),
        min_signal_dates=int(min_signal_dates),
        pit_lineage=pit_lineage,
        feature_df=feature_df,
        observation_sample_df=feature_df.head(int(observation_sample_limit)).copy(),
    )


def execute_variant(
    feature_df: pd.DataFrame,
    variant: ResearchVariant,
    *,
    fee_bps: float,
) -> VariantExecution:
    """Execute one state-machine variant using VectorBT's authoritative ledger."""

    fee_bps_value = _safe_finite_float_or_none(fee_bps)
    if fee_bps_value is None or fee_bps_value < 0.0:
        raise ValueError("fee_bps must be a finite non-negative number")
    frames = build_position_signal_frames(
        feature_df,
        ring_id=variant.ring_id,
        entry_rule_id=variant.entry_rule_id,
        exit_rule_id=variant.exit_rule_id,
        max_holding_sessions=variant.max_holding_sessions,
    )
    portfolio = VectorbtAdapter(engine="numba").create_signal_portfolio(
        close=frames.close,
        entries=frames.entries,
        exits=frames.exits,
        direction="longonly",
        init_cash=1_000_000.0,
        fees=fee_bps_value / 20_000.0,
        slippage=0.0,
        cash_sharing=False,
        group_by=False,
        accumulate=False,
        size=1.0,
        size_type="percent",
        freq="D",
    )
    trade_records = _normalize_and_reconcile_trade_records(
        portfolio,
        frames.state_events,
    )
    daily_returns = _build_active_portfolio_returns(portfolio, frames)
    return VariantExecution(
        variant=variant,
        portfolio=portfolio,
        signal_frames=frames,
        trade_records_df=trade_records,
        daily_portfolio_returns=daily_returns,
        state_events=frames.state_events.copy(),
    )


def _validate_research_parameters(
    *,
    bootstrap_resamples: int,
    min_trades: int,
    min_signal_dates: int,
    observation_sample_limit: int,
) -> None:
    for name, value in (
        ("bootstrap_resamples", bootstrap_resamples),
        ("min_trades", min_trades),
        ("min_signal_dates", min_signal_dates),
        ("observation_sample_limit", observation_sample_limit),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")


def _assert_unambiguous_provider_adjusted_provenance(conn: Any) -> None:
    rows = conn.execute(
        "SELECT DISTINCT value FROM sync_metadata "
        "WHERE key = 'stock_price_adjustment_mode'"
    ).fetchall()
    modes = {str(row[0]) for row in rows if row and row[0] is not None}
    if modes != {"provider_adjusted_v1"}:
        observed = ", ".join(sorted(modes)) if modes else "missing"
        raise RuntimeError(
            "Incompatible market.duckdb metadata: required "
            "stock_price_adjustment_mode=provider_adjusted_v1; observed "
            f"{observed}"
        )


def _normalize_and_reconcile_trade_records(
    portfolio: Any,
    state_events: pd.DataFrame,
) -> pd.DataFrame:
    records_readable = getattr(portfolio.trades, "records_readable", None)
    if records_readable is None:
        raise RuntimeError("VectorBT trade records_readable is unavailable")
    records = pd.DataFrame(records_readable).copy().reset_index(drop=True)
    required_columns = {"Column", "Entry Timestamp", "Exit Timestamp"}
    missing = sorted(required_columns.difference(records.columns))
    if missing:
        raise RuntimeError(
            "VectorBT trade ledger is missing required columns: " + ", ".join(missing)
        )
    if "Status" in records.columns and not records["Status"].eq("Closed").all():
        raise RuntimeError("VectorBT trade ledger contains an unclosed state-machine trade")

    entries = state_events.loc[state_events["event_type"].eq("entry")]
    exits = state_events.loc[state_events["event_type"].eq("exit")]
    if len(entries) != len(exits) or len(entries) != len(records):
        raise RuntimeError(
            "VectorBT trade ledger does not reconcile to the state-event pair count"
        )
    expected_entries = {
        (str(row.code), pd.Timestamp(str(row.date)))
        for row in entries.itertuples(index=False)
    }
    expected_exits = {
        (str(row.code), pd.Timestamp(str(row.date)))
        for row in exits.itertuples(index=False)
    }
    observed_entries = {
        (str(row["Column"]), pd.Timestamp(str(row["Entry Timestamp"])))
        for _, row in records.iterrows()
    }
    observed_exits = {
        (str(row["Column"]), pd.Timestamp(str(row["Exit Timestamp"])))
        for _, row in records.iterrows()
    }
    if observed_entries != expected_entries or observed_exits != expected_exits:
        raise RuntimeError(
            "VectorBT fills do not reconcile to the independently generated state events"
        )
    records["Column"] = records["Column"].astype(str)
    records["Entry Timestamp"] = pd.to_datetime(records["Entry Timestamp"], errors="raise")
    records["Exit Timestamp"] = pd.to_datetime(records["Exit Timestamp"], errors="raise")
    records["code"] = records["Column"]
    records["entry_date"] = records["Entry Timestamp"]
    records["exit_date"] = records["Exit Timestamp"]
    return records


def _build_active_portfolio_returns(
    portfolio: Any,
    frames: PositionSignalFrames,
) -> pd.Series:
    """Average only held returns, plus entry-fill fees booked by VectorBT."""

    raw_returns = pd.DataFrame(portfolio.returns()).reindex(
        index=frames.close.index,
        columns=frames.close.columns,
    )
    held_returns = raw_returns.where(frames.held_intervals)
    held_count = frames.held_intervals.sum(axis=1).astype(float)
    held_mean = held_returns.sum(axis=1, min_count=1).div(held_count.where(held_count > 0))

    entry_fee_returns = raw_returns.where(frames.entries)
    entry_count = frames.entries.sum(axis=1).astype(float)
    entry_fee_adjustment = entry_fee_returns.sum(axis=1, min_count=1).fillna(0.0).div(
        (held_count + entry_count).where((held_count + entry_count) > 0)
    ).fillna(0.0)
    daily_returns = held_mean.fillna(0.0) + entry_fee_adjustment
    daily_returns.name = "portfolio_return"
    return daily_returns.astype(float)
