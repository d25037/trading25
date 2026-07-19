"""Typed, namespaced Daily Ranking research orchestration.

Signal-time relations and forward outcomes are deliberately separate.  A consumer
must first materialize a signal-only cohort and can only then attach outcomes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
import re
from typing import Any, Literal, Sequence, cast
from uuid import uuid4

from src.domains.analytics.daily_ranking_core import (
    LIQUIDITY_MIN_OBSERVATIONS,
    liquidity_state_sql,
    percent_rank_sql,
    valuation_sql_expressions,
)
from src.domains.analytics.daily_ranking_event_time_prices import (
    DailyRankingPriceDiagnostics,
    DailyRankingPriceLineage,
    DailyRankingPriceRequest,
    build_daily_ranking_event_time_prices,
)
from src.domains.analytics.readonly_duckdb_support import normalize_code_sql
from src.shared.utils.market_code_alias import (
    MARKET_CODES_BY_SCOPE,
    normalize_market_scope,
)

_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_FORWARD_TOKEN_RE = re.compile(r"\bforward_[a-z0-9_]*", re.IGNORECASE)
_NIKKEI_SYNTHETIC_INDEX_CODE = "N225_UNDERPX"
_FEATURE_WARMUP_CALENDAR_DAYS = 720

MarketScope = Literal["all", "prime", "standard", "growth", "unknown"]
DailyRankingPercentileFeature = Literal[
    "forecast_per_to_per_ratio",
    "forecast_p_op_to_per_ratio",
    "forecast_operating_profit_growth_ratio",
    "per_to_fop_growth_ratio",
    "forecast_per_to_fop_growth_ratio",
]

_PERCENTILE_FEATURES: tuple[DailyRankingPercentileFeature, ...] = (
    "forecast_per_to_per_ratio",
    "forecast_p_op_to_per_ratio",
    "forecast_operating_profit_growth_ratio",
    "per_to_fop_growth_ratio",
    "forecast_per_to_fop_growth_ratio",
)
_MARKET_SCOPES: frozenset[str] = frozenset(
    {"all", "prime", "standard", "growth", "unknown"}
)

DAILY_RANKING_RESEARCH_PANEL_TABLE = "daily_ranking_research_panel"
DAILY_RANKING_RESEARCH_RANKED_TABLE = "daily_ranking_research_ranked"
DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE = (
    "daily_ranking_research_liquidity_ranked"
)
DAILY_RANKING_RESEARCH_SCOPED_TABLE = "daily_ranking_research_scoped"
DAILY_RANKING_RESEARCH_RELATIONS_TABLE = "daily_ranking_research_relations"

# Tasks 8-10 remove this list and the adapter after these consumers migrate.
DEPRECATED_DAILY_RANKING_RESEARCH_BRIDGE_CALLERS: tuple[str, ...] = (
    "ranking_crowded_long_tail_evidence",
    "ranking_daily_triage_lens",
    "ranking_fixed_return_priority_evidence",
    "ranking_forecast_operating_profit_growth_evidence",
    "ranking_liquidity_price_action_recomposition",
    "ranking_liquidity_z_long_evidence",
    "ranking_long_scaffold_factor_cross_evidence",
    "ranking_long_scaffold_value_composite_evidence",
    "ranking_moving_average_replacement_evidence",
    "ranking_n225_neutral_rerating_benchmark",
    "ranking_psr_valuation_evidence",
    "ranking_roe_quality_evidence",
    "ranking_sector_strength_evidence",
    "ranking_short_red_evidence",
    "ranking_short_sector_strength_evidence",
    "ranking_short_value_composite_evidence",
    "ranking_sma5_atr_deviation_evidence",
    "ranking_sma5_below_streak_evidence",
    "ranking_sma5_count_long_evidence",
    "ranking_sma5_count_short_evidence",
    "ranking_sma5_deviation_evidence",
    "ranking_sma5_position_state_evidence",
    "ranking_technical_fit_score_shape_evidence",
    "ranking_trend_acceleration_conditional_lift",
    "ranking_trend_slope_evidence",
)
DAILY_RANKING_RESEARCH_BRIDGE_DEPRECATED = True


@dataclass(frozen=True)
class RelationRef:
    """Validated metadata for one materialized DuckDB relation."""

    name: str
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    row_count: int

    def __post_init__(self) -> None:
        if not _NAMESPACE_RE.fullmatch(self.name):
            raise ValueError(f"invalid DuckDB relation name: {self.name}")
        if not self.columns or len(set(self.columns)) != len(self.columns):
            raise ValueError("relation columns must be non-empty and unique")
        if not self.key_columns or not set(self.key_columns).issubset(self.columns):
            raise ValueError("relation key columns must be present in columns")
        if self.row_count < 0:
            raise ValueError("relation row_count must be non-negative")


@dataclass(frozen=True)
class DailyRankingPanelRequest:
    """Typed signal-panel request; query padding is owned by the builder."""

    namespace: str
    analysis_start_date: date | None
    analysis_end_date: date | None
    horizons: tuple[int, ...]
    market_scopes: tuple[MarketScope, ...]
    include_liquidity: bool = True
    percentile_features: tuple[DailyRankingPercentileFeature, ...] = ()

    def __post_init__(self) -> None:
        if not _NAMESPACE_RE.fullmatch(self.namespace) or len(self.namespace) > 48:
            raise ValueError(f"invalid relation namespace: {self.namespace!r}")
        if self.analysis_start_date is not None and not isinstance(
            self.analysis_start_date, date
        ):
            raise TypeError("analysis_start_date must be a date or None")
        if self.analysis_end_date is not None and not isinstance(
            self.analysis_end_date, date
        ):
            raise TypeError("analysis_end_date must be a date or None")
        if (
            self.analysis_start_date is not None
            and self.analysis_end_date is not None
            and self.analysis_start_date > self.analysis_end_date
        ):
            raise ValueError("analysis_start_date must not be after analysis_end_date")
        horizons = tuple(sorted({int(value) for value in self.horizons}))
        if not horizons or any(value <= 0 for value in horizons):
            raise ValueError("horizons must contain positive integers")
        scopes = tuple(dict.fromkeys(str(value) for value in self.market_scopes))
        if not scopes or any(value not in _MARKET_SCOPES for value in scopes):
            raise ValueError("market_scopes contain an unsupported scope")
        features = tuple(dict.fromkeys(str(value) for value in self.percentile_features))
        if any(value not in _PERCENTILE_FEATURES for value in features):
            raise ValueError("percentile_features contain an unsupported feature")
        object.__setattr__(self, "horizons", horizons)
        object.__setattr__(self, "market_scopes", cast(tuple[MarketScope, ...], scopes))
        object.__setattr__(
            self,
            "percentile_features",
            cast(tuple[DailyRankingPercentileFeature, ...], features),
        )


@dataclass(frozen=True)
class DailyRankingLineageAudit:
    """Price and valuation lineage consumed by the signal panel."""

    price: DailyRankingPriceLineage
    valuation_basis_row_count: int
    valuation_basis_sha256: str
    verification_status: str
    no_stock_data_fallback: bool


@dataclass(frozen=True)
class DailyRankingBuildDiagnostics:
    """Cardinality and optional-stage diagnostics for one generation."""

    query_start_date: date | None
    query_end_date: date | None
    signal_panel_rows: int
    ranked_signal_rows: int
    liquidity_ranked_signal_rows: int | None
    signal_panel_key_rows: int
    ranked_signal_key_rows: int
    insufficient_history_rows: int
    incomplete_outcome_rows: int
    liquidity_stage_executed: bool
    percentile_features: tuple[DailyRankingPercentileFeature, ...]
    signal_panel_schema: tuple[str, ...]
    ranked_signal_schema: tuple[str, ...]
    liquidity_ranked_signal_schema: tuple[str, ...] | None
    prices: DailyRankingPriceDiagnostics


@dataclass(frozen=True)
class DailyRankingResearchRelations:
    signal_prices: RelationRef
    forward_outcomes: RelationRef
    signal_panel: RelationRef
    ranked_signals: RelationRef
    liquidity_ranked_signals: RelationRef | None
    lineage: DailyRankingLineageAudit
    diagnostics: DailyRankingBuildDiagnostics


@dataclass(frozen=True)
class DailyRankingResearchPanelSpec:
    """Deprecated fixed-alias response retained until Tasks 8-10."""

    panel_table: str
    ranked_table: str
    liquidity_ranked_table: str
    scoped_table: str
    relations_table: str
    legacy_panel_table: str
    legacy_ranked_table: str
    legacy_liquidity_ranked_table: str
    market_source: str
    market_scopes: tuple[str, ...]
    horizons: tuple[int, ...]
    query_start: str | None
    query_end: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    include_relation_percentiles: bool
    event_time_basis_only: bool


def normalize_daily_ranking_market_scopes(
    market_scopes: Sequence[str],
) -> tuple[str, ...]:
    """Normalize production market aliases to canonical research scopes."""

    normalized = tuple(
        dict.fromkeys(
            normalize_market_scope(value, default=value.strip().lower())
            for value in market_scopes
        )
    )
    if not normalized or any(value not in _MARKET_SCOPES for value in normalized):
        raise ValueError(
            "market_scopes must contain prime, standard, growth, unknown, or all"
        )
    return cast(tuple[str, ...], normalized)


def daily_ranking_query_start_date(
    start_date: str | None,
    *,
    warmup_calendar_days: int = _FEATURE_WARMUP_CALENDAR_DAYS,
) -> str | None:
    """Deprecated helper retained for callers awaiting typed request migration."""

    parsed = _parse_optional_date(start_date)
    return _format_optional_date(
        None if parsed is None else parsed - timedelta(days=int(warmup_calendar_days))
    )


def daily_ranking_query_end_date(
    end_date: str | None,
    *,
    max_horizon: int,
) -> str | None:
    """Deprecated helper retained for callers awaiting typed request migration."""

    parsed = _parse_optional_date(end_date)
    return _format_optional_date(
        None
        if parsed is None
        else parsed + timedelta(days=int(max_horizon) * 4 + 30)
    )


def assert_daily_ranking_research_tables(conn: Any) -> None:
    """Fail closed unless canonical Market v4 research inputs are present."""

    required = {
        "stock_data_raw",
        "stock_master_daily",
        "stock_adjustment_bases",
        "stock_adjustment_basis_segments",
        "daily_valuation",
        "topix_data",
        "indices_data",
    }
    observed = {
        str(row[0])
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    }
    missing = sorted(required - observed)
    if missing:
        raise ValueError(
            "market.duckdb is missing required Market v4 tables: " + ", ".join(missing)
        )


def build_daily_ranking_research_base(
    conn: Any,
    request: DailyRankingPanelRequest,
) -> DailyRankingResearchRelations:
    """Build one generation of signal-only research relations and outcomes."""

    assert_daily_ranking_research_tables(conn)
    _require_research_columns(conn)
    query_start, query_end = _resolve_query_bounds(request)
    market_codes = _market_codes_for_scopes(request.market_scopes)
    price_relations = build_daily_ranking_event_time_prices(
        conn,
        DailyRankingPriceRequest(
            namespace=request.namespace,
            query_start=_format_optional_date(query_start),
            query_end=_format_optional_date(query_end),
            analysis_start_date=_format_optional_date(request.analysis_start_date),
            analysis_end_date=_format_optional_date(request.analysis_end_date),
            horizons=request.horizons,
            market_codes=market_codes,
        ),
    )
    generation = price_relations.signal_features.removesuffix(
        "_signal_price_features"
    )
    signal_panel_name = f"{generation}_signal_panel"
    ranked_name = f"{generation}_ranked_signals"
    liquidity_name = f"{generation}_liquidity_ranked_signals"
    created = [
        price_relations.signal_features,
        price_relations.forward_outcomes,
        signal_panel_name,
        ranked_name,
        liquidity_name,
    ]
    try:
        _materialize_signal_panel(
            conn,
            request=request,
            signal_prices=price_relations.signal_features,
            relation_name=signal_panel_name,
            query_start=query_start,
            query_end=query_end,
        )
        _materialize_ranked_signals(
            conn,
            request=request,
            signal_panel=signal_panel_name,
            relation_name=ranked_name,
        )
        if request.include_liquidity:
            conn.execute(
                f"""
                CREATE TEMP VIEW {liquidity_name} AS
                SELECT * EXCLUDE (liquidity_scope),
                       liquidity_regime AS liquidity_scope
                FROM {ranked_name}
                WHERE market_scope <> 'all'
                """
            )

        signal_prices_ref = _relation_ref(
            conn,
            price_relations.signal_features,
            key_columns=("code", "date"),
        )
        outcomes_ref = _relation_ref(
            conn,
            price_relations.forward_outcomes,
            key_columns=("code", "date"),
        )
        signal_panel_ref = _relation_ref(
            conn,
            signal_panel_name,
            key_columns=("code", "date", "market_scope"),
            forbid_outcomes=True,
        )
        ranked_ref = _relation_ref(
            conn,
            ranked_name,
            key_columns=("code", "date", "market_scope"),
            forbid_outcomes=True,
        )
        liquidity_ref = (
            _relation_ref(
                conn,
                liquidity_name,
                key_columns=("code", "date", "market_scope", "liquidity_scope"),
                forbid_outcomes=True,
            )
            if request.include_liquidity
            else None
        )
        valuation_rows = _count(conn, signal_panel_name, "valuation_basis_id IS NOT NULL")
        lineage = DailyRankingLineageAudit(
            price=price_relations.lineage,
            valuation_basis_row_count=valuation_rows,
            valuation_basis_sha256=_ordered_sha256(
                conn,
                f"SELECT code, date, valuation_basis_id FROM {signal_panel_name} "
                "ORDER BY code, date, market_scope",
            ),
            verification_status="verified",
            no_stock_data_fallback=price_relations.lineage.no_stock_data_fallback,
        )
        diagnostics = DailyRankingBuildDiagnostics(
            query_start_date=query_start,
            query_end_date=query_end,
            signal_panel_rows=signal_panel_ref.row_count,
            ranked_signal_rows=ranked_ref.row_count,
            liquidity_ranked_signal_rows=(
                None if liquidity_ref is None else liquidity_ref.row_count
            ),
            signal_panel_key_rows=_distinct_key_count(
                conn, signal_panel_ref.name, signal_panel_ref.key_columns
            ),
            ranked_signal_key_rows=_distinct_key_count(
                conn, ranked_ref.name, ranked_ref.key_columns
            ),
            insufficient_history_rows=_count(
                conn, signal_panel_ref.name, "close_lag_150d IS NULL"
            ),
            incomplete_outcome_rows=int(
                conn.execute(
                    f"""
                    SELECT count(*)
                    FROM {signal_prices_ref.name} signal
                    LEFT JOIN {outcomes_ref.name} outcome USING (code, date)
                    WHERE outcome.code IS NULL
                    """
                ).fetchone()[0]
            ),
            liquidity_stage_executed=request.include_liquidity,
            percentile_features=request.percentile_features,
            signal_panel_schema=signal_panel_ref.columns,
            ranked_signal_schema=ranked_ref.columns,
            liquidity_ranked_signal_schema=(
                None if liquidity_ref is None else liquidity_ref.columns
            ),
            prices=price_relations.diagnostics,
        )
        return DailyRankingResearchRelations(
            signal_prices=signal_prices_ref,
            forward_outcomes=outcomes_ref,
            signal_panel=signal_panel_ref,
            ranked_signals=ranked_ref,
            liquidity_ranked_signals=liquidity_ref,
            lineage=lineage,
            diagnostics=diagnostics,
        )
    except Exception:
        for relation_name in reversed(created):
            _drop_relation_if_exists(conn, relation_name)
        raise


def materialize_daily_ranking_signal_cohort(
    conn: Any,
    relations: DailyRankingResearchRelations,
    *,
    name: str,
    select_sql: str,
) -> RelationRef:
    """Freeze cohort membership from signal columns only."""

    _validate_logical_name(name)
    lowered = select_sql.lower()
    if (
        relations.forward_outcomes.name.lower() in lowered
        or _FORWARD_TOKEN_RE.search(lowered)
    ):
        raise ValueError("signal cohort selection must not reference a forward outcome")
    generation = _research_generation(relations)
    relation_name = f"{generation}_cohort_{name}_g_{uuid4().hex}"
    try:
        conn.execute(f"CREATE TEMP TABLE {relation_name} AS {select_sql}")
        return _relation_ref(
            conn,
            relation_name,
            key_columns=_cohort_key_columns(conn, relation_name),
            forbid_outcomes=True,
        )
    except Exception:
        _drop_relation_if_exists(conn, relation_name)
        raise


def attach_daily_ranking_outcomes(
    conn: Any,
    cohort: RelationRef,
    relations: DailyRankingResearchRelations,
    *,
    name: str,
) -> RelationRef:
    """Attach outcomes to an already frozen, generation-matched cohort."""

    _validate_logical_name(name)
    generation = _research_generation(relations)
    if not cohort.name.startswith(f"{generation}_cohort_"):
        raise ValueError("cohort must be materialized from this research generation")
    if any(column.startswith("forward_") for column in cohort.columns):
        raise ValueError("cohort must not contain forward outcome columns")
    outcome_columns = tuple(
        column
        for column in relations.forward_outcomes.columns
        if column not in {"code", "date"}
    )
    relation_name = f"{generation}_evaluated_{name}_g_{uuid4().hex}"
    cohort_select = ", ".join(f"cohort.{column}" for column in cohort.columns)
    outcome_select = ", ".join(f"outcome.{column}" for column in outcome_columns)
    comma = ", " if outcome_select else ""
    try:
        conn.execute(
            f"""
            CREATE TEMP TABLE {relation_name} AS
            SELECT {cohort_select}{comma}{outcome_select}
            FROM {cohort.name} cohort
            LEFT JOIN {relations.forward_outcomes.name} outcome
              ON outcome.code = cohort.code AND outcome.date = cohort.date
            """
        )
        result = _relation_ref(
            conn,
            relation_name,
            key_columns=cohort.key_columns,
        )
        if result.row_count != cohort.row_count:
            raise RuntimeError("outcome attachment changed frozen cohort membership")
        return result
    except Exception:
        _drop_relation_if_exists(conn, relation_name)
        raise


def create_daily_ranking_research_panel(
    conn: Any,
    *,
    query_start: str | None,
    query_end: str | None,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    horizons: Sequence[int],
    market_scopes: Sequence[str],
    market_source: str = "stock_master_daily_exact_date",
    include_liquidity_ranked: bool = True,
    include_relation_percentiles: bool = True,
    event_time_basis_only: bool = False,
    price_feature_relation: str | None = None,
    price_outcome_relation: str | None = None,
) -> DailyRankingResearchPanelSpec:
    """Deprecated fixed-name bridge backed only by the typed event-time owner.

    ``query_*`` and prebuilt price relation arguments are accepted only for source
    compatibility.  Query bounds and projection ownership remain canonical here.
    """

    del price_feature_relation, price_outcome_relation
    if market_source != "stock_master_daily_exact_date":
        raise ValueError(f"Unsupported market_source for PIT research: {market_source}")
    resolved_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    percentile_features = _PERCENTILE_FEATURES if include_relation_percentiles else ()
    relations = build_daily_ranking_research_base(
        conn,
        DailyRankingPanelRequest(
            namespace="legacy_daily_ranking",
            analysis_start_date=_parse_optional_date(analysis_start_date),
            analysis_end_date=_parse_optional_date(analysis_end_date),
            horizons=tuple(int(value) for value in horizons),
            market_scopes=cast(tuple[MarketScope, ...], resolved_scopes),
            include_liquidity=include_liquidity_ranked,
            percentile_features=percentile_features,
        ),
    )
    _publish_deprecated_fixed_aliases(conn, relations)
    return DailyRankingResearchPanelSpec(
        panel_table=DAILY_RANKING_RESEARCH_PANEL_TABLE,
        ranked_table=DAILY_RANKING_RESEARCH_RANKED_TABLE,
        liquidity_ranked_table=DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE,
        scoped_table=DAILY_RANKING_RESEARCH_SCOPED_TABLE,
        relations_table=DAILY_RANKING_RESEARCH_RELATIONS_TABLE,
        legacy_panel_table="ranking_color_panel",
        legacy_ranked_table="ranking_color_ranked",
        legacy_liquidity_ranked_table="ranking_color_liquidity_ranked",
        market_source=market_source,
        market_scopes=resolved_scopes,
        horizons=tuple(sorted({int(value) for value in horizons})),
        query_start=query_start,
        query_end=query_end,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        include_relation_percentiles=include_relation_percentiles,
        event_time_basis_only=True,
    )


def deprecated_create_daily_ranking_observation_panel(
    conn: Any,
    **kwargs: Any,
) -> None:
    """Deprecated private bridge exported by Ranking Color until Tasks 8-10."""

    create_daily_ranking_research_panel(conn, **kwargs)


def deprecated_offset_daily_ranking_calendar_date(
    value: str | None,
    *,
    days: int,
) -> str | None:
    """Deprecated calendar helper exported until Tasks 8-10 migrate."""

    parsed = _parse_optional_date(value)
    return _format_optional_date(
        None if parsed is None else parsed + timedelta(days=days)
    )


def _materialize_signal_panel(
    conn: Any,
    *,
    request: DailyRankingPanelRequest,
    signal_prices: str,
    relation_name: str,
    query_start: date | None,
    query_end: date | None,
) -> None:
    market_code = normalize_code_sql("smd.code")
    valuation_code = normalize_code_sql("valuation.code")
    market_case = _market_scope_case_sql("market_code", "market_name")
    benchmark_conditions: list[str] = []
    benchmark_params: list[date] = []
    if query_start is not None:
        benchmark_conditions.append("CAST(date AS DATE) >= ?")
        benchmark_params.append(query_start)
    if query_end is not None:
        benchmark_conditions.append("CAST(date AS DATE) <= ?")
        benchmark_params.append(query_end)
    benchmark_where = (
        "" if not benchmark_conditions else "WHERE " + " AND ".join(benchmark_conditions)
    )
    market_filter = (
        "TRUE"
        if "all" in request.market_scopes
        else f"market.market IN ({_sql_strings(request.market_scopes)})"
    )
    liquidity_ctes = _liquidity_sql(request.include_liquidity)
    conn.execute(
        f"""
        CREATE TEMP TABLE {relation_name} AS
        WITH market_ranked AS (
            SELECT
                {market_code} AS code,
                CAST(smd.date AS DATE) AS date,
                CAST(smd.company_name AS VARCHAR) AS company_name,
                CAST(smd.market_code AS VARCHAR) AS market_code,
                CAST(smd.market_name AS VARCHAR) AS market_name,
                CAST(smd.scale_category AS VARCHAR) AS scale_category,
                row_number() OVER (
                    PARTITION BY {market_code}, CAST(smd.date AS DATE)
                    ORDER BY CASE WHEN smd.code = {market_code} THEN 0 ELSE 1 END,
                             length(smd.code), smd.code
                ) AS alias_rank
            FROM stock_master_daily smd
        ),
        market_master AS (
            SELECT code, date, company_name, market_code, market_name,
                   scale_category, {market_case} AS market
            FROM market_ranked
            WHERE alias_rank = 1
        ),
        topix_ranked AS (
            SELECT CAST(date AS DATE) AS date, CAST(close AS DOUBLE) AS topix_close,
                   lag(CAST(close AS DOUBLE), 20) OVER (ORDER BY CAST(date AS DATE))
                       AS topix_close_lag_20d,
                   lag(CAST(close AS DOUBLE), 60) OVER (ORDER BY CAST(date AS DATE))
                       AS topix_close_lag_60d
            FROM topix_data
            {benchmark_where}
        ),
        n225_ranked AS (
            SELECT CAST(date AS DATE) AS date, CAST(close AS DOUBLE) AS n225_close,
                   lag(CAST(close AS DOUBLE), 20) OVER (ORDER BY CAST(date AS DATE))
                       AS n225_close_lag_20d,
                   lag(CAST(close AS DOUBLE), 60) OVER (ORDER BY CAST(date AS DATE))
                       AS n225_close_lag_60d
            FROM indices_data
            WHERE upper(code) = '{_NIKKEI_SYNTHETIC_INDEX_CODE}'
              AND ({'TRUE' if not benchmark_conditions else ' AND '.join(benchmark_conditions)})
        ),
        panel_source AS (
            SELECT
                price.code,
                CAST(price.date AS DATE) AS date,
                market.market AS market_scope,
                price.price_basis_id,
                price.open, price.high, price.low, price.close, price.volume,
                price.med_adv60_jpy, price.med_adv60_sessions,
                price.close_lag_20d, price.close_lag_60d,
                price.close_lag_120d, price.close_lag_150d,
                price.close_lag_252d, price.close_lag_504d,
                price.atr20, price.atr20_sessions, price.atr60, price.atr60_sessions,
                price.atr20_pct, price.atr60_pct, price.atr20_to_atr60,
                price.atr20_change_20d_pct,
                price.recent_return_20d_pct, price.recent_return_60d_pct,
                price.recent_return_120d_pct, price.recent_return_150d_pct,
                price.recent_return_252d_pct, price.recent_return_504d_pct,
                price.ols_move_20d_pct, price.ols_r2_20,
                price.ols_move_60d_pct, price.ols_r2_60,
                market.company_name, market.market, market.market_code,
                market.scale_category,
                CAST(valuation.per AS DOUBLE) AS per,
                CAST(valuation.forward_per AS DOUBLE) AS forecast_per,
                CAST(valuation.pbr AS DOUBLE) AS pbr,
                CAST(valuation.p_op AS DOUBLE) AS p_op,
                CAST(valuation.forward_p_op AS DOUBLE) AS forecast_p_op,
                CAST(valuation.basis_version AS VARCHAR) AS valuation_basis_id,
                CAST(valuation.market_cap AS DOUBLE) / 1000000000.0
                    AS market_cap_bil_jpy,
                coalesce(
                    CAST(valuation.free_float_market_cap AS DOUBLE),
                    CAST(valuation.market_cap AS DOUBLE)
                ) AS free_float_market_cap_jpy,
                topix.topix_close,
                CASE WHEN topix.topix_close_lag_20d > 0 THEN
                    (topix.topix_close / topix.topix_close_lag_20d - 1.0) * 100.0
                END AS topix_recent_return_20d_pct,
                CASE WHEN topix.topix_close_lag_60d > 0 THEN
                    (topix.topix_close / topix.topix_close_lag_60d - 1.0) * 100.0
                END AS topix_recent_return_60d_pct,
                n225.n225_close,
                CASE WHEN n225.n225_close_lag_20d > 0 THEN
                    (n225.n225_close / n225.n225_close_lag_20d - 1.0) * 100.0
                END AS n225_recent_return_20d_pct,
                CASE WHEN n225.n225_close_lag_60d > 0 THEN
                    (n225.n225_close / n225.n225_close_lag_60d - 1.0) * 100.0
                END AS n225_recent_return_60d_pct
            FROM {signal_prices} price
            JOIN market_master market USING (code, date)
            JOIN daily_valuation valuation
              ON {valuation_code} = price.code
             AND CAST(valuation.date AS DATE) = price.date
             AND CAST(valuation.basis_version AS VARCHAR) = price.price_basis_id
            LEFT JOIN topix_ranked topix USING (date)
            LEFT JOIN n225_ranked n225 USING (date)
            WHERE {market_filter}
        ),
        {liquidity_ctes}
        panel_with_relations AS (
            SELECT
                liquidity.*,
                CASE WHEN per > 0 AND forecast_per > 0
                    THEN forecast_per / per END AS forecast_per_to_per_ratio,
                CASE WHEN per > 0 AND forecast_p_op > 0
                    THEN forecast_p_op / per END AS forecast_p_op_to_per_ratio,
                CASE WHEN p_op > 0 AND forecast_p_op > 0
                    THEN p_op / forecast_p_op END
                    AS forecast_operating_profit_growth_ratio,
                CASE WHEN p_op > 0 AND forecast_p_op > 0
                    THEN (p_op / forecast_p_op - 1.0) * 100.0 END
                    AS forecast_operating_profit_growth_pct,
                CASE WHEN per > 0 AND p_op > 0 AND forecast_p_op > 0
                    THEN per / (p_op / forecast_p_op) END AS per_to_fop_growth_ratio,
                CASE WHEN forecast_per > 0 AND p_op > 0 AND forecast_p_op > 0
                    THEN forecast_per / (p_op / forecast_p_op) END
                    AS forecast_per_to_fop_growth_ratio
            FROM liquidity
        )
        SELECT * FROM panel_with_relations
        """,
        [*benchmark_params, *benchmark_params],
    )


def _liquidity_sql(enabled: bool) -> str:
    if not enabled:
        return (
            "liquidity AS (SELECT panel_source.*, CAST(NULL AS DOUBLE) "
            "AS liquidity_residual_z, CAST('missing' AS VARCHAR) "
            "AS liquidity_regime FROM panel_source),"
        )
    regime = liquidity_state_sql(
        residual_z_sql="liquidity_residual_z",
        recent_return_20d_pct_sql="recent_return_20d_pct",
        recent_return_60d_pct_sql="recent_return_60d_pct",
    )
    return f"""
        liquidity_inputs AS (
            SELECT *,
                CASE WHEN med_adv60_sessions >= 60 AND med_adv60_jpy > 0
                          AND free_float_market_cap_jpy > 0
                    THEN ln(med_adv60_jpy) END AS log_adv60,
                CASE WHEN med_adv60_sessions >= 60 AND med_adv60_jpy > 0
                          AND free_float_market_cap_jpy > 0
                    THEN ln(free_float_market_cap_jpy) END AS log_free_float_market_cap
            FROM panel_source
        ),
        liquidity_fit AS (
            SELECT date, market,
                   count(log_adv60) AS observations,
                   regr_intercept(log_adv60, log_free_float_market_cap) AS alpha,
                   regr_slope(log_adv60, log_free_float_market_cap) AS beta
            FROM liquidity_inputs
            WHERE log_adv60 IS NOT NULL AND log_free_float_market_cap IS NOT NULL
            GROUP BY date, market
        ),
        liquidity_residuals AS (
            SELECT inputs.*, fit.observations, fit.beta,
                   CASE WHEN fit.observations >= {LIQUIDITY_MIN_OBSERVATIONS}
                              AND isfinite(fit.beta) AND fit.beta > 0
                       THEN inputs.log_adv60
                            - (fit.alpha + fit.beta * inputs.log_free_float_market_cap)
                   END AS liquidity_residual
            FROM liquidity_inputs inputs
            LEFT JOIN liquidity_fit fit USING (date, market)
        ),
        liquidity_scale AS (
            SELECT *,
                   sqrt(sum(liquidity_residual * liquidity_residual) OVER (
                       PARTITION BY date, market
                   ) / nullif(observations - 2, 0)) AS liquidity_residual_std
            FROM liquidity_residuals
        ),
        liquidity_z AS (
            SELECT *, CASE WHEN liquidity_residual_std > 0
                THEN liquidity_residual / liquidity_residual_std END
                AS liquidity_residual_z
            FROM liquidity_scale
        ),
        liquidity AS (
            SELECT * EXCLUDE (
                log_adv60, log_free_float_market_cap, observations, beta,
                liquidity_residual, liquidity_residual_std
            ), ({regime}) AS liquidity_regime
            FROM liquidity_z
        ),
        """


def _materialize_ranked_signals(
    conn: Any,
    *,
    request: DailyRankingPanelRequest,
    signal_panel: str,
    relation_name: str,
) -> None:
    panel_columns = _schema(conn, signal_panel)
    scope_union = (
        f"UNION ALL SELECT {', '.join(panel_columns[:2])}, 'all' AS market_scope, "
        f"{', '.join(panel_columns[3:])} FROM {signal_panel}"
        if "all" in request.market_scopes
        else ""
    )
    percentile_specs: tuple[tuple[str, str], ...] = (
        ("per", "per"),
        ("forecast_per", "forecast_per"),
        ("forecast_p_op", "forecast_p_op"),
        ("pbr", "pbr"),
        *((feature, feature) for feature in request.percentile_features),
    )
    raw_percentile_expressions: list[str] = []
    final_percentile_expressions: list[str] = []
    for feature, column in percentile_specs:
        positive_only = feature in {"per", "forecast_per", "forecast_p_op", "pbr"}
        value_sql = f"CASE WHEN {column} > 0 THEN {column} END" if positive_only else column
        population_sql = (
            f"market_scope, date, {column} > 0"
            if positive_only
            else f"market_scope, date, {column} IS NOT NULL"
        )
        raw_name = f"{feature}_percent_rank"
        raw_percentile_expressions.append(
            f"{percent_rank_sql(value_sql=value_sql, partition_by_sql=population_sql)} "
            f"AS {raw_name}"
        )
        final_percentile_expressions.append(
            f"CASE WHEN {value_sql} IS NOT NULL THEN {raw_name} END "
            f"AS {feature}_percentile"
        )
    raw_names = ", ".join(f"{feature}_percent_rank" for feature, _ in percentile_specs)
    valuation = valuation_sql_expressions(
        percentile_population_sql=(
            "CASE WHEN market_scope = 'all' THEN 'requested_union' ELSE 'per_market' END"
        ),
        per_percentile_sql="per_percentile",
        forward_per_percentile_sql="forecast_per_percentile",
        forward_p_op_percentile_sql="forecast_p_op_percentile",
        pbr_percentile_sql="pbr_percentile",
        per_sql="per",
        forward_per_sql="forecast_per",
    )
    conn.execute(
        f"""
        CREATE TEMP TABLE {relation_name} AS
        WITH scoped AS (
            SELECT * FROM {signal_panel}
            {scope_union}
        ),
        percentile_window AS (
            SELECT scoped.*, {', '.join(raw_percentile_expressions)}
            FROM scoped
        ),
        percentiles AS (
            SELECT * EXCLUDE ({raw_names}),
                   {', '.join(final_percentile_expressions)}
            FROM percentile_window
        )
        SELECT percentiles.*, 'all_liquidity' AS liquidity_scope,
               {valuation.strong_value_confirmation} AS strong_value_confirmation,
               {valuation.medium_value_confirmation} AS medium_value_confirmation,
               {valuation.overvalued_warning} AS overvalued_warning,
               {valuation.very_overvalued_warning} AS very_overvalued_warning,
               {valuation.no_positive_earnings_valuation}
                   AS no_positive_earnings_valuation,
               {valuation.no_value_confirmation} AS no_value_confirmation,
               ({valuation.signal}) AS valuation_signal
        FROM percentiles
        """
    )


def _publish_deprecated_fixed_aliases(
    conn: Any,
    relations: DailyRankingResearchRelations,
) -> None:
    panel_cohort = materialize_daily_ranking_signal_cohort(
        conn,
        relations,
        name="legacy_panel",
        select_sql=f"SELECT {', '.join(relations.signal_panel.columns)} "
        f"FROM {relations.signal_panel.name}",
    )
    panel_evaluated = attach_daily_ranking_outcomes(
        conn, panel_cohort, relations, name="legacy_panel"
    )
    ranked_cohort = materialize_daily_ranking_signal_cohort(
        conn,
        relations,
        name="legacy_ranked",
        select_sql=f"SELECT {', '.join(relations.ranked_signals.columns)} "
        f"FROM {relations.ranked_signals.name}",
    )
    ranked_evaluated = attach_daily_ranking_outcomes(
        conn, ranked_cohort, relations, name="legacy_ranked"
    )
    _create_legacy_view(conn, "ranking_color_panel", panel_evaluated)
    _create_legacy_view(conn, "ranking_color_panel_relations", panel_evaluated)
    _create_legacy_view(conn, "ranking_color_ranked", ranked_evaluated)
    _create_legacy_view(conn, DAILY_RANKING_RESEARCH_PANEL_TABLE, panel_evaluated)
    _create_legacy_view(conn, DAILY_RANKING_RESEARCH_RELATIONS_TABLE, panel_evaluated)
    _create_legacy_view(conn, DAILY_RANKING_RESEARCH_RANKED_TABLE, ranked_evaluated)
    _drop_relation_if_exists(conn, "ranking_color_liquidity_ranked")
    _drop_relation_if_exists(conn, DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE)
    if relations.liquidity_ranked_signals is not None:
        liquidity_cohort = materialize_daily_ranking_signal_cohort(
            conn,
            relations,
            name="legacy_liquidity",
            select_sql=(
                f"SELECT {', '.join(relations.liquidity_ranked_signals.columns)} "
                f"FROM {relations.liquidity_ranked_signals.name}"
            ),
        )
        liquidity_evaluated = attach_daily_ranking_outcomes(
            conn, liquidity_cohort, relations, name="legacy_liquidity"
        )
        _create_legacy_view(conn, "ranking_color_liquidity_ranked", liquidity_evaluated)
        _create_legacy_view(
            conn, DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE, liquidity_evaluated
        )
    scoped_sources = [DAILY_RANKING_RESEARCH_RANKED_TABLE]
    if relations.liquidity_ranked_signals is not None:
        scoped_sources.append(DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE)
    scoped_columns = ", ".join(_schema(conn, DAILY_RANKING_RESEARCH_RANKED_TABLE))
    scoped_select = " UNION ALL ".join(
        f"SELECT {scoped_columns} FROM {source}" for source in scoped_sources
    )
    _drop_relation_if_exists(conn, "ranking_color_scoped")
    conn.execute(f"CREATE TEMP VIEW ranking_color_scoped AS {scoped_select}")
    _drop_relation_if_exists(conn, DAILY_RANKING_RESEARCH_SCOPED_TABLE)
    conn.execute(
        f"CREATE TEMP VIEW {DAILY_RANKING_RESEARCH_SCOPED_TABLE} AS "
        f"SELECT {scoped_columns} FROM ranking_color_scoped"
    )


_LEGACY_COLUMN_NAMES: dict[str, str] = {
    "forecast_per": "forward_per",
    "forecast_p_op": "forward_p_op",
    "forecast_per_to_per_ratio": "forward_per_to_per_ratio",
    "forecast_p_op_to_per_ratio": "forward_p_op_to_per_ratio",
    "forecast_per_to_fop_growth_ratio": "forward_per_to_fop_growth_ratio",
    "forecast_per_percentile": "forward_per_percentile",
    "forecast_p_op_percentile": "forward_p_op_percentile",
    "forecast_per_to_per_ratio_percentile": "forward_per_to_per_ratio_percentile",
    "forecast_p_op_to_per_ratio_percentile": "forward_p_op_to_per_ratio_percentile",
    "forecast_per_to_fop_growth_ratio_percentile": (
        "forward_per_to_fop_growth_ratio_percentile"
    ),
}


def _create_legacy_view(conn: Any, name: str, relation: RelationRef) -> None:
    _drop_relation_if_exists(conn, name)
    select_columns = [
        f"{column} AS {_LEGACY_COLUMN_NAMES[column]}"
        if column in _LEGACY_COLUMN_NAMES
        else column
        for column in relation.columns
    ]
    horizons = sorted(
        {
            int(match.group(1))
            for column in relation.columns
            if (match := re.fullmatch(r"forward_close_return_(\d+)d_pct", column))
        }
    )
    for horizon in horizons:
        select_columns.extend(
            (
                f"forward_close_return_{horizon}d_pct "
                f"- forward_close_excess_return_{horizon}d_pct "
                f"AS topix_close_return_{horizon}d_pct",
                f"forward_close_return_{horizon}d_pct "
                f"- forward_close_n225_excess_return_{horizon}d_pct "
                f"AS n225_close_return_{horizon}d_pct",
            )
        )
    conn.execute(
        f"CREATE TEMP VIEW {name} AS SELECT {', '.join(select_columns)} "
        f"FROM {relation.name}"
    )


def _require_research_columns(conn: Any) -> None:
    required = {
        "stock_master_daily": {
            "date",
            "code",
            "company_name",
            "market_code",
            "market_name",
            "scale_category",
        },
        "daily_valuation": {
            "code",
            "date",
            "per",
            "forward_per",
            "pbr",
            "p_op",
            "forward_p_op",
            "market_cap",
            "free_float_market_cap",
            "basis_version",
        },
    }
    missing: list[str] = []
    for table, columns in required.items():
        observed = {
            str(row[1]) for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()
        }
        absent = sorted(columns - observed)
        if absent:
            missing.append(f"{table}=({', '.join(absent)})")
    if missing:
        raise RuntimeError("Daily Ranking research columns are missing: " + "; ".join(missing))


def _resolve_query_bounds(
    request: DailyRankingPanelRequest,
) -> tuple[date | None, date | None]:
    return (
        None
        if request.analysis_start_date is None
        else request.analysis_start_date - timedelta(days=_FEATURE_WARMUP_CALENDAR_DAYS),
        None
        if request.analysis_end_date is None
        else request.analysis_end_date
        + timedelta(days=max(request.horizons) * 4 + 30),
    )


def _market_codes_for_scopes(scopes: Sequence[str]) -> tuple[str, ...]:
    if "all" in scopes or "unknown" in scopes:
        return ()
    return tuple(
        dict.fromkeys(
            code
            for scope in scopes
            for code in MARKET_CODES_BY_SCOPE[scope]
        )
    )


def _market_scope_case_sql(market_code: str, market_name: str) -> str:
    clauses = " ".join(
        f"WHEN lower(trim({market_code})) IN ({_sql_strings(aliases)}) THEN '{scope}'"
        for scope, aliases in MARKET_CODES_BY_SCOPE.items()
    )
    name_clauses = " ".join(
        f"WHEN lower(trim({market_name})) IN ({_sql_strings(aliases)}) THEN '{scope}'"
        for scope, aliases in MARKET_CODES_BY_SCOPE.items()
    )
    return f"CASE {clauses} {name_clauses} ELSE 'unknown' END"


def _sql_strings(values: Sequence[str]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def _relation_ref(
    conn: Any,
    name: str,
    *,
    key_columns: tuple[str, ...],
    forbid_outcomes: bool = False,
) -> RelationRef:
    columns = _schema(conn, name)
    if forbid_outcomes and any(column.startswith("forward_") for column in columns):
        raise RuntimeError(f"signal relation contains forward outcome columns: {name}")
    _assert_date_columns(conn, name)
    row_count = _count(conn, name)
    if _distinct_key_count(conn, name, key_columns) != row_count:
        raise RuntimeError(f"relation keys are not unique: {name}")
    return RelationRef(name, columns, key_columns, row_count)


def _schema(conn: Any, relation: str) -> tuple[str, ...]:
    return tuple(
        str(row[1]) for row in conn.execute(f"PRAGMA table_info('{relation}')").fetchall()
    )


def _assert_date_columns(conn: Any, relation: str) -> None:
    schema = {
        str(row[1]): str(row[2]).upper()
        for row in conn.execute(f"PRAGMA table_info('{relation}')").fetchall()
    }
    invalid = {
        column: sql_type
        for column, sql_type in schema.items()
        if (
            column == "date"
            or column.startswith("date_")
            or column.endswith("_date")
            or "_date_" in column
        )
        and sql_type != "DATE"
    }
    if invalid:
        raise RuntimeError(f"relation date columns must be DATE: {relation} {invalid}")


def _distinct_key_count(
    conn: Any,
    relation: str,
    key_columns: Sequence[str],
) -> int:
    columns = ", ".join(key_columns)
    return int(
        conn.execute(
            f"SELECT count(*) FROM (SELECT {columns} FROM {relation} GROUP BY {columns})"
        ).fetchone()[0]
    )


def _count(conn: Any, relation: str, predicate: str | None = None) -> int:
    where = "" if predicate is None else f" WHERE {predicate}"
    return int(conn.execute(f"SELECT count(*) FROM {relation}{where}").fetchone()[0])


def _ordered_sha256(conn: Any, query: str) -> str:
    digest = hashlib.sha256()
    cursor = conn.execute(query)
    while rows := cursor.fetchmany(10_000):
        for row in rows:
            digest.update(repr(tuple(row)).encode())
            digest.update(b"\n")
    return digest.hexdigest()


def _research_generation(relations: DailyRankingResearchRelations) -> str:
    suffix = "_ranked_signals"
    if not relations.ranked_signals.name.endswith(suffix):
        raise ValueError("ranked relation does not expose a research generation")
    generation = relations.ranked_signals.name.removesuffix(suffix)
    expected = {
        f"{generation}_signal_price_features",
        f"{generation}_forward_price_outcomes",
        f"{generation}_signal_panel",
    }
    observed = {
        relations.signal_prices.name,
        relations.forward_outcomes.name,
        relations.signal_panel.name,
    }
    if observed != expected:
        raise ValueError("research relations do not share one generation")
    return generation


def _cohort_key_columns(conn: Any, relation: str) -> tuple[str, ...]:
    columns = set(_schema(conn, relation))
    if not {"code", "date"}.issubset(columns):
        raise ValueError("signal cohort must contain code and date")
    return (
        ("code", "date", "market_scope")
        if "market_scope" in columns
        else ("code", "date")
    )


def _validate_logical_name(name: str) -> None:
    if not _NAMESPACE_RE.fullmatch(name):
        raise ValueError(f"invalid DuckDB relation name: {name}")


def _drop_relation_if_exists(conn: Any, name: str) -> None:
    row = conn.execute(
        "SELECT table_type FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    if row is None:
        return
    object_type = "VIEW" if "VIEW" in str(row[0]).upper() else "TABLE"
    conn.execute(f"DROP {object_type} {name}")


def _parse_optional_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _format_optional_date(value: date | None) -> str | None:
    return None if value is None else value.isoformat()
