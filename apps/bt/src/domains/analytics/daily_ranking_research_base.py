"""Typed, namespaced Daily Ranking research orchestration.

Signal-time relations and forward outcomes are deliberately separate.  A consumer
must first materialize a signal-only cohort and can only then attach outcomes.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date, timedelta
import hashlib
import re
from threading import RLock
from typing import Any, Iterator, Literal, Sequence, cast
from uuid import uuid4
import weakref

from src.domains.analytics.daily_ranking_core import (
    LIQUIDITY_MIN_OBSERVATIONS,
    liquidity_state_sql,
    percent_rank_sql,
    valuation_sql_expressions,
)
from src.domains.analytics.daily_ranking_event_time_prices import (
    DAILY_RANKING_PRICE_HISTORY_COLUMNS,
    DAILY_RANKING_SIGNAL_FEATURE_COLUMNS,
    DailyRankingPriceDiagnostics,
    DailyRankingPriceLineage,
    DailyRankingPriceRequest,
    build_daily_ranking_event_time_prices,
    daily_ranking_valid_raw_bar_sql,
    daily_ranking_forward_outcome_columns,
)
from src.domains.analytics.readonly_duckdb_support import normalize_code_sql
from src.shared.utils.market_code_alias import (
    MARKET_CODES_BY_SCOPE,
    normalize_market_scope,
)

_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_SQL_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
_FORWARD_TOKEN_RE = re.compile(r"\bforward_[a-z0-9_]*", re.IGNORECASE)
_NIKKEI_SYNTHETIC_INDEX_CODE = "N225_UNDERPX"
_BASE_FEATURE_LOOKBACK_SESSIONS = 504
DAILY_RANKING_BASE_REQUIRED_VALID_SESSIONS = _BASE_FEATURE_LOOKBACK_SESSIONS + 1
_SAFE_SQL_TYPE_RE = re.compile(r"^(?:BIGINT|BOOLEAN|DATE|DOUBLE|INTEGER|VARCHAR)$")
_FORBIDDEN_SIGNAL_EXPRESSION_RE = re.compile(
    r"(?:;|--|/\*|\*/|\b(?:attach|copy|create|delete|drop|from|insert|join|pragma|"
    r"read_[a-z0-9_]*|select|union|update|with)\b|\bforward_[a-z0-9_]*)",
    re.IGNORECASE,
)
_SQL_EXPRESSION_WORDS = frozenset(
    {
        "and",
        "as",
        "between",
        "case",
        "coalesce",
        "else",
        "end",
        "false",
        "in",
        "is",
        "isfinite",
        "like",
        "not",
        "null",
        "or",
        "then",
        "true",
        "when",
    }
)

MarketScope = Literal["all", "prime", "standard", "growth", "unknown"]
RelationKind = Literal[
    "price_history",
    "signal_prices",
    "forward_outcomes",
    "signal_panel",
    "ranked_signals",
    "liquidity_ranked_signals",
    "cohort",
    "evaluated",
    "signal_features",
    "untrusted",
]
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

RelationSchema = tuple[tuple[str, str], ...]

_BIGINT_SIGNAL_PRICE_COLUMNS = frozenset(
    {"volume", "med_adv60_sessions", "atr20_sessions", "atr60_sessions"}
)
_VARCHAR_SIGNAL_PRICE_COLUMNS = frozenset({"code", "price_basis_id"})
DAILY_RANKING_SIGNAL_PRICE_SCHEMA: RelationSchema = tuple(
    (
        column,
        "DATE"
        if column == "date"
        else "VARCHAR"
        if column in _VARCHAR_SIGNAL_PRICE_COLUMNS
        else "BIGINT"
        if column in _BIGINT_SIGNAL_PRICE_COLUMNS
        else "DOUBLE",
    )
    for column in DAILY_RANKING_SIGNAL_FEATURE_COLUMNS
)
DAILY_RANKING_PRICE_HISTORY_SCHEMA: RelationSchema = tuple(
    (
        column,
        "DATE"
        if column == "date"
        else "VARCHAR"
        if column in {"code", "price_basis_id"}
        else "BIGINT"
        if column == "volume"
        else "DOUBLE",
    )
    for column in DAILY_RANKING_PRICE_HISTORY_COLUMNS
)
_SIGNAL_PANEL_EXTRA_SCHEMA: RelationSchema = (
    ("company_name", "VARCHAR"),
    ("market", "VARCHAR"),
    ("market_code", "VARCHAR"),
    ("scale_category", "VARCHAR"),
    ("per", "DOUBLE"),
    ("forecast_per", "DOUBLE"),
    ("pbr", "DOUBLE"),
    ("p_op", "DOUBLE"),
    ("forecast_p_op", "DOUBLE"),
    ("valuation_basis_id", "VARCHAR"),
    ("market_cap_bil_jpy", "DOUBLE"),
    ("free_float_market_cap_jpy", "DOUBLE"),
    ("topix_close", "DOUBLE"),
    ("topix_recent_return_20d_pct", "DOUBLE"),
    ("topix_recent_return_60d_pct", "DOUBLE"),
    ("n225_close", "DOUBLE"),
    ("n225_recent_return_20d_pct", "DOUBLE"),
    ("n225_recent_return_60d_pct", "DOUBLE"),
    ("liquidity_residual_z", "DOUBLE"),
    ("liquidity_regime", "VARCHAR"),
    ("forecast_per_to_per_ratio", "DOUBLE"),
    ("forecast_p_op_to_per_ratio", "DOUBLE"),
    ("forecast_operating_profit_growth_ratio", "DOUBLE"),
    ("forecast_operating_profit_growth_pct", "DOUBLE"),
    ("per_to_fop_growth_ratio", "DOUBLE"),
    ("forecast_per_to_fop_growth_ratio", "DOUBLE"),
)
_PANEL_SOURCE_SCHEMA: RelationSchema = (
    DAILY_RANKING_SIGNAL_PRICE_SCHEMA[:2]
    + (("market_scope", "VARCHAR"),)
    + DAILY_RANKING_SIGNAL_PRICE_SCHEMA[2:]
    + _SIGNAL_PANEL_EXTRA_SCHEMA[:18]
)
_LIQUIDITY_PANEL_SCHEMA: RelationSchema = (
    _PANEL_SOURCE_SCHEMA + _SIGNAL_PANEL_EXTRA_SCHEMA[18:20]
)
DAILY_RANKING_SIGNAL_PANEL_SCHEMA: RelationSchema = (
    DAILY_RANKING_SIGNAL_PRICE_SCHEMA[:2]
    + (("market_scope", "VARCHAR"),)
    + DAILY_RANKING_SIGNAL_PRICE_SCHEMA[2:]
    + _SIGNAL_PANEL_EXTRA_SCHEMA
)
_BASE_PERCENTILE_SCHEMA: RelationSchema = (
    ("per_percentile", "DOUBLE"),
    ("forecast_per_percentile", "DOUBLE"),
    ("forecast_p_op_percentile", "DOUBLE"),
    ("pbr_percentile", "DOUBLE"),
)
_VALUATION_CLASSIFICATION_SCHEMA: RelationSchema = (
    ("liquidity_scope", "VARCHAR"),
    ("strong_value_confirmation", "BOOLEAN"),
    ("medium_value_confirmation", "BOOLEAN"),
    ("overvalued_warning", "BOOLEAN"),
    ("very_overvalued_warning", "BOOLEAN"),
    ("no_positive_earnings_valuation", "BOOLEAN"),
    ("no_value_confirmation", "BOOLEAN"),
    ("valuation_signal", "VARCHAR"),
)

@dataclass(frozen=True)
class RelationRef:
    """Validated metadata for one materialized DuckDB relation."""

    name: str
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    row_count: int
    column_types: tuple[str, ...] = ()
    generation: str = ""
    kind: RelationKind = "untrusted"
    _capability: object | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        _validate_sql_identifier(self.name)
        for column in (*self.columns, *self.key_columns):
            _validate_sql_identifier(column)
        if self.generation:
            _validate_sql_identifier(self.generation)
        if not self.columns or len(set(self.columns)) != len(self.columns):
            raise ValueError("relation columns must be non-empty and unique")
        if not self.key_columns or not set(self.key_columns).issubset(self.columns):
            raise ValueError("relation key columns must be present in columns")
        if self.row_count < 0:
            raise ValueError("relation row_count must be non-negative")
        if self.column_types and len(self.column_types) != len(self.columns):
            raise ValueError("relation column types must align with columns")


@dataclass(frozen=True)
class _RelationFingerprint:
    row_count: int
    distinct_key_count: int
    null_key_count: int
    key_xor_v1: int
    key_sum_v1: int
    key_xor_v2: int
    key_sum_v2: int
    content_xor_v1: int
    content_sum_v1: int
    content_xor_v2: int
    content_sum_v2: int

    @property
    def key_aggregates(self) -> tuple[int, ...]:
        return (
            self.key_xor_v1,
            self.key_sum_v1,
            self.key_xor_v2,
            self.key_sum_v2,
        )

    @property
    def content_aggregates(self) -> tuple[int, ...]:
        return (
            self.content_xor_v1,
            self.content_sum_v1,
            self.content_xor_v2,
            self.content_sum_v2,
        )


@dataclass(frozen=True)
class _IssuedRelation:
    """Process-local seal for one exact RelationRef object and physical relation."""

    ref: weakref.ReferenceType[RelationRef]
    connection: Any
    provenance: tuple[Any, ...]
    physical_identity: tuple[Any, ...]
    schema: RelationSchema
    fingerprint: _RelationFingerprint


@dataclass
class _RelationValidationScope:
    connection: Any
    validated: dict[int, RelationRef]


_ISSUED_RELATIONS: dict[int, _IssuedRelation] = {}
_ISSUED_RELATIONS_LOCK = RLock()
_ACTIVE_RELATION_VALIDATION_SCOPE: ContextVar[_RelationValidationScope | None] = (
    ContextVar("daily_ranking_relation_validation_scope", default=None)
)


@contextmanager
def _daily_ranking_validation_scope(  # pyright: ignore[reportUnusedFunction]
    conn: Any,
) -> Iterator[None]:
    """Deduplicate currentness scans only within one synchronous builder call."""

    active = _ACTIVE_RELATION_VALIDATION_SCOPE.get()
    if active is not None and active.connection is conn:
        yield
        return
    token = _ACTIVE_RELATION_VALIDATION_SCOPE.set(
        _RelationValidationScope(connection=conn, validated={})
    )
    try:
        yield
    finally:
        _ACTIVE_RELATION_VALIDATION_SCOPE.reset(token)


def validate_daily_ranking_signal_relation(
    conn: Any,
    relation: RelationRef,
    *,
    authority: RelationRef | None = None,
    required_columns: Sequence[str] = (),
) -> None:
    """Validate a current trusted signal relation and optional build authority."""

    signal_kinds: frozenset[RelationKind] = frozenset(
        {
            "signal_prices",
            "signal_panel",
            "ranked_signals",
            "liquidity_ranked_signals",
            "cohort",
            "signal_features",
        }
    )
    if relation.kind not in signal_kinds:
        raise ValueError("feature builders require a signal relation")
    if relation._capability is None or not relation.generation:
        raise ValueError("feature builders require a trusted signal relation")
    if not relation.name.startswith(f"{relation.generation}_"):
        raise ValueError("signal relation name does not match its generation")
    if any(column.startswith("forward_") for column in relation.columns):
        raise ValueError("signal relation contains forward outcome columns")
    if authority is not None and (
        relation.generation != authority.generation
        or relation._capability is not authority._capability
    ):
        raise ValueError("signal relation generation/capability mismatch")
    missing = sorted(set(required_columns) - set(relation.columns))
    if missing:
        raise ValueError(
            "signal relation is missing required columns: " + ", ".join(missing)
        )
    scope = _ACTIVE_RELATION_VALIDATION_SCOPE.get()
    if (
        scope is not None
        and scope.connection is conn
        and scope.validated.get(id(relation)) is relation
    ):
        return
    _assert_ref_current(conn, relation)
    if scope is not None and scope.connection is conn:
        scope.validated[id(relation)] = relation


def validate_daily_ranking_price_history_relation(
    conn: Any,
    relation: RelationRef,
    *,
    authority: RelationRef,
) -> None:
    """Validate the exact issued warmup history for one signal generation."""

    validate_daily_ranking_signal_relation(conn, authority)
    if relation.kind != "price_history":
        raise ValueError("rolling feature builders require a price-history relation")
    if (
        relation.generation != authority.generation
        or relation._capability is not authority._capability
    ):
        raise ValueError("price-history generation/capability mismatch")
    if relation.key_columns != ("code", "date", "price_basis_id"):
        raise ValueError("price-history relation has invalid keys")
    if tuple(zip(relation.columns, relation.column_types, strict=True)) != (
        DAILY_RANKING_PRICE_HISTORY_SCHEMA
    ):
        raise ValueError("price-history relation has invalid schema")
    _assert_ref_current(conn, relation)


def publish_daily_ranking_signal_features(
    conn: Any,
    *,
    source: RelationRef,
    relation_name: str,
    expected_schema: RelationSchema,
) -> RelationRef:
    """Validate and publish one feature relation under the source authority."""

    validate_daily_ranking_signal_relation(conn, source)
    if not set(source.key_columns).issubset(_column_names(expected_schema)):
        raise ValueError("feature relation schema must retain all source key columns")
    published = _relation_ref(
        conn,
        relation_name,
        key_columns=source.key_columns,
        expected_schema=expected_schema,
        generation=source.generation,
        kind="signal_features",
        capability=source._capability,
        forbid_outcomes=True,
    )
    key_columns = ", ".join(source.key_columns)
    membership_delta = _count(
        conn,
        (
            f"((SELECT {key_columns} FROM {source.name} "
            f"EXCEPT SELECT {key_columns} FROM {published.name}) "
            f"UNION ALL (SELECT {key_columns} FROM {published.name} "
            f"EXCEPT SELECT {key_columns} FROM {source.name}))"
        ),
    )
    if membership_delta:
        raise RuntimeError("signal feature publication changed source membership")
    return published


@dataclass(frozen=True)
class SignalExpression:
    """Restricted signal-only SQL expression with declared source columns."""

    sql: str
    referenced_columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.sql.strip():
            raise ValueError("signal expression SQL must not be empty")
        if len(set(self.referenced_columns)) != len(self.referenced_columns):
            raise ValueError("signal expression columns must be unique")


@dataclass(frozen=True)
class SignalDerivedColumn:
    """One typed signal-time column added while freezing a cohort."""

    name: str
    expression: SignalExpression
    sql_type: str

    def __post_init__(self) -> None:
        if not _NAMESPACE_RE.fullmatch(self.name):
            raise ValueError(f"invalid derived signal column: {self.name}")
        if not self.sql_type.strip():
            raise ValueError("derived signal column SQL type must not be empty")


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
    required_valid_sessions: int = DAILY_RANKING_BASE_REQUIRED_VALID_SESSIONS

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
        if isinstance(self.required_valid_sessions, bool) or not isinstance(
            self.required_valid_sessions, int
        ):
            raise TypeError("required_valid_sessions must be an integer")
        if self.required_valid_sessions < DAILY_RANKING_BASE_REQUIRED_VALID_SESSIONS:
            raise ValueError(
                f"required_valid_sessions must be at least "
                f"{DAILY_RANKING_BASE_REQUIRED_VALID_SESSIONS}"
            )
        horizons = tuple(sorted({int(value) for value in self.horizons}))
        if not horizons or any(value <= 0 for value in horizons):
            raise ValueError("horizons must contain positive integers")
        scopes = tuple(dict.fromkeys(str(value) for value in self.market_scopes))
        if not scopes or any(value not in _MARKET_SCOPES for value in scopes):
            raise ValueError("market_scopes contain an unsupported scope")
        features = tuple(
            dict.fromkeys(str(value) for value in self.percentile_features)
        )
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
    price_history: RelationRef
    signal_prices: RelationRef
    forward_outcomes: RelationRef
    signal_panel: RelationRef
    ranked_signals: RelationRef
    liquidity_ranked_signals: RelationRef | None
    lineage: DailyRankingLineageAudit
    diagnostics: DailyRankingBuildDiagnostics
    generation: str
    _capability: object = field(repr=False, compare=False)
    _cohorts: dict[str, RelationRef] = field(
        default_factory=dict,
        repr=False,
        compare=False,
    )


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


def assert_daily_ranking_research_tables(conn: Any) -> None:
    """Fail closed unless canonical Market v5 research inputs are present."""

    required = {
        "stock_data_raw",
        "stock_data",
        "stock_master_daily",
        "stock_provider_windows",
        "stock_adjustment_events",
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
            "market.duckdb is missing required Market v5 tables: " + ", ".join(missing)
        )


def build_daily_ranking_research_base(
    conn: Any,
    request: DailyRankingPanelRequest,
) -> DailyRankingResearchRelations:
    """Build one generation of signal-only research relations and outcomes."""

    assert_daily_ranking_research_tables(conn)
    _require_research_columns(conn)
    market_codes = _market_codes_for_scopes(request.market_scopes)
    query_start, query_end = _resolve_query_bounds(
        conn,
        request,
        market_codes=market_codes,
    )
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
    generation = price_relations.signal_features.removesuffix("_signal_price_features")
    capability = object()
    signal_panel_name = f"{generation}_signal_panel"
    ranked_name = f"{generation}_ranked_signals"
    liquidity_name = f"{generation}_liquidity_ranked_signals"
    created = [
        price_relations.signal_features,
        price_relations.forward_outcomes,
        price_relations.price_history,
        signal_panel_name,
        ranked_name,
        liquidity_name,
    ]
    try:
        _assert_exact_schema(
            conn,
            price_relations.signal_features,
            DAILY_RANKING_SIGNAL_PRICE_SCHEMA,
            label="signal price",
        )
        _assert_exact_schema(
            conn,
            price_relations.forward_outcomes,
            _forward_outcome_schema(request.horizons),
            label="forward outcome",
        )
        _assert_market_alias_consistency(conn, price_relations.signal_features)
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
            ranked_columns = _column_names(_ranked_signal_schema(request))
            liquidity_select = ", ".join(
                "liquidity_regime AS liquidity_scope"
                if column == "liquidity_scope"
                else column
                for column in ranked_columns
            )
            conn.execute(
                f"""
                CREATE TEMP VIEW {liquidity_name} AS
                SELECT {liquidity_select}
                FROM {ranked_name}
                WHERE market_scope <> 'all'
                """
            )

        price_history_ref = _relation_ref(
            conn,
            price_relations.price_history,
            key_columns=("code", "date", "price_basis_id"),
            expected_schema=DAILY_RANKING_PRICE_HISTORY_SCHEMA,
            generation=generation,
            kind="price_history",
            capability=capability,
            forbid_outcomes=True,
        )
        signal_prices_ref = _relation_ref(
            conn,
            price_relations.signal_features,
            key_columns=("code", "date"),
            expected_schema=DAILY_RANKING_SIGNAL_PRICE_SCHEMA,
            generation=generation,
            kind="signal_prices",
            capability=capability,
        )
        outcomes_ref = _relation_ref(
            conn,
            price_relations.forward_outcomes,
            key_columns=("code", "date"),
            expected_schema=_forward_outcome_schema(request.horizons),
            generation=generation,
            kind="forward_outcomes",
            capability=capability,
        )
        signal_panel_ref = _relation_ref(
            conn,
            signal_panel_name,
            key_columns=("code", "date", "market_scope"),
            expected_schema=DAILY_RANKING_SIGNAL_PANEL_SCHEMA,
            generation=generation,
            kind="signal_panel",
            capability=capability,
            forbid_outcomes=True,
        )
        ranked_ref = _relation_ref(
            conn,
            ranked_name,
            key_columns=("code", "date", "market_scope"),
            expected_schema=_ranked_signal_schema(request),
            generation=generation,
            kind="ranked_signals",
            capability=capability,
            forbid_outcomes=True,
        )
        liquidity_ref = (
            _relation_ref(
                conn,
                liquidity_name,
                key_columns=("code", "date", "market_scope", "liquidity_scope"),
                expected_schema=_ranked_signal_schema(request),
                generation=generation,
                kind="liquidity_ranked_signals",
                capability=capability,
                forbid_outcomes=True,
            )
            if request.include_liquidity
            else None
        )
        valuation_rows = _count(
            conn, signal_panel_name, "valuation_basis_id IS NOT NULL"
        )
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
                    WHERE {_incomplete_outcome_predicate(request.horizons)}
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
            price_history=price_history_ref,
            signal_prices=signal_prices_ref,
            forward_outcomes=outcomes_ref,
            signal_panel=signal_panel_ref,
            ranked_signals=ranked_ref,
            liquidity_ranked_signals=liquidity_ref,
            lineage=lineage,
            diagnostics=diagnostics,
            generation=generation,
            _capability=capability,
        )
    except Exception:
        for relation_name in reversed(created):
            _drop_relation_if_exists(conn, relation_name)
        raise


def materialize_daily_ranking_signal_cohort(
    conn: Any,
    relations: DailyRankingResearchRelations,
    *,
    source: RelationRef,
    name: str,
    columns: Sequence[str] | None = None,
    predicate: SignalExpression | None = None,
    derived_columns: Sequence[SignalDerivedColumn] = (),
    order_by: Sequence[tuple[str, Literal["asc", "desc"]]] = (),
    limit: int | None = None,
) -> RelationRef:
    """Freeze a validated signal-only cohort from one returned source relation."""

    _validate_logical_name(name)
    generation = _research_generation(relations)
    allowed_sources = tuple(
        relation
        for relation in (
            relations.signal_prices,
            relations.signal_panel,
            relations.ranked_signals,
            relations.liquidity_ranked_signals,
        )
        if relation is not None
    )
    returned_by_build = any(source is relation for relation in allowed_sources)
    if not returned_by_build and source.kind != "signal_features":
        raise ValueError("source must be a signal relation returned by this build")
    _validate_relation_provenance(source, relations)
    if source.kind == "signal_features":
        validate_daily_ranking_signal_relation(
            conn,
            source,
            authority=relations.ranked_signals,
        )
    else:
        _assert_ref_current(conn, source)
    selected_columns = source.columns if columns is None else tuple(columns)
    if not selected_columns or len(set(selected_columns)) != len(selected_columns):
        raise ValueError("cohort projection columns must be non-empty and unique")
    if not set(selected_columns).issubset(source.columns):
        raise ValueError("cohort projection must use source signal columns only")
    _validate_signal_expression(predicate, source)
    derived_names = tuple(column.name for column in derived_columns)
    if len(set(derived_names)) != len(derived_names) or set(derived_names) & set(
        selected_columns
    ):
        raise ValueError("derived signal column names must be unique")
    for derived in derived_columns:
        _validate_signal_expression(derived.expression, source)
        if not _SAFE_SQL_TYPE_RE.fullmatch(derived.sql_type.upper()):
            raise ValueError(f"unsupported derived signal SQL type: {derived.sql_type}")
    for order_column, direction in order_by:
        if order_column not in source.columns or direction not in {"asc", "desc"}:
            raise ValueError("cohort ordering must use a source column and asc/desc")
    if limit is not None and (
        not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0
    ):
        raise ValueError("cohort limit must be a positive integer")
    source_types = dict(zip(source.columns, source.column_types, strict=True))
    expected_schema: RelationSchema = tuple(
        (column, source_types[column]) for column in selected_columns
    ) + tuple((column.name, column.sql_type.upper()) for column in derived_columns)
    relation_name = f"{generation}_cohort_{name}_g_{uuid4().hex}"
    projection = [f"source.{column}" for column in selected_columns]
    projection.extend(
        f"CAST(({column.expression.sql}) AS {column.sql_type.upper()}) AS {column.name}"
        for column in derived_columns
    )
    where_sql = "TRUE" if predicate is None else predicate.sql
    order_sql = (
        ""
        if not order_by
        else " ORDER BY "
        + ", ".join(
            f"source.{column} {direction.upper()}" for column, direction in order_by
        )
    )
    limit_sql = "" if limit is None else f" LIMIT {limit}"
    try:
        conn.execute(
            f"CREATE TEMP TABLE {relation_name} AS SELECT {', '.join(projection)} "
            f"FROM {source.name} source WHERE {where_sql}{order_sql}{limit_sql}"
        )
        result = _relation_ref(
            conn,
            relation_name,
            key_columns=_cohort_key_columns(conn, relation_name),
            expected_schema=expected_schema,
            generation=generation,
            kind="cohort",
            capability=relations._capability,
            forbid_outcomes=True,
        )
        relations._cohorts[result.name] = result
        return result
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
    if relations._cohorts.get(cohort.name) is not cohort:
        raise ValueError("cohort must be a registered frozen cohort")
    _validate_relation_provenance(cohort, relations, expected_kind="cohort")
    _assert_ref_current(conn, cohort)
    if any(column.startswith("forward_") for column in cohort.columns):
        raise ValueError("cohort must not contain forward outcome columns")
    _assert_ref_current(conn, relations.forward_outcomes)
    outcome_columns = tuple(
        column
        for column in relations.forward_outcomes.columns
        if column not in {"code", "date"}
    )
    relation_name = f"{generation}_evaluated_{name}_g_{uuid4().hex}"
    cohort_select = ", ".join(f"cohort.{column}" for column in cohort.columns)
    outcome_select = ", ".join(f"outcome.{column}" for column in outcome_columns)
    comma = ", " if outcome_select else ""
    outcome_schema = tuple(
        (column, sql_type)
        for column, sql_type in zip(
            relations.forward_outcomes.columns,
            relations.forward_outcomes.column_types,
            strict=True,
        )
        if column not in {"code", "date"}
    )
    expected_schema = (
        tuple(zip(cohort.columns, cohort.column_types, strict=True)) + outcome_schema
    )
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
            expected_schema=expected_schema,
            generation=generation,
            kind="evaluated",
            capability=relations._capability,
        )
        if result.row_count != cohort.row_count:
            raise RuntimeError("outcome attachment changed frozen cohort membership")
        return result
    except Exception:
        _drop_relation_if_exists(conn, relation_name)
        raise


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
    valuation_state_code = normalize_code_sql("valuation_state.code")
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
        ""
        if not benchmark_conditions
        else "WHERE " + " AND ".join(benchmark_conditions)
    )
    market_filter = (
        "TRUE"
        if "all" in request.market_scopes
        else f"market.market IN ({_sql_strings(request.market_scopes)})"
    )
    liquidity_ctes = _liquidity_sql(request.include_liquidity)
    liquidity_columns = ", ".join(_column_names(_LIQUIDITY_PANEL_SCHEMA))
    signal_panel_columns = ", ".join(_column_names(DAILY_RANKING_SIGNAL_PANEL_SCHEMA))
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
              AND ({"TRUE" if not benchmark_conditions else " AND ".join(benchmark_conditions)})
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
                price.price_basis_id AS valuation_basis_id,
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
            JOIN current_basis_fundamentals_state valuation_state
              ON {valuation_state_code} = price.code
            JOIN daily_valuation valuation
             ON {valuation_code} = price.code
             AND CAST(valuation.date AS DATE) = price.date
             AND CAST(valuation.price_basis_date AS DATE) = price.date
             AND valuation.fundamentals_adjustment_basis_date =
                 valuation_state.fundamentals_adjustment_basis_date
             AND valuation.source_fingerprint = valuation_state.source_fingerprint
            LEFT JOIN topix_ranked topix USING (date)
            LEFT JOIN n225_ranked n225 USING (date)
            WHERE {market_filter}
        ),
        {liquidity_ctes}
        panel_with_relations AS (
            SELECT
                {liquidity_columns},
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
        SELECT {signal_panel_columns} FROM panel_with_relations
        """,
        [*benchmark_params, *benchmark_params],
    )


def _liquidity_sql(enabled: bool) -> str:
    panel_columns = ", ".join(_column_names(_PANEL_SOURCE_SCHEMA))
    if not enabled:
        return (
            f"liquidity AS (SELECT {panel_columns}, CAST(NULL AS DOUBLE) "
            "AS liquidity_residual_z, CAST('missing' AS VARCHAR) "
            "AS liquidity_regime FROM panel_source),"
        )
    regime = liquidity_state_sql(
        residual_z_sql="liquidity_residual_z",
        recent_return_20d_pct_sql="recent_return_20d_pct",
        recent_return_60d_pct_sql="recent_return_60d_pct",
    )
    input_columns = panel_columns + ", log_adv60, log_free_float_market_cap"
    residual_columns = input_columns + ", observations, beta, liquidity_residual"
    scale_columns = residual_columns + ", liquidity_residual_std"
    standardized = _standardized_liquidity_residual_sql(
        residual_sql="liquidity_residual",
        residual_std_sql="liquidity_residual_std",
    )
    return f"""
        liquidity_inputs AS (
            SELECT {panel_columns},
                CASE WHEN med_adv60_sessions >= 60
                          AND isfinite(med_adv60_jpy) AND med_adv60_jpy > 0
                          AND isfinite(free_float_market_cap_jpy)
                          AND free_float_market_cap_jpy > 0
                    THEN ln(med_adv60_jpy) END AS log_adv60,
                CASE WHEN med_adv60_sessions >= 60
                          AND isfinite(med_adv60_jpy) AND med_adv60_jpy > 0
                          AND isfinite(free_float_market_cap_jpy)
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
            SELECT {", ".join(f"inputs.{column}" for column in _column_names(_PANEL_SOURCE_SCHEMA))},
                   inputs.log_adv60, inputs.log_free_float_market_cap,
                   fit.observations, fit.beta,
                   CASE WHEN fit.observations >= {LIQUIDITY_MIN_OBSERVATIONS}
                              AND isfinite(fit.alpha)
                              AND isfinite(fit.beta) AND fit.beta > 0
                       THEN inputs.log_adv60
                            - (fit.alpha + fit.beta * inputs.log_free_float_market_cap)
                   END AS liquidity_residual
            FROM liquidity_inputs inputs
            LEFT JOIN liquidity_fit fit USING (date, market)
        ),
        liquidity_scale AS (
            SELECT {residual_columns},
                   sqrt(sum(liquidity_residual * liquidity_residual) OVER (
                       PARTITION BY date, market
                   ) / nullif(observations - 2, 0)) AS liquidity_residual_std
            FROM liquidity_residuals
        ),
        liquidity_z AS (
            SELECT {scale_columns}, ({standardized}) AS liquidity_residual_z
            FROM liquidity_scale
        ),
        liquidity AS (
            SELECT {panel_columns}, liquidity_residual_z,
                   ({regime}) AS liquidity_regime
            FROM liquidity_z
        ),
        """


def _standardized_liquidity_residual_sql(
    *,
    residual_sql: str,
    residual_std_sql: str,
) -> str:
    """Return the current liquidity residual standardization expression."""

    return (
        f"CASE WHEN isfinite({residual_sql}) AND isfinite({residual_std_sql}) "
        f"AND {residual_std_sql} > 0 THEN {residual_sql} / {residual_std_sql} END"
    )


def _materialize_ranked_signals(
    conn: Any,
    *,
    request: DailyRankingPanelRequest,
    signal_panel: str,
    relation_name: str,
) -> None:
    panel_columns = _column_names(DAILY_RANKING_SIGNAL_PANEL_SCHEMA)
    panel_select = ", ".join(panel_columns)
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
        value_sql = (
            f"CASE WHEN {column} > 0 THEN {column} END" if positive_only else column
        )
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
    percentile_columns = tuple(
        f"{feature}_percentile" for feature, _ in percentile_specs
    )
    percentile_select = ", ".join((*panel_columns, *percentile_columns))
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
            SELECT {panel_select} FROM {signal_panel}
            {scope_union}
        ),
        percentile_window AS (
            SELECT {panel_select}, {", ".join(raw_percentile_expressions)}
            FROM scoped
        ),
        percentiles AS (
            SELECT {panel_select}, {", ".join(final_percentile_expressions)}
            FROM percentile_window
        )
        SELECT {percentile_select}, 'all_liquidity' AS liquidity_scope,
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
            "price_basis_date",
            "fundamentals_adjustment_basis_date",
            "source_fingerprint",
        },
    }
    missing: list[str] = []
    for table, columns in required.items():
        observed = {
            str(row[1])
            for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()
        }
        absent = sorted(columns - observed)
        if absent:
            missing.append(f"{table}=({', '.join(absent)})")
    if missing:
        raise RuntimeError(
            "Daily Ranking research columns are missing: " + "; ".join(missing)
        )


def _resolve_query_bounds(
    conn: Any,
    request: DailyRankingPanelRequest,
    *,
    market_codes: Sequence[str],
) -> tuple[date | None, date | None]:
    query_start = _resolve_valid_session_query_start(
        conn,
        analysis_start_date=request.analysis_start_date,
        market_codes=market_codes,
        required_valid_sessions=request.required_valid_sessions,
    )
    return (
        query_start,
        None
        if request.analysis_end_date is None
        else request.analysis_end_date + timedelta(days=max(request.horizons) * 4 + 30),
    )


def _resolve_valid_session_query_start(
    conn: Any,
    *,
    analysis_start_date: date | None,
    market_codes: Sequence[str],
    required_valid_sessions: int = DAILY_RANKING_BASE_REQUIRED_VALID_SESSIONS,
) -> date | None:
    if analysis_start_date is None:
        return None
    market_filter = ""
    params: list[object] = [analysis_start_date]
    if market_codes:
        placeholders = ",".join("?" for _ in market_codes)
        market_filter = f"AND smd.market_code IN ({placeholders})"
        params.extend(market_codes)
    raw_code = normalize_code_sql("raw.code")
    master_code = normalize_code_sql("smd.code")
    valid_raw_bar = daily_ranking_valid_raw_bar_sql("raw")
    row = conn.execute(
        f"""
        WITH valid_market_sessions AS (
            SELECT DISTINCT CAST(raw.date AS DATE) AS date
            FROM stock_data_raw raw
            JOIN stock_master_daily smd
              ON {master_code} = {raw_code}
             AND CAST(smd.date AS DATE) = CAST(raw.date AS DATE)
            WHERE CAST(raw.date AS DATE) <= ?
              AND {valid_raw_bar}
              {market_filter}
        ), required_history AS (
            SELECT date
            FROM valid_market_sessions
            ORDER BY date DESC
            LIMIT {required_valid_sessions}
        )
        SELECT min(date) FROM required_history
        """,
        params,
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return cast(date, row[0])


def _market_codes_for_scopes(scopes: Sequence[str]) -> tuple[str, ...]:
    if "all" in scopes or "unknown" in scopes:
        return ()
    return tuple(
        dict.fromkeys(code for scope in scopes for code in MARKET_CODES_BY_SCOPE[scope])
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


def _column_names(schema: RelationSchema) -> tuple[str, ...]:
    return tuple(column for column, _ in schema)


def _forward_outcome_schema(horizons: Sequence[int]) -> RelationSchema:
    expected_columns = daily_ranking_forward_outcome_columns(horizons)
    return tuple(
        (
            column,
            "DATE"
            if column == "date" or column.startswith("forward_outcome_completion_date_")
            else "VARCHAR"
            if column == "code" or column.startswith("completion_basis_id_")
            else "DOUBLE",
        )
        for column in expected_columns
    )


def _ranked_signal_schema(request: DailyRankingPanelRequest) -> RelationSchema:
    optional_percentiles = tuple(
        (f"{feature}_percentile", "DOUBLE") for feature in request.percentile_features
    )
    return (
        DAILY_RANKING_SIGNAL_PANEL_SCHEMA
        + _BASE_PERCENTILE_SCHEMA
        + optional_percentiles
        + _VALUATION_CLASSIFICATION_SCHEMA
    )


def _schema_with_types(conn: Any, relation: str) -> RelationSchema:
    return tuple(
        (str(row[1]), str(row[2]).upper())
        for row in conn.execute(f"PRAGMA table_info('{relation}')").fetchall()
    )


def _assert_exact_schema(
    conn: Any,
    relation: str,
    expected_schema: RelationSchema,
    *,
    label: str,
) -> None:
    actual_schema = _schema_with_types(conn, relation)
    if actual_schema != expected_schema:
        raise RuntimeError(
            f"{label} schema mismatch: "
            f"expected={expected_schema!r}, actual={actual_schema!r}"
        )


def _assert_market_alias_consistency(conn: Any, signal_relation: str) -> None:
    """Reject semantic conflicts before choosing a normalized exact-date alias."""

    normalized_code = normalize_code_sql("smd.code")
    conflict = conn.execute(
        f"""
        SELECT signal.code, signal.date
        FROM {signal_relation} signal
        JOIN stock_master_daily smd
          ON {normalized_code} = signal.code
         AND CAST(smd.date AS DATE) = signal.date
        GROUP BY signal.code, signal.date
        HAVING count(DISTINCT struct_pack(
            company_name := CAST(smd.company_name AS VARCHAR),
            market_code := CAST(smd.market_code AS VARCHAR),
            market_name := CAST(smd.market_name AS VARCHAR),
            scale_category := CAST(smd.scale_category AS VARCHAR)
        )) > 1
        LIMIT 1
        """
    ).fetchone()
    if conflict is not None:
        raise RuntimeError(
            "stock_master_daily market membership alias conflict for exact-date signal: "
            f"code={conflict[0]}, date={conflict[1]}"
        )


def _incomplete_outcome_predicate(horizons: Sequence[int]) -> str:
    incomplete = ["outcome.code IS NULL"]
    for horizon in horizons:
        incomplete.extend(
            (
                f"outcome.forward_outcome_completion_date_{horizon}d IS NULL",
                f"outcome.forward_close_return_{horizon}d_pct IS NULL",
                f"NOT isfinite(outcome.forward_close_return_{horizon}d_pct)",
                f"outcome.forward_close_excess_return_{horizon}d_pct IS NULL",
                f"NOT isfinite(outcome.forward_close_excess_return_{horizon}d_pct)",
                f"outcome.completion_basis_id_{horizon}d IS NULL",
            )
        )
    return " OR ".join(incomplete)


def _relation_ref(
    conn: Any,
    name: str,
    *,
    key_columns: tuple[str, ...],
    expected_schema: RelationSchema,
    generation: str,
    kind: RelationKind,
    capability: object,
    forbid_outcomes: bool = False,
) -> RelationRef:
    _validate_sql_identifier(name)
    _validate_sql_identifier(generation)
    for column in key_columns:
        _validate_sql_identifier(column)
    for column, sql_type in expected_schema:
        _validate_sql_identifier(column)
        if not _SAFE_SQL_TYPE_RE.fullmatch(sql_type):
            raise ValueError(f"invalid SQL type: {sql_type}")
    _assert_exact_schema(conn, name, expected_schema, label=kind)
    columns = _column_names(expected_schema)
    if forbid_outcomes and any(column.startswith("forward_") for column in columns):
        raise RuntimeError(f"signal relation contains forward outcome columns: {name}")
    _assert_date_columns(conn, name)
    fingerprint = _relation_fingerprint(
        conn,
        relation=name,
        columns=columns,
        key_columns=key_columns,
    )
    row_count = fingerprint.row_count
    if fingerprint.distinct_key_count != row_count:
        raise RuntimeError(f"relation keys are not unique: {name}")
    if fingerprint.null_key_count:
        raise RuntimeError(f"relation keys must not contain NULL: {name}")
    relation = RelationRef(
        name=name,
        columns=columns,
        key_columns=key_columns,
        row_count=row_count,
        column_types=tuple(sql_type for _, sql_type in expected_schema),
        generation=generation,
        kind=kind,
        _capability=capability,
    )
    _register_relation_ref(conn, relation, fingerprint=fingerprint)
    return relation


def _schema(conn: Any, relation: str) -> tuple[str, ...]:
    return tuple(
        str(row[1])
        for row in conn.execute(f"PRAGMA table_info('{relation}')").fetchall()
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
    _validate_sql_identifier(relation)
    for column in key_columns:
        _validate_sql_identifier(column)
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
    expected_kinds = (
        (relations.price_history, "price_history"),
        (relations.signal_prices, "signal_prices"),
        (relations.forward_outcomes, "forward_outcomes"),
        (relations.signal_panel, "signal_panel"),
        (relations.ranked_signals, "ranked_signals"),
    )
    if relations.liquidity_ranked_signals is not None:
        expected_kinds += (
            (relations.liquidity_ranked_signals, "liquidity_ranked_signals"),
        )
    for relation, expected_kind in expected_kinds:
        if (
            relation.generation != relations.generation
            or relation.kind != expected_kind
            or relation._capability is not relations._capability
        ):
            raise ValueError("research relation provenance does not match its build")
    return relations.generation


def _validate_relation_provenance(
    relation: RelationRef,
    relations: DailyRankingResearchRelations,
    *,
    expected_kind: RelationKind | None = None,
) -> None:
    if (
        relation.generation != relations.generation
        or relation._capability is not relations._capability
        or (expected_kind is not None and relation.kind != expected_kind)
    ):
        raise ValueError("relation provenance does not match this research build")


def _assert_ref_current(conn: Any, relation: RelationRef) -> None:
    with _ISSUED_RELATIONS_LOCK:
        issued = _ISSUED_RELATIONS.get(id(relation))
    if issued is None or issued.ref() is not relation:
        raise ValueError("RelationRef was not issued by the trusted relation registry")
    if issued.connection is not conn:
        raise ValueError("RelationRef is registered to a different DuckDB connection")
    if issued.provenance != _relation_provenance(relation):
        raise ValueError("RelationRef immutable provenance changed after issuance")
    expected_schema = tuple(zip(relation.columns, relation.column_types, strict=True))
    _assert_exact_schema(conn, relation.name, expected_schema, label=relation.kind)
    if _physical_relation_identity(conn, relation.name) != issued.physical_identity:
        raise RuntimeError(
            f"relation physical identity was replaced after validation: {relation.name}"
        )
    fingerprint = _relation_fingerprint(
        conn,
        relation=relation.name,
        columns=relation.columns,
        key_columns=relation.key_columns,
    )
    if fingerprint.row_count != relation.row_count:
        raise RuntimeError(
            f"relation membership changed after validation: {relation.name}"
        )
    if fingerprint.distinct_key_count != relation.row_count:
        raise RuntimeError(f"relation keys changed after validation: {relation.name}")
    if fingerprint.null_key_count:
        raise RuntimeError(f"relation keys changed after validation: {relation.name}")
    if fingerprint.key_aggregates != issued.fingerprint.key_aggregates:
        raise RuntimeError(f"relation key fingerprint changed: {relation.name}")
    if fingerprint.content_aggregates != issued.fingerprint.content_aggregates:
        raise RuntimeError(f"relation content fingerprint changed: {relation.name}")


def _register_relation_ref(
    conn: Any,
    relation: RelationRef,
    *,
    fingerprint: _RelationFingerprint,
) -> None:
    """Seal an internally issued ref to exact object, connection, and contents."""

    relation_id = id(relation)

    def unregister(ref: weakref.ReferenceType[RelationRef]) -> None:
        with _ISSUED_RELATIONS_LOCK:
            current = _ISSUED_RELATIONS.get(relation_id)
            if current is not None and current.ref is ref:
                _ISSUED_RELATIONS.pop(relation_id, None)

    ref = weakref.ref(relation, unregister)
    issued = _IssuedRelation(
        ref=ref,
        connection=conn,
        provenance=_relation_provenance(relation),
        physical_identity=_physical_relation_identity(conn, relation.name),
        schema=tuple(zip(relation.columns, relation.column_types, strict=True)),
        fingerprint=fingerprint,
    )
    with _ISSUED_RELATIONS_LOCK:
        _ISSUED_RELATIONS[relation_id] = issued


def _relation_provenance(relation: RelationRef) -> tuple[Any, ...]:
    return (
        relation.name,
        relation.columns,
        relation.key_columns,
        relation.row_count,
        relation.column_types,
        relation.generation,
        relation.kind,
        relation._capability,
    )


def _physical_relation_identity(conn: Any, relation: str) -> tuple[Any, ...]:
    _validate_sql_identifier(relation)
    rows = conn.execute(
        """
        SELECT 'table', database_oid, schema_oid, table_oid, temporary
        FROM duckdb_tables() WHERE table_name = ?
        UNION ALL
        SELECT 'view', database_oid, schema_oid, view_oid, temporary
        FROM duckdb_views() WHERE view_name = ?
        ORDER BY temporary DESC
        """,
        [relation, relation],
    ).fetchall()
    if len(rows) != 1:
        raise RuntimeError(
            f"relation physical identity is missing or ambiguous: {relation}"
        )
    return tuple(rows[0])


def _relation_fingerprint(
    conn: Any,
    *,
    relation: str,
    columns: Sequence[str],
    key_columns: Sequence[str],
) -> _RelationFingerprint:
    """Return one bounded DB-side aggregate row for relation currentness."""

    quoted_columns = ", ".join(_quoted_identifier(column) for column in columns)
    quoted_keys = ", ".join(_quoted_identifier(column) for column in key_columns)
    null_key_predicate = " OR ".join(
        f"{_quoted_identifier(column)} IS NULL" for column in key_columns
    )
    relation_name = _quoted_identifier(relation)
    row = conn.execute(
        f"""
        WITH hashed AS (
            SELECT row({quoted_keys}) AS key_row,
                   ({null_key_predicate}) AS has_null_key,
                   hash(row('daily-ranking:key:v1', {quoted_keys})) AS key_h1,
                   hash(row('daily-ranking:key:v2', {quoted_keys})) AS key_h2,
                   hash(row('daily-ranking:content:v1', {quoted_columns})) AS content_h1,
                   hash(row('daily-ranking:content:v2', {quoted_columns})) AS content_h2
            FROM {relation_name}
        )
        SELECT count(*)::BIGINT,
               count(DISTINCT key_row)::BIGINT,
               count(*) FILTER (WHERE has_null_key)::BIGINT,
               coalesce(bit_xor(key_h1), 0::UBIGINT),
               coalesce(sum(key_h1), 0::HUGEINT),
               coalesce(bit_xor(key_h2), 0::UBIGINT),
               coalesce(sum(key_h2), 0::HUGEINT),
               coalesce(bit_xor(content_h1), 0::UBIGINT),
               coalesce(sum(content_h1), 0::HUGEINT),
               coalesce(bit_xor(content_h2), 0::UBIGINT),
               coalesce(sum(content_h2), 0::HUGEINT)
        FROM hashed
        """
    ).fetchone()
    if row is None or len(row) != 11:
        raise RuntimeError(f"relation fingerprint query failed: {relation}")
    return _RelationFingerprint(*(int(value) for value in row))


def _validate_sql_identifier(identifier: str) -> str:
    if not _SQL_IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"invalid SQL identifier: {identifier!r}")
    return identifier


def _quoted_identifier(identifier: str) -> str:
    return f'"{_validate_sql_identifier(identifier)}"'


def _validate_signal_expression(
    expression: SignalExpression | None,
    source: RelationRef,
) -> None:
    if expression is None:
        return
    sql = expression.sql.strip()
    unquoted = re.sub(r"'(?:''|[^'])*'", "", sql)
    if _FORBIDDEN_SIGNAL_EXPRESSION_RE.search(unquoted):
        raise ValueError("signal expression contains a forbidden SQL construct")
    if not set(expression.referenced_columns).issubset(source.columns):
        raise ValueError("signal expression references columns outside its source")
    tokens = {
        token.lower() for token in re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", unquoted)
    }
    source_columns = {column.lower() for column in source.columns}
    declared = {column.lower() for column in expression.referenced_columns}
    observed_source = tokens & source_columns
    if observed_source != declared:
        raise ValueError(
            "signal expression referenced_columns must exactly declare source columns"
        )
    allowed = (
        _SQL_EXPRESSION_WORDS
        | declared
        | {
            "abs",
            "cast",
            "date",
            "double",
            "greatest",
            "integer",
            "least",
            "varchar",
        }
    )
    unknown = sorted(tokens - allowed)
    if unknown:
        raise ValueError(
            "signal expression contains unsupported identifiers: " + ", ".join(unknown)
        )


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


def _format_optional_date(value: date | None) -> str | None:
    return None if value is None else value.isoformat()
