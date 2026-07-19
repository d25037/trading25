from __future__ import annotations

from collections.abc import Callable
import copy
from dataclasses import replace
from datetime import date, timedelta
from typing import Any

import duckdb
import pytest

import src.domains.analytics.daily_ranking_research_base as research_base
from src.domains.analytics.daily_ranking_feature_builders import (
    AtrFeaturesRequest,
    LongScaffoldFeaturesRequest,
    PsrFeaturesRequest,
    RoeFeaturesRequest,
    SectorStrengthFeaturesRequest,
    ShortScaffoldFeaturesRequest,
    SmaFeaturesRequest,
    build_atr_features,
    build_long_scaffold_features,
    build_psr_features,
    build_roe_features,
    build_sector_strength_features,
    build_short_scaffold_features,
    build_sma_features,
    _cleans_legacy_intermediates,
    publish_legacy_psr_features,
)
from src.domains.analytics.daily_ranking_research_base import (
    RelationRef,
    _relation_ref as _issue_relation_ref,
    validate_daily_ranking_signal_relation,
)


_GENERATION = "feature_contract_g_0123456789abcdef"
_SOURCE_NAME = f"{_GENERATION}_source"
_SOURCE_SCHEMA = (
    ("code", "VARCHAR"),
    ("date", "DATE"),
    ("market_scope", "VARCHAR"),
    ("valuation_basis_id", "VARCHAR"),
    ("close", "DOUBLE"),
    ("atr20_pct", "DOUBLE"),
    ("atr60_pct", "DOUBLE"),
    ("atr20_to_atr60", "DOUBLE"),
    ("atr20_change_20d_pct", "DOUBLE"),
    ("recent_return_20d_pct", "DOUBLE"),
    ("recent_return_60d_pct", "DOUBLE"),
    ("topix_recent_return_20d_pct", "DOUBLE"),
    ("topix_recent_return_60d_pct", "DOUBLE"),
    ("per_percentile", "DOUBLE"),
    ("forecast_per_percentile", "DOUBLE"),
    ("forecast_p_op_percentile", "DOUBLE"),
    ("pbr_percentile", "DOUBLE"),
    ("forecast_per_to_per_ratio", "DOUBLE"),
    ("market_cap_bil_jpy", "DOUBLE"),
    ("liquidity_scope", "VARCHAR"),
    ("liquidity_residual_z", "DOUBLE"),
    ("valuation_signal", "VARCHAR"),
)
_EXPECTED_FEATURE_SCHEMAS = {
    "atr": (
        ("atr20_pct", "DOUBLE"),
        ("atr60_pct", "DOUBLE"),
        ("atr20_to_atr60", "DOUBLE"),
        ("atr20_change_20d_pct", "DOUBLE"),
    ),
    "short": (
        ("liquidity_regime", "VARCHAR"),
        ("atr20_pct", "DOUBLE"),
        ("atr60_pct", "DOUBLE"),
        ("atr20_to_atr60", "DOUBLE"),
        ("atr20_change_20d_pct", "DOUBLE"),
        ("strong_value_confirmation", "BOOLEAN"),
        ("medium_value_confirmation", "BOOLEAN"),
        ("overvalued_percentile", "BOOLEAN"),
        ("missing_earnings_warning", "BOOLEAN"),
        ("weak_trend", "BOOLEAN"),
        ("overvalued_or_no_earnings_warning", "BOOLEAN"),
        ("no_value_confirmation", "BOOLEAN"),
        ("atr20_acceleration", "BOOLEAN"),
        ("atr20_to_atr60_overheat", "BOOLEAN"),
    ),
    "psr": (
        ("actual_sales", "DOUBLE"),
        ("actual_sales_disclosed_date", "DATE"),
        ("psr", "DOUBLE"),
        ("psr_percentile", "DOUBLE"),
        ("psr_signal", "VARCHAR"),
    ),
    "roe": (
        ("quality_disclosed_date", "DATE"),
        ("quality_period_end", "DATE"),
        ("adjusted_eps", "DOUBLE"),
        ("adjusted_bps", "DOUBLE"),
        ("adjusted_forecast_eps", "DOUBLE"),
        ("roe", "DOUBLE"),
        ("forecast_roe", "DOUBLE"),
        ("roe_percentile", "DOUBLE"),
        ("forecast_roe_percentile", "DOUBLE"),
        ("roe_signal", "VARCHAR"),
        ("forecast_roe_signal", "VARCHAR"),
    ),
    "sma": (
        ("sma5", "DOUBLE"),
        ("sma5_deviation_pct", "DOUBLE"),
        ("close_below_sma5_flag", "INTEGER"),
        ("close_below_sma5_count_3d", "INTEGER"),
        ("sma5_above_count_5d", "INTEGER"),
        ("below_sma5_streak_ge3_flag", "BOOLEAN"),
        ("sma5_below_streak_bucket", "VARCHAR"),
        ("sma5_count_group", "VARCHAR"),
        ("sma5_deviation_bucket", "VARCHAR"),
    ),
    "sector": (
        ("sector_33_code", "VARCHAR"),
        ("sector_33_name", "VARCHAR"),
        ("sector_observation_count", "BIGINT"),
        ("sector_code_count", "BIGINT"),
        ("sector_index_code", "VARCHAR"),
        ("sector_index_return_5d_pct", "DOUBLE"),
        ("sector_index_return_20d_pct", "DOUBLE"),
        ("sector_index_return_60d_pct", "DOUBLE"),
        ("sector_index_5d_topix_excess_pct", "DOUBLE"),
        ("sector_index_20d_topix_excess_pct", "DOUBLE"),
        ("sector_index_60d_topix_excess_pct", "DOUBLE"),
        ("sector_constituent_20d_topix_excess_pct", "DOUBLE"),
        ("sector_constituent_60d_topix_excess_pct", "DOUBLE"),
        ("sector_20d_topix_excess_pct", "DOUBLE"),
        ("sector_60d_topix_excess_pct", "DOUBLE"),
        ("sector_breadth_20d_pct", "DOUBLE"),
        ("sector_index_5d_strength_rank", "DOUBLE"),
        ("sector_20d_strength_rank", "DOUBLE"),
        ("sector_60d_strength_rank", "DOUBLE"),
        ("sector_constituent_20d_strength_rank", "DOUBLE"),
        ("sector_constituent_60d_strength_rank", "DOUBLE"),
        ("sector_breadth_strength_rank", "DOUBLE"),
        ("sector_index_strength_score", "DOUBLE"),
        ("sector_constituent_strength_score", "DOUBLE"),
        ("sector_strength_score", "DOUBLE"),
        ("sector_strength_bucket", "VARCHAR"),
        ("sector_consistency_bucket", "VARCHAR"),
    ),
    "long": (
        ("sector_33_code", "VARCHAR"),
        ("sector_33_name", "VARCHAR"),
        ("sector_strength_bucket", "VARCHAR"),
        ("sector_strength_score", "DOUBLE"),
        ("sector_index_strength_score", "DOUBLE"),
        ("sector_constituent_strength_score", "DOUBLE"),
        ("long_index_leadership_score", "DOUBLE"),
        ("long_constituent_breadth_leadership_score", "DOUBLE"),
        ("long_hybrid_leadership_score", "DOUBLE"),
        ("balanced_sector_strength_bucket_label", "VARCHAR"),
        ("long_hybrid_bucket_label", "VARCHAR"),
        ("momentum_20_60_top20_flag", "BOOLEAN"),
        ("atr20_pct", "DOUBLE"),
        ("atr60_pct", "DOUBLE"),
        ("atr20_to_atr60", "DOUBLE"),
        ("atr20_change_20d_pct", "DOUBLE"),
        ("atr20_acceleration_flag", "BOOLEAN"),
        ("atr20_acceleration_ex_overheat_flag", "BOOLEAN"),
        ("atr20_to_atr60_overheat", "BOOLEAN"),
        ("weak_trend", "BOOLEAN"),
        ("low_forecast_per_score", "DOUBLE"),
        ("low_pbr_score", "DOUBLE"),
        ("value_composite_equal_score", "DOUBLE"),
    ),
}


@pytest.fixture
def feature_connection() -> Any:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TEMP TABLE feature_contract_g_0123456789abcdef_source (
            code VARCHAR,
            date DATE,
            market_scope VARCHAR,
            valuation_basis_id VARCHAR,
            close DOUBLE,
            atr20_pct DOUBLE,
            atr60_pct DOUBLE,
            atr20_to_atr60 DOUBLE,
            atr20_change_20d_pct DOUBLE,
            recent_return_20d_pct DOUBLE,
            recent_return_60d_pct DOUBLE,
            topix_recent_return_20d_pct DOUBLE,
            topix_recent_return_60d_pct DOUBLE,
            per_percentile DOUBLE,
            forecast_per_percentile DOUBLE,
            forecast_p_op_percentile DOUBLE,
            pbr_percentile DOUBLE,
            forecast_per_to_per_ratio DOUBLE,
            market_cap_bil_jpy DOUBLE,
            liquidity_scope VARCHAR,
            liquidity_residual_z DOUBLE,
            valuation_signal VARCHAR
        )
        """
    )
    start = date(2024, 1, 1)
    rows: list[tuple[object, ...]] = []
    for offset in range(70):
        current = start + timedelta(days=offset)
        rows.extend(
            (
                (
                    "1111",
                    current,
                    "prime",
                    "basis-a",
                    100.0 + offset,
                    2.0,
                    1.5,
                    1.2,
                    30.0,
                    12.0,
                    18.0,
                    2.0,
                    4.0,
                    0.1,
                    0.1,
                    0.2,
                    0.1,
                    0.7,
                    1.0,
                    "neutral_rerating",
                    0.5,
                    "strong_value_confirmation",
                ),
                (
                    "2222",
                    current,
                    "prime",
                    "basis-b",
                    200.0 - offset * 0.2,
                    3.0,
                    2.0,
                    1.3,
                    10.0,
                    -2.0,
                    5.0,
                    2.0,
                    4.0,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    1.1,
                    2.0,
                    "crowded_rerating",
                    1.2,
                    "very_overvalued_warning",
                ),
                (
                    "3333",
                    current,
                    "prime",
                    "basis-c",
                    80.0 + offset * 0.1,
                    1.0,
                    1.0,
                    1.0,
                    5.0,
                    1.0,
                    1.0,
                    2.0,
                    4.0,
                    None,
                    None,
                    None,
                    0.5,
                    None,
                    3.0,
                    "stale_liquidity",
                    -1.2,
                    "no_positive_earnings_valuation",
                ),
            )
        )
    conn.executemany(
        f"INSERT INTO {_SOURCE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.execute(
        """
        CREATE TABLE daily_valuation (
            code VARCHAR, date DATE, basis_version VARCHAR, psr DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO daily_valuation
        SELECT code, date, valuation_basis_id,
               CASE code WHEN '1111' THEN NULL WHEN '2222' THEN 2.2 ELSE NULL END
        FROM feature_contract_g_0123456789abcdef_source
        UNION ALL
        SELECT code, date, 'basis-latest', 999.0
        FROM feature_contract_g_0123456789abcdef_source
        """
    )
    conn.execute(
        """
        CREATE TABLE statements (
            code VARCHAR,
            disclosed_date DATE,
            sales DOUBLE,
            type_of_current_period VARCHAR,
            type_of_document VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO statements VALUES
            ('1111', DATE '2023-05-15', 300000000.0, 'FY', 'FinancialStatements'),
            ('2222', DATE '2023-05-15', 100000000.0, 'FY', 'FinancialStatements'),
            ('3333', DATE '2023-05-15', 0.0, 'FY', 'FinancialStatements')
        """
    )
    conn.execute(
        """
        CREATE TABLE statement_metrics_adjusted (
            code VARCHAR,
            disclosed_date DATE,
            period_end DATE,
            period_type VARCHAR,
            adjusted_eps DOUBLE,
            adjusted_bps DOUBLE,
            adjusted_forecast_eps DOUBLE,
            basis_version VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO statement_metrics_adjusted VALUES
            ('1111', DATE '2023-05-15', DATE '2023-03-31', 'FY', 25, 100, 30, 'basis-a'),
            ('2222', DATE '2023-05-15', DATE '2023-03-31', 'FY', 2, 100, 3, 'basis-b'),
            ('3333', DATE '2023-05-15', DATE '2023-03-31', 'FY', 0, 100, NULL, 'basis-c'),
            ('1111', DATE '2023-05-15', DATE '2023-03-31', 'FY', 900, 100, 999, 'basis-latest'),
            ('2222', DATE '2023-05-15', DATE '2023-03-31', 'FY', 900, 100, 999, 'basis-latest'),
            ('3333', DATE '2023-05-15', DATE '2023-03-31', 'FY', 900, 100, 999, 'basis-latest')
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_master_daily (
            code VARCHAR,
            date DATE,
            company_name VARCHAR,
            market_name VARCHAR,
            market_code VARCHAR,
            scale_category VARCHAR,
            sector_33_code VARCHAR,
            sector_33_name VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO stock_master_daily
        SELECT code, date, 'Company ' || code, 'Prime', '0111', 'TOPIX Small 1',
               CASE code WHEN '1111' THEN '0050' ELSE '1050' END,
               CASE code WHEN '1111' THEN 'Fishery' ELSE 'Mining' END
        FROM feature_contract_g_0123456789abcdef_source
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_data AS
        SELECT code, date, close AS open, close AS high, close AS low, close,
               1000::BIGINT AS volume
        FROM feature_contract_g_0123456789abcdef_source
        """
    )
    conn.execute("CREATE TABLE index_master (code VARCHAR, category VARCHAR)")
    conn.execute("INSERT INTO index_master VALUES ('0040', 'sector33'), ('0041', 'sector33')")
    conn.execute("CREATE TABLE indices_data (code VARCHAR, date DATE, close DOUBLE)")
    conn.execute(
        """
        INSERT INTO indices_data
        SELECT code, DATE '2024-01-01' + CAST(i AS INTEGER),
               CASE code WHEN '0040' THEN 100.0 + i * 2.0 ELSE 100.0 + i * 0.2 END
        FROM range(70) days(i)
        CROSS JOIN (VALUES ('0040'), ('0041')) codes(code)
        """
    )
    conn.execute("CREATE TABLE topix_data (date DATE, close DOUBLE)")
    conn.execute(
        "INSERT INTO topix_data SELECT DATE '2024-01-01' + CAST(i AS INTEGER), 100.0 + i * 0.5 FROM range(70) days(i)"
    )
    try:
        yield conn
    finally:
        conn.close()


def _relation_ref(
    conn: Any,
    name: str = _SOURCE_NAME,
    *,
    capability: object | None = None,
    generation: str = _GENERATION,
    kind: str = "ranked_signals",
) -> RelationRef:
    schema = tuple(
        (str(row[1]), str(row[2]).upper())
        for row in conn.execute(f"PRAGMA table_info('{name}')").fetchall()
    )
    keys = ("code", "date", "market_scope")
    return _issue_relation_ref(
        conn,
        name,
        key_columns=keys,
        expected_schema=schema,
        generation=generation,
        kind=kind,  # type: ignore[arg-type]
        capability=object() if capability is None else capability,
        forbid_outcomes=True,
    )


def _assert_feature_contract(conn: Any, relation: RelationRef, source: RelationRef) -> None:
    assert relation.name.startswith(f"{source.generation}_")
    assert relation.generation == source.generation
    assert relation.kind == "signal_features"
    assert relation._capability is source._capability
    assert relation.key_columns == source.key_columns
    assert relation.row_count == source.row_count
    assert not any(column.startswith("forward_") for column in relation.columns)
    observed_schema = tuple(
        (str(row[1]), str(row[2]).upper())
        for row in conn.execute(f"PRAGMA table_info('{relation.name}')").fetchall()
    )
    assert observed_schema == tuple(
        zip(relation.columns, relation.column_types, strict=True)
    )
    distinct_count = conn.execute(
        f"SELECT count(*) FROM (SELECT {', '.join(relation.key_columns)} "
        f"FROM {relation.name} GROUP BY {', '.join(relation.key_columns)})"
    ).fetchone()[0]
    assert distinct_count == relation.row_count
    assert conn.execute(
        f"SELECT count(*) FROM ((SELECT {', '.join(source.key_columns)} "
        f"FROM {source.name} EXCEPT SELECT {', '.join(relation.key_columns)} "
        f"FROM {relation.name}) UNION ALL "
        f"(SELECT {', '.join(relation.key_columns)} FROM {relation.name} "
        f"EXCEPT SELECT {', '.join(source.key_columns)} FROM {source.name}))"
    ).fetchone()[0] == 0


def _feature_rows(conn: Any, relation: RelationRef) -> list[tuple[object, ...]]:
    return conn.execute(
        f"SELECT {', '.join(relation.columns)} FROM {relation.name} "
        f"ORDER BY {', '.join(relation.key_columns)}"
    ).fetchall()


def test_all_public_builders_publish_generation_safe_explicit_signal_relations(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    atr = build_atr_features(conn, AtrFeaturesRequest(source=source, namespace="atr_case"))
    sector = build_sector_strength_features(
        conn,
        SectorStrengthFeaturesRequest(
            source=source,
            population_source=source,
            namespace="sector_case",
        ),
    )
    psr = build_psr_features(conn, PsrFeaturesRequest(source=source, namespace="psr_case"))
    sma = build_sma_features(conn, SmaFeaturesRequest(source=source, namespace="sma_case"))
    roe = build_roe_features(conn, RoeFeaturesRequest(source=source, namespace="roe_case"))
    short = build_short_scaffold_features(
        conn,
        ShortScaffoldFeaturesRequest(
            source=source,
            atr_features=atr,
            namespace="short_case",
        ),
    )
    leadership = _leadership_ref(conn, source)
    long = build_long_scaffold_features(
        conn,
        LongScaffoldFeaturesRequest(
            source=source,
            leadership_features=leadership,
            short_scaffold_features=short,
            namespace="long_case",
        ),
    )

    for relation in (atr, sector, psr, sma, roe, short, long):
        _assert_feature_contract(conn, relation, source)
    for family, relation in (
        ("atr", atr),
        ("sector", sector),
        ("psr", psr),
        ("sma", sma),
        ("roe", roe),
        ("short", short),
        ("long", long),
    ):
        assert tuple(
            zip(
                relation.columns[len(source.key_columns) :],
                relation.column_types[len(source.key_columns) :],
                strict=True,
            )
        ) == _EXPECTED_FEATURE_SCHEMAS[family]


def test_builders_reject_untrusted_stale_cross_generation_and_outcome_refs(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    with pytest.raises(ValueError, match="trusted signal"):
        build_atr_features(
            conn,
            AtrFeaturesRequest(
                source=replace(source, _capability=None),
                namespace="untrusted",
            ),
        )
    with pytest.raises(ValueError, match="signal relation"):
        build_atr_features(
            conn,
            AtrFeaturesRequest(
                source=replace(source, kind="forward_outcomes"),
                namespace="outcome",
            ),
        )
    conn.execute(f"DELETE FROM {_SOURCE_NAME} WHERE code = '3333'")
    with pytest.raises(RuntimeError, match="membership changed"):
        build_atr_features(
            conn,
            AtrFeaturesRequest(source=source, namespace="stale"),
        )


def test_builders_reject_cross_generation_dependencies(feature_connection: Any) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    atr = build_atr_features(conn, AtrFeaturesRequest(source=source, namespace="atr_peer"))
    with pytest.raises(ValueError, match="generation|capability"):
        build_short_scaffold_features(
            conn,
            ShortScaffoldFeaturesRequest(
                source=source,
                atr_features=replace(atr, generation="another_generation"),
                namespace="short_peer",
            ),
        )


def test_signal_validation_rejects_forged_replaced_and_copied_refs(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    forged = RelationRef(
        name=source.name,
        columns=source.columns,
        key_columns=source.key_columns,
        row_count=source.row_count,
        column_types=source.column_types,
        generation=source.generation,
        kind=source.kind,
        _capability=object(),
    )

    for candidate in (forged, replace(source), copy.copy(source)):
        with pytest.raises(ValueError, match="issued|registered|provenance"):
            validate_daily_ranking_signal_relation(conn, candidate)


@pytest.mark.parametrize(
    "mutation_sql",
    (
        f"UPDATE {_SOURCE_NAME} SET close = close + 0.25 WHERE code = '1111'",
        f"UPDATE {_SOURCE_NAME} SET code = '4444' WHERE code = '3333'",
    ),
)
def test_signal_validation_rejects_same_count_content_or_key_mutation(
    feature_connection: Any,
    mutation_sql: str,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    conn.execute(mutation_sql)

    with pytest.raises(RuntimeError, match="changed|fingerprint|current"):
        validate_daily_ranking_signal_relation(conn, source)


def test_signal_validation_rejects_drop_recreate_with_identical_rows(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    conn.execute(f"CREATE TEMP TABLE replacement_source AS SELECT * FROM {_SOURCE_NAME}")
    conn.execute(f"DROP TABLE {_SOURCE_NAME}")
    conn.execute(f"ALTER TABLE replacement_source RENAME TO {_SOURCE_NAME}")

    with pytest.raises(RuntimeError, match="replaced|identity|current"):
        validate_daily_ranking_signal_relation(conn, source)


def test_signal_validation_pins_connection_and_immutable_capability(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    other = duckdb.connect(":memory:")
    try:
        with pytest.raises(ValueError, match="different DuckDB connection"):
            validate_daily_ranking_signal_relation(other, source)
    finally:
        other.close()

    object.__setattr__(source, "_capability", object())
    with pytest.raises(ValueError, match="immutable provenance"):
        validate_daily_ranking_signal_relation(conn, source)


def test_signal_validation_recomputes_physical_schema(feature_connection: Any) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    conn.execute(f"ALTER TABLE {_SOURCE_NAME} ADD COLUMN injected DOUBLE")

    with pytest.raises(RuntimeError, match="schema mismatch"):
        validate_daily_ranking_signal_relation(conn, source)


def test_public_builder_scope_validates_each_exact_ref_once_per_call(
    feature_connection: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    original = research_base._assert_ref_current
    calls: list[RelationRef] = []

    def recording_assert(connection: Any, relation: RelationRef) -> None:
        calls.append(relation)
        original(connection, relation)

    monkeypatch.setattr(research_base, "_assert_ref_current", recording_assert)

    build_atr_features(
        conn, AtrFeaturesRequest(source=source, namespace="atr_scoped_once")
    )
    assert calls == [source]

    calls.clear()
    build_atr_features(
        conn, AtrFeaturesRequest(source=source, namespace="atr_scoped_again")
    )
    assert calls == [source]

    calls.clear()
    conn.execute(
        f"UPDATE {_SOURCE_NAME} SET close = close + 1.0 WHERE code = '1111'"
    )
    with pytest.raises(RuntimeError, match="content fingerprint changed"):
        build_atr_features(
            conn,
            AtrFeaturesRequest(source=source, namespace="atr_scoped_stale"),
        )
    assert calls == [source]


def test_sector_scope_validates_distinct_source_and_population_once_each(
    feature_connection: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    population_name = f"{_GENERATION}_population"
    conn.execute(
        f"CREATE TEMP TABLE {population_name} AS SELECT * FROM {source.name}"
    )
    population = _relation_ref(
        conn,
        population_name,
        capability=source._capability,
        generation=source.generation,
        kind="signal_features",
    )
    original = research_base._assert_ref_current
    calls: list[RelationRef] = []

    def recording_assert(connection: Any, relation: RelationRef) -> None:
        calls.append(relation)
        original(connection, relation)

    monkeypatch.setattr(research_base, "_assert_ref_current", recording_assert)

    build_sector_strength_features(
        conn,
        SectorStrengthFeaturesRequest(
            source=source,
            population_source=population,
            namespace="sector_scoped_once",
        ),
    )

    assert [id(relation) for relation in calls] == [id(source), id(population)]


@pytest.mark.parametrize("scale", (1, 2))
def test_relation_fingerprint_is_bounded_db_side_and_scale_invariant(
    feature_connection: Any,
    scale: int,
) -> None:
    conn = feature_connection
    scaled_name = f"{_GENERATION}_scaled_{scale}"
    copies = [f"SELECT * FROM {_SOURCE_NAME}"]
    if scale == 2:
        copies.append(
            f"SELECT 'x' || code AS code, * EXCLUDE (code) FROM {_SOURCE_NAME}"
        )
    conn.execute(f"CREATE TEMP TABLE {scaled_name} AS {' UNION ALL '.join(copies)}")
    tracked = _TrackingConnection(conn)
    source = _relation_ref(tracked, scaled_name)
    tracked.queries.clear()
    tracked.fingerprint_fetches.clear()

    build_atr_features(
        tracked,
        AtrFeaturesRequest(source=source, namespace=f"atr_scale_{scale}"),
    )

    fingerprint_sql = [
        sql for sql in tracked.queries if "daily-ranking:key:v1" in sql
    ]
    assert len(fingerprint_sql) == 2
    assert all("ORDER BY" not in sql.upper() for sql in fingerprint_sql)
    assert tracked.fingerprint_fetches == [("fetchone", 1), ("fetchone", 1)]


@pytest.mark.parametrize(
    ("columns", "keys"),
    (
        (("code; DROP TABLE victim",), ("code; DROP TABLE victim",)),
        (("code",), ("code) OR TRUE --",)),
        (("code\"quoted",), ("code\"quoted",)),
    ),
)
def test_relation_ref_rejects_identifier_injection_before_sql(
    columns: tuple[str, ...],
    keys: tuple[str, ...],
) -> None:
    with pytest.raises(ValueError, match="identifier"):
        RelationRef(
            name="safe_relation",
            columns=columns,
            key_columns=keys,
            row_count=0,
            column_types=tuple("VARCHAR" for _ in columns),
        )


def test_psr_and_roe_require_exact_source_basis_and_ignore_latest_poison(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    psr = build_psr_features(
        conn,
        PsrFeaturesRequest(source=source, namespace="psr_basis_poison"),
    )
    roe = build_roe_features(
        conn,
        RoeFeaturesRequest(source=source, namespace="roe_basis_poison"),
    )

    assert psr.row_count == source.row_count
    assert roe.row_count == source.row_count
    assert conn.execute(
        f"SELECT max(psr) FROM {psr.name} WHERE code = '2222'"
    ).fetchone()[0] == pytest.approx(2.2)
    assert conn.execute(
        f"SELECT max(roe), max(forecast_roe) FROM {roe.name} WHERE code = '1111'"
    ).fetchone() == pytest.approx((25.0, 30.0))


@pytest.mark.parametrize("builder", (build_psr_features, build_roe_features))
def test_fundamental_builders_reject_source_without_valuation_basis(
    feature_connection: Any,
    builder: Callable[[Any, Any], RelationRef],
) -> None:
    conn = feature_connection
    conn.execute(
        f"CREATE TEMP TABLE {_GENERATION}_without_basis AS "
        f"SELECT * EXCLUDE (valuation_basis_id) FROM {_SOURCE_NAME}"
    )
    source = _relation_ref(conn, f"{_GENERATION}_without_basis")
    request = (
        PsrFeaturesRequest(source=source, namespace="psr_missing_basis")
        if builder is build_psr_features
        else RoeFeaturesRequest(source=source, namespace="roe_missing_basis")
    )

    with pytest.raises(ValueError, match="valuation_basis_id"):
        builder(conn, request)


def test_fundamental_aliases_deduplicate_identical_payloads_without_amplification(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    conn.execute(
        "INSERT INTO daily_valuation "
        "SELECT code || '0', date, basis_version, psr FROM daily_valuation "
        "WHERE code = '2222' AND basis_version = 'basis-b'"
    )
    conn.execute(
        "INSERT INTO statement_metrics_adjusted "
        "SELECT code || '0', disclosed_date, period_end, period_type, adjusted_eps, "
        "adjusted_bps, adjusted_forecast_eps, basis_version "
        "FROM statement_metrics_adjusted "
        "WHERE code = '1111' AND basis_version = 'basis-a'"
    )
    source = _relation_ref(conn)

    psr = build_psr_features(
        conn, PsrFeaturesRequest(source=source, namespace="psr_alias_identical")
    )
    roe = build_roe_features(
        conn, RoeFeaturesRequest(source=source, namespace="roe_alias_identical")
    )

    assert psr.row_count == source.row_count
    assert roe.row_count == source.row_count
    assert conn.execute(
        f"SELECT max(psr) FROM {psr.name} WHERE code = '2222'"
    ).fetchone()[0] == pytest.approx(2.2)
    assert conn.execute(
        f"SELECT max(roe) FROM {roe.name} WHERE code = '1111'"
    ).fetchone()[0] == pytest.approx(25.0)


def test_roe_alias_validation_filters_fy_before_comparing_exact_natural_keys(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    conn.execute(
        "INSERT INTO statement_metrics_adjusted VALUES "
        "('1111', DATE '2023-05-15', DATE '2023-03-31', '1Q', "
        "5, 100, 6, 'basis-a')"
    )
    conn.execute(
        "INSERT INTO statement_metrics_adjusted "
        "SELECT '11110', disclosed_date, period_end, period_type, adjusted_eps, "
        "adjusted_bps, adjusted_forecast_eps, basis_version "
        "FROM statement_metrics_adjusted WHERE code = '1111'"
    )
    source = _relation_ref(conn)

    roe = build_roe_features(
        conn, RoeFeaturesRequest(source=source, namespace="roe_alias_period_types")
    )

    assert roe.row_count == source.row_count
    assert conn.execute(
        f"SELECT max(roe), max(forecast_roe) FROM {roe.name} WHERE code = '1111'"
    ).fetchone() == pytest.approx((25.0, 30.0))


@pytest.mark.parametrize("family", ("psr", "roe"))
def test_fundamental_aliases_reject_conflicting_payloads_deterministically(
    feature_connection: Any,
    family: str,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    if family == "psr":
        conn.execute(
            "INSERT INTO daily_valuation VALUES "
            "('22220', DATE '2024-01-01', 'basis-b', 77.0)"
        )
        build = lambda: build_psr_features(  # noqa: E731
            conn, PsrFeaturesRequest(source=source, namespace="psr_alias_conflict")
        )
        relation = "daily_valuation"
    else:
        conn.execute(
            "INSERT INTO statement_metrics_adjusted VALUES "
            "('11110', DATE '2023-05-15', DATE '2023-03-31', 'FY', "
            "77, 100, 88, 'basis-a')"
        )
        build = lambda: build_roe_features(  # noqa: E731
            conn, RoeFeaturesRequest(source=source, namespace="roe_alias_conflict")
        )
        relation = "statement_metrics_adjusted"

    with pytest.raises(RuntimeError, match=f"{relation}.*conflicting"):
        build()


def test_sector_features_require_complete_history_and_preserve_every_source_key(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    result = build_sector_strength_features(
        conn,
        SectorStrengthFeaturesRequest(
            source=source,
            population_source=source,
            namespace="sector_complete_history",
        ),
    )
    feature_columns = result.columns[len(source.key_columns) :]

    assert result.row_count == source.row_count
    early = conn.execute(
        f"SELECT {', '.join(feature_columns)} FROM {result.name} "
        "WHERE code = '1111' AND date = DATE '2024-01-30'"
    ).fetchone()
    assert early == tuple(None for _ in feature_columns)
    assert conn.execute(
        f"SELECT count(*) FROM {result.name} WHERE sector_strength_score IS NOT NULL "
        "AND (sector_index_5d_strength_rank IS NULL "
        "OR sector_20d_strength_rank IS NULL OR sector_60d_strength_rank IS NULL "
        "OR sector_constituent_20d_strength_rank IS NULL "
        "OR sector_constituent_60d_strength_rank IS NULL "
        "OR sector_breadth_strength_rank IS NULL)"
    ).fetchone()[0] == 0


def test_sector_mixed_constituent_history_uses_legacy_breadth_denominator(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    conn.execute(
        f"UPDATE {_SOURCE_NAME} SET recent_return_20d_pct = 10.0, "
        "recent_return_60d_pct = 10.0 "
        "WHERE code = '2222' AND date = DATE '2024-03-05'"
    )
    conn.execute(
        f"UPDATE {_SOURCE_NAME} SET recent_return_20d_pct = NULL, "
        "recent_return_60d_pct = NULL "
        "WHERE code = '3333' AND date = DATE '2024-03-05'"
    )
    source = _relation_ref(conn)
    result = build_sector_strength_features(
        conn,
        SectorStrengthFeaturesRequest(
            source=source,
            population_source=source,
            namespace="sector_mixed_history",
        ),
    )
    feature_columns = result.columns[len(source.key_columns) :]

    _assert_feature_contract(conn, result, source)
    complete = conn.execute(
        f"SELECT sector_observation_count, sector_code_count, "
        f"sector_constituent_20d_topix_excess_pct, "
        f"sector_constituent_60d_topix_excess_pct, sector_breadth_20d_pct, "
        f"sector_constituent_20d_strength_rank, "
        f"sector_constituent_60d_strength_rank, sector_breadth_strength_rank, "
        f"sector_index_strength_score, sector_constituent_strength_score, "
        f"sector_strength_score, sector_strength_bucket, sector_consistency_bucket "
        f"FROM {result.name} WHERE code = '2222' AND date = DATE '2024-03-05'"
    ).fetchone()
    _assert_golden_rows(
        [complete],
        [
            (
                2,
                2,
                8.0,
                6.0,
                50.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                "sector_weak",
                "sector_weak_consistent",
            )
        ],
    )
    missing = conn.execute(
        f"SELECT {', '.join(feature_columns)} FROM {result.name} "
        "WHERE code = '3333' AND date = DATE '2024-03-05'"
    ).fetchone()
    assert missing == tuple(None for _ in feature_columns)


def test_sma_preserves_invalid_rows_and_requires_complete_valid_session_windows(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    conn.execute(
        f"UPDATE {_SOURCE_NAME} SET close = 0 "
        "WHERE code = '1111' AND date = DATE '2024-01-03'"
    )
    source = _relation_ref(conn)
    result = build_sma_features(
        conn, SmaFeaturesRequest(source=source, namespace="sma_valid_sessions")
    )
    feature_columns = result.columns[len(source.key_columns) :]

    assert result.row_count == source.row_count
    invalid = conn.execute(
        f"SELECT {', '.join(feature_columns)} FROM {result.name} "
        "WHERE code = '1111' AND date = DATE '2024-01-03'"
    ).fetchone()
    assert invalid == tuple(None for _ in feature_columns)
    fourth_valid = conn.execute(
        f"SELECT {', '.join(feature_columns)} FROM {result.name} "
        "WHERE code = '1111' AND date = DATE '2024-01-05'"
    ).fetchone()
    assert fourth_valid == tuple(None for _ in feature_columns)
    fifth_valid = conn.execute(
        f"SELECT sma5, sma5_deviation_bucket, close_below_sma5_count_3d, "
        f"sma5_above_count_5d FROM {result.name} "
        "WHERE code = '1111' AND date = DATE '2024-01-06'"
    ).fetchone()
    assert fifth_valid[0] == pytest.approx((100 + 101 + 103 + 104 + 105) / 5)
    assert fifth_valid[1] is not None
    assert fifth_valid[2:] == (None, None)


def test_legacy_wrapper_repeats_without_uuid_leaks_and_failure_is_atomic(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    conn.execute(
        f"CREATE TEMP VIEW daily_ranking_research_ranked AS "
        f"SELECT * FROM {_SOURCE_NAME}"
    )

    publish_legacy_psr_features(conn)
    first_rows = conn.execute(
        "SELECT * FROM ranking_psr_valuation_panel ORDER BY code, date, market_scope"
    ).fetchall()
    publish_legacy_psr_features(conn)
    assert conn.execute(
        "SELECT * FROM ranking_psr_valuation_panel ORDER BY code, date, market_scope"
    ).fetchall() == first_rows
    assert _legacy_uuid_tables(conn) == set()

    conn.execute(
        "INSERT INTO daily_valuation VALUES "
        "('22220', DATE '2024-01-01', 'basis-b', 77.0)"
    )
    with pytest.raises(RuntimeError, match="conflicting"):
        publish_legacy_psr_features(conn)
    assert conn.execute(
        "SELECT * FROM ranking_psr_valuation_panel ORDER BY code, date, market_scope"
    ).fetchall() == first_rows
    assert _legacy_uuid_tables(conn) == set()


def test_legacy_cleanup_preserves_similar_names_that_do_not_share_exact_prefix(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    success_name = "legacyxfeatureygz_unrelated_success"
    failure_name = "legacyxfeatureygz_unrelated_failure"

    @_cleans_legacy_intermediates
    def publish_success(connection: Any) -> None:
        connection.execute(f"CREATE TEMP TABLE {success_name} (value INTEGER)")

    @_cleans_legacy_intermediates
    def publish_failure(connection: Any) -> None:
        connection.execute(f"CREATE TEMP TABLE {failure_name} (value INTEGER)")
        raise RuntimeError("expected publisher failure")

    publish_success(conn)
    with pytest.raises(RuntimeError, match="expected publisher failure"):
        publish_failure(conn)

    assert conn.execute(
        "SELECT table_name FROM duckdb_tables() "
        "WHERE table_name IN (?, ?) ORDER BY table_name",
        [success_name, failure_name],
    ).fetchall() == [(failure_name,), (success_name,)]


def test_atr_and_short_scaffold_match_frozen_golden_rows_for_every_source_key(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    atr = build_atr_features(conn, AtrFeaturesRequest(source=source, namespace="atr_parity"))
    short = build_short_scaffold_features(
        conn,
        ShortScaffoldFeaturesRequest(
            source=source,
            atr_features=atr,
            namespace="short_parity",
        ),
    )
    source_rows = conn.execute(
        f"SELECT code, date, market_scope, atr20_pct, atr60_pct, atr20_to_atr60, "
        f"atr20_change_20d_pct, liquidity_scope, pbr_percentile, "
        f"forecast_per_percentile, per_percentile, forecast_per_to_per_ratio, "
        f"forecast_p_op_percentile, recent_return_20d_pct, recent_return_60d_pct "
        f"FROM {source.name} ORDER BY code, date, market_scope"
    ).fetchall()
    expected_atr: list[tuple[object, ...]] = []
    expected_short: list[tuple[object, ...]] = []
    for row in source_rows:
        (
            code, day, scope, atr20, atr60, atr_ratio, atr_change, liquidity,
            pbr_pct, forecast_per_pct, per_pct, forecast_ratio, forecast_pop_pct,
            return20, return60,
        ) = row
        keys = (code, day, scope)
        strong = (pbr_pct <= 0.2 and forecast_per_pct <= 0.2) or (
            per_pct is not None and per_pct <= 0.2 and forecast_ratio <= 0.8
        )
        medium = pbr_pct <= 0.2 or (
            per_pct is not None and per_pct <= 0.2 and forecast_ratio <= 1.0
        )
        overvalued = any(
            value is not None and value >= 0.8
            for value in (per_pct, forecast_per_pct, forecast_pop_pct, pbr_pct)
        )
        missing = per_pct is None and forecast_per_pct is None
        weak = return20 <= 0 or return60 <= 0
        expected_atr.append(keys + (atr20, atr60, atr_ratio, atr_change))
        expected_short.append(
            keys
            + (
                liquidity, atr20, atr60, atr_ratio, atr_change, strong, medium,
                overvalued, missing, weak, overvalued or missing, not medium,
                atr_change >= 25 and atr_ratio < 1.25,
                atr_change >= 25 and atr_ratio >= 1.25,
            )
        )

    _assert_golden_rows(_feature_rows(conn, atr), expected_atr)
    _assert_golden_rows(_feature_rows(conn, short), expected_short)


def test_sector_strength_matches_frozen_complete_history_golden_rows(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    result = build_sector_strength_features(
        conn,
        SectorStrengthFeaturesRequest(
            source=source,
            population_source=source,
            namespace="sector_owner_parity",
        ),
    )
    rows = _feature_rows(conn, result)
    assert len(rows) == 210
    for row in rows:
        code, day, scope = row[:3]
        offset = (day - date(2024, 1, 1)).days
        if offset < 60:
            assert row[3:] == tuple(None for _ in row[3:])
            continue
        strong = code == "1111"
        sector_code = "0050" if strong else "1050"
        sector_name = "Fishery" if strong else "Mining"
        observations = 1 if strong else 2
        index_step = 2.0 if strong else 0.2
        current = 100.0 + index_step * offset
        topix = 100.0 + 0.5 * offset
        returns = tuple(
            100.0 * (current / (current - index_step * lag) - 1.0)
            for lag in (5, 20, 60)
        )
        excess = tuple(
            100.0
            * (
                current / (current - index_step * lag)
                - topix / (topix - 0.5 * lag)
            )
            for lag in (5, 20, 60)
        )
        rank = 1.0 if strong else 0.0
        constituent20 = 10.0 if strong else -2.5
        constituent60 = 14.0 if strong else -1.0
        breadth = 100.0 if strong else 0.0
        expected = (
            code, day, scope, sector_code, sector_name, observations, observations,
            "0040" if strong else "0041", *returns, *excess,
            constituent20, constituent60, excess[1], excess[2], breadth,
            rank, rank, rank, rank, rank, rank, rank, rank, rank,
            "sector_strong" if strong else "sector_weak",
            "sector_strong_consistent" if strong else "sector_weak_consistent",
        )
        _assert_golden_rows([row], [expected])


def test_fundamental_features_match_literal_golden_rows_for_every_source_key(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    psr = build_psr_features(conn, PsrFeaturesRequest(source=source, namespace="psr_golden"))
    roe = build_roe_features(conn, RoeFeaturesRequest(source=source, namespace="roe_golden"))
    source_keys = conn.execute(
        f"SELECT code, date, market_scope FROM {source.name} ORDER BY code, date, market_scope"
    ).fetchall()
    expected_psr: list[tuple[object, ...]] = []
    expected_roe: list[tuple[object, ...]] = []
    for code, day, scope in source_keys:
        keys = (code, day, scope)
        psr_values = {
            "1111": (300_000_000.0, 10.0 / 3.0, 1.0, "psr_very_overvalued"),
            "2222": (100_000_000.0, 2.2, 0.0, "psr_undervalued"),
            "3333": (None, None, None, "missing_psr"),
        }[code]
        expected_psr.append(keys + (psr_values[0], date(2023, 5, 15) if psr_values[0] else None, *psr_values[1:]))
        roe_values = {
            "1111": (25.0, 100.0, 30.0, 25.0, 30.0, 1.0, 1.0, "roe_very_high", "forward_roe_very_high"),
            "2222": (2.0, 100.0, 3.0, 2.0, 3.0, 0.5, 0.0, None, "forward_roe_low"),
            "3333": (0.0, 100.0, None, 0.0, None, 0.0, None, "roe_low", "missing_forward_roe"),
        }[code]
        expected_roe.append(
            keys + (date(2023, 5, 15), date(2023, 3, 31), *roe_values)
        )
    _assert_golden_rows(_feature_rows(conn, psr), expected_psr)
    _assert_golden_rows(_feature_rows(conn, roe), expected_roe)


def test_sma_formulas_preserve_existing_rolling_definitions(feature_connection: Any) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    result = build_sma_features(
        conn,
        SmaFeaturesRequest(source=source, namespace="sma_formula"),
    )
    row = conn.execute(
        f"SELECT sma5, sma5_deviation_pct, sma5_above_count_5d, "
        f"close_below_sma5_count_3d FROM {result.name} "
        "WHERE code = '1111' AND date = DATE '2024-01-09'"
    ).fetchone()
    assert row == pytest.approx((106.0, (108.0 / 106.0 - 1.0) * 100.0, 5, 0))


def test_sma_features_match_frozen_valid_session_golden_rows(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    result = build_sma_features(
        conn,
        SmaFeaturesRequest(source=source, namespace="sma_owner_parity"),
    )
    source_rows = conn.execute(
        f"SELECT code, date, market_scope, close FROM {source.name} "
        "ORDER BY code, date, market_scope"
    ).fetchall()
    closes: dict[str, list[float]] = {}
    eligible_flags: dict[str, list[tuple[int, int]]] = {}
    expected: list[tuple[object, ...]] = []
    for code, day, scope, close in source_rows:
        code_closes = closes.setdefault(code, [])
        code_flags = eligible_flags.setdefault(code, [])
        code_closes.append(close)
        if len(code_closes) < 5:
            expected.append((code, day, scope, *(None for _ in range(9))))
            continue
        sma5 = sum(code_closes[-5:]) / 5.0
        deviation = (close / sma5 - 1.0) * 100.0
        below = int(close < sma5)
        above = int(close > sma5)
        code_flags.append((below, above))
        below_count = sum(flag[0] for flag in code_flags[-3:]) if len(code_flags) >= 3 else None
        above_count = sum(flag[1] for flag in code_flags[-5:]) if len(code_flags) >= 5 else None
        streak = below_count == 3 if below_count is not None else None
        streak_bucket = (
            "below_sma5_streak_ge3" if streak else "below_sma5_streak_other"
        ) if streak is not None else None
        count_group = None
        if above_count is not None:
            count_group = (
                "sma5_above_count_0_1" if above_count <= 1
                else "sma5_above_count_2_3" if above_count <= 3
                else "sma5_above_count_4_5"
            )
        deviation_bucket = (
            "below_sma5_le_neg2" if deviation <= -2
            else "below_sma5_neg2_to_0" if deviation <= 0
            else "above_sma5_0_to_2" if deviation <= 2
            else "above_sma5_2_to_5" if deviation <= 5
            else "above_sma5_gt_5"
        )
        expected.append(
            (
                code, day, scope, sma5, deviation, below, below_count, above_count,
                streak, streak_bucket, count_group, deviation_bucket,
            )
        )
    _assert_golden_rows(_feature_rows(conn, result), expected)


def test_long_scaffold_matches_literal_golden_rows_for_every_source_key(
    feature_connection: Any,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    atr = build_atr_features(
        conn,
        AtrFeaturesRequest(source=source, namespace="atr_long_owner_parity"),
    )
    short = build_short_scaffold_features(
        conn,
        ShortScaffoldFeaturesRequest(
            source=source,
            atr_features=atr,
            namespace="short_long_owner_parity",
        ),
    )
    leadership = _leadership_ref(conn, source)
    result = build_long_scaffold_features(
        conn,
        LongScaffoldFeaturesRequest(
            source=source,
            leadership_features=leadership,
            short_scaffold_features=short,
            namespace="long_owner_parity",
        ),
    )
    source_rows = conn.execute(
        f"SELECT code, date, market_scope, atr20_pct, atr60_pct, atr20_to_atr60, "
        f"atr20_change_20d_pct, recent_return_20d_pct, recent_return_60d_pct, "
        f"forecast_per_percentile, pbr_percentile FROM {source.name} "
        "ORDER BY code, date, market_scope"
    ).fetchall()
    expected: list[tuple[object, ...]] = []
    for row in source_rows:
        (
            code, day, scope, atr20, atr60, atr_ratio, atr_change,
            return20, return60, forecast_per_pct, pbr_pct,
        ) = row
        low_forward = None if forecast_per_pct is None else 1.0 - forecast_per_pct
        low_pbr = None if pbr_pct is None else 1.0 - pbr_pct
        expected.append(
            (
                code, day, scope,
                "0050" if code == "1111" else "1050",
                "Fishery" if code == "1111" else "Mining",
                "sector_strong", 0.9, 0.9, 0.9, 0.9, 0.9, 0.9,
                "Balanced Strong", "Long Strong", True,
                atr20, atr60, atr_ratio, atr_change,
                atr_change >= 25 and atr_ratio < 1.25,
                atr_change >= 25 and atr_ratio < 1.25 and return20 < 30,
                atr_change >= 25 and atr_ratio >= 1.25,
                return20 <= 0 or return60 <= 0,
                low_forward, low_pbr,
                None if low_forward is None or low_pbr is None
                else (low_forward + low_pbr) / 2.0,
            )
        )
    _assert_golden_rows(_feature_rows(conn, result), expected)


def test_psr_roe_and_long_scaffold_formula_values(feature_connection: Any) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    atr = build_atr_features(conn, AtrFeaturesRequest(source=source, namespace="atr_values"))
    short = build_short_scaffold_features(
        conn,
        ShortScaffoldFeaturesRequest(
            source=source,
            atr_features=atr,
            namespace="short_values",
        ),
    )
    psr = build_psr_features(conn, PsrFeaturesRequest(source=source, namespace="psr_values"))
    roe = build_roe_features(conn, RoeFeaturesRequest(source=source, namespace="roe_values"))
    leadership = _leadership_ref(conn, source)
    long = build_long_scaffold_features(
        conn,
        LongScaffoldFeaturesRequest(
            source=source,
            leadership_features=leadership,
            short_scaffold_features=short,
            namespace="long_values",
        ),
    )
    psr_value, psr_signal = conn.execute(
        f"SELECT psr, psr_signal FROM {psr.name} "
        "WHERE code = '1111' ORDER BY date LIMIT 1"
    ).fetchone()
    assert psr_value == pytest.approx(1_000_000_000.0 / 300_000_000.0)
    assert psr_signal == "psr_very_overvalued"
    assert conn.execute(
        f"SELECT roe, forecast_roe FROM {roe.name} "
        "WHERE code = '1111' ORDER BY date LIMIT 1"
    ).fetchone() == pytest.approx((25.0, 30.0))
    assert conn.execute(
        f"SELECT value_composite_equal_score FROM {long.name} "
        "WHERE code = '1111' ORDER BY date LIMIT 1"
    ).fetchone()[0] == pytest.approx(0.9)


def _leadership_ref(conn: Any, source: RelationRef) -> RelationRef:
    name = f"{_GENERATION}_leadership"
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {name} AS
        SELECT code, date, market_scope,
               CASE code WHEN '1111' THEN '0050' ELSE '1050' END::VARCHAR
                   AS sector_33_code,
               CASE code WHEN '1111' THEN 'Fishery' ELSE 'Mining' END::VARCHAR
                   AS sector_33_name,
               'sector_strong'::VARCHAR AS sector_strength_bucket,
               0.9::DOUBLE AS sector_strength_score,
               0.9::DOUBLE AS sector_index_strength_score,
               0.9::DOUBLE AS sector_constituent_strength_score,
               0.9::DOUBLE AS long_index_leadership_score,
               0.9::DOUBLE AS long_constituent_breadth_leadership_score,
               0.9::DOUBLE AS long_hybrid_leadership_score,
               'Balanced Strong'::VARCHAR AS balanced_sector_strength_bucket_label,
               'Long Strong'::VARCHAR AS long_hybrid_bucket_label,
               TRUE::BOOLEAN AS momentum_20_60_top20_flag
        FROM {source.name}
        """
    )
    return _relation_ref(
        conn,
        name,
        capability=source._capability,
        generation=source.generation,
        kind="signal_features",
    )


def _assert_golden_rows(
    actual: list[tuple[object, ...]],
    expected: list[tuple[object, ...]],
) -> None:
    assert expected
    assert len(actual) == len(expected)
    for actual_row, expected_row in zip(actual, expected, strict=True):
        for actual_value, expected_value in zip(
            actual_row, expected_row, strict=True
        ):
            if isinstance(actual_value, float) and isinstance(expected_value, float):
                assert actual_value == pytest.approx(expected_value, abs=1e-12)
            else:
                assert actual_value == expected_value


def _legacy_uuid_tables(conn: Any) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE temporary AND table_name LIKE 'legacy_feature_g_%'"
        ).fetchall()
    }


class _TrackingCursor:
    def __init__(
        self,
        cursor: Any,
        *,
        fingerprint: bool,
        fetches: list[tuple[str, int]],
    ) -> None:
        self._cursor = cursor
        self._fingerprint = fingerprint
        self._fetches = fetches

    def fetchone(self) -> Any:
        row = self._cursor.fetchone()
        if self._fingerprint:
            self._fetches.append(("fetchone", 0 if row is None else 1))
        return row

    def fetchall(self) -> Any:
        rows = self._cursor.fetchall()
        if self._fingerprint:
            self._fetches.append(("fetchall", len(rows)))
        return rows

    def fetchmany(self, size: int = 1) -> Any:
        rows = self._cursor.fetchmany(size)
        if self._fingerprint:
            self._fetches.append(("fetchmany", len(rows)))
        return rows

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


class _TrackingConnection:
    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self.queries: list[str] = []
        self.fingerprint_fetches: list[tuple[str, int]] = []

    def execute(self, query: str, parameters: Any = None) -> _TrackingCursor:
        self.queries.append(query)
        cursor = (
            self._connection.execute(query)
            if parameters is None
            else self._connection.execute(query, parameters)
        )
        return _TrackingCursor(
            cursor,
            fingerprint="daily-ranking:key:v1" in query,
            fetches=self.fingerprint_fetches,
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)
