from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import date, timedelta
from typing import Any

import duckdb
import pytest

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
)
from src.domains.analytics.daily_ranking_research_base import RelationRef


_GENERATION = "feature_contract_g_0123456789abcdef"
_SOURCE_NAME = f"{_GENERATION}_source"
_SOURCE_SCHEMA = (
    ("code", "VARCHAR"),
    ("date", "DATE"),
    ("market_scope", "VARCHAR"),
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


@pytest.fixture
def feature_connection() -> Any:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TEMP TABLE feature_contract_g_0123456789abcdef_source (
            code VARCHAR,
            date DATE,
            market_scope VARCHAR,
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
        f"INSERT INTO {_SOURCE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.execute(
        """
        CREATE TABLE daily_valuation (code VARCHAR, date DATE, psr DOUBLE)
        """
    )
    conn.execute(
        """
        INSERT INTO daily_valuation
        SELECT code, date,
               CASE code WHEN '1111' THEN NULL WHEN '2222' THEN 2.2 ELSE NULL END
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
            ('3333', DATE '2023-05-15', DATE '2023-03-31', 'FY', 0, 100, NULL, 'basis-c')
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
    return RelationRef(
        name=name,
        columns=tuple(column for column, _ in schema),
        key_columns=keys,
        row_count=int(conn.execute(f"SELECT count(*) FROM {name}").fetchone()[0]),
        column_types=tuple(sql_type for _, sql_type in schema),
        generation=generation,
        kind=kind,  # type: ignore[arg-type]
        _capability=object() if capability is None else capability,
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


def test_atr_and_short_scaffold_formulas_match_existing_helpers(
    feature_connection: Any,
) -> None:
    from src.domains.analytics.ranking_short_red_evidence import _create_feature_panel

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
    _install_legacy_ranked_views(conn)
    conn.execute(
        """
        CREATE TEMP VIEW atr_expansion_scoped AS
        SELECT code, date, market_scope, atr20_pct, atr60_pct,
               atr20_to_atr60, atr20_change_20d_pct
        FROM feature_contract_g_0123456789abcdef_source
        """
    )
    _create_feature_panel(conn)

    projected = ", ".join(short.columns)
    assert _feature_rows(conn, short) == conn.execute(
        f"SELECT {projected} FROM ranking_short_red_feature_panel "
        f"ORDER BY {', '.join(short.key_columns)}"
    ).fetchall()


def test_atr_projection_matches_source_owner_relation(feature_connection: Any) -> None:
    from src.domains.analytics.atr_expansion_forward_response import (
        _create_observation_panel,
    )

    conn = feature_connection
    source = _relation_ref(conn)
    result = build_atr_features(
        conn,
        AtrFeaturesRequest(source=source, namespace="atr_owner_parity"),
    )
    conn.execute(
        f"CREATE TEMP VIEW atr_owner_source AS "
        f"SELECT * EXCLUDE (market_scope) FROM {source.name}"
    )
    conn.execute(
        """
        CREATE TEMP TABLE atr_owner_outcomes AS
        SELECT code, date, date AS forward_outcome_completion_date_5d,
               0.0::DOUBLE AS forward_close_return_5d_pct,
               0.0::DOUBLE AS forward_close_excess_return_5d_pct
        FROM atr_owner_source
        """
    )
    _create_observation_panel(
        conn,
        query_start=None,
        query_end=None,
        analysis_start_date=None,
        analysis_end_date=None,
        atr_windows=(20, 60),
        return_windows=(20, 60),
        horizons=(5,),
        market_source="stock_master_daily_exact_date",
        market_scopes=("prime",),
        price_feature_relation="atr_owner_source",
        price_outcome_relation="atr_owner_outcomes",
    )

    _assert_relation_overlap_parity(conn, result, "atr_expansion_scoped")


def test_sector_strength_matches_source_owner_signal_fields(
    feature_connection: Any,
) -> None:
    from src.domains.analytics.ranking_sector_strength_evidence import (
        _create_sector_strength_tables,
    )

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
    _install_legacy_ranked_views(conn)
    _create_sector_strength_tables(conn, horizons=(5,))

    _assert_relation_overlap_parity(conn, result, "ranking_sector_signal_panel")


@pytest.mark.parametrize(
    ("builder", "request_factory", "legacy_builder", "legacy_relation"),
    [
        (
            build_psr_features,
            lambda source: PsrFeaturesRequest(source=source, namespace="psr_parity"),
            "psr",
            "ranking_psr_valuation_panel",
        ),
        (
            build_roe_features,
            lambda source: RoeFeaturesRequest(source=source, namespace="roe_parity"),
            "roe",
            "ranking_roe_quality_panel",
        ),
    ],
)
def test_fundamental_feature_formulas_match_existing_helpers(
    feature_connection: Any,
    builder: Callable[[Any, Any], RelationRef],
    request_factory: Callable[[RelationRef], object],
    legacy_builder: str,
    legacy_relation: str,
) -> None:
    conn = feature_connection
    source = _relation_ref(conn)
    result = builder(conn, request_factory(source))
    _install_legacy_ranked_views(conn)
    if legacy_builder == "psr":
        from src.domains.analytics.ranking_psr_valuation_evidence import (
            _create_psr_valuation_panel,
        )

        _create_psr_valuation_panel(conn)
    else:
        from src.domains.analytics.ranking_roe_quality_evidence import (
            _create_roe_quality_panel,
        )

        _create_roe_quality_panel(conn)
    projected = ", ".join(
        {
            "forecast_roe": "forward_roe AS forecast_roe",
            "forecast_roe_percentile": (
                "forward_roe_percentile AS forecast_roe_percentile"
            ),
            "forecast_roe_signal": "forward_roe_signal AS forecast_roe_signal",
        }.get(column, column)
        for column in result.columns
    )
    assert _feature_rows(conn, result) == conn.execute(
        f"SELECT {projected} FROM {legacy_relation} "
        f"ORDER BY {', '.join(result.key_columns)}"
    ).fetchall()


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


def test_sma_features_match_existing_below_streak_helper(
    feature_connection: Any,
) -> None:
    from src.domains.analytics.ranking_short_red_evidence import _create_feature_panel
    from src.domains.analytics.ranking_sma5_below_streak_evidence import (
        _create_sma5_below_streak_panel,
    )

    conn = feature_connection
    source = _relation_ref(conn)
    result = build_sma_features(
        conn,
        SmaFeaturesRequest(source=source, namespace="sma_owner_parity"),
    )
    _install_legacy_ranked_views(conn)
    _install_legacy_atr_view(conn)
    _create_feature_panel(conn)
    leadership = _leadership_ref(conn, source)
    conn.execute(
        f"CREATE TEMP VIEW long_sector_leadership_base_panel AS "
        f"SELECT * FROM {leadership.name}"
    )
    _create_sma5_below_streak_panel(conn)

    shared_columns = tuple(
        column for column in result.columns if column != "sma5_deviation_bucket"
    )
    _assert_relation_overlap_parity(
        conn,
        result,
        "ranking_sma5_below_streak_panel",
        columns=shared_columns,
    )


def test_long_scaffold_matches_existing_value_composite_helper(
    feature_connection: Any,
) -> None:
    from src.domains.analytics.ranking_long_scaffold_value_composite_evidence import (
        _create_value_composite_panel,
    )
    from src.domains.analytics.ranking_short_red_evidence import _create_feature_panel

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
    _install_legacy_ranked_views(conn)
    _install_legacy_atr_view(conn)
    _create_feature_panel(conn)
    conn.execute(
        f"CREATE TEMP VIEW long_sector_leadership_base_panel AS "
        f"SELECT * FROM {leadership.name}"
    )
    _create_value_composite_panel(conn)

    _assert_relation_overlap_parity(
        conn,
        result,
        "ranking_long_scaffold_value_composite_panel",
        legacy_aliases={"low_forecast_per_score": "low_forward_per_score"},
    )


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


def _install_legacy_ranked_views(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP VIEW daily_ranking_research_ranked AS
        SELECT * EXCLUDE (forecast_per_percentile, forecast_p_op_percentile,
                          forecast_per_to_per_ratio),
               forecast_per_percentile AS forward_per_percentile,
               forecast_p_op_percentile AS forward_p_op_percentile,
               forecast_per_to_per_ratio AS forward_per_to_per_ratio,
               0.0::DOUBLE AS forward_close_return_5d_pct
        FROM feature_contract_g_0123456789abcdef_source
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP VIEW daily_ranking_research_liquidity_ranked AS
        SELECT * FROM daily_ranking_research_ranked
        """
    )
    conn.execute(
        "CREATE OR REPLACE TEMP VIEW ranking_color_ranked AS "
        "SELECT * FROM daily_ranking_research_ranked"
    )
    conn.execute(
        "CREATE OR REPLACE TEMP VIEW ranking_color_liquidity_ranked AS "
        "SELECT * FROM daily_ranking_research_liquidity_ranked"
    )


def _install_legacy_atr_view(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP VIEW atr_expansion_scoped AS
        SELECT code, date, market_scope, atr20_pct, atr60_pct,
               atr20_to_atr60, atr20_change_20d_pct
        FROM feature_contract_g_0123456789abcdef_source
        """
    )


def _assert_relation_overlap_parity(
    conn: Any,
    relation: RelationRef,
    legacy_relation: str,
    *,
    columns: tuple[str, ...] | None = None,
    legacy_aliases: dict[str, str] | None = None,
) -> None:
    projected_columns = relation.columns if columns is None else columns
    aliases = {} if legacy_aliases is None else legacy_aliases
    key_order = ", ".join(f"legacy.{column}" for column in relation.key_columns)
    actual_projection = ", ".join(
        f"feature.{column}" for column in projected_columns
    )
    legacy_projection = ", ".join(
        f"legacy.{aliases.get(column, column)}" for column in projected_columns
    )
    key_join = ", ".join(relation.key_columns)
    actual = conn.execute(
        f"SELECT {actual_projection} FROM {relation.name} feature "
        f"JOIN {legacy_relation} legacy USING ({key_join}) ORDER BY {key_order}"
    ).fetchall()
    expected = conn.execute(
        f"SELECT {legacy_projection} FROM {legacy_relation} legacy "
        f"ORDER BY {key_order}"
    ).fetchall()
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
