from __future__ import annotations

import ast
import copy
from dataclasses import replace
from datetime import date
import inspect
from pathlib import Path
import re

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.daily_ranking_research_base import (
    DAILY_RANKING_RESEARCH_BRIDGE_DEPRECATED,
    DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE,
    DAILY_RANKING_RESEARCH_PANEL_TABLE,
    DAILY_RANKING_RESEARCH_RANKED_TABLE,
    DAILY_RANKING_RESEARCH_RELATIONS_TABLE,
    DAILY_RANKING_RESEARCH_SCOPED_TABLE,
    DEPRECATED_DAILY_RANKING_RESEARCH_BRIDGE_CALLERS,
    DEPRECATED_DAILY_RANKING_RESEARCH_DIRECT_CALLERS,
    DEPRECATED_DAILY_RANKING_RESEARCH_INDIRECT_CALLERS,
    DailyRankingPanelRequest,
    RelationRef,
    SignalDerivedColumn,
    SignalExpression,
    _standardized_liquidity_residual_sql,
    attach_daily_ranking_outcomes,
    build_daily_ranking_research_base,
    materialize_daily_ranking_signal_cohort,
    publish_daily_ranking_signal_features,
)
from src.domains.analytics import daily_ranking_research_base


def _build_market_v4_research_fixture(path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(path))
    conn.execute(
        """
        CREATE TABLE market_schema_version (
            version INTEGER, applied_at TEXT, notes TEXT
        );
        INSERT INTO market_schema_version VALUES (4, NULL, NULL);
        CREATE TABLE sync_metadata (
            key TEXT PRIMARY KEY, value TEXT, updated_at TEXT
        );
        INSERT INTO sync_metadata VALUES (
            'stock_price_adjustment_mode',
            'local_projection_v2_event_time',
            NULL
        );
        CREATE TABLE stock_data_raw (
            code TEXT, date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT, adjustment_factor DOUBLE
        );
        CREATE TABLE stock_master_daily (
            date DATE, code TEXT, company_name TEXT, market_code TEXT,
            market_name TEXT, scale_category TEXT
        );
        CREATE TABLE stock_adjustment_bases (
            code TEXT, basis_id TEXT, valid_from DATE,
            valid_to_exclusive DATE, adjustment_through_date DATE,
            source_fingerprint TEXT, materialized_through_date DATE,
            status TEXT
        );
        CREATE TABLE stock_adjustment_basis_segments (
            code TEXT, basis_id TEXT, source_date_from DATE,
            source_date_to_exclusive DATE, cumulative_factor DOUBLE
        );
        CREATE TABLE daily_valuation (
            code TEXT, date DATE, price_basis_date DATE, per DOUBLE,
            forward_per DOUBLE, pbr DOUBLE, p_op DOUBLE, forward_p_op DOUBLE,
            market_cap DOUBLE, free_float_market_cap DOUBLE, basis_version TEXT
        );
        CREATE TABLE topix_data (
            date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE
        );
        CREATE TABLE indices_data (
            code TEXT, date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT
        );
        """
    )
    dates = tuple(pd.bdate_range("2024-01-04", periods=90).date)
    securities = (
        ("1111", "Alpha", "0111", "Prime", 100.0, 0.25),
        ("2222", "Beta", "0111", "Prime", 180.0, -0.05),
        ("3333", "Gamma", "0112", "Standard", 80.0, 0.15),
    )
    raw_rows: list[tuple[object, ...]] = []
    master_rows: list[tuple[object, ...]] = []
    valuation_rows: list[tuple[object, ...]] = []
    for index, session_date in enumerate(dates):
        for security_index, (
            code,
            company_name,
            market_code,
            market_name,
            base,
            slope,
        ) in enumerate(securities):
            close = base + index * slope
            basis_id = f"event-pit-v1:{code}:{dates[0]}"
            raw_rows.append(
                (
                    code,
                    session_date,
                    close * 0.995,
                    close * 1.01,
                    close * 0.99,
                    close,
                    10_000 + index * 10 + security_index * 1_000,
                    1.0,
                )
            )
            master_rows.append(
                (
                    session_date,
                    code,
                    company_name,
                    market_code,
                    market_name,
                    None,
                )
            )
            valuation_rows.append(
                (
                    code,
                    session_date,
                    session_date,
                    10.0 + security_index * 5.0,
                    8.0 + security_index * 4.0,
                    0.8 + security_index * 0.7,
                    7.0 + security_index * 2.0,
                    6.0 + security_index * 2.0,
                    100_000_000.0 + security_index * 50_000_000.0,
                    80_000_000.0 + security_index * 40_000_000.0,
                    basis_id,
                )
            )
    conn.executemany(
        "INSERT INTO stock_data_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?)", raw_rows
    )
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?)", master_rows
    )
    conn.executemany(
        "INSERT INTO daily_valuation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        valuation_rows,
    )
    basis_rows = [
        (
            code,
            f"event-pit-v1:{code}:{dates[0]}",
            dates[0],
            None,
            dates[0],
            f"fixture-{code}",
            dates[-1],
            "ready",
        )
        for code, *_ in securities
    ]
    segment_rows = [
        (code, f"event-pit-v1:{code}:{dates[0]}", dates[0], None, 1.0)
        for code, *_ in securities
    ]
    conn.executemany(
        "INSERT INTO stock_adjustment_bases VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        basis_rows,
    )
    conn.executemany(
        "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, ?, ?, ?)",
        segment_rows,
    )
    topix_rows = []
    n225_rows = []
    for index, session_date in enumerate(dates):
        topix_close = 2_000.0 + index * 2.0
        n225_close = 30_000.0 + index * 20.0
        topix_rows.append(
            (
                session_date,
                topix_close * 0.995,
                topix_close * 1.005,
                topix_close * 0.99,
                topix_close,
            )
        )
        n225_rows.append(
            (
                "N225_UNDERPX",
                session_date,
                n225_close * 0.995,
                n225_close * 1.005,
                n225_close * 0.99,
                n225_close,
                0,
            )
        )
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)", topix_rows)
    conn.executemany("INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?)", n225_rows)
    return conn


def _request(
    namespace: str,
    *,
    horizons: tuple[int, ...] = (2,),
    include_liquidity: bool = True,
    percentile_features: tuple[str, ...] = (),
) -> DailyRankingPanelRequest:
    return DailyRankingPanelRequest(
        namespace=namespace,
        analysis_start_date=date(2024, 4, 4),
        analysis_end_date=date(2024, 4, 12),
        horizons=horizons,
        market_scopes=("prime",),
        include_liquidity=include_liquidity,
        percentile_features=percentile_features,
    )


def _relation_type_map(
    conn: duckdb.DuckDBPyConnection,
    relation: RelationRef,
) -> dict[str, str]:
    return {
        str(row[1]): str(row[2]).upper()
        for row in conn.execute(f"PRAGMA table_info('{relation.name}')").fetchall()
    }


def _assert_unique_keys(
    conn: duckdb.DuckDBPyConnection,
    relation: RelationRef,
) -> None:
    keys = ", ".join(relation.key_columns)
    duplicate_count = conn.execute(
        f"SELECT count(*) FROM ("
        f"SELECT {keys} FROM {relation.name} GROUP BY {keys} HAVING count(*) > 1)"
    ).fetchone()[0]
    assert duplicate_count == 0


def _actual_imported_callers(*, module: str, symbol: str) -> set[str]:
    analytics_root = Path(daily_ranking_research_base.__file__).parent
    callers: set[str] = set()
    for path in analytics_root.glob("*.py"):
        tree = ast.parse(path.read_text())
        local_names = {
            alias.asname or alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module == module
            for alias in node.names
            if alias.name == symbol
        }
        if local_names and any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in local_names
            for node in ast.walk(tree)
        ):
            callers.add(path.stem)
    return callers


def test_relation_and_request_identifiers_are_validated() -> None:
    with pytest.raises(
        ValueError,
        match="^invalid SQL identifier: 'unsafe;drop'$",
    ):
        RelationRef("unsafe;drop", ("code",), ("code",), 0)
    with pytest.raises(ValueError, match="namespace"):
        _request("Unsafe-Namespace")


def test_deprecated_bridge_is_explicit_and_does_not_import_ranking_color() -> None:
    assert DAILY_RANKING_RESEARCH_BRIDGE_DEPRECATED is True
    direct = _actual_imported_callers(
        module="src.domains.analytics.daily_ranking_research_base",
        symbol="create_daily_ranking_research_panel",
    )
    indirect = _actual_imported_callers(
        module="src.domains.analytics.ranking_color_evidence",
        symbol="_create_observation_panel",
    )
    assert direct == set(DEPRECATED_DAILY_RANKING_RESEARCH_DIRECT_CALLERS)
    assert indirect == set(DEPRECATED_DAILY_RANKING_RESEARCH_INDIRECT_CALLERS)
    assert direct.isdisjoint(indirect)
    assert len(direct) == 25
    assert len(indirect) == 5
    assert set(DEPRECATED_DAILY_RANKING_RESEARCH_BRIDGE_CALLERS) == direct | indirect
    assert (
        "ranking_color_evidence"
        not in Path(daily_ranking_research_base.__file__).read_text()
    )


def test_namespaced_builds_coexist_with_explicit_unique_date_schemas(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        first = build_daily_ranking_research_base(conn, _request("alpha"))
        second = build_daily_ranking_research_base(conn, _request("beta"))

        first_refs = (
            first.signal_prices,
            first.forward_outcomes,
            first.signal_panel,
            first.ranked_signals,
            first.liquidity_ranked_signals,
        )
        second_refs = (
            second.signal_prices,
            second.forward_outcomes,
            second.signal_panel,
            second.ranked_signals,
            second.liquidity_ranked_signals,
        )
        assert None not in first_refs
        assert None not in second_refs
        assert {ref.name for ref in first_refs if ref is not None}.isdisjoint(
            ref.name for ref in second_refs if ref is not None
        )
        for relation in (*first_refs, *second_refs):
            assert relation is not None
            assert relation.columns == tuple(
                row[1]
                for row in conn.execute(
                    f"PRAGMA table_info('{relation.name}')"
                ).fetchall()
            )
            assert (
                relation.row_count
                == conn.execute(f"SELECT count(*) FROM {relation.name}").fetchone()[0]
            )
            _assert_unique_keys(conn, relation)
            date_types = {
                column: sql_type
                for column, sql_type in _relation_type_map(conn, relation).items()
                if column == "date" or "_date_" in column
            }
            assert date_types
            assert set(date_types.values()) == {"DATE"}

        for relation in (
            first.signal_prices,
            first.signal_panel,
            first.ranked_signals,
            first.liquidity_ranked_signals,
        ):
            assert relation is not None
            assert not any(column.startswith("forward_") for column in relation.columns)
        assert first.lineage.no_stock_data_fallback
        assert first.lineage.verification_status == "verified"
    finally:
        conn.close()


def test_disabled_optional_stages_return_none_and_do_not_expose_stale_relations(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        enabled = build_daily_ranking_research_base(
            conn,
            _request(
                "repeatable",
                percentile_features=("forecast_per_to_per_ratio",),
            ),
        )
        disabled = build_daily_ranking_research_base(
            conn,
            _request("repeatable", include_liquidity=False),
        )

        assert enabled.liquidity_ranked_signals is not None
        assert disabled.liquidity_ranked_signals is None
        assert enabled.ranked_signals.name != disabled.ranked_signals.name
        assert "forecast_per_to_per_ratio_percentile" in enabled.ranked_signals.columns
        assert (
            "forecast_per_to_per_ratio_percentile"
            not in disabled.ranked_signals.columns
        )
        assert disabled.diagnostics.liquidity_stage_executed is False
        assert disabled.diagnostics.percentile_features == ()
        disabled_generation = disabled.ranked_signals.name.removesuffix(
            "_ranked_signals"
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                [f"{disabled_generation}_liquidity_ranked_signals"],
            ).fetchone()[0]
            == 0
        )
    finally:
        conn.close()


def test_deprecated_bridge_specs_remain_generation_bound_across_rebuilds(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        first = daily_ranking_research_base.create_daily_ranking_research_panel(
            conn,
            query_start=None,
            query_end=None,
            analysis_start_date="2024-04-04",
            analysis_end_date="2024-04-12",
            horizons=(2,),
            market_scopes=("prime",),
            include_liquidity_ranked=True,
        )
        second = daily_ranking_research_base.create_daily_ranking_research_panel(
            conn,
            query_start=None,
            query_end=None,
            analysis_start_date="2024-04-04",
            analysis_end_date="2024-04-12",
            horizons=(2,),
            market_scopes=("prime",),
            include_liquidity_ranked=False,
        )
        third = daily_ranking_research_base.create_daily_ranking_research_panel(
            conn,
            query_start=None,
            query_end=None,
            analysis_start_date="2024-04-04",
            analysis_end_date="2024-04-12",
            horizons=(2,),
            market_scopes=("prime",),
            include_liquidity_ranked=True,
        )

        assert first.liquidity_ranked_table is not None
        assert second.liquidity_ranked_table is None
        assert third.liquidity_ranked_table is not None
        fixed_aliases = {
            DAILY_RANKING_RESEARCH_PANEL_TABLE,
            DAILY_RANKING_RESEARCH_RANKED_TABLE,
            DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE,
            DAILY_RANKING_RESEARCH_SCOPED_TABLE,
            DAILY_RANKING_RESEARCH_RELATIONS_TABLE,
            "ranking_color_panel",
            "ranking_color_panel_relations",
            "ranking_color_ranked",
            "ranking_color_liquidity_ranked",
            "ranking_color_scoped",
        }
        for spec in (first, second, third):
            names = {
                spec.panel_table,
                spec.ranked_table,
                spec.scoped_table,
                spec.relations_table,
                spec.legacy_panel_table,
                spec.legacy_ranked_table,
            }
            if spec.liquidity_ranked_table is not None:
                names.add(spec.liquidity_ranked_table)
            if spec.legacy_liquidity_ranked_table is not None:
                names.add(spec.legacy_liquidity_ranked_table)
            assert names.isdisjoint(fixed_aliases)
            for relation_name in names:
                assert relation_name.startswith("legacy_daily_ranking_g_")
                assert (
                    conn.execute(f"SELECT count(*) FROM {relation_name}").fetchone()[0]
                    >= 0
                )
        assert first.panel_table != second.panel_table != third.panel_table
    finally:
        conn.close()


def test_deprecated_bridge_alias_publish_is_atomic_and_rolls_back_orphans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        daily_ranking_research_base.create_daily_ranking_research_panel(
            conn,
            query_start=None,
            query_end=None,
            analysis_start_date="2024-04-04",
            analysis_end_date="2024-04-12",
            horizons=(2,),
            market_scopes=("prime",),
        )
        before_objects = set(
            conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name LIKE 'legacy_daily_ranking_g_%'"
            )
            .fetchnumpy()["table_name"]
            .tolist()
        )
        before_alias_sql = conn.execute(
            "SELECT view_name, sql FROM duckdb_views() "
            "WHERE view_name LIKE 'ranking_color_%' "
            "OR view_name LIKE 'daily_ranking_research_%' ORDER BY view_name"
        ).fetchall()
        original = daily_ranking_research_base._create_legacy_view
        calls = 0

        def fail_during_publish(
            connection: duckdb.DuckDBPyConnection,
            name: str,
            relation: RelationRef,
        ) -> None:
            nonlocal calls
            calls += 1
            original(connection, name, relation)
            if calls == 3:
                raise RuntimeError("injected alias publish failure")

        monkeypatch.setattr(
            daily_ranking_research_base,
            "_create_legacy_view",
            fail_during_publish,
        )
        with pytest.raises(RuntimeError, match="injected alias publish failure"):
            daily_ranking_research_base.create_daily_ranking_research_panel(
                conn,
                query_start=None,
                query_end=None,
                analysis_start_date="2024-04-04",
                analysis_end_date="2024-04-12",
                horizons=(2,),
                market_scopes=("prime",),
                include_liquidity_ranked=False,
            )

        after_objects = set(
            conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name LIKE 'legacy_daily_ranking_g_%'"
            )
            .fetchnumpy()["table_name"]
            .tolist()
        )
        after_alias_sql = conn.execute(
            "SELECT view_name, sql FROM duckdb_views() "
            "WHERE view_name LIKE 'ranking_color_%' "
            "OR view_name LIKE 'daily_ranking_research_%' ORDER BY view_name"
        ).fetchall()
        assert after_objects == before_objects
        assert after_alias_sql == before_alias_sql
    finally:
        conn.close()


def test_unknown_scope_is_filtered_by_exact_date_market_membership(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(
            "UPDATE stock_master_daily "
            "SET market_code = '9999', market_name = 'Unclassified' "
            "WHERE code = '3333'"
        )
        request = DailyRankingPanelRequest(
            namespace="unknown_scope",
            analysis_start_date=date(2024, 4, 4),
            analysis_end_date=date(2024, 4, 12),
            horizons=(2,),
            market_scopes=("unknown",),
            include_liquidity=False,
        )

        relations = build_daily_ranking_research_base(conn, request)

        assert conn.execute(
            f"SELECT DISTINCT code, market_scope FROM {relations.signal_panel.name}"
        ).fetchall() == [("3333", "unknown")]
    finally:
        conn.close()


def test_equal_normalized_market_aliases_are_accepted_but_conflicts_fail(
    tmp_path: Path,
) -> None:
    equal_conn = _build_market_v4_research_fixture(tmp_path / "equal.duckdb")
    try:
        equal_conn.execute(
            "INSERT INTO stock_master_daily "
            "SELECT date, '11110', company_name, market_code, market_name, "
            "scale_category FROM stock_master_daily WHERE code = '1111'"
        )
        relations = build_daily_ranking_research_base(
            equal_conn, _request("equal_alias")
        )
        assert relations.signal_panel.row_count > 0
        assert (
            equal_conn.execute(
                f"SELECT count(*) FROM {relations.signal_panel.name} WHERE code = '1111'"
            ).fetchone()[0]
            > 0
        )
    finally:
        equal_conn.close()

    conflict_conn = _build_market_v4_research_fixture(tmp_path / "conflict.duckdb")
    try:
        conflict_conn.execute(
            "INSERT INTO stock_master_daily "
            "SELECT date, '11110', 'Conflicting Alpha', '0112', 'Standard', "
            "'DIFFERENT' FROM stock_master_daily WHERE code = '1111'"
        )
        with pytest.raises(RuntimeError, match="market membership alias conflict"):
            build_daily_ranking_research_base(
                conflict_conn,
                _request("conflicting_alias"),
            )
    finally:
        conflict_conn.close()


def test_incomplete_outcome_diagnostics_inspect_every_requested_horizon(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    original = daily_ranking_research_base.build_daily_ranking_event_time_prices

    def build_with_partial_horizon(
        connection: duckdb.DuckDBPyConnection,
        request: object,
    ) -> object:
        result = original(connection, request)
        selected = connection.execute(
            f"SELECT code, date FROM {result.forward_outcomes} "
            "WHERE forward_outcome_completion_date_1d IS NOT NULL "
            "AND forward_outcome_completion_date_20d IS NOT NULL "
            "ORDER BY date, code LIMIT 1"
        ).fetchone()
        assert selected is not None
        connection.execute(
            f"UPDATE {result.forward_outcomes} SET "
            "forward_outcome_completion_date_20d = NULL, "
            "forward_close_return_20d_pct = NULL, "
            "forward_close_excess_return_20d_pct = NULL, "
            "completion_basis_id_20d = NULL WHERE code = ? AND date = ?",
            selected,
        )
        return result

    monkeypatch.setattr(
        daily_ranking_research_base,
        "build_daily_ranking_event_time_prices",
        build_with_partial_horizon,
    )
    try:
        request = DailyRankingPanelRequest(
            namespace="partial_horizon",
            analysis_start_date=date(2024, 3, 1),
            analysis_end_date=date(2024, 3, 1),
            horizons=(1, 20),
            market_scopes=("prime",),
            include_liquidity=False,
        )
        relations = build_daily_ranking_research_base(conn, request)

        assert relations.diagnostics.incomplete_outcome_rows == 1
    finally:
        conn.close()


def test_published_relation_builders_do_not_use_wildcard_projection() -> None:
    source = "\n".join(
        inspect.getsource(function)
        for function in (
            daily_ranking_research_base._materialize_signal_panel,
            daily_ranking_research_base._liquidity_sql,
            daily_ranking_research_base._materialize_ranked_signals,
            daily_ranking_research_base.materialize_daily_ranking_signal_cohort,
            daily_ranking_research_base.attach_daily_ranking_outcomes,
        )
    )

    assert "SELECT *" not in source.upper()
    assert re.search(r"\b[a-z_][a-z0-9_]*\.\*", source, re.IGNORECASE) is None


def test_upstream_signal_schema_drift_fails_before_panel_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    original = daily_ranking_research_base.build_daily_ranking_event_time_prices

    def build_with_extra_column(
        connection: duckdb.DuckDBPyConnection,
        request: object,
    ) -> object:
        result = original(connection, request)
        connection.execute(
            f"ALTER TABLE {result.signal_features} ADD COLUMN upstream_extra DOUBLE"
        )
        return result

    monkeypatch.setattr(
        daily_ranking_research_base,
        "build_daily_ranking_event_time_prices",
        build_with_extra_column,
    )
    try:
        with pytest.raises(RuntimeError, match="signal price schema mismatch"):
            build_daily_ranking_research_base(conn, _request("schema_drift"))
        assert (
            conn.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_name LIKE 'schema_drift_g_%'"
            ).fetchone()[0]
            == 0
        )
    finally:
        conn.close()


@pytest.mark.parametrize("scale", ("inf", "nan"))
def test_liquidity_standardization_rejects_non_finite_scale(scale: str) -> None:
    sql = _standardized_liquidity_residual_sql(
        residual_sql="residual",
        residual_std_sql="residual_std",
    )
    value = duckdb.sql(
        f"SELECT {sql} FROM (SELECT 1.0 AS residual, "
        f"CAST('{scale}' AS DOUBLE) AS residual_std)"
    ).fetchone()[0]

    assert value is None


def test_future_source_append_does_not_change_prior_signal_relations(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        baseline = build_daily_ranking_research_base(conn, _request("stable_before"))
        baseline_cohort = materialize_daily_ranking_signal_cohort(
            conn,
            baseline,
            source=baseline.ranked_signals,
            name="stable_membership",
            columns=("code", "date", "market_scope"),
            predicate=SignalExpression(
                "per_percentile <= 0.5",
                referenced_columns=("per_percentile",),
            ),
        )
        baseline_rows = conn.execute(
            f"SELECT {', '.join(baseline.ranked_signals.columns)} "
            f"FROM {baseline.ranked_signals.name} ORDER BY date, code"
        ).fetchall()
        baseline_membership = conn.execute(
            f"SELECT code, date, market_scope FROM {baseline_cohort.name} "
            "ORDER BY date, code, market_scope"
        ).fetchall()
        baseline_hashes = (
            baseline.lineage.price.signal_basis_sha256,
            baseline.lineage.price.signal_segment_sha256,
            baseline.lineage.price.completion_basis_sha256,
            baseline.lineage.price.completion_segment_sha256,
            baseline.lineage.price.price_projection_sha256,
            baseline.lineage.valuation_basis_sha256,
        )
        future_date = date(2024, 5, 17)
        old_basis_id = "event-pit-v1:1111:2024-01-04"
        future_basis_id = f"event-pit-v1:1111:{future_date}"
        conn.execute(
            "UPDATE stock_adjustment_bases SET valid_to_exclusive = ? "
            "WHERE basis_id = ?",
            [future_date, old_basis_id],
        )
        conn.execute(
            "INSERT INTO stock_adjustment_bases VALUES "
            "('1111', ?, ?, NULL, ?, 'fixture-1111-future', ?, 'ready')",
            [future_basis_id, future_date, future_date, future_date],
        )
        conn.execute(
            "INSERT INTO stock_adjustment_basis_segments VALUES "
            "('1111', ?, ?, NULL, 1.0)",
            [future_basis_id, future_date],
        )
        conn.execute(
            "INSERT INTO stock_data_raw VALUES "
            "('1111', ?, 999.0, 1000.0, 998.0, 999.0, 999999, 1.0)",
            [future_date],
        )
        conn.execute(
            "INSERT INTO stock_master_daily VALUES "
            "(?, '1111', 'Future Alpha', '0111', 'Prime', NULL)",
            [future_date],
        )
        conn.execute(
            "INSERT INTO daily_valuation VALUES "
            "('1111', ?, ?, 999.0, 999.0, 999.0, 999.0, 999.0, "
            "999999999.0, 999999999.0, ?)",
            [future_date, future_date, future_basis_id],
        )
        conn.execute(
            "INSERT INTO topix_data VALUES (?, 999.0, 999.0, 999.0, 999.0)",
            [future_date],
        )
        conn.execute(
            "INSERT INTO indices_data VALUES "
            "('N225_UNDERPX', ?, 999.0, 999.0, 999.0, 999.0, 0)",
            [future_date],
        )
        appended = build_daily_ranking_research_base(conn, _request("stable_after"))
        appended_cohort = materialize_daily_ranking_signal_cohort(
            conn,
            appended,
            source=appended.ranked_signals,
            name="stable_membership",
            columns=("code", "date", "market_scope"),
            predicate=SignalExpression(
                "per_percentile <= 0.5",
                referenced_columns=("per_percentile",),
            ),
        )
        appended_rows = conn.execute(
            f"SELECT {', '.join(appended.ranked_signals.columns)} "
            f"FROM {appended.ranked_signals.name} ORDER BY date, code"
        ).fetchall()
        appended_membership = conn.execute(
            f"SELECT code, date, market_scope FROM {appended_cohort.name} "
            "ORDER BY date, code, market_scope"
        ).fetchall()
        appended_hashes = (
            appended.lineage.price.signal_basis_sha256,
            appended.lineage.price.signal_segment_sha256,
            appended.lineage.price.completion_basis_sha256,
            appended.lineage.price.completion_segment_sha256,
            appended.lineage.price.price_projection_sha256,
            appended.lineage.valuation_basis_sha256,
        )

        assert appended_rows == baseline_rows
        assert appended_membership == baseline_membership
        assert appended_hashes == baseline_hashes
        assert (
            conn.execute(
                "SELECT count(*) FROM stock_adjustment_bases WHERE basis_id IN (?, ?)",
                [old_basis_id, future_basis_id],
            ).fetchone()[0]
            == 2
        )
    finally:
        conn.close()


def test_outcomes_attach_only_after_signal_membership_is_materialized(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        relations = build_daily_ranking_research_base(conn, _request("selection"))
        cohort = materialize_daily_ranking_signal_cohort(
            conn,
            relations,
            source=relations.ranked_signals,
            name="ranking_color_cohort",
            columns=("code", "date", "market_scope", "per_percentile"),
            predicate=SignalExpression(
                "market_scope = 'prime'",
                referenced_columns=("market_scope",),
            ),
            derived_columns=(
                SignalDerivedColumn(
                    name="valuation_bucket",
                    expression=SignalExpression(
                        "CASE WHEN per_percentile <= 0.2 "
                        "THEN 'cheapest' ELSE 'other' END",
                        referenced_columns=("per_percentile",),
                    ),
                    sql_type="VARCHAR",
                ),
            ),
        )
        evaluated = attach_daily_ranking_outcomes(
            conn,
            cohort,
            relations,
            name="ranking_color_evaluated",
        )

        assert cohort.row_count == evaluated.row_count
        assert not any(column.startswith("forward_") for column in cohort.columns)
        assert "forward_close_excess_return_2d_pct" in evaluated.columns
        assert (
            conn.execute(
                f"SELECT code, date, valuation_bucket FROM {cohort.name} ORDER BY date, code"
            ).fetchall()
            == conn.execute(
                f"SELECT code, date, valuation_bucket FROM {evaluated.name} "
                "ORDER BY date, code"
            ).fetchall()
        )
        with pytest.raises(ValueError, match="signal relation returned by this build"):
            materialize_daily_ranking_signal_cohort(
                conn,
                relations,
                source=relations.forward_outcomes,
                name="unsafe_cohort",
                columns=("code", "date"),
            )
    finally:
        conn.close()


def test_exact_published_signal_features_can_freeze_before_outcome_attach(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        relations = build_daily_ranking_research_base(conn, _request("feature_cohort"))
        source = relations.ranked_signals
        composite_name = f"{relations.generation}_explicit_feature_composite"
        conn.execute(
            f"CREATE TEMP TABLE {composite_name} AS "
            f"SELECT code, date, market_scope, per_percentile, "
            f"CAST(1.0 - per_percentile AS DOUBLE) AS value_score "
            f"FROM {source.name}"
        )
        composite = publish_daily_ranking_signal_features(
            conn,
            source=source,
            relation_name=composite_name,
            expected_schema=(
                ("code", "VARCHAR"),
                ("date", "DATE"),
                ("market_scope", "VARCHAR"),
                ("per_percentile", "DOUBLE"),
                ("value_score", "DOUBLE"),
            ),
        )

        cohort = materialize_daily_ranking_signal_cohort(
            conn,
            relations,
            source=composite,
            name="published_feature_membership",
            predicate=SignalExpression(
                "value_score >= 0.5",
                referenced_columns=("value_score",),
            ),
        )
        evaluated = attach_daily_ranking_outcomes(
            conn,
            cohort,
            relations,
            name="published_feature_evaluated",
        )

        assert composite.kind == "signal_features"
        assert cohort.row_count == evaluated.row_count
        assert cohort.key_columns == ("code", "date", "market_scope")
        assert not any(column.startswith("forward_") for column in cohort.columns)
        assert conn.execute(
            f"SELECT code, date, market_scope, value_score FROM {cohort.name} "
            "ORDER BY date, code, market_scope"
        ).fetchall() == conn.execute(
            f"SELECT code, date, market_scope, value_score FROM {evaluated.name} "
            "ORDER BY date, code, market_scope"
        ).fetchall()
    finally:
        conn.close()


def test_feature_cohort_rejects_copied_cross_generation_and_evaluated_refs(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    other_conn = _build_market_v4_research_fixture(tmp_path / "other.duckdb")
    try:
        relations = build_daily_ranking_research_base(conn, _request("feature_guard"))
        source = relations.ranked_signals
        feature_name = f"{relations.generation}_guard_feature"
        conn.execute(
            f"CREATE TEMP TABLE {feature_name} AS "
            f"SELECT code, date, market_scope, per_percentile FROM {source.name}"
        )
        feature = publish_daily_ranking_signal_features(
            conn,
            source=source,
            relation_name=feature_name,
            expected_schema=(
                ("code", "VARCHAR"),
                ("date", "DATE"),
                ("market_scope", "VARCHAR"),
                ("per_percentile", "DOUBLE"),
            ),
        )
        other = build_daily_ranking_research_base(conn, _request("feature_guard_other"))
        real_cohort = materialize_daily_ranking_signal_cohort(
            conn,
            relations,
            source=relations.ranked_signals,
            name="guard_real",
        )
        evaluated = attach_daily_ranking_outcomes(
            conn,
            real_cohort,
            relations,
            name="guard_evaluated",
        )
        manually_forged = RelationRef(
            name=feature.name,
            columns=feature.columns,
            key_columns=feature.key_columns,
            row_count=feature.row_count,
            column_types=feature.column_types,
            generation=feature.generation,
            kind=feature.kind,
            _capability=feature._capability,
        )
        other_relations = build_daily_ranking_research_base(
            other_conn,
            _request("feature_guard_connection"),
        )
        other_source = other_relations.ranked_signals
        other_name = f"{other_relations.generation}_connection_feature"
        other_conn.execute(
            f"CREATE TEMP TABLE {other_name} AS "
            f"SELECT code, date, market_scope, per_percentile "
            f"FROM {other_source.name}"
        )
        other_feature = publish_daily_ranking_signal_features(
            other_conn,
            source=other_source,
            relation_name=other_name,
            expected_schema=(
                ("code", "VARCHAR"),
                ("date", "DATE"),
                ("market_scope", "VARCHAR"),
                ("per_percentile", "DOUBLE"),
            ),
        )

        for candidate in (
            copy.copy(feature),
            replace(feature),
            manually_forged,
            other.ranked_signals,
            other_feature,
        ):
            with pytest.raises(
                ValueError,
                match="issued|registered|generation|provenance|source",
            ):
                materialize_daily_ranking_signal_cohort(
                    conn,
                    relations,
                    source=candidate,
                    name="rejected_feature",
                )
        with pytest.raises(ValueError, match="signal relation returned by this build"):
            materialize_daily_ranking_signal_cohort(
                conn,
                relations,
                source=evaluated,
                name="rejected_evaluated",
            )
    finally:
        conn.close()
        other_conn.close()


def test_cohort_provenance_rejects_outcome_derived_or_forged_relation_refs(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        relations = build_daily_ranking_research_base(conn, _request("provenance"))
        forged_name = (
            relations.ranked_signals.name.removesuffix("_ranked_signals")
            + "_ranked_signals_forged"
        )
        conn.execute(
            f"CREATE TEMP VIEW {forged_name} AS "
            f"SELECT signal.code, signal.date, signal.market_scope, "
            f"outcome.forward_close_return_2d_pct "
            f"FROM {relations.ranked_signals.name} signal "
            f"LEFT JOIN {relations.forward_outcomes.name} outcome USING (code, date)"
        )
        forged = replace(
            relations.ranked_signals,
            name=forged_name,
            columns=(
                "code",
                "date",
                "market_scope",
                "forward_close_return_2d_pct",
            ),
            column_types=("VARCHAR", "DATE", "VARCHAR", "DOUBLE"),
        )

        with pytest.raises(ValueError, match="signal relation returned by this build"):
            materialize_daily_ranking_signal_cohort(
                conn,
                relations,
                source=forged,
                name="forged",
                columns=("code", "date", "market_scope"),
            )

        real = materialize_daily_ranking_signal_cohort(
            conn,
            relations,
            source=relations.ranked_signals,
            name="real",
            columns=("code", "date", "market_scope"),
        )
        fake_cohort_name = real.name + "_forged"
        conn.execute(
            f"CREATE TEMP VIEW {fake_cohort_name} AS SELECT * FROM {real.name}"
        )
        fake_cohort = replace(real, name=fake_cohort_name)
        with pytest.raises(ValueError, match="registered frozen cohort"):
            attach_daily_ranking_outcomes(
                conn,
                fake_cohort,
                relations,
                name="forged_evaluation",
            )
    finally:
        conn.close()


def test_missing_highest_selected_outcome_does_not_backfill_membership(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(
            "DELETE FROM stock_data_raw "
            "WHERE code = '2222' AND date > DATE '2024-04-04'"
        )
        relations = build_daily_ranking_research_base(conn, _request("no_backfill"))
        cohort = materialize_daily_ranking_signal_cohort(
            conn,
            relations,
            source=relations.ranked_signals,
            name="highest_per",
            columns=("code", "date", "market_scope", "per_percentile"),
            order_by=(("per_percentile", "desc"), ("date", "asc"), ("code", "asc")),
            limit=1,
        )
        selected = conn.execute(f"SELECT code, date FROM {cohort.name}").fetchone()
        assert selected == ("2222", date(2024, 4, 4))

        evaluated = attach_daily_ranking_outcomes(
            conn,
            cohort,
            relations,
            name="highest_per",
        )

        assert evaluated.row_count == cohort.row_count == 1
        assert (
            conn.execute(f"SELECT code, date FROM {evaluated.name}").fetchone()
            == selected
        )
        assert (
            conn.execute(
                f"SELECT forward_close_return_2d_pct FROM {evaluated.name}"
            ).fetchone()[0]
            is None
        )
    finally:
        conn.close()


def test_post_issuance_outcome_mutation_rejects_stale_ref(tmp_path: Path) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        relations = build_daily_ranking_research_base(conn, _request("stale_outcome"))
        cohort = materialize_daily_ranking_signal_cohort(
            conn,
            relations,
            source=relations.ranked_signals,
            name="stale_outcome_cohort",
            columns=("code", "date", "market_scope"),
            limit=1,
        )
        conn.execute(
            f"UPDATE {relations.forward_outcomes.name} "
            "SET forward_close_return_2d_pct = NULL"
        )

        with pytest.raises(RuntimeError, match="content fingerprint changed"):
            attach_daily_ranking_outcomes(
                conn,
                cohort,
                relations,
                name="stale_outcome_evaluated",
            )
    finally:
        conn.close()
