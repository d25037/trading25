from __future__ import annotations

import duckdb
import pytest

from src.domains.analytics.daily_ranking_core import (
    DailyRankingLiquidityInputs,
    DailyRankingTechnicalInputs,
    DailyRankingValuationMetrics,
    classify_liquidity_state,
    classify_technical_state,
    classify_valuation_state,
    liquidity_state_sql,
    normalize_percentile_population,
    percent_rank_sql,
    technical_state_sql,
    valuation_state_sql,
)


def test_liquidity_policy_uses_production_boundaries() -> None:
    crowded = classify_liquidity_state(
        DailyRankingLiquidityInputs(
            residual_z=1.0,
            recent_return_20d_pct=0.01,
            recent_return_60d_pct=0.01,
        )
    )
    zero_boundary = classify_liquidity_state(
        DailyRankingLiquidityInputs(
            residual_z=1.0,
            recent_return_20d_pct=0.0,
            recent_return_60d_pct=1.0,
        )
    )

    assert crowded.regime == "crowded_rerating"
    assert zero_boundary.regime == "distribution_stress"


def test_technical_state_has_canonical_internal_name() -> None:
    state = classify_technical_state(
        DailyRankingTechnicalInputs(
            atr20_change_20d_pct=25.0,
            atr20_to_atr60=1.24,
            recent_return_20d_pct=29.99,
            recent_return_60d_percentile=0.9,
            recent_return_20d_percentile=0.9,
        )
    )

    assert state.atr20_acceleration_ex_overheat
    assert state.api_flags == ("atr20_acceleration", "momentum_20_60_top20")


def test_valuation_state_does_not_treat_unsupported_population_as_no_earnings() -> None:
    state = classify_valuation_state(
        DailyRankingValuationMetrics(
            percentile_population="non_prime_unsupported",
            per_percentile=None,
            forward_per_percentile=None,
            forward_p_op_percentile=None,
            pbr_percentile=None,
            per=None,
            forward_per=None,
        )
    )

    assert state.signal == "unsupported"
    assert not state.no_positive_earnings_valuation


def test_percentile_population_is_explicit_and_percent_rank_preserves_ties() -> None:
    assert normalize_percentile_population("prime") == "prime"
    with pytest.raises(ValueError, match="Unsupported Daily Ranking percentile population"):
        normalize_percentile_population(None)

    assert percent_rank_sql(
        value_sql="per",
        partition_by_sql="date, percentile_population",
    ) == "percent_rank() OVER (PARTITION BY date, percentile_population ORDER BY per)"

    conn = duckdb.connect(":memory:")
    try:
        ranks = conn.execute(
            f"""
            WITH ranks(date, percentile_population, code, per) AS (
                VALUES
                    (DATE '2024-01-04', 'prime', '1111', 10.0),
                    (DATE '2024-01-04', 'prime', '2222', 10.0),
                    (DATE '2024-01-04', 'prime', '3333', 20.0)
            )
            SELECT code, {percent_rank_sql(
                value_sql='per',
                partition_by_sql='date, percentile_population',
            )} AS percentile
            FROM ranks
            ORDER BY code
            """
        ).fetchall()
    finally:
        conn.close()

    assert ranks == [("1111", 0.0), ("2222", 0.0), ("3333", 1.0)]


def test_sql_policy_expressions_match_python_boundary_states() -> None:
    conn = duckdb.connect(":memory:")
    try:
        rows = conn.execute(
            f"""
            WITH cases(
                case_id, percentile_population, per_percentile, forward_per_percentile,
                forward_p_op_percentile, pbr_percentile, per, forward_per,
                residual_z, recent_return_20d_pct, recent_return_60d_pct,
                atr20_change_20d_pct, atr20_to_atr60,
                recent_return_20d_percentile, recent_return_60d_percentile
            ) AS (
                VALUES
                    ('value', 'prime', 0.2, 0.2, NULL, 0.3, 10.0, 9.0,
                     1.0, 0.01, 0.01, 25.0, 1.24, 0.9, 0.9),
                    ('distribution', 'prime', NULL, NULL, NULL, NULL, NULL, NULL,
                     1.0, 0.0, 1.0, NULL, NULL, NULL, NULL),
                    ('missing', 'prime', NULL, NULL, NULL, NULL, NULL, NULL,
                     NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                    ('unsupported', 'non_prime_unsupported', NULL, NULL, NULL, NULL,
                     NULL, NULL, -1.0, 1.0, 1.0, 24.99, 1.24, 0.8, 0.8)
            )
            SELECT
                *,
                {valuation_state_sql(
                    percentile_population_sql='percentile_population',
                    per_percentile_sql='per_percentile',
                    forward_per_percentile_sql='forward_per_percentile',
                    forward_p_op_percentile_sql='forward_p_op_percentile',
                    pbr_percentile_sql='pbr_percentile',
                    per_sql='per',
                    forward_per_sql='forward_per',
                )} AS valuation_signal,
                {liquidity_state_sql(
                    residual_z_sql='residual_z',
                    recent_return_20d_pct_sql='recent_return_20d_pct',
                    recent_return_60d_pct_sql='recent_return_60d_pct',
                )} AS liquidity_regime,
                {technical_state_sql(
                    atr20_change_20d_pct_sql='atr20_change_20d_pct',
                    atr20_to_atr60_sql='atr20_to_atr60',
                    recent_return_20d_pct_sql='recent_return_20d_pct',
                    recent_return_20d_percentile_sql='recent_return_20d_percentile',
                    recent_return_60d_percentile_sql='recent_return_60d_percentile',
                )} AS technical_flags
            FROM cases
            ORDER BY case_id
            """
        ).fetchall()
    finally:
        conn.close()

    for row in rows:
        (
            _, population, per_percentile, forward_per_percentile, forward_p_op_percentile,
            pbr_percentile, per, forward_per, residual_z, return_20d, return_60d,
            atr20_change, atr20_to_atr60, return_20d_percentile,
            return_60d_percentile, sql_valuation, sql_liquidity, sql_flags,
        ) = row
        valuation = classify_valuation_state(
            DailyRankingValuationMetrics(
                percentile_population=population,
                per_percentile=per_percentile,
                forward_per_percentile=forward_per_percentile,
                forward_p_op_percentile=forward_p_op_percentile,
                pbr_percentile=pbr_percentile,
                per=per,
                forward_per=forward_per,
            )
        )
        liquidity = classify_liquidity_state(
            DailyRankingLiquidityInputs(
                residual_z=residual_z,
                recent_return_20d_pct=return_20d,
                recent_return_60d_pct=return_60d,
            )
        )
        technical = classify_technical_state(
            DailyRankingTechnicalInputs(
                atr20_change_20d_pct=atr20_change,
                atr20_to_atr60=atr20_to_atr60,
                recent_return_20d_pct=return_20d,
                recent_return_20d_percentile=return_20d_percentile,
                recent_return_60d_percentile=return_60d_percentile,
            )
        )

        assert sql_valuation == valuation.signal
        assert sql_liquidity == liquidity.regime
        assert tuple(sql_flags.split(",") if sql_flags else ()) == technical.api_flags
