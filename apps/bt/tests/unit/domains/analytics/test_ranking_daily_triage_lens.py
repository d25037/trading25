from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.domains.analytics.ranking_daily_triage_lens import (
    run_ranking_daily_triage_lens_from_panel,
    write_ranking_daily_triage_lens_bundle,
)


def test_triage_lens_scores_few_inspect_candidates_and_reports_attention_metrics() -> None:
    panel = pd.DataFrame(
        [
            _row("2024-01-02", "1001", "crowded_rerating", 0.03, 0.04, True, True, False, 0.88, 0.86, 0.04, 0.03, 14.0),
            _row("2024-01-02", "1002", "neutral_rerating", 0.04, 0.05, True, True, False, 0.84, 0.20, 0.06, 0.04, 8.0),
            _row("2024-01-02", "1003", "neutral_rerating", 0.08, 0.11, True, False, False, 0.30, 0.18, 0.08, 0.06, 3.0),
            _row("2024-01-02", "1004", "crowded_rerating", 0.85, 0.90, False, False, True, 0.10, 0.92, 0.86, 0.80, 22.0),
            _row("2024-01-02", "1005", "distribution_stress", 0.91, 0.82, False, False, True, 0.12, 0.10, 0.90, 0.88, -14.0),
            _row("2024-01-03", "2001", "crowded_rerating", 0.05, 0.06, True, False, False, 0.89, 0.83, 0.05, 0.04, -12.0),
            _row("2024-01-03", "2002", "neutral_rerating", 0.04, 0.07, True, True, False, 0.82, 0.18, 0.04, 0.04, 11.0),
            _row("2024-01-03", "2003", "neutral_rerating", 0.07, 0.09, True, False, False, 0.25, 0.16, 0.07, 0.07, 1.0),
            _row("2024-01-03", "2004", "crowded_rerating", 0.94, 0.89, False, False, True, 0.88, 0.91, 0.94, 0.92, 15.0),
        ]
    )

    result = run_ranking_daily_triage_lens_from_panel(
        panel,
        horizons=(20,),
        top_ks=(2,),
        severe_loss_threshold_pct=-10.0,
        strong_gain_threshold_pct=10.0,
    )

    candidate_map = dict(
        zip(
            result.daily_triage_candidates_df["code"],
            result.daily_triage_candidates_df["triage_bucket"],
            strict=False,
        )
    )
    assert candidate_map["1001"] == "inspect"
    assert candidate_map["1002"] == "inspect"
    assert candidate_map["1003"] == "watch"
    assert candidate_map["1004"] == "kill"
    assert candidate_map["1005"] == "kill"

    metrics = result.attention_efficiency_df.iloc[0]
    assert metrics["top_k"] == 2
    assert metrics["horizon"] == 20
    assert metrics["candidate_count"] == 6
    assert metrics["candidate_outcome_count"] == 6
    assert metrics["candidate_outcome_coverage_pct"] == 100.0
    assert metrics["universe_count"] == 9
    assert metrics["universe_outcome_count"] == 9
    assert metrics["universe_outcome_coverage_pct"] == 100.0
    assert metrics["selected_count"] == 4
    assert metrics["attention_reduction_pct"] > 50.0
    assert metrics["precision_positive_pct"] == 75.0
    assert metrics["precision_strong_gain_pct"] == 50.0
    assert metrics["severe_loss_rate_pct"] == 25.0
    assert metrics["right_tail_capture_pct"] == 50.0
    assert metrics["future_winner_capture_pct"] == 50.0

    leakage = result.kill_leakage_df.iloc[0]
    assert leakage["killed_count"] == 3
    assert leakage["killed_strong_gain_count"] == 2
    assert leakage["kill_leakage_pct"] == 50.0


def test_triage_lens_writes_bundle(tmp_path: Path) -> None:
    result = run_ranking_daily_triage_lens_from_panel(
        pd.DataFrame(
            [
                _row("2024-01-02", "1001", "crowded_rerating", 0.03, 0.04, True, True, False, 0.88, 0.86, 0.04, 0.03, 14.0),
                _row("2024-01-02", "1002", "neutral_rerating", 0.04, 0.05, True, True, False, 0.84, 0.20, 0.06, 0.04, 8.0),
            ]
        ),
        horizons=(20,),
        top_ks=(1,),
    )

    bundle = write_ranking_daily_triage_lens_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )

    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()
    assert bundle.summary_path.exists()


def test_triage_lens_treats_psr_percentiles_as_optional() -> None:
    panel = pd.DataFrame(
        [
            _row("2024-01-02", "1001", "crowded_rerating", 0.03, 0.04, True, True, False, 0.88, 0.86, 0.04, 0.03, 14.0),
            _row("2024-01-02", "1002", "neutral_rerating", 0.04, 0.05, True, True, False, 0.84, 0.20, 0.06, 0.04, 8.0),
        ]
    ).drop(columns=["psr_percentile", "forward_psr_percentile"])

    result = run_ranking_daily_triage_lens_from_panel(
        panel,
        horizons=(20,),
        top_ks=(1,),
    )

    assert result.observation_count == 2
    assert {"psr_percentile", "forward_psr_percentile"}.issubset(
        result.daily_triage_candidates_df.columns
    )
    assert result.daily_triage_candidates_df["psr_percentile"].isna().all()


def test_attention_selection_missing_outcome_does_not_backfill_lower_score() -> None:
    panel = pd.DataFrame(
        [
            _row(
                "2024-01-02",
                "1001",
                "crowded_rerating",
                0.03,
                0.04,
                True,
                True,
                False,
                0.88,
                0.86,
                0.04,
                0.03,
                float("nan"),
            ),
            _row(
                "2024-01-02",
                "1002",
                "neutral_rerating",
                0.04,
                0.05,
                True,
                True,
                False,
                0.88,
                0.86,
                0.06,
                0.04,
                2.0,
            ),
            _row(
                "2024-01-02",
                "1003",
                "neutral_rerating",
                0.20,
                0.20,
                True,
                False,
                False,
                0.10,
                0.10,
                0.20,
                0.20,
                100.0,
            ),
        ]
    )

    result = run_ranking_daily_triage_lens_from_panel(
        panel,
        horizons=(20,),
        top_ks=(2,),
    )

    row = result.attention_efficiency_df.iloc[0]
    assert row["candidate_count"] == 3
    assert row["candidate_outcome_count"] == 2
    assert row["selected_count"] == 2
    assert row["selected_outcome_count"] == 1
    assert row["outcome_status"] == "incomplete"
    assert row[
        [
            "mean_forward_excess_return_pct",
            "median_forward_excess_return_pct",
            "precision_positive_pct",
            "precision_strong_gain_pct",
            "severe_loss_rate_pct",
            "right_tail_capture_pct",
            "future_winner_capture_pct",
        ]
    ].isna().all()


def test_all_scope_uses_only_canonical_all_rows() -> None:
    panel = pd.DataFrame(
        [
            _row(
                "2024-01-02",
                "1001",
                "crowded_rerating",
                0.03,
                0.04,
                True,
                True,
                False,
                0.88,
                0.86,
                0.04,
                0.03,
                10.0,
                market_scope="all",
            ),
            _row(
                "2024-01-02",
                "1002",
                "neutral_rerating",
                0.20,
                0.20,
                True,
                False,
                False,
                0.10,
                0.10,
                0.20,
                0.20,
                2.0,
                market_scope="all",
            ),
            _row(
                "2024-01-02",
                "1001",
                "crowded_rerating",
                0.03,
                0.04,
                True,
                True,
                False,
                0.88,
                0.86,
                0.04,
                0.03,
                -50.0,
                market_scope="prime",
            ),
        ]
    )

    result = run_ranking_daily_triage_lens_from_panel(
        panel,
        horizons=(20,),
        market_scopes=("all",),
        top_ks=(1,),
    )

    assert result.market_scopes == ("all",)
    assert result.daily_triage_candidates_df["market_scope"].tolist() == ["all", "all"]
    assert set(result.daily_triage_candidates_df["code"]) == {"1001", "1002"}
    metrics = result.attention_efficiency_df.iloc[0]
    assert metrics["candidate_count"] == 2
    assert metrics["selected_count"] == 1
    assert metrics["mean_forward_excess_return_pct"] == 10.0


def test_mixed_scopes_freeze_top_k_separately_with_scope_aware_keys() -> None:
    panel = pd.DataFrame(
        [
            _row(
                "2024-01-02",
                "1001",
                "crowded_rerating",
                0.03,
                0.04,
                True,
                True,
                False,
                0.88,
                0.86,
                0.04,
                0.03,
                10.0,
                market_scope="prime",
            ),
            _row(
                "2024-01-02",
                "1002",
                "neutral_rerating",
                0.20,
                0.20,
                True,
                False,
                False,
                0.10,
                0.10,
                0.20,
                0.20,
                1.0,
                market_scope="prime",
            ),
            _row(
                "2024-01-02",
                "1001",
                "neutral_rerating",
                0.20,
                0.20,
                True,
                False,
                False,
                0.10,
                0.10,
                0.20,
                0.20,
                -5.0,
                market_scope="standard",
            ),
            _row(
                "2024-01-02",
                "1003",
                "crowded_rerating",
                0.03,
                0.04,
                True,
                True,
                False,
                0.88,
                0.86,
                0.04,
                0.03,
                20.0,
                market_scope="standard",
            ),
        ]
    )

    result = run_ranking_daily_triage_lens_from_panel(
        panel,
        horizons=(20,),
        market_scopes=("prime", "standard"),
        top_ks=(1,),
    )

    assert set(result.daily_triage_candidates_df["market_scope"]) == {
        "prime",
        "standard",
    }
    metrics = result.attention_efficiency_df.iloc[0]
    assert metrics["candidate_count"] == 4
    assert metrics["selected_count"] == 2
    assert metrics["evaluated_date_count"] == 1
    assert metrics["mean_forward_excess_return_pct"] == 15.0


def test_kill_row_missing_outcome_does_not_reduce_candidate_coverage() -> None:
    panel = pd.DataFrame(
        [
            _row(
                "2024-01-02",
                "1001",
                "crowded_rerating",
                0.03,
                0.04,
                True,
                True,
                False,
                0.88,
                0.86,
                0.04,
                0.03,
                10.0,
            ),
            _row(
                "2024-01-02",
                "1002",
                "distribution_stress",
                0.90,
                0.90,
                False,
                False,
                True,
                0.10,
                0.10,
                0.90,
                0.90,
                float("nan"),
            ),
        ]
    )

    result = run_ranking_daily_triage_lens_from_panel(
        panel,
        horizons=(20,),
        top_ks=(1,),
    )

    metrics = result.attention_efficiency_df.iloc[0]
    assert metrics["candidate_count"] == 1
    assert metrics["candidate_outcome_count"] == 1
    assert metrics["candidate_outcome_coverage_pct"] == 100.0
    assert metrics["universe_count"] == 2
    assert metrics["universe_outcome_count"] == 1
    assert metrics["universe_outcome_coverage_pct"] == 50.0
    assert metrics["outcome_status"] == "complete"
    assert metrics["mean_forward_excess_return_pct"] == 10.0


def test_attention_aggregate_fails_closed_when_any_date_is_incomplete() -> None:
    panel = pd.DataFrame(
        [
            _row(
                "2024-01-02",
                "1001",
                "crowded_rerating",
                0.03,
                0.04,
                True,
                True,
                False,
                0.88,
                0.86,
                0.04,
                0.03,
                10.0,
            ),
            _row(
                "2024-01-03",
                "2001",
                "crowded_rerating",
                0.03,
                0.04,
                True,
                True,
                False,
                0.88,
                0.86,
                0.04,
                0.03,
                float("nan"),
            ),
        ]
    )

    result = run_ranking_daily_triage_lens_from_panel(
        panel,
        horizons=(20,),
        top_ks=(1,),
    )

    metrics = result.attention_efficiency_df.iloc[0]
    assert metrics["evaluated_date_count"] == 2
    assert metrics["complete_date_count"] == 1
    assert metrics["incomplete_date_count"] == 1
    assert metrics["effect_date_count"] == 0
    assert metrics["outcome_status"] == "incomplete"
    assert metrics[
        [
            "mean_forward_excess_return_pct",
            "median_forward_excess_return_pct",
            "precision_positive_pct",
            "precision_strong_gain_pct",
            "severe_loss_rate_pct",
            "right_tail_capture_pct",
            "future_winner_capture_pct",
        ]
    ].isna().all()


def _row(
    date: str,
    code: str,
    liquidity_regime: str,
    pbr_percentile: float,
    forward_per_percentile: float,
    strong_value_confirmation: bool,
    atr20_acceleration_ex_overheat_flag: bool,
    overvalued_warning: bool,
    long_hybrid_leadership_score: float,
    sector_strength_score: float,
    psr_percentile: float,
    forward_psr_percentile: float,
    forward_return_20d: float,
    *,
    market_scope: str = "prime",
) -> dict[str, object]:
    return {
        "market_scope": market_scope,
        "date": date,
        "code": code,
        "company_name": f"Company {code}",
        "sector_33_code": "0050",
        "sector_33_name": "Test Sector",
        "liquidity_regime": liquidity_regime,
        "valuation_signal": "Deep Value" if strong_value_confirmation else "Overvalued",
        "strong_value_confirmation": strong_value_confirmation,
        "medium_value_confirmation": strong_value_confirmation,
        "overvalued_warning": overvalued_warning,
        "very_overvalued_warning": False,
        "no_value_confirmation": not strong_value_confirmation,
        "pbr_percentile": pbr_percentile,
        "forward_per_percentile": forward_per_percentile,
        "psr_percentile": psr_percentile,
        "forward_psr_percentile": forward_psr_percentile,
        "recent_return_20d_pct": 12.0,
        "recent_return_60d_pct": 18.0,
        "sector_strength_bucket": (
            "sector_strong" if sector_strength_score >= 0.8 else "sector_neutral"
        ),
        "sector_strength_score": sector_strength_score,
        "long_hybrid_leadership_score": long_hybrid_leadership_score,
        "atr20_acceleration_ex_overheat_flag": atr20_acceleration_ex_overheat_flag,
        "atr20_to_atr60_overheat_flag": False,
        "forward_close_excess_return_20d_pct": forward_return_20d,
    }
