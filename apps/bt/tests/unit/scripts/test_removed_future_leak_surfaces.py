"""Regression guard for research surfaces deleted due to future leakage."""

from pathlib import Path


def test_removed_daily_ranking_compatibility_surface_has_no_active_code() -> None:
    repo_root = Path(__file__).resolve().parents[5]
    analytics_root = repo_root / "apps" / "bt" / "src" / "domains" / "analytics"
    forbidden_tokens = (
        "DAILY_RANKING_RESEARCH_RANKED_TABLE",
        "DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE",
        "create_daily_ranking_research_panel",
        "daily_ranking_query_start_date",
        "daily_ranking_query_end_date",
        "event_time_basis_only=",
        "price_feature_relation=",
        "price_outcome_relation=",
        "ranking_technical_fit_price_projection",
    )

    matches: list[str] = []
    for path in analytics_root.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        for token in forbidden_tokens:
            if token in path.name or token in text:
                matches.append(f"{path.name}: {token}")

    assert matches == []


def test_removed_future_leaking_research_has_no_active_surface() -> None:
    repo_root = Path(__file__).resolve().parents[5]
    forbidden_tokens = (
        "topix100_streak" + "_353",
        "topix100_streak_lightgbm" + "_feature_panel",
        "topix100_streak_lightgbm" + "_validation_support",
        "topix100_top1_open_to_open_5d" + "_duplicate_policy_analysis",
        "topix100_top1_open_to_open_5d" + "_fixed_committee_overlay",
        "topix_streak" + "_extreme_mode",
        "topix_streak" + "_multi_timeframe_mode",
        "topix_extreme_mode" + "_mean_reversion_comparison",
        "topix_downside_return_standard_deviation" + "_trend_breadth_overlay",
        "topix_downside_return_standard_deviation" + "_shock_confirmation_vote_overlay",
        "topix_downside_return_standard_deviation" + "_shock_confirmation_committee_overlay",
        "topix100_sma_ratio_rank" + "_future_close_lightgbm",
        "topix100_price_vs_sma_q10_bounce" + "_regime_conditioning",
        "topix100_sma50_raw_vs_atr" + "_q10_bounce",
        "topix100-streak" + "-3-53",
        "topix100-top1-open-to-open-5d" + "-duplicate-policy-analysis",
        "topix100-top1-open-to-open-5d" + "-fixed-committee-overlay",
        "topix-streak" + "-extreme-mode",
        "topix-streak" + "-multi-timeframe-mode",
        "topix-extreme-mode" + "-mean-reversion-comparison",
        "topix-downside-return-standard-deviation" + "-trend-breadth-overlay",
        "topix-downside-return-standard-deviation" + "-shock-confirmation-vote-overlay",
        "topix-downside-return-standard-deviation" + "-shock-confirmation-committee-overlay",
        "topix100-sma-ratio" + "-lightgbm",
        "topix100-price-vs-sma-q10-bounce" + "-regime-conditioning",
        "topix100-sma50-raw-vs-atr" + "-q10-bounce",
        "topix_streak" + "_state",
    )
    active_roots = (
        repo_root / "apps" / "bt" / "src",
        repo_root / "apps" / "bt" / "scripts" / "research",
        repo_root / "apps" / "bt" / "docs" / "experiments",
        repo_root / "apps" / "ts" / "packages" / "web" / "src",
    )
    scanned_suffixes = {".py", ".md", ".toml", ".ts", ".tsx"}

    matches: list[str] = []
    for root in active_roots:
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in scanned_suffixes:
                continue
            relative_path = path.relative_to(repo_root).as_posix()
            text = path.read_text(encoding="utf-8", errors="ignore")
            for token in forbidden_tokens:
                if token in relative_path or token in text:
                    matches.append(f"{relative_path}: {token}")

    assert matches == []
