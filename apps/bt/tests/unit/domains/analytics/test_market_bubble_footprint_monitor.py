from __future__ import annotations

import pandas as pd

from src.domains.analytics import market_bubble_footprint_monitor as monitor


def test_market_bubble_footprint_latest_uses_market_db_without_bundle(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "market.duckdb"
    db_path.touch()
    captured: dict[str, object] = {}

    def fake_as_of(
        db_path_arg: str,
        db_mtime_ns_arg: int,
        markets_arg: tuple[str, ...],
        date_arg: str | None,
    ) -> dict[str, object]:
        captured.update(
            {
                "db_path": db_path_arg,
                "db_mtime_ns": db_mtime_ns_arg,
                "markets": markets_arg,
                "date": date_arg,
            }
        )
        return {
            "date": "2026-06-01",
            "markets": list(markets_arg),
            "overallRegime": "normal",
            "overallScore": 0,
            "nearBlowoff": False,
            "researchExperimentId": "market-behavior/market-bubble-footprint",
            "reratingExperimentId": "market-behavior/rerating-bubble-regime-forward-response",
            "horizons": [],
        }

    monkeypatch.setattr(monitor, "_market_duckdb_path", lambda: str(db_path))
    monkeypatch.setattr(monitor, "_cached_market_bubble_footprint_as_of", fake_as_of)

    payload = monitor.get_latest_market_bubble_footprint(
        markets=("prime", "standard", "growth"),
        date="2026-06-01",
    )

    assert payload["date"] == "2026-06-01"
    assert captured == {
        "db_path": str(db_path),
        "db_mtime_ns": db_path.stat().st_mtime_ns,
        "markets": ("prime", "standard", "growth"),
        "date": "2026-06-01",
    }


def test_market_bubble_footprint_db_baseline_cache_round_trips(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(monitor, "get_cache_dir", lambda: tmp_path)
    baseline = pd.DataFrame([_footprint_row(date="2026-05-29", horizon=60)])

    monitor._write_market_bubble_footprint_baseline_cache(
        "market.duckdb",
        123,
        ("prime", "standard", "growth"),
        baseline,
    )

    cached = monitor._load_market_bubble_footprint_baseline_cache(
        "market.duckdb",
        123,
        ("prime", "standard", "growth"),
    )
    stale = monitor._load_market_bubble_footprint_baseline_cache(
        "market.duckdb",
        456,
        ("prime", "standard", "growth"),
    )

    assert cached is not None
    assert cached["snapshot_date"].tolist() == ["2026-05-29"]
    assert cached["horizon"].tolist() == [60]
    assert stale is None


def test_market_bubble_footprint_reclassifies_as_of_against_prior_baseline() -> None:
    baseline = pd.DataFrame(
        [
            _footprint_row(date="2026-05-28", horizon=60, dispersion=10.0, expensive_share=18.0),
            _footprint_row(date="2026-05-29", horizon=60, dispersion=99.0, expensive_share=99.0),
        ]
    )
    raw = pd.DataFrame(
        [
            _footprint_row(
                date="2026-05-29",
                horizon=60,
                breadth_up_pct=24.77,
                pct_above_sma50=38.17,
                dispersion=39.9,
                expensive_share=24.06,
                cap_weight_return_pct=6.66,
                equal_weight_return_pct=0.1,
            )
        ]
    )

    frame = monitor._reclassify_footprint_against_baseline(
        raw,
        baseline=baseline,
        target_date="2026-05-29",
    )

    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["snapshot_date"] == "2026-05-29"
    assert row["return_p90_p10_spread_pct_hist_pct"] == 1.0
    assert row["expensive_mcap_share_pct_hist_pct"] == 1.0
    assert int(row["bubble_score"]) == 5
    assert row["bubble_regime"] == "blowoff_watch"
    assert set(str(row["active_flags"]).split(",")) == {
        "breadth_narrowing",
        "market_cap_concentration",
        "valuation_pressure",
        "return_dispersion",
        "cap_weight_leadership",
    }


def _footprint_row(
    *,
    date: str,
    horizon: int,
    breadth_up_pct: float = 60.0,
    pct_above_sma50: float = 60.0,
    dispersion: float = 10.0,
    expensive_share: float = 10.0,
    cap_weight_return_pct: float = 1.0,
    equal_weight_return_pct: float = 0.5,
) -> dict[str, object]:
    return {
        "snapshot_date": date,
        "anchor_date": "2026-04-01",
        "horizon": horizon,
        "observation_count": 1200,
        "code_count": 1200,
        "breadth_up_pct": breadth_up_pct,
        "equal_weight_return_pct": equal_weight_return_pct,
        "cap_weight_return_pct": cap_weight_return_pct,
        "return_p90_p10_spread_pct": dispersion,
        "top5_positive_contribution_share_pct": 20.0,
        "top10_positive_contribution_share_pct": 25.0,
        "top5_mcap_share_pct": 15.0,
        "top10_mcap_share_pct": 21.0,
        "expensive_mcap_share_pct": expensive_share,
        "expensive_count_share_pct": 10.0,
        "no_positive_earnings_count_share_pct": 5.0,
        "pct_above_sma50": pct_above_sma50,
        "pct_above_sma200": 43.26,
        "median_trading_value_ratio_20v232": 1.0,
        "p90_trading_value_ratio_20v232": 1.0,
        "topix_return_pct": 1.0,
    }
