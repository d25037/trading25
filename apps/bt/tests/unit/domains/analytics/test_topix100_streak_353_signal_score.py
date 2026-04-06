from pathlib import Path

from src.domains.analytics.topix100_streak_353_signal_score import (
    Topix100Streak353SignalScorecard,
    _LookupRow,
    score_topix100_streak_353_signal,
)


def _build_scorecard() -> Topix100Streak353SignalScorecard:
    return Topix100Streak353SignalScorecard(
        run_id="test-run",
        bundle_path=Path("/tmp/test-run"),
        universe_long_score_5d=0.005,
        universe_short_score_1d=-0.001,
        rows_by_subset={
            "universe": {
                "universe": _LookupRow("universe", "universe", 0.001, 0.005, 100, 100),
            },
            "short_mode": {
                "bearish": _LookupRow("short_mode", "bearish", 0.004, 0.009, 80, 80),
                "bullish": _LookupRow("short_mode", "bullish", -0.002, 0.002, 80, 80),
            },
            "bucket+short_mode": {
                "Q1|bearish": _LookupRow("bucket+short_mode", "Q1|bearish", 0.006, 0.015, 60, 60),
                "Q7|bullish": _LookupRow("bucket+short_mode", "Q7|bullish", -0.003, 0.001, 60, 60),
            },
            "bucket+short_mode+long_mode": {
                "Q1|bearish|bearish": _LookupRow(
                    "bucket+short_mode+long_mode",
                    "Q1|bearish|bearish",
                    0.006,
                    0.018,
                    50,
                    50,
                ),
            },
            "volume+short_mode": {
                "volume_low|bearish": _LookupRow(
                    "volume+short_mode",
                    "volume_low|bearish",
                    0.005,
                    0.010,
                    60,
                    60,
                ),
                "volume_high|bullish": _LookupRow(
                    "volume+short_mode",
                    "volume_high|bullish",
                    -0.004,
                    0.002,
                    60,
                    60,
                ),
            },
            "volume+short_mode+long_mode": {
                "volume_low|bearish|bearish": _LookupRow(
                    "volume+short_mode+long_mode",
                    "volume_low|bearish|bearish",
                    0.006,
                    0.013,
                    50,
                    50,
                ),
                "volume_high|bullish|bullish": _LookupRow(
                    "volume+short_mode+long_mode",
                    "volume_high|bullish|bullish",
                    -0.005,
                    0.001,
                    50,
                    50,
                ),
            },
            "bucket+volume+short_mode+long_mode": {
                "Q1|volume_low|bearish|bearish": _LookupRow(
                    "bucket+volume+short_mode+long_mode",
                    "Q1|volume_low|bearish|bearish",
                    0.007,
                    0.025,
                    40,
                    40,
                ),
                "Q7|volume_high|bullish|bullish": _LookupRow(
                    "bucket+volume+short_mode+long_mode",
                    "Q7|volume_high|bullish|bullish",
                    -0.006,
                    0.000,
                    40,
                    40,
                ),
            },
        },
    )


def test_score_prefers_exact_long_setup():
    score = score_topix100_streak_353_signal(
        price_decile=1,
        volume_bucket="low",
        short_mode="bearish",
        long_mode="bearish",
        scorecard=_build_scorecard(),
    )

    assert score.long_score_5d is not None
    assert score.short_score_1d is not None
    assert score.long_score_5d > 0.011
    assert score.short_score_1d < 0


def test_score_prefers_bullish_high_volume_for_short_edge():
    score = score_topix100_streak_353_signal(
        price_decile=7,
        volume_bucket="high",
        short_mode="bullish",
        long_mode="bullish",
        scorecard=_build_scorecard(),
    )

    assert score.long_score_5d is not None
    assert score.short_score_1d is not None
    assert score.long_score_5d < 0.0035
    assert score.short_score_1d > 0.001


def test_score_returns_none_when_features_are_missing():
    score = score_topix100_streak_353_signal(
        price_decile=7,
        volume_bucket=None,
        short_mode="bullish",
        long_mode="bullish",
        scorecard=_build_scorecard(),
    )

    assert score.long_score_5d is None
    assert score.short_score_1d is None
