"""Static value-composite ranking configuration."""

from __future__ import annotations

from dataclasses import dataclass

from src.domains.analytics.value_composite_scoring import (
    EQUAL_VALUE_COMPOSITE_WEIGHTS,
    PRIME_SIZE75_FORWARD_PER25_VALUE_COMPOSITE_WEIGHTS,
    PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
    STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS,
)
from src.entrypoints.http.schemas.ranking import (
    RankingRiskFlag,
    ValueCompositeForwardEpsMode,
    ValueCompositeProfileId,
    ValueCompositeScoreMethod,
)

VALUE_COMPOSITE_METRIC_KEY = "standard_value_composite"
VALUE_COMPOSITE_SCORE_POLICY_SUFFIX = "requires PBR > 0 and forward PER > 0"


@dataclass(frozen=True)
class ValueCompositeProfileSpec:
    profile_id: ValueCompositeProfileId
    label: str
    score_method: ValueCompositeScoreMethod
    rebalance_months: int
    min_adv60_mil_jpy: float | None = None
    breakout_window: int | None = None
    breakout_lookback_sessions: int | None = None
    breakout_score_boost: float | None = None


VALUE_COMPOSITE_WEIGHTS_BY_METHOD: dict[
    ValueCompositeScoreMethod, dict[str, float]
] = {
    "standard_pbr_tilt": STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS,
    "prime_size_tilt": PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
    "prime_size75_forward_per25": PRIME_SIZE75_FORWARD_PER25_VALUE_COMPOSITE_WEIGHTS,
    "equal_weight": EQUAL_VALUE_COMPOSITE_WEIGHTS,
}
VALUE_COMPOSITE_AUTO_SCORE_METHOD_BY_MARKET: dict[str, ValueCompositeScoreMethod] = {
    "prime": "prime_size_tilt",
    "standard": "standard_pbr_tilt",
}
VALUE_COMPOSITE_SCORE_POLICY_BY_METHOD: dict[ValueCompositeScoreMethod, str] = {
    "standard_pbr_tilt": (
        "Standard PBR tilt research weights: 35% small market cap + 40% low PBR + "
        f"25% low forward PER; {VALUE_COMPOSITE_SCORE_POLICY_SUFFIX}"
    ),
    "prime_size_tilt": (
        "Prime size tilt research weights: 46.5% small market cap + 5% low PBR + "
        f"48.5% low forward PER; {VALUE_COMPOSITE_SCORE_POLICY_SUFFIX}"
    ),
    "prime_size75_forward_per25": (
        "Prime production candidate weights: 75% small market cap + 0% low PBR + "
        f"25% low forward PER; {VALUE_COMPOSITE_SCORE_POLICY_SUFFIX}"
    ),
    "equal_weight": (
        "Equal weight across small market cap, low PBR, and low forward PER; "
        f"{VALUE_COMPOSITE_SCORE_POLICY_SUFFIX}"
    ),
}
VALUE_COMPOSITE_PROFILE_BY_ID: dict[ValueCompositeProfileId, ValueCompositeProfileSpec] = {
    "standard_breakout_120d20": ValueCompositeProfileSpec(
        profile_id="standard_breakout_120d20",
        label="Standard value + 120d breakout boost",
        score_method="prime_size_tilt",
        rebalance_months=3,
        min_adv60_mil_jpy=10.0,
        breakout_window=120,
        breakout_lookback_sessions=20,
        breakout_score_boost=0.10,
    ),
    "prime_size75_forward_per25": ValueCompositeProfileSpec(
        profile_id="prime_size75_forward_per25",
        label="Prime size75 / forward PER25",
        score_method="prime_size75_forward_per25",
        rebalance_months=2,
        min_adv60_mil_jpy=10.0,
    ),
}
VALUE_COMPOSITE_FORWARD_EPS_MODE_LABELS: dict[ValueCompositeForwardEpsMode, str] = {
    "latest": "latest revised forecast EPS when available, otherwise FY forecast EPS",
    "fy": "latest FY forecast EPS only",
}
SHORT_TERM_OVERHEAT_RETURN_20D_THRESHOLD_PCT = 30.0
OVERHEAT_RISK_FLAG: RankingRiskFlag = "overheat"
PRIME_VALUATION_PERCENTILE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("per", "per_percentile"),
    ("forward_per", "forward_per_percentile"),
    ("forward_p_op", "forward_p_op_percentile"),
    ("pbr", "pbr_percentile"),
)
