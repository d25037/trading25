from __future__ import annotations

from src.application.services.ranking_collection_filters import matches_fundamental_state
from src.application.services.ranking_liquidity import classify_prime_liquidity_regime
from src.application.services.ranking_technical_flags import classify_technical_flags
from src.application.contracts.ranking import RankingItem


def _ranking_item(**changes: object) -> RankingItem:
    return RankingItem(
        rank=1,
        code="1111",
        companyName="Alpha",
        marketCode="0111",
        sector33Name="Services",
        currentPrice=100.0,
        volume=1000.0,
        **changes,
    )


def test_production_adapters_preserve_shared_signal_contract() -> None:
    item = _ranking_item(
        per=10.0,
        forwardPer=8.0,
        perPercentile=0.2,
        forwardPerPercentile=0.2,
        pbrPercentile=0.2,
    )

    assert matches_fundamental_state(item, fundamental_state="deep_value")
    assert classify_prime_liquidity_regime(1.0, 0.01, 0.01) == "crowded_rerating"
    assert classify_technical_flags(
        recent_return_20d_pct=29.99,
        momentum_20d_percentile=0.9,
        momentum_60d_percentile=0.9,
        atr20_to_atr60=1.24,
        atr20_change_20d_pct=25.0,
    ) == ("atr20_acceleration", "momentum_20_60_top20")
