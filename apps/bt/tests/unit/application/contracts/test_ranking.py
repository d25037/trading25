import pytest

from src.application.contracts import ranking as ranking_contracts


MODEL_NAMES = (
    "RankingItem",
    "Rankings",
    "IndexPerformanceItem",
    "MarketRankingResponse",
    "MarketRankingSymbolResponse",
    "FundamentalRankingItem",
    "FundamentalRankings",
    "MarketFundamentalRankingResponse",
    "ValueCompositeTechnicalMetrics",
    "ValueCompositeRankingItem",
    "ValueCompositeRankingResponse",
    "ValueCompositeScoreResponse",
)

EXPECTED_SCHEMA_ORDERS = {
    "RankingItem": (
        (
            "rank",
            "code",
            "companyName",
            "marketCode",
            "sector33Name",
            "sectorStrengthScore",
            "sectorStrengthBucket",
            "currentPrice",
            "volume",
            "tradingValue",
            "tradingValueAverage",
            "previousPrice",
            "basePrice",
            "changeAmount",
            "changePercentage",
            "lookbackDays",
            "sma5AboveCount5d",
            "sma5BelowStreak",
            "per",
            "perPercentile",
            "forwardPer",
            "forwardPerPercentile",
            "pOp",
            "forwardPOp",
            "forwardPOpPercentile",
            "forecastOperatingProfitGrowthRatio",
            "forecastOperatingProfitGrowthPct",
            "psr",
            "psrPercentile",
            "forwardPsr",
            "forwardPsrPercentile",
            "forwardEpsDisclosedDate",
            "forwardEpsSource",
            "pbr",
            "pbrPercentile",
            "valueCompositeScore",
            "overvaluationCompositeScore",
            "marketCap",
            "liquidityResidualZ",
            "liquidityRegime",
            "adv60ToFreeFloatPct",
            "riskFlags",
            "technicalFlags",
        ),
        (
            "rank",
            "code",
            "companyName",
            "marketCode",
            "sector33Name",
            "currentPrice",
            "volume",
        ),
    ),
    "Rankings": (
        ("tradingValue", "gainers", "losers", "periodHigh", "periodLow"),
        (),
    ),
    "IndexPerformanceItem": (
        (
            "code",
            "name",
            "category",
            "currentDate",
            "baseDate",
            "currentClose",
            "baseClose",
            "changeAmount",
            "changePercentage",
            "lookbackDays",
            "sectorStrengthScore",
            "sectorStrengthBucket",
            "sector20dTopixExcessPct",
            "sector60dTopixExcessPct",
            "sectorBreadth20dPct",
            "sectorStockCount",
        ),
        (
            "code",
            "name",
            "category",
            "currentDate",
            "baseDate",
            "currentClose",
            "baseClose",
            "changeAmount",
            "changePercentage",
            "lookbackDays",
        ),
    ),
    "MarketRankingResponse": (
        (
            "date",
            "markets",
            "lookbackDays",
            "periodDays",
            "sectorStrengthFamily",
            "rankings",
            "indexPerformance",
            "lastUpdated",
        ),
        ("date", "markets", "lookbackDays", "periodDays", "rankings", "lastUpdated"),
    ),
    "MarketRankingSymbolResponse": (
        ("date", "item", "lastUpdated"),
        ("date", "item", "lastUpdated"),
    ),
    "FundamentalRankingItem": (
        (
            "rank",
            "code",
            "companyName",
            "marketCode",
            "sector33Name",
            "currentPrice",
            "volume",
            "epsValue",
            "disclosedDate",
            "periodType",
            "source",
        ),
        (
            "rank",
            "code",
            "companyName",
            "marketCode",
            "sector33Name",
            "currentPrice",
            "volume",
            "epsValue",
            "disclosedDate",
            "periodType",
            "source",
        ),
    ),
    "FundamentalRankings": (("ratioHigh", "ratioLow"), ()),
    "MarketFundamentalRankingResponse": (
        ("date", "markets", "metricKey", "rankings", "lastUpdated"),
        ("date", "markets", "metricKey", "rankings", "lastUpdated"),
    ),
    "ValueCompositeTechnicalMetrics": (
        (
            "featureDate",
            "breakoutFeatureDate",
            "reboundFrom252dLowPct",
            "return252dPct",
            "volatility20dPct",
            "volatility60dPct",
            "downsideVolatility60dPct",
            "avgTradingValue60dMilJpy",
            "avgTradingValue60dSourceSessions",
            "newHigh20d",
            "daysSinceNewHigh20d",
            "closeToPriorHigh20dPct",
            "newHigh120d",
            "daysSinceNewHigh120d",
            "closeToPriorHigh120dPct",
        ),
        (),
    ),
    "ValueCompositeRankingItem": (
        (
            "rank",
            "code",
            "companyName",
            "marketCode",
            "sector33Name",
            "currentPrice",
            "volume",
            "score",
            "scoreBeforeBoost",
            "breakoutBoost",
            "liquidityEligible",
            "avgTradingValue60dMilJpy",
            "lowPbrScore",
            "smallMarketCapScore",
            "lowForwardPerScore",
            "pbr",
            "forwardPer",
            "marketCapBilJpy",
            "bps",
            "forwardEps",
            "latestFyDisclosedDate",
            "forwardEpsDisclosedDate",
            "forwardEpsSource",
            "technicalMetrics",
        ),
        (
            "rank",
            "code",
            "companyName",
            "marketCode",
            "sector33Name",
            "currentPrice",
            "volume",
            "score",
            "lowPbrScore",
            "smallMarketCapScore",
            "lowForwardPerScore",
            "pbr",
            "forwardPer",
            "marketCapBilJpy",
        ),
    ),
    "ValueCompositeRankingResponse": (
        (
            "date",
            "markets",
            "metricKey",
            "profileId",
            "profileLabel",
            "scoreMethod",
            "forwardEpsMode",
            "rebalanceMonths",
            "breakoutWindow",
            "breakoutLookbackSessions",
            "breakoutScoreBoost",
            "applyLiquidityFilter",
            "scorePolicy",
            "weights",
            "itemCount",
            "items",
            "lastUpdated",
        ),
        (
            "date",
            "markets",
            "scoreMethod",
            "forwardEpsMode",
            "scorePolicy",
            "weights",
            "itemCount",
            "lastUpdated",
        ),
    ),
    "ValueCompositeScoreResponse": (
        (
            "date",
            "code",
            "companyName",
            "marketCode",
            "market",
            "metricKey",
            "scoreMethod",
            "forwardEpsMode",
            "scorePolicy",
            "weights",
            "universeCount",
            "scoreAvailable",
            "unsupportedReason",
            "item",
            "lastUpdated",
        ),
        ("date", "code", "forwardEpsMode", "scoreAvailable", "lastUpdated"),
    ),
}


def _daily_item() -> ranking_contracts.RankingItem:
    return ranking_contracts.RankingItem(
        rank=1,
        code="7203",
        companyName="Toyota Motor",
        marketCode="0111",
        sector33Name="Transportation Equipment",
        sectorStrengthScore=0.8,
        sectorStrengthBucket="sector_strong",
        currentPrice=3000.0,
        volume=1_000_000.0,
        tradingValue=3_000_000_000.0,
        riskFlags=["overheat"],
        technicalFlags=["atr20_acceleration"],
    )


def _value_item() -> ranking_contracts.ValueCompositeRankingItem:
    return ranking_contracts.ValueCompositeRankingItem(
        rank=1,
        code="7203",
        companyName="Toyota Motor",
        marketCode="0111",
        sector33Name="Transportation Equipment",
        currentPrice=3000.0,
        volume=1_000_000.0,
        score=0.9,
        lowPbrScore=0.8,
        smallMarketCapScore=0.7,
        lowForwardPerScore=0.6,
        pbr=1.1,
        forwardPer=9.5,
        marketCapBilJpy=40_000.0,
        technicalMetrics=ranking_contracts.ValueCompositeTechnicalMetrics(
            featureDate="2026-07-14",
            newHigh20d=True,
        ),
    )


def test_ranking_aliases_and_sector_strength_normalization_are_stable() -> None:
    assert ranking_contracts.ValueCompositeScoreMethod.__args__ == (
        "standard_pbr_tilt",
        "prime_size_tilt",
        "prime_size75_forward_per25",
        "equal_weight",
    )
    assert ranking_contracts.SectorStrengthFamily.__args__ == (
        "balanced_sector_strength",
        "long_hybrid_leadership",
    )
    assert (
        ranking_contracts.normalize_sector_strength_family(
            "balanced_sector_strength"
        )
        == "balanced_sector_strength"
    )
    with pytest.raises(ValueError, match="Unsupported sectorStrengthFamily"):
        ranking_contracts.normalize_sector_strength_family("unknown")


def test_daily_ranking_response_graph_serialization_is_stable() -> None:
    item = _daily_item()
    item.changePercentage = 2.5
    response = ranking_contracts.MarketRankingResponse(
        date="2026-07-14",
        markets=["0111"],
        lookbackDays=20,
        periodDays=5,
        rankings=ranking_contracts.Rankings(gainers=[item]),
        indexPerformance=[
            ranking_contracts.IndexPerformanceItem(
                code="TOPIX",
                name="TOPIX",
                category="benchmark",
                currentDate="2026-07-14",
                baseDate="2026-06-16",
                currentClose=2900.0,
                baseClose=2800.0,
                changeAmount=100.0,
                changePercentage=3.57,
                lookbackDays=20,
            )
        ],
        lastUpdated="2026-07-14T15:00:00+09:00",
    )

    assert response.model_dump() == {
        "date": "2026-07-14",
        "markets": ["0111"],
        "lookbackDays": 20,
        "periodDays": 5,
        "sectorStrengthFamily": "balanced_sector_strength",
        "rankings": {
            "tradingValue": [],
            "gainers": [item.model_dump()],
            "losers": [],
            "periodHigh": [],
            "periodLow": [],
        },
        "indexPerformance": [response.indexPerformance[0].model_dump()],
        "lastUpdated": "2026-07-14T15:00:00+09:00",
    }
    assert item.changePercentage == 2.5


def test_fundamental_ranking_response_graph_serialization_is_stable() -> None:
    item = ranking_contracts.FundamentalRankingItem(
        rank=1,
        code="7203",
        companyName="Toyota Motor",
        marketCode="0111",
        sector33Name="Transportation Equipment",
        currentPrice=3000.0,
        volume=1_000_000.0,
        epsValue=1.25,
        disclosedDate="2026-07-10",
        periodType="FY",
        source="revised",
    )
    response = ranking_contracts.MarketFundamentalRankingResponse(
        date="2026-07-14",
        markets=["0111"],
        metricKey="eps_forecast_to_actual",
        rankings=ranking_contracts.FundamentalRankings(ratioHigh=[item]),
        lastUpdated="2026-07-14T15:00:00+09:00",
    )

    assert response.model_dump() == {
        "date": "2026-07-14",
        "markets": ["0111"],
        "metricKey": "eps_forecast_to_actual",
        "rankings": {"ratioHigh": [item.model_dump()], "ratioLow": []},
        "lastUpdated": "2026-07-14T15:00:00+09:00",
    }


def test_value_composite_response_graphs_serialization_is_stable() -> None:
    item = _value_item()
    response = ranking_contracts.ValueCompositeRankingResponse(
        date="2026-07-14",
        markets=["0111"],
        profileId="standard_breakout_120d20",
        profileLabel="Standard breakout",
        scoreMethod="standard_pbr_tilt",
        forwardEpsMode="latest",
        scorePolicy="low-is-better percentile composite",
        weights={"pbr": 0.5, "forwardPer": 0.5},
        itemCount=1,
        items=[item],
        lastUpdated="2026-07-14T15:00:00+09:00",
    )
    score_response = ranking_contracts.ValueCompositeScoreResponse(
        date="2026-07-14",
        code="7203",
        companyName="Toyota Motor",
        marketCode="0111",
        market="prime",
        scoreMethod="standard_pbr_tilt",
        forwardEpsMode="latest",
        scorePolicy="low-is-better percentile composite",
        weights={"pbr": 0.5, "forwardPer": 0.5},
        universeCount=100,
        scoreAvailable=True,
        item=item,
        lastUpdated="2026-07-14T15:00:00+09:00",
    )

    assert response.model_dump() == {
        "date": "2026-07-14",
        "markets": ["0111"],
        "metricKey": "standard_value_composite",
        "profileId": "standard_breakout_120d20",
        "profileLabel": "Standard breakout",
        "scoreMethod": "standard_pbr_tilt",
        "forwardEpsMode": "latest",
        "rebalanceMonths": None,
        "breakoutWindow": None,
        "breakoutLookbackSessions": None,
        "breakoutScoreBoost": None,
        "applyLiquidityFilter": True,
        "scorePolicy": "low-is-better percentile composite",
        "weights": {"pbr": 0.5, "forwardPer": 0.5},
        "itemCount": 1,
        "items": [item.model_dump()],
        "lastUpdated": "2026-07-14T15:00:00+09:00",
    }
    assert score_response.model_dump() == {
        "date": "2026-07-14",
        "code": "7203",
        "companyName": "Toyota Motor",
        "marketCode": "0111",
        "market": "prime",
        "metricKey": "standard_value_composite",
        "scoreMethod": "standard_pbr_tilt",
        "forwardEpsMode": "latest",
        "scorePolicy": "low-is-better percentile composite",
        "weights": {"pbr": 0.5, "forwardPer": 0.5},
        "universeCount": 100,
        "scoreAvailable": True,
        "unsupportedReason": None,
        "item": item.model_dump(),
        "lastUpdated": "2026-07-14T15:00:00+09:00",
    }


def test_ranking_mutable_defaults_are_optional_and_independent() -> None:
    first_item = _daily_item()
    second_item = _daily_item()
    first_rankings = ranking_contracts.Rankings()
    second_rankings = ranking_contracts.Rankings()
    first_score = ranking_contracts.ValueCompositeScoreResponse(
        date="2026-07-14",
        code="7203",
        forwardEpsMode="latest",
        scoreAvailable=False,
        lastUpdated="2026-07-14T15:00:00+09:00",
    )
    second_score = ranking_contracts.ValueCompositeScoreResponse(
        date="2026-07-14",
        code="6758",
        forwardEpsMode="latest",
        scoreAvailable=False,
        lastUpdated="2026-07-14T15:00:00+09:00",
    )

    assert first_item.riskFlags is not second_item.riskFlags
    assert first_item.technicalFlags is not second_item.technicalFlags
    assert first_rankings.gainers is not second_rankings.gainers
    assert first_score.weights is not second_score.weights

    for model_name in MODEL_NAMES:
        schema = getattr(ranking_contracts, model_name).model_json_schema()
        properties, required = EXPECTED_SCHEMA_ORDERS[model_name]
        assert tuple(schema["properties"]) == properties
        assert tuple(schema.get("required", ())) == required

    default_backed_fields = {
        "RankingItem": {"riskFlags", "technicalFlags"},
        "Rankings": {"tradingValue", "gainers", "losers", "periodHigh", "periodLow"},
        "MarketRankingResponse": {"indexPerformance"},
        "FundamentalRankings": {"ratioHigh", "ratioLow"},
        "ValueCompositeRankingResponse": {"items"},
        "ValueCompositeScoreResponse": {"weights"},
    }
    for model_name, field_names in default_backed_fields.items():
        required = set(
            getattr(ranking_contracts, model_name)
            .model_json_schema()
            .get("required", ())
        )
        assert field_names.isdisjoint(required)
