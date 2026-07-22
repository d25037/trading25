import hashlib
import json
from typing import get_args

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
            "actualEps",
            "forecastEps",
            "forecastToActualRatio",
            "forecastEpsChangeRate",
            "disclosedDate",
            "actualDisclosedDate",
            "forecastDisclosedDate",
            "periodType",
            "source",
            "fundamentalsAdjustmentBasisDate",
            "providerAsOf",
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
            "actualEps",
            "forecastEps",
            "forecastToActualRatio",
            "forecastEpsChangeRate",
            "disclosedDate",
            "actualDisclosedDate",
            "forecastDisclosedDate",
            "periodType",
            "source",
        ),
    ),
    "FundamentalRankings": (
        ("ratioHigh", "ratioLow", "forecastHigh", "forecastLow", "actualHigh", "actualLow"),
        (),
    ),
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

EXPECTED_RAW_ANNOTATIONS_SHA256 = (
    "be914039d896c1b609b9a3a49d3df8e2f45d0e6cdf6d1f01cce196eac5f54823"
)

EXPECTED_SCHEMA_DOC_SHA256 = {
    "RankingItem": "50442ab998d4fcef806cf5ff3f7f7c880889e410f15235a05e3ae7ad63b53db1",
    "Rankings": "930861925254d3595453cba5e25d171dcd183ce128358ba529dbf5334f2fd589",
    "IndexPerformanceItem": "f12f5bec10af52e872f95ba51e56b8f9461cb39b7086d8eae226397f02a12f2e",
    "MarketRankingResponse": "6176aa7854f70cedfd16a6e12d31c9ae95b35e52e210dce6a58992e13ceefeaf",
    "MarketRankingSymbolResponse": "afef60b257b4ad45a2e4ad894da412734c089fadb040e272d98f3791e9cba01e",
    "FundamentalRankingItem": "a7ecdc3d69ee85b7f1e552bc62159ea775b082fe4a66faad415ab8c30d80fe06",
    "FundamentalRankings": "ac0395ccaf14dfbcda5da4ce97b5c1449d7f7974e976772727cc46911440f474",
    "MarketFundamentalRankingResponse": "7ec5d6ec76a9594ed62dcc82246cc9d84f3b3fe2eee6999e753edf6407c25f17",
    "ValueCompositeTechnicalMetrics": "f3edda92490bafe41fd059a0415103265c92b406cf94732b38877d2da18ae735",
    "ValueCompositeRankingItem": "a8e8df99fe9ab1073930ba6de086ee0e759fc83802f9cc0ba172132e205586cd",
    "ValueCompositeRankingResponse": "231dddc76bee7ae689e65b22e26c60a903b308330b53ceb5e3e91b02cd5920a8",
    "ValueCompositeScoreResponse": "fa25d302469a639581472d0a197af9e56e7c4aa2b5fd9c04ebc079dda3a814bd",
}

EXPECTED_ALIAS_ARGS = {
    "ValueCompositeScoreMethod": (
        "standard_pbr_tilt",
        "prime_size_tilt",
        "prime_size75_forward_per25",
        "equal_weight",
    ),
    "ValueCompositeProfileId": (
        "standard_breakout_120d20",
        "prime_size75_forward_per25",
    ),
    "ValueCompositeForwardEpsMode": ("latest", "fy"),
    "ValueCompositeScoreUnavailableReason": (
        "not_found",
        "unsupported_market",
        "forward_eps_missing",
        "bps_missing",
        "not_rankable",
    ),
    "LiquidityRegime": (
        "neutral_rerating",
        "crowded_rerating",
        "distribution_stress",
        "stale_liquidity",
        "neutral",
    ),
    "RankingRiskFlag": ("overheat", "stale_rally_fade"),
    "RankingTechnicalFlag": ("atr20_acceleration", "momentum_20_60_top20"),
    "RankingRegimeStateFilter": (
        "neutral_rerating",
        "crowded_rerating",
        "distribution_stress",
        "stale_liquidity",
        "neutral",
    ),
    "RankingRiskStateFilter": ("overheat", "stale_rally_fade"),
    "RankingTechnicalStateFilter": (
        "atr20_acceleration",
        "momentum_20_60_top20",
    ),
    "RankingFundamentalStateFilter": (
        "deep_value",
        "value_confirmed",
        "undervalued",
        "expensive_or",
        "overvalued",
        "very_overvalued",
        "no_earnings",
    ),
    "SectorStrengthBucket": ("sector_strong", "sector_neutral", "sector_weak"),
    "SectorStrengthFamily": (
        "balanced_sector_strength",
        "long_hybrid_leadership",
    ),
}

EXPECTED_DAILY_ITEM_DUMP = {
    "rank": 1,
    "code": "7203",
    "companyName": "Toyota Motor",
    "marketCode": "0111",
    "sector33Name": "Transportation Equipment",
    "sectorStrengthScore": 0.8,
    "sectorStrengthBucket": "sector_strong",
    "currentPrice": 3000.0,
    "volume": 1_000_000.0,
    "tradingValue": 3_000_000_000.0,
    "tradingValueAverage": None,
    "previousPrice": None,
    "basePrice": None,
    "changeAmount": None,
    "changePercentage": 2.5,
    "lookbackDays": None,
    "sma5AboveCount5d": None,
    "sma5BelowStreak": None,
    "per": None,
    "perPercentile": None,
    "forwardPer": None,
    "forwardPerPercentile": None,
    "pOp": None,
    "forwardPOp": None,
    "forwardPOpPercentile": None,
    "forecastOperatingProfitGrowthRatio": None,
    "forecastOperatingProfitGrowthPct": None,
    "psr": None,
    "psrPercentile": None,
    "forwardPsr": None,
    "forwardPsrPercentile": None,
    "forwardEpsDisclosedDate": None,
    "forwardEpsSource": None,
    "pbr": None,
    "pbrPercentile": None,
    "valueCompositeScore": None,
    "overvaluationCompositeScore": None,
    "marketCap": None,
    "liquidityResidualZ": None,
    "liquidityRegime": None,
    "adv60ToFreeFloatPct": None,
    "riskFlags": ["overheat"],
    "technicalFlags": ["atr20_acceleration"],
}

EXPECTED_INDEX_PERFORMANCE_DUMP = {
    "code": "TOPIX",
    "name": "TOPIX",
    "category": "benchmark",
    "currentDate": "2026-07-14",
    "baseDate": "2026-06-16",
    "currentClose": 2900.0,
    "baseClose": 2800.0,
    "changeAmount": 100.0,
    "changePercentage": 3.57,
    "lookbackDays": 20,
    "sectorStrengthScore": None,
    "sectorStrengthBucket": None,
    "sector20dTopixExcessPct": None,
    "sector60dTopixExcessPct": None,
    "sectorBreadth20dPct": None,
    "sectorStockCount": None,
}

EXPECTED_FUNDAMENTAL_ITEM_DUMP = {
    "rank": 1,
    "code": "7203",
    "companyName": "Toyota Motor",
    "marketCode": "0111",
    "sector33Name": "Transportation Equipment",
    "currentPrice": 3000.0,
    "volume": 1_000_000.0,
    "epsValue": 1.25,
    "actualEps": 100.0,
    "forecastEps": 125.0,
    "forecastToActualRatio": 1.25,
    "forecastEpsChangeRate": 25.0,
    "disclosedDate": "2026-07-10",
    "actualDisclosedDate": "2026-05-10",
    "forecastDisclosedDate": "2026-07-10",
    "periodType": "FY",
    "source": "revised",
    "fundamentalsAdjustmentBasisDate": None,
    "providerAsOf": None,
}

EXPECTED_TECHNICAL_METRICS_DUMP = {
    "featureDate": "2026-07-14",
    "breakoutFeatureDate": None,
    "reboundFrom252dLowPct": None,
    "return252dPct": None,
    "volatility20dPct": None,
    "volatility60dPct": None,
    "downsideVolatility60dPct": None,
    "avgTradingValue60dMilJpy": None,
    "avgTradingValue60dSourceSessions": None,
    "newHigh20d": True,
    "daysSinceNewHigh20d": None,
    "closeToPriorHigh20dPct": None,
    "newHigh120d": None,
    "daysSinceNewHigh120d": None,
    "closeToPriorHigh120dPct": None,
}

EXPECTED_VALUE_ITEM_DUMP = {
    "rank": 1,
    "code": "7203",
    "companyName": "Toyota Motor",
    "marketCode": "0111",
    "sector33Name": "Transportation Equipment",
    "currentPrice": 3000.0,
    "volume": 1_000_000.0,
    "score": 0.9,
    "scoreBeforeBoost": None,
    "breakoutBoost": None,
    "liquidityEligible": None,
    "avgTradingValue60dMilJpy": None,
    "lowPbrScore": 0.8,
    "smallMarketCapScore": 0.7,
    "lowForwardPerScore": 0.6,
    "pbr": 1.1,
    "forwardPer": 9.5,
    "marketCapBilJpy": 40_000.0,
    "bps": None,
    "forwardEps": None,
    "latestFyDisclosedDate": None,
    "forwardEpsDisclosedDate": None,
    "forwardEpsSource": None,
    "technicalMetrics": EXPECTED_TECHNICAL_METRICS_DUMP,
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
    for alias_name, expected_args in EXPECTED_ALIAS_ARGS.items():
        assert get_args(getattr(ranking_contracts, alias_name)) == expected_args

    assert (
        ranking_contracts.normalize_sector_strength_family(
            "balanced_sector_strength"
        )
        == "balanced_sector_strength"
    )
    with pytest.raises(ValueError, match="Unsupported sectorStrengthFamily"):
        ranking_contracts.normalize_sector_strength_family("unknown")


def test_ranking_raw_annotations_are_frozen_strings() -> None:
    annotations = {
        model_name: getattr(ranking_contracts, model_name).__annotations__
        for model_name in MODEL_NAMES
    }
    assert all(
        isinstance(annotation, str)
        for model_annotations in annotations.values()
        for annotation in model_annotations.values()
    )
    serialized = json.dumps(annotations, ensure_ascii=False, separators=(",", ":"))
    assert hashlib.sha256(serialized.encode()).hexdigest() == (
        EXPECTED_RAW_ANNOTATIONS_SHA256
    )


def test_ranking_complete_schemas_and_docstrings_are_frozen() -> None:
    for model_name, expected_fingerprint in EXPECTED_SCHEMA_DOC_SHA256.items():
        model = getattr(ranking_contracts, model_name)
        payload = {"schema": model.model_json_schema(), "doc": model.__doc__}
        serialized = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        assert hashlib.sha256(serialized.encode()).hexdigest() == (
            expected_fingerprint
        )


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
            "gainers": [EXPECTED_DAILY_ITEM_DUMP],
            "losers": [],
            "periodHigh": [],
            "periodLow": [],
        },
        "indexPerformance": [EXPECTED_INDEX_PERFORMANCE_DUMP],
        "lastUpdated": "2026-07-14T15:00:00+09:00",
    }
    assert item.changePercentage == 2.5

    symbol_response = ranking_contracts.MarketRankingSymbolResponse(
        date="2026-07-14",
        item=item,
        lastUpdated="2026-07-14T15:00:00+09:00",
    )
    assert symbol_response.model_dump() == {
        "date": "2026-07-14",
        "item": EXPECTED_DAILY_ITEM_DUMP,
        "lastUpdated": "2026-07-14T15:00:00+09:00",
    }


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
        actualEps=100.0,
        forecastEps=125.0,
        forecastToActualRatio=1.25,
        forecastEpsChangeRate=25.0,
        disclosedDate="2026-07-10",
        actualDisclosedDate="2026-05-10",
        forecastDisclosedDate="2026-07-10",
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
        "rankings": {
            "ratioHigh": [EXPECTED_FUNDAMENTAL_ITEM_DUMP],
            "ratioLow": [],
            "forecastHigh": [],
            "forecastLow": [],
            "actualHigh": [],
            "actualLow": [],
        },
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
        "items": [EXPECTED_VALUE_ITEM_DUMP],
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
        "item": EXPECTED_VALUE_ITEM_DUMP,
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
        "FundamentalRankings": {
            "ratioHigh",
            "ratioLow",
            "forecastHigh",
            "forecastLow",
            "actualHigh",
            "actualLow",
        },
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
