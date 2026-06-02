from __future__ import annotations

from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app


def test_market_bubble_footprint_latest_returns_horizon_intensity(monkeypatch) -> None:
    payload = {
        "date": "2026-05-29",
        "markets": ["prime", "standard", "growth"],
        "overallRegime": "blowoff_watch",
        "overallScore": 4,
        "nearBlowoff": True,
        "researchExperimentId": "market-behavior/market-bubble-footprint",
        "reratingExperimentId": "market-behavior/rerating-bubble-regime-forward-response",
        "horizons": [
            {
                "horizon": 20,
                "score": 4,
                "regime": "blowoff_watch",
                "nearBlowoff": False,
                "breadthUpPct": 42.88,
                "pctAboveSma50": 38.19,
                "pctAboveSma200": 43.26,
                "expensiveMcapSharePct": 24.05,
                "returnP90P10SpreadPct": 30.61,
                "returnDispersionPercentile": 0.9748,
                "capWeightLeadershipPct": 5.59,
                "activeFlags": [
                    "breadth_narrowing",
                    "valuation_pressure",
                    "return_dispersion",
                    "cap_weight_leadership",
                ],
            },
            {
                "horizon": 60,
                "score": 3,
                "regime": "crowded",
                "nearBlowoff": True,
                "breadthUpPct": 24.77,
                "pctAboveSma50": 38.17,
                "pctAboveSma200": 43.37,
                "expensiveMcapSharePct": 24.06,
                "returnP90P10SpreadPct": 39.90,
                "returnDispersionPercentile": 0.8974,
                "capWeightLeadershipPct": 6.55,
                "activeFlags": [
                    "breadth_narrowing",
                    "valuation_pressure",
                    "cap_weight_leadership",
                ],
            },
        ],
    }

    monkeypatch.setattr(
        "src.entrypoints.http.routes.analytics_market.get_latest_market_bubble_footprint",
        lambda *, markets, date=None: {**payload, "date": date or payload["date"]},
    )

    client = TestClient(create_app())
    response = client.get(
        "/api/analytics/market-bubble-footprint/latest",
        params={"markets": "prime,standard,growth", "date": "2026-05-28"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["date"] == "2026-05-28"
    assert body["overallRegime"] == "blowoff_watch"
    assert body["nearBlowoff"] is True
    assert body["horizons"][1]["horizon"] == 60
    assert body["horizons"][1]["nearBlowoff"] is True
