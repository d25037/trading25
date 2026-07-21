"""Shared Daily Ranking state and risk flag constants."""

from __future__ import annotations

from src.application.contracts import ranking as ranking_contracts
from src.domains.analytics.daily_ranking_core import (
    ATR20_ACCELERATION_API_FLAG,
    MOMENTUM_20_60_TOP20_API_FLAG,
    OVERHEAT_RISK_FLAG as CORE_OVERHEAT_RISK_FLAG,
)

OVERHEAT_RISK_FLAG: ranking_contracts.RankingRiskFlag = CORE_OVERHEAT_RISK_FLAG
STALE_RALLY_FADE_RISK_FLAG: ranking_contracts.RankingRiskFlag = "stale_rally_fade"
ATR20_ACCELERATION_TECHNICAL_FLAG: ranking_contracts.RankingTechnicalFlag = (
    ATR20_ACCELERATION_API_FLAG
)
MOMENTUM_20_60_TOP20_TECHNICAL_FLAG: ranking_contracts.RankingTechnicalFlag = (
    MOMENTUM_20_60_TOP20_API_FLAG
)
RISK_FLAG_STATE_FILTERS: frozenset[ranking_contracts.RankingRiskFlag] = frozenset(
    (OVERHEAT_RISK_FLAG, STALE_RALLY_FADE_RISK_FLAG)
)
