import type { MarketRankingParams } from '../src/analytics/types.js';

const removedLegacyParam: MarketRankingParams = {
  // @ts-expect-error liquidityState was removed; use regimeState or riskState
  liquidityState: 'crowded_rerating',
};

void removedLegacyParam;
