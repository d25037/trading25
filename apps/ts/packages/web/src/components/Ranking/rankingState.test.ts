import { describe, expect, it } from 'vitest';
import type { RankingParams } from '@/types/ranking';
import { applyRankingPreset, getRankingPreset, RANKING_PRESET_OPTIONS } from './rankingState';

describe('rankingState', () => {
  it('exposes a Stale preset that applies the stale liquidity regime', () => {
    const baseParams: RankingParams = {
      markets: 'prime',
      lookbackDays: 1,
      liquidityState: 'neutral_rerating',
      riskState: 'overheat',
      technicalState: 'atr20_acceleration',
    };

    expect(RANKING_PRESET_OPTIONS).toContainEqual({ value: 'stale', label: 'Stale' });
    expect(applyRankingPreset(baseParams, 'stale')).toEqual({
      ...baseParams,
      liquidityState: undefined,
      regimeState: 'stale_liquidity',
      riskState: undefined,
      technicalState: undefined,
    });
    expect(getRankingPreset({ regimeState: 'stale_liquidity' })).toBe('stale');
  });
});
