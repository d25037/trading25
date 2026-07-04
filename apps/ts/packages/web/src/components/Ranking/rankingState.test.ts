import { describe, expect, it } from 'vitest';
import type { DailyRankingTableFilters, RankingParams } from '@/types/ranking';
import {
  applyRankingPreset,
  getRankingPreset,
  getRankingPresetDescription,
  RANKING_PRESET_OPTIONS,
} from './rankingState';

describe('rankingState', () => {
  it('exposes research-backed daily ranking presets without stale or rally-fade presets', () => {
    const presetValues = RANKING_PRESET_OPTIONS.map((option) => option.value);

    expect(presetValues).toEqual([
      'all',
      'core_long',
      'earnings_priority',
      'aggressive_rerating',
      'overvalued_breakdown',
      'momentum_value',
      'neutral_rerating',
      'neutral_rerating_good',
      'crowded_momentum',
      'crowded_rerating',
      'crowded_rerating_good',
      'overheat',
      'custom',
    ]);
    expect(presetValues).not.toContain('stale');
    expect(presetValues).not.toContain('rally_fade');
  });

  it('applies the core long preset as the expanded neutral rerating scaffold', () => {
    const baseParams: RankingParams = {
      markets: 'prime',
      lookbackDays: 1,
      liquidityState: 'neutral_rerating',
      riskState: 'overheat',
      technicalState: 'atr20_acceleration',
      sectorStrengthFamily: 'balanced_sector_strength',
    };
    const baseFilters: DailyRankingTableFilters = {
      text: 'sony',
      maxForwardPer: 18,
      watchlistId: 12,
    };

    expect(applyRankingPreset(baseParams, baseFilters, 'core_long')).toEqual({
      rankingParams: {
        ...baseParams,
        liquidityState: undefined,
        regimeState: undefined,
        fundamentalState: undefined,
        riskState: undefined,
        technicalState: undefined,
      },
      rankingTableFilters: {
        regimeState: 'neutral_rerating',
        technicalState: 'atr20_acceleration',
        valuationSignal: 'deep_value',
        minLiquidityZ: -1,
        maxLiquidityZ: 2,
      },
    });
    expect(
      getRankingPreset({
        regimeState: 'neutral_rerating',
        technicalState: 'atr20_acceleration',
        valuationSignal: 'deep_value',
        minLiquidityZ: -1,
        maxLiquidityZ: 2,
      })
    ).toBe('core_long');
    expect(
      getRankingPreset({
        text: 'sony',
        regimeState: 'neutral_rerating',
        technicalState: 'atr20_acceleration',
        valuationSignal: 'deep_value',
        minLiquidityZ: -1,
        maxLiquidityZ: 2,
      })
    ).toBe('custom');
    expect(
      getRankingPreset({
        regimeState: 'neutral_rerating',
        technicalState: 'atr20_acceleration',
        valuationSignal: 'deep_value',
        minLiquidityZ: 0,
        maxLiquidityZ: 2,
      })
    ).toBe('custom');
  });

  it('applies the short preset as a derived table condition', () => {
    const next = applyRankingPreset(
      { markets: 'prime', regimeState: 'neutral_rerating', fundamentalState: 'deep_value' },
      { text: '7203', valuationSignal: 'deep_value' },
      'overvalued_breakdown'
    );

    expect(next).toEqual({
      rankingParams: {
        markets: 'prime',
        liquidityState: undefined,
        regimeState: undefined,
        fundamentalState: undefined,
        riskState: undefined,
        technicalState: undefined,
      },
      rankingTableFilters: {
        valuationSignal: 'expensive_or',
        warningSignal: 'sma5_weak_0_1',
        maxSectorScore: 0.4,
      },
    });
    expect(getRankingPreset(next.rankingTableFilters)).toBe('overvalued_breakdown');
  });

  it('treats presets as names for filter-only combinations', () => {
    expect(
      getRankingPreset({
        regimeState: 'neutral_rerating',
        technicalState: 'atr20_acceleration',
        valuationSignal: 'deep_value',
        minLiquidityZ: -1,
        maxLiquidityZ: 2,
      })
    ).toBe('core_long');
    expect(
      getRankingPreset({
        regimeState: 'neutral_rerating',
        technicalState: 'atr20_acceleration',
        valuationSignal: 'deep_value',
        warningSignal: 'overheat',
        minLiquidityZ: -1,
        maxLiquidityZ: 2,
      })
    ).toBe('custom');
  });

  it('describes preset conditions for UI disclosure', () => {
    expect(getRankingPresetDescription('earnings_priority')).toContain('Fwd OP/OP');
    expect(getRankingPresetDescription('aggressive_rerating')).toContain('z 1..2');
    expect(getRankingPresetDescription('overvalued_breakdown')).toContain('SMA5 Weak 0/1');
  });
});
