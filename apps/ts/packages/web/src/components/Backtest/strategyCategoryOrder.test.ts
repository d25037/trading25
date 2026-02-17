import { describe, expect, it } from 'vitest';
import {
  compareManagedStrategyCategory,
  isManagedStrategyCategory,
  MANAGED_STRATEGY_CATEGORIES,
} from './strategyCategoryOrder';

describe('strategyCategoryOrder', () => {
  it('exposes managed categories in expected order', () => {
    expect(MANAGED_STRATEGY_CATEGORIES).toEqual(['production', 'experimental', 'legacy']);
  });

  it('detects managed categories', () => {
    expect(isManagedStrategyCategory('production')).toBe(true);
    expect(isManagedStrategyCategory('experimental')).toBe(true);
    expect(isManagedStrategyCategory('legacy')).toBe(true);
    expect(isManagedStrategyCategory('reference')).toBe(false);
  });

  it('sorts two managed categories by configured order', () => {
    expect(compareManagedStrategyCategory('production', 'legacy')).toBeLessThan(0);
    expect(compareManagedStrategyCategory('legacy', 'experimental')).toBeGreaterThan(0);
  });

  it('puts managed category before unmanaged category', () => {
    expect(compareManagedStrategyCategory('experimental', 'reference')).toBeLessThan(0);
    expect(compareManagedStrategyCategory('reference', 'legacy')).toBeGreaterThan(0);
  });

  it('falls back to localeCompare for two unmanaged categories', () => {
    expect(compareManagedStrategyCategory('alpha', 'beta')).toBeLessThan(0);
    expect(compareManagedStrategyCategory('beta', 'alpha')).toBeGreaterThan(0);
  });
});
