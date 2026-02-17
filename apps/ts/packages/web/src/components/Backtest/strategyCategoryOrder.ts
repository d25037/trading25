import type { StrategyMoveTargetCategory } from '@/types/backtest';

export const MANAGED_STRATEGY_CATEGORIES: readonly StrategyMoveTargetCategory[] = [
  'production',
  'experimental',
  'legacy',
];

export function isManagedStrategyCategory(category: string): category is StrategyMoveTargetCategory {
  return MANAGED_STRATEGY_CATEGORIES.includes(category as StrategyMoveTargetCategory);
}

export function compareManagedStrategyCategory(a: string, b: string): number {
  const aIndex = MANAGED_STRATEGY_CATEGORIES.indexOf(a as StrategyMoveTargetCategory);
  const bIndex = MANAGED_STRATEGY_CATEGORIES.indexOf(b as StrategyMoveTargetCategory);
  const aManaged = aIndex !== -1;
  const bManaged = bIndex !== -1;

  if (aManaged && bManaged) return aIndex - bIndex;
  if (aManaged) return -1;
  if (bManaged) return 1;
  return a.localeCompare(b);
}
