/**
 * Dataset V2 - Configuration Helpers
 * Helper functions for working with configurations
 */

import type { DatasetConfig, MarketType } from '../types';

/**
 * Get market codes from market types - V1 compatibility helper
 */
export function getMarketCodes(markets: MarketType[]): string[] {
  const marketMap: Record<MarketType, string> = {
    prime: '0111',
    standard: '0112',
    growth: '0113',
  };

  return markets.map((market) => marketMap[market]);
}

/**
 * Get date range from configuration
 * Returns undefined if both startDate and endDate are not specified,
 * allowing JQuants API to use its default range based on subscription plan.
 */
export function getDateRange(config: DatasetConfig): { from: Date; to: Date } | undefined {
  // If neither date is specified, return undefined to let API decide
  if (!config.startDate && !config.endDate) {
    return undefined;
  }
  // If only one is specified, use defaults for the other
  return {
    from: config.startDate ?? new Date(Date.now() - 10 * 365 * 24 * 60 * 60 * 1000),
    to: config.endDate ?? new Date(),
  };
}
