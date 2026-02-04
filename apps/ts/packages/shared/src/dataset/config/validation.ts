/**
 * Dataset V2 - Configuration Validation
 * Validation functions for dataset configurations
 */

import type { DatasetConfig, MarketType } from '../types';

/**
 * Validate a dataset configuration
 * Throws an error if the configuration is invalid
 */
export function validateConfig(config: DatasetConfig): void {
  if (!config.outputPath) {
    throw new Error('outputPath is required');
  }

  if (!config.markets || config.markets.length === 0) {
    throw new Error('At least one market must be specified');
  }

  const validMarkets: MarketType[] = ['prime', 'standard', 'growth'];
  for (const market of config.markets) {
    if (!validMarkets.includes(market)) {
      throw new Error(`Invalid market: ${market}. Valid markets: ${validMarkets.join(', ')}`);
    }
  }

  if (config.startDate && config.endDate && config.startDate >= config.endDate) {
    throw new Error('startDate must be before endDate');
  }

  if (config.maxStocks && config.maxStocks <= 0) {
    throw new Error('maxStocks must be positive');
  }
}
