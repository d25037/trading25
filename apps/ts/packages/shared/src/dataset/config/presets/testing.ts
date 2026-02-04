/**
 * Dataset - Development & Testing Presets
 * quickTesting preset for CI/development
 *
 * Note: Date range is not specified - JQuants API automatically returns
 * the maximum available data based on subscription plan.
 */

import type { DatasetConfig } from '../../types';
import { createConfig } from '../builder';

/**
 * Quick testing (3 stocks)
 * Lightweight for rapid development and CI testing
 */
export function quickTesting(outputPath = './quick-testing.db'): DatasetConfig {
  return createConfig({
    outputPath,
    markets: ['prime'],
    maxStocks: 3,
    includeMargin: true,
    includeStatements: true,
    includeTOPIX: true,
    includeSectorIndices: true,
  });
}
