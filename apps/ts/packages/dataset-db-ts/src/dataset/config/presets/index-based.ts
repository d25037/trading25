/**
 * Dataset - Index-Based Presets
 * TOPIX-based presets (topix100, topix500, mid400, primeExTopix500)
 *
 * Note: Date range is not specified - JQuants API automatically returns
 * the maximum available data based on subscription plan.
 */

import type { DatasetConfig } from '../../types';
import { createConfig } from '../builder';

/**
 * TOPIX 100 stocks (Core30 + Large70) - V1 equivalent: topix100
 */
export function topix100(outputPath = './topix-100.db'): DatasetConfig {
  return createConfig({
    outputPath,
    markets: ['prime', 'standard', 'growth'],
    includeMargin: true,
    includeStatements: true,
    includeTOPIX: true,
    includeSectorIndices: true,
    scaleCategories: ['TOPIX Core30', 'TOPIX Large70'],
  });
}

/**
 * TOPIX Mid400 stocks - V1 equivalent: mid400
 */
export function mid400(outputPath = './mid-400.db'): DatasetConfig {
  return createConfig({
    outputPath,
    markets: ['prime', 'standard', 'growth'],
    includeMargin: true,
    includeStatements: true,
    includeTOPIX: true,
    includeSectorIndices: true,
    scaleCategories: ['TOPIX Mid400'],
  });
}

/**
 * TOPIX 500 stocks (Core30 + Large70 + Mid400)
 * Suitable for analysis of large-cap stocks with TOPIX reference
 */
export function topix500(outputPath = './topix-500.db'): DatasetConfig {
  return createConfig({
    outputPath,
    markets: ['prime', 'standard', 'growth'],
    includeMargin: true,
    includeStatements: true,
    includeTOPIX: true,
    includeSectorIndices: true,
    scaleCategories: ['TOPIX Core30', 'TOPIX Large70', 'TOPIX Mid400'],
  });
}

/**
 * Prime market excluding TOPIX 500 (smaller cap Prime stocks)
 * Focuses on smaller Prime market stocks outside the major indices
 */
export function primeExTopix500(outputPath = './prime-ex-topix500.db'): DatasetConfig {
  return createConfig({
    outputPath,
    markets: ['prime'],
    includeMargin: true,
    includeStatements: true,
    includeTOPIX: true,
    includeSectorIndices: true,
    excludeScaleCategories: ['TOPIX Core30', 'TOPIX Large70', 'TOPIX Mid400'],
  });
}
