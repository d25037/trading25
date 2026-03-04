/**
 * Dataset V2 - Default Configuration
 * Base configuration values and defaults
 *
 * Note: Date range (startDate/endDate) is intentionally omitted.
 * JQuants API automatically returns the maximum available data
 * based on subscription plan when from/to parameters are not specified.
 */

import type { DatasetConfig } from '../types';

/**
 * Base default configuration used across all presets
 */
export const DEFAULT_CONFIG: DatasetConfig = {
  outputPath: './dataset.db',
  markets: ['prime'],
  includeMargin: false, // V1 default was false
  includeTOPIX: true,
  includeSectorIndices: true,
  includeStatements: true, // V1 default was true
  // startDate/endDate intentionally omitted - let JQuants API decide based on plan
  // ETF and sector filtering - globally applied to prevent fetching ETFs
  excludeEmptySector33: true, // Always exclude stocks without sector33Code
  excludeSectorCodes: ['9999'], // Always exclude ETFs (sector code 9999)
};
