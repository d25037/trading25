/**
 * Screening Module
 * Stock screening functionality for technical analysis
 */

// Re-export result types from detection modules
export type { RangeBreakResult } from './range-break-detection';
// Detection algorithms
export {
  analyzePriceStrength,
  calculateSupportResistance,
  detectRangeBreak,
  findMaxHighInRange,
  getRecentHighsLows,
  isRangeBreakAt,
} from './range-break-detection';
// Core engine
export { ScreeningEngine } from './screening-engine';
// Types
export type {
  DatabaseStockRow,
  FilterCriteria,
  FuturePricePoint,
  FutureReturns,
  RangeBreakDetails,
  RangeBreakParams,
  ScreeningConfig,
  ScreeningDateRange,
  ScreeningDetails,
  ScreeningInput,
  ScreeningResult,
  ScreeningType,
  StockDataPoint,
  VolumeAnalysis,
} from './types';
export { DEFAULT_SCREENING_CONFIG } from './types';
// Volume utilities
export {
  calculateVolumeMA,
  calculateVolumeStats,
  checkVolumeCondition,
  checkVolumeConditionInRange,
  getVolumeAnalysis,
  getVolumeDataInRange,
} from './volume-utils';
