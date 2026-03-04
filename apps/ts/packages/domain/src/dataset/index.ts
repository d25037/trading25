/**
 * Dataset - Simplified Public API
 * Main exports for the simplified dataset architecture
 */

// ===== CORE TYPES =====
export type {
  ApiError,
  BuildResult,
  DatabaseError,
  // Configuration types
  DatasetConfig,
  // Error types
  DatasetError,
  DatasetStats,
  DateRange,
  DebugConfig,
  MarginData,
  MarketType,
  ProgressCallback,
  // Progress and results
  ProgressInfo,
  SectorData,
  StatementsData,
  // Data types
  StockData,
  StockInfo,
  TopixData,
} from './types';

// ===== CORE CLIENTS =====

export { resolveDatasetConcurrency } from './backend-concurrency';
// Batch execution (request pacing is handled by bt backend and BatchExecutor)
export {
  BatchExecutor,
  categorizeErrorType,
  createBatchExecutor,
} from '@trading25/api-clients/base/BatchExecutor';
export { ApiClient } from './api-client';
// ===== CONFIGURATION =====
export {
  createConfig,
  // Custom configuration functions
  createCustomConfig,
  createForDateRangeConfig,
  DATASET_PRESET_NAMES,
  // Preset metadata
  type DatasetPreset,
  getDateRange as getDatasetDateRange,
  getMarketCodes,
  getPresetConfig,
  getPresetEstimatedTime,
  getPresetStockRange,
  isValidPreset,
  PRESET_METADATA,
  type PresetMetadata,
  presets,
  validateConfig,
} from './config';
// ===== PROGRESS REPORTING =====
export {
  ConsoleProgressFormatter,
  createConsoleProgressCallback,
  createSilentProgressCallback,
  MultiStageProgressTracker,
  ProgressTracker,
} from './progress';
// ===== STREAMING UTILITIES =====
export {
  type StreamConfig,
  StreamingFetchers,
  StreamingUtils,
  type StreamResult,
} from './streaming/memory-efficient-fetchers';
export {
  createDebugConfig,
  DEFAULT_DEBUG_CONFIG,
} from './types';
// ===== UTILITIES =====
export {
  // Array utilities
  chunkArray,
  createCustomDateRange,
  // Date utilities
  createDateRange,
  createErrorSummary,
  debounce,
  filterStocksByMarkets,
  filterStocksByScaleCategories,
  filterStocksBySectors,
  formatDateForApi,
  formatFileSize,
  generateUniqueFilename,
  getDateRangeStrings,
  getDaysInRange,
  // Market utilities
  getMarketCode,
  getMarketType,
  getUniqueValues,
  groupStocksByMarket,
  groupStocksBySector,
  isDateInRange,
  // Type guards
  isDefined,
  isNonEmptyArray,
  isNonEmptyString,
  isValidDateRange,
  isValidSectorCode,
  // Validation utilities
  isValidStockCode,
  measureTime,
  removeDuplicatesBy,
  // Error utilities
  safeJsonStringify,
  // String utilities
  sanitizeFilePath,
  // Performance utilities
  sleep,
} from './utils';
// ===== LEGACY VALIDATORS =====
export {
  validateDataArray,
  validateDatasetConfig,
  validateDatasetConsistency,
  validateDateRange,
  validateFilePath,
  validateSectorCode,
  validateStockCode,
} from './validators';
// ===== RUNTIME VALIDATORS =====
export {
  MarginDataSchema,
  MarketTypeSchema,
  SectorDataSchema,
  StatementsDataSchema,
  // Zod schemas (for advanced usage)
  StockDataSchema,
  StockInfoSchema,
  safeValidateStockDataArray,
  // Safe validation functions
  safeValidateStockInfo,
  TopixDataSchema,
  validateMarginData,
  validateMarketType,
  validateSectorData,
  validateStatementsData,
  // Validation functions
  validateStockData,
  validateStockDataArray,
  validateStockInfo,
  validateStockInfoArray,
  validateTopixData,
} from './validators/runtime-validators';
