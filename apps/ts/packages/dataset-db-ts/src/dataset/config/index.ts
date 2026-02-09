/**
 * Dataset - Configuration Index
 * Unified export point for all configuration functions
 */

// ===== Core Configuration Functions =====
export { createConfig } from './builder';
// ===== Custom Configuration Functions =====
export { createCustomConfig, createForDateRangeConfig } from './custom';
export { getDateRange, getMarketCodes } from './helpers';
// ===== Presets =====
export {
  DATASET_PRESET_NAMES,
  // Preset metadata
  type DatasetPreset,
  // Basic market coverage
  fullMarket,
  getPresetConfig,
  getPresetEstimatedTime,
  getPresetStockRange,
  isValidPreset,
  mid400,
  PRESET_METADATA,
  type PresetMetadata,
  // Preset collection
  presets,
  primeExTopix500,
  primeMarket,
  // Development & testing
  quickTesting,
  // Index-based
  topix100,
  topix500,
} from './presets';
export { validateConfig } from './validation';
