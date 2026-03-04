/**
 * Dataset - All Presets Export
 * Unified export point for all preset configurations
 */

// Basic market coverage presets
export { fullMarket, growthMarket, primeMarket, standardMarket } from './basic';
// Index-based presets
export { mid400, primeExTopix500, topix100, topix500 } from './index-based';
// Preset metadata (centralized configuration)
export {
  DATASET_PRESET_NAMES,
  type DatasetPreset,
  getPresetConfig,
  getPresetEstimatedTime,
  getPresetStockRange,
  isValidPreset,
  PRESET_METADATA,
  type PresetMetadata,
} from './metadata';
// Development & testing presets
export { quickTesting } from './testing';

// Import all preset functions for the presets object
import { fullMarket, growthMarket, primeMarket, standardMarket } from './basic';
import { mid400, primeExTopix500, topix100, topix500 } from './index-based';
import { quickTesting } from './testing';

/**
 * All presets collected in one object for easy access
 */
export const presets = {
  // Basic market coverage
  fullMarket,
  primeMarket,
  standardMarket,
  growthMarket,

  // Development & testing
  quickTesting,

  // Index-based
  topix100,
  topix500,
  mid400,
  primeExTopix500,
};
