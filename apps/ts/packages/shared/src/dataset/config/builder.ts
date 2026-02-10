/**
 * Dataset V2 - Configuration Builder
 * Functions for creating and building dataset configurations
 */

import type { DatasetConfig } from '../types';
import { DEFAULT_CONFIG } from './defaults';

/**
 * Create a dataset configuration with given options
 * Merges provided options with default configuration
 */
export function createConfig(options: Partial<DatasetConfig> = {}): DatasetConfig {
  return {
    ...DEFAULT_CONFIG,
    ...options,
  };
}
