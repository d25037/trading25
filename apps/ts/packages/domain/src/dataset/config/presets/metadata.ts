/**
 * Dataset - Preset Metadata
 * Centralized metadata for all dataset presets
 */

import type { DatasetConfig } from '../../types';
import { fullMarket, growthMarket, primeMarket, standardMarket } from './basic';
import { mid400, primeExTopix500, topix100, topix500 } from './index-based';
import { quickTesting } from './testing';

/**
 * All available preset names as a const array (single source of truth)
 */
export const DATASET_PRESET_NAMES = [
  'fullMarket',
  'primeMarket',
  'standardMarket',
  'growthMarket',
  'quickTesting',
  'topix100',
  'topix500',
  'mid400',
  'primeExTopix500',
] as const;

/**
 * Preset type derived from DATASET_PRESET_NAMES
 */
export type DatasetPreset = (typeof DATASET_PRESET_NAMES)[number];

/**
 * Metadata for a dataset preset
 */
export interface PresetMetadata {
  /** Factory function to create the dataset config */
  factory: (outputPath: string) => DatasetConfig;
  /** Estimated time to complete dataset creation */
  estimatedTime: string;
  /** Expected stock count range for validation */
  stockRange: { min: number; max: number };
  /** Human-readable description */
  description: string;
}

/**
 * Centralized preset metadata
 * Single source of truth for all preset configurations
 */
export const PRESET_METADATA: Record<DatasetPreset, PresetMetadata> = {
  fullMarket: {
    factory: fullMarket,
    estimatedTime: '30-60 minutes',
    stockRange: { min: 3500, max: 4500 },
    description: 'Full market coverage including all TSE stocks',
  },
  primeMarket: {
    factory: primeMarket,
    estimatedTime: '20-40 minutes',
    stockRange: { min: 1600, max: 2000 },
    description: 'Prime market stocks only',
  },
  standardMarket: {
    factory: standardMarket,
    estimatedTime: '15-30 minutes',
    stockRange: { min: 1300, max: 1700 },
    description: 'Standard market stocks only',
  },
  growthMarket: {
    factory: growthMarket,
    estimatedTime: '10-25 minutes',
    stockRange: { min: 500, max: 800 },
    description: 'Growth market stocks only',
  },
  quickTesting: {
    factory: quickTesting,
    estimatedTime: '1-3 minutes',
    stockRange: { min: 5, max: 50 },
    description: 'Small dataset for quick testing and development',
  },
  topix100: {
    factory: topix100,
    estimatedTime: '10-20 minutes',
    stockRange: { min: 90, max: 110 },
    description: 'TOPIX 100 index constituents',
  },
  topix500: {
    factory: topix500,
    estimatedTime: '10-20 minutes',
    stockRange: { min: 450, max: 550 },
    description: 'TOPIX 500 index constituents',
  },
  mid400: {
    factory: mid400,
    estimatedTime: '10-20 minutes',
    stockRange: { min: 350, max: 450 },
    description: 'TOPIX Mid400 index constituents',
  },
  primeExTopix500: {
    factory: primeExTopix500,
    estimatedTime: '10-30 minutes',
    stockRange: { min: 1100, max: 1500 },
    description: 'Prime market stocks excluding TOPIX 500',
  },
};

/**
 * Get dataset config for a preset
 * @param preset - Preset name
 * @param outputPath - Output file path
 * @returns Dataset configuration with preset name included
 * @throws Error if preset is unknown
 */
export function getPresetConfig(preset: DatasetPreset, outputPath: string): DatasetConfig {
  const metadata = PRESET_METADATA[preset];
  if (!metadata) {
    throw new Error(`Unknown preset: ${preset}`);
  }
  return { ...metadata.factory(outputPath), preset };
}

/**
 * Get estimated time for a preset
 * @param preset - Preset name
 * @returns Estimated time string
 */
export function getPresetEstimatedTime(preset: DatasetPreset): string {
  return PRESET_METADATA[preset]?.estimatedTime ?? '10-30 minutes';
}

/**
 * Get expected stock range for a preset
 * @param preset - Preset name
 * @returns Stock range or null if preset is unknown
 */
export function getPresetStockRange(preset: DatasetPreset): { min: number; max: number } | null {
  return PRESET_METADATA[preset]?.stockRange ?? null;
}

/**
 * Check if a string is a valid preset name
 * @param value - Value to check
 * @returns True if valid preset name
 */
export function isValidPreset(value: string): value is DatasetPreset {
  return value in PRESET_METADATA;
}
