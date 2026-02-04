import * as fs from 'node:fs';
import * as path from 'node:path';
import { fileURLToPath } from 'node:url';

export interface MarginData {
  date: string;
  code: string;
  shortMarginTradeVolume: number;
  longMarginTradeVolume: number;
  shortNegotiableMarginTradeVolume: number;
  longNegotiableMarginTradeVolume: number;
  shortStandardizedMarginTradeVolume: number;
  longStandardizedMarginTradeVolume: number;
  issueType: string;
}

let cachedMarginData: MarginData[] | null = null;

function parseMarginDataLine(line: string): MarginData | null {
  const parts = line.split(',');
  const date = parts[0];
  if (!date) return null;

  return {
    date,
    code: parts[1] ?? '',
    shortMarginTradeVolume: Number.parseFloat(parts[2] ?? '0') || 0,
    longMarginTradeVolume: Number.parseFloat(parts[3] ?? '0') || 0,
    shortNegotiableMarginTradeVolume: Number.parseFloat(parts[4] ?? '0') || 0,
    longNegotiableMarginTradeVolume: Number.parseFloat(parts[5] ?? '0') || 0,
    shortStandardizedMarginTradeVolume: Number.parseFloat(parts[6] ?? '0') || 0,
    longStandardizedMarginTradeVolume: Number.parseFloat(parts[7] ?? '0') || 0,
    issueType: parts[8] ?? '',
  };
}

export function loadToyotaMarginData(): MarginData[] {
  if (cachedMarginData) return cachedMarginData;

  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const csvPath = path.join(__dirname, 'toyota_7203_margin.csv');
  const csvContent = fs.readFileSync(csvPath, 'utf-8');
  const lines = csvContent.split('\n').slice(1); // Skip header

  const parsedData = lines
    .filter((line) => line.trim())
    .map(parseMarginDataLine)
    .filter((d): d is MarginData => d !== null);

  cachedMarginData = parsedData;
  return parsedData;
}

export function getShortMarginVolumes(): number[] {
  return loadToyotaMarginData().map((d) => d.shortMarginTradeVolume);
}

export function getLongMarginVolumes(): number[] {
  return loadToyotaMarginData().map((d) => d.longMarginTradeVolume);
}

export function getShortNegotiableMarginVolumes(): number[] {
  return loadToyotaMarginData().map((d) => d.shortNegotiableMarginTradeVolume);
}

export function getLongNegotiableMarginVolumes(): number[] {
  return loadToyotaMarginData().map((d) => d.longNegotiableMarginTradeVolume);
}

export function getShortStandardizedMarginVolumes(): number[] {
  return loadToyotaMarginData().map((d) => d.shortStandardizedMarginTradeVolume);
}

export function getLongStandardizedMarginVolumes(): number[] {
  return loadToyotaMarginData().map((d) => d.longStandardizedMarginTradeVolume);
}

/**
 * Calculate margin ratio (short margin / long margin)
 * Higher values indicate more bearish sentiment
 */
export function getMarginRatios(): number[] {
  return loadToyotaMarginData().map((d) => {
    if (d.longMarginTradeVolume === 0) return 0;
    return d.shortMarginTradeVolume / d.longMarginTradeVolume;
  });
}

/**
 * Calculate total margin volume (short + long)
 */
export function getTotalMarginVolumes(): number[] {
  return loadToyotaMarginData().map((d) => d.shortMarginTradeVolume + d.longMarginTradeVolume);
}

/**
 * Calculate net margin position (long - short)
 * Positive values indicate bullish bias, negative indicates bearish bias
 */
export function getNetMarginPositions(): number[] {
  return loadToyotaMarginData().map((d) => d.longMarginTradeVolume - d.shortMarginTradeVolume);
}

/**
 * Get margin data formatted for margin-volume-ratio calculation
 */
export function getLongMarginVolumeData(): Array<{ date: string; marginVolume: number }> {
  return loadToyotaMarginData().map((d) => ({
    date: d.date,
    marginVolume: d.longMarginTradeVolume,
  }));
}

/**
 * Get short margin data formatted for margin-volume-ratio calculation
 */
export function getShortMarginVolumeData(): Array<{ date: string; marginVolume: number }> {
  return loadToyotaMarginData().map((d) => ({
    date: d.date,
    marginVolume: d.shortMarginTradeVolume,
  }));
}
