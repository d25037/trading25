import * as fs from 'node:fs';
import * as path from 'node:path';
import { fileURLToPath } from 'node:url';

export interface OHLCData {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

let cachedData: OHLCData[] | null = null;

export function loadToyotaData(): OHLCData[] {
  if (cachedData) return cachedData;

  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const csvPath = path.join(__dirname, 'toyota_7203_daily.csv');
  const csvContent = fs.readFileSync(csvPath, 'utf-8');
  const lines = csvContent.split('\n').slice(1); // Skip header

  const parsedData = lines
    .filter((line) => line.trim())
    .map((line) => {
      const parts = line.split(',');
      const date = parts[0];
      if (!date) return null;

      return {
        date,
        open: Number.parseFloat(parts[2] ?? '0') || 0,
        high: Number.parseFloat(parts[3] ?? '0') || 0,
        low: Number.parseFloat(parts[4] ?? '0') || 0,
        close: Number.parseFloat(parts[5] ?? '0') || 0,
        volume: Number.parseFloat(parts[6] ?? '0') || 0,
      };
    })
    .filter((d): d is OHLCData => d !== null && d.close > 0);

  cachedData = parsedData;
  return parsedData;
}

export function getClosePrices(): number[] {
  return loadToyotaData().map((d) => d.close);
}

export function getHighPrices(): number[] {
  return loadToyotaData().map((d) => d.high);
}

export function getLowPrices(): number[] {
  return loadToyotaData().map((d) => d.low);
}

export function getVolumes(): number[] {
  return loadToyotaData().map((d) => d.volume);
}

/**
 * Get daily volume data formatted for margin-volume-ratio calculation
 */
export function getDailyVolumeData(): Array<{ date: string; volume: number }> {
  return loadToyotaData().map((d) => ({
    date: d.date,
    volume: d.volume,
  }));
}
