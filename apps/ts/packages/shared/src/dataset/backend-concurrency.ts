/**
 * Dataset fetch concurrency resolver for bt-backed API access.
 * TS side no longer uses J-Quants plan limits directly.
 */

const DEFAULT_CONCURRENCY = 4;
const MAX_CONCURRENCY = 16;

function parsePositiveInt(value: string | undefined): number | null {
  if (!value) return null;
  const parsed = Number.parseInt(value, 10);
  if (Number.isNaN(parsed) || parsed <= 0) return null;
  return parsed;
}

export function resolveDatasetConcurrency(totalItems: number): number {
  const configured =
    parsePositiveInt(process.env.BT_DATASET_CONCURRENCY) ?? parsePositiveInt(process.env.DATASET_CONCURRENCY);
  const base = configured ?? DEFAULT_CONCURRENCY;
  const clamped = Math.min(base, MAX_CONCURRENCY);
  if (totalItems <= 0) {
    return 1;
  }
  return Math.max(1, Math.min(clamped, totalItems));
}

