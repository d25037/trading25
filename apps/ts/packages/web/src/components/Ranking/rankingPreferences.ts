import { useCallback, useEffect, useState } from 'react';
import type { RankingParams } from '@/types/ranking';

export type SectorStrengthFamily = NonNullable<RankingParams['sectorStrengthFamily']>;

export const DEFAULT_SECTOR_STRENGTH_FAMILY: SectorStrengthFamily = 'balanced_sector_strength';

const SECTOR_STRENGTH_FAMILY_VALUES = new Set<string>(['balanced_sector_strength', 'long_hybrid_leadership']);

function isSectorStrengthFamily(value: unknown): value is SectorStrengthFamily {
  return typeof value === 'string' && SECTOR_STRENGTH_FAMILY_VALUES.has(value);
}

function readStoredSectorStrengthFamily(storageKey: string | undefined): SectorStrengthFamily | null {
  if (!storageKey || typeof window === 'undefined') return null;
  try {
    const storedValue = window.localStorage.getItem(storageKey);
    return isSectorStrengthFamily(storedValue) ? storedValue : null;
  } catch {
    return null;
  }
}

function writeStoredSectorStrengthFamily(storageKey: string | undefined, value: SectorStrengthFamily): void {
  if (!storageKey || typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(storageKey, value);
  } catch {
    // Keep the control usable when localStorage is unavailable.
  }
}

export function usePersistentSectorStrengthFamily(
  storageKey: string | undefined,
  initialValue: SectorStrengthFamily = DEFAULT_SECTOR_STRENGTH_FAMILY
): readonly [SectorStrengthFamily, (value: SectorStrengthFamily) => void] {
  const [value, setValue] = useState<SectorStrengthFamily>(
    () => readStoredSectorStrengthFamily(storageKey) ?? initialValue
  );

  useEffect(() => {
    setValue(readStoredSectorStrengthFamily(storageKey) ?? initialValue);
  }, [initialValue, storageKey]);

  const setPersistentValue = useCallback(
    (nextValue: SectorStrengthFamily) => {
      setValue(nextValue);
      writeStoredSectorStrengthFamily(storageKey, nextValue);
    },
    [storageKey]
  );

  return [value, setPersistentValue] as const;
}
