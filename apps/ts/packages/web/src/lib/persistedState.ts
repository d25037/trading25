export const CHART_STORE_STORAGE_KEY = 'trading25-chart-store';
export const UI_STORE_STORAGE_KEY = 'trading25-ui-store';
export const SCREENING_STORE_STORAGE_KEY = 'trading25-screening-store';
export const BACKTEST_STORE_STORAGE_KEY = 'trading25-backtest-store';
export const ACTIVE_SYNC_JOB_STORAGE_KEY = 'trading25.settings.sync.activeJobId';

export function readStoredString(storage: Storage, key: string): string | null {
  try {
    const value = storage.getItem(key);
    if (typeof value !== 'string') {
      return null;
    }
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  } catch {
    return null;
  }
}

export function writeStoredString(storage: Storage, key: string, value: string | null): void {
  try {
    if (value) {
      storage.setItem(key, value);
      return;
    }
    storage.removeItem(key);
  } catch {
    // Storage access can fail in restricted environments.
  }
}
