import type { StartSyncRequest, SyncDataBackend, SyncMode } from '../../utils/api-client.js';

export class SyncRequestValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SyncRequestValidationError';
  }
}

/**
 * Parse backend option
 */
export function parseDataBackend(value?: string): SyncDataBackend | undefined {
  if (!value) {
    return undefined;
  }

  const normalized = value.trim().toLowerCase();
  if (normalized === 'duckdb') {
    return 'duckdb-parquet';
  }
  if (normalized === 'duckdb-parquet') {
    return normalized;
  }

  throw new SyncRequestValidationError(`Invalid --data-backend: ${value}. Use duckdb-parquet`);
}

/**
 * Build sync request payload
 */
export function buildStartSyncRequest(
  mode: SyncMode,
  rawBackend?: string
): StartSyncRequest {
  const backend = parseDataBackend(rawBackend);
  if (!backend) {
    return { mode };
  }

  return {
    mode,
    dataPlane: {
      backend,
    },
  };
}
