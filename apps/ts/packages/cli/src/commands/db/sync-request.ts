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
  if (normalized === 'default' || normalized === 'duckdb-parquet' || normalized === 'sqlite') {
    return normalized;
  }

  throw new SyncRequestValidationError(
    `Invalid --data-backend: ${value}. Use default | duckdb-parquet | sqlite`
  );
}

/**
 * Build sync request payload
 */
export function buildStartSyncRequest(
  mode: SyncMode,
  rawBackend?: string,
  sqliteMirror?: boolean,
  noSqliteMirror?: boolean
): StartSyncRequest {
  if (sqliteMirror && noSqliteMirror) {
    throw new SyncRequestValidationError('Cannot use --sqlite-mirror and --no-sqlite-mirror together');
  }

  const backend = parseDataBackend(rawBackend);
  const mirrorOverride = sqliteMirror ? true : noSqliteMirror ? false : undefined;

  if (!backend && mirrorOverride === undefined) {
    return { mode };
  }

  if (backend === 'sqlite' && mirrorOverride !== undefined) {
    throw new SyncRequestValidationError(
      '--sqlite-mirror / --no-sqlite-mirror cannot be used with --data-backend sqlite'
    );
  }

  return {
    mode,
    dataPlane: {
      backend: backend ?? 'default',
      ...(mirrorOverride !== undefined ? { sqliteMirror: mirrorOverride } : {}),
    },
  };
}
