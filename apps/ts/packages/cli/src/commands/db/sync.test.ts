import { describe, expect, it } from 'bun:test';
import { buildStartSyncRequest, parseDataBackend } from './sync-request.js';

describe('db sync command helpers', () => {
  it('parses backend aliases', () => {
    expect(parseDataBackend(undefined)).toBeUndefined();
    expect(parseDataBackend('duckdb')).toBe('duckdb-parquet');
    expect(parseDataBackend('DUCKDB-PARQUET')).toBe('duckdb-parquet');
  });

  it('throws for unsupported backend values', () => {
    expect(() => parseDataBackend('postgres')).toThrow('Invalid --data-backend');
  });

  it('builds minimal request without data-plane options', () => {
    expect(buildStartSyncRequest('auto')).toEqual({ mode: 'auto' });
  });

  it('builds request for explicit backend override', () => {
    expect(buildStartSyncRequest('initial', 'duckdb-parquet')).toEqual({
      mode: 'initial',
      dataPlane: {
        backend: 'duckdb-parquet',
      },
    });
  });

  it('rejects unsupported backend values', () => {
    expect(() => buildStartSyncRequest('auto', 'sqlite')).toThrow('Invalid --data-backend');
  });
});
