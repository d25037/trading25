import { describe, expect, it } from 'bun:test';
import { buildStartSyncRequest, parseDataBackend } from './sync-request.js';

describe('db sync command helpers', () => {
  it('parses backend aliases', () => {
    expect(parseDataBackend(undefined)).toBeUndefined();
    expect(parseDataBackend('duckdb')).toBe('duckdb-parquet');
    expect(parseDataBackend('DUCKDB-PARQUET')).toBe('duckdb-parquet');
    expect(parseDataBackend('default')).toBe('default');
    expect(parseDataBackend('sqlite')).toBe('sqlite');
  });

  it('throws for unsupported backend values', () => {
    expect(() => parseDataBackend('postgres')).toThrow('Invalid --data-backend');
  });

  it('builds minimal request without data-plane options', () => {
    expect(buildStartSyncRequest('auto')).toEqual({ mode: 'auto' });
  });

  it('builds request with mirror override only', () => {
    expect(buildStartSyncRequest('incremental', undefined, undefined, true)).toEqual({
      mode: 'incremental',
      dataPlane: {
        backend: 'default',
        sqliteMirror: false,
      },
    });
  });

  it('builds request for explicit backend override', () => {
    expect(buildStartSyncRequest('initial', 'duckdb-parquet', true, undefined)).toEqual({
      mode: 'initial',
      dataPlane: {
        backend: 'duckdb-parquet',
        sqliteMirror: true,
      },
    });
  });

  it('rejects incompatible mirror flags', () => {
    expect(() => buildStartSyncRequest('auto', undefined, true, true)).toThrow(
      'Cannot use --sqlite-mirror and --no-sqlite-mirror together'
    );
  });

  it('rejects mirror override with sqlite backend', () => {
    expect(() => buildStartSyncRequest('auto', 'sqlite', true, undefined)).toThrow(
      '--sqlite-mirror / --no-sqlite-mirror cannot be used with --data-backend sqlite'
    );
  });
});
