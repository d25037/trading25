import { describe, expect, it } from 'vitest';
import { resolveBtApiUrl } from './btApiUrl';

describe('resolveBtApiUrl', () => {
  it('defaults to the local bt FastAPI URL', () => {
    expect(resolveBtApiUrl({})).toBe('http://localhost:3002');
  });

  it('uses BT_API_URL when provided', () => {
    expect(resolveBtApiUrl({ BT_API_URL: 'http://127.0.0.1:3999' })).toBe('http://127.0.0.1:3999');
  });

  it('does not treat API_BASE_URL as an alias', () => {
    expect(resolveBtApiUrl({ API_BASE_URL: 'http://legacy.example:3002' })).toBe('http://localhost:3002');
  });
});
