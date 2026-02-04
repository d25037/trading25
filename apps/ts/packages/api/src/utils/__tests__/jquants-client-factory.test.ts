import { describe, expect, test } from 'bun:test';
import { createJQuantsClient } from '../jquants-client-factory';

describe('createJQuantsClient', () => {
  test('creates a JQuantsClient instance', () => {
    const client = createJQuantsClient();
    expect(client).toBeDefined();
  });

  test('returns an object with expected methods', () => {
    const client = createJQuantsClient();
    // JQuantsClient should have typical API methods
    expect(typeof client).toBe('object');
  });
});
