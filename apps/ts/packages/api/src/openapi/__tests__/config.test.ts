import { describe, expect, test } from 'bun:test';
import { API_VERSION, openapiConfig, scalarConfig } from '../config';

describe('OpenAPI config', () => {
  test('API_VERSION is a semver string', () => {
    expect(API_VERSION).toMatch(/^\d+\.\d+\.\d+$/);
  });

  test('openapiConfig has valid structure', () => {
    expect(openapiConfig.openapi).toBe('3.1.0');
    expect(openapiConfig.info.title).toBe('Trading25 API');
    expect(openapiConfig.info.version).toBe(API_VERSION);
    expect(openapiConfig.servers.length).toBeGreaterThan(0);
    expect(openapiConfig.tags.length).toBeGreaterThan(0);
  });

  test('scalarConfig has required properties', () => {
    expect(scalarConfig.spec.url).toBe('/openapi.json');
    expect(scalarConfig.theme).toBe('default');
    expect(scalarConfig.layout).toBe('modern');
  });

  test('all tags have name and description', () => {
    for (const tag of openapiConfig.tags) {
      expect(tag.name).toBeDefined();
      expect(tag.description).toBeDefined();
      expect(tag.name.length).toBeGreaterThan(0);
    }
  });
});
