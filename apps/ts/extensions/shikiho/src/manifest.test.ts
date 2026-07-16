import { describe, expect, test } from 'bun:test';
import manifest from '../manifest.json';

describe('Shikiho extension manifest', () => {
  test('injects both bridges at document_start', () => {
    const shikiho = manifest.content_scripts.find((script) => script.js.includes('shikiho-content.js'));
    const localhost = manifest.content_scripts.find((script) => script.js.includes('localhost-content.js'));

    expect(shikiho?.run_at).toBe('document_start');
    expect(localhost?.run_at).toBe('document_start');
  });
});
