import { describe, expect, test } from 'bun:test';
import {
  normalizeShikihoCode,
  parseShikihoBridgeResponse,
  parseShikihoDiagnostic,
  parseShikihoSnapshot,
  SHIKIHO_BRIDGE_CHANNEL,
  type ShikihoBridgeRequestV1,
} from './contract';

function validQuote(overrides: Record<string, unknown> = {}) {
  return {
    tradingDate: '2026-07-10',
    observedAt: '2026-07-10T14:45:00+09:00',
    delayMinutes: 15,
    currentPrice: 102,
    open: 100,
    high: 105,
    low: 98,
    previousClose: 99,
    volume: 12_300,
    openTime: '09:00',
    highTime: '13:20',
    lowTime: null,
    sourceLabel: '会社四季報オンライン',
    ...overrides,
  };
}

function validSnapshot(overrides: Record<string, unknown> = {}) {
  return {
    schemaVersion: 1,
    extractorVersion: '1.0.0',
    code: '7203',
    companyName: 'トヨタ自動車',
    sourceUrl: 'https://shikiho.toyokeizai.net/stocks/7203',
    capturedAt: '2026-07-10T01:02:03.000Z',
    pageUpdatedAt: '2026-07-09T00:00:00+09:00',
    editionLabel: '2026年3集',
    contentHash: 'sha256:example',
    status: 'captured',
    features: '4輪世界首位',
    consolidatedBusinesses: '自動車事業',
    commentary: [{ heading: '連続減益', body: '原価低減を進める。' }],
    score: {
      overall: 4,
      growth: 5,
      profitability: 5,
      safety: 2,
      scale: 5,
      value: 3,
      priceMomentum: null,
    },
    comparisonCompanies: [{ code: '7201', name: '日産自動車' }],
    industries: ['自動車'],
    marketThemes: ['EV'],
    profile: [{ label: '本社', value: '愛知県豊田市' }],
    missingFields: [],
    ...overrides,
  };
}

function validBridgeResponse(overrides: Record<string, unknown> = {}) {
  return {
    channel: 'trading25.shikiho.v1',
    direction: 'extension-to-page',
    type: 'snapshot',
    requestId: 'request-1',
    code: '7203',
    snapshot: validSnapshot(),
    diagnostic: null,
    ...overrides,
  };
}

describe('Shikiho bridge contract', () => {
  test('requires forceRefresh on get-snapshot page requests', () => {
    const request: ShikihoBridgeRequestV1 = {
      channel: SHIKIHO_BRIDGE_CHANNEL,
      direction: 'page-to-extension',
      type: 'get_snapshot',
      requestId: 'request-1',
      code: '7203',
      forceRefresh: false,
    };

    expect(request).toEqual({
      channel: SHIKIHO_BRIDGE_CHANNEL,
      direction: 'page-to-extension',
      type: 'get_snapshot',
      requestId: 'request-1',
      code: '7203',
      forceRefresh: false,
    });
  });

  test('normalizes compatible stock codes', () => {
    expect(normalizeShikihoCode('7203')).toBe('7203');
    expect(normalizeShikihoCode('72030')).toBe('7203');
    expect(normalizeShikihoCode('720A')).toBeNull();
  });

  test('rejects foreign hosts and code/source mismatches', () => {
    expect(parseShikihoSnapshot(validSnapshot({ sourceUrl: 'https://example.com/stocks/7203' }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ code: '6758' }))).toBeNull();
  });

  test('accepts only extension-to-page protocol messages', () => {
    expect(parseShikihoBridgeResponse(validBridgeResponse())).not.toBeNull();
    expect(parseShikihoBridgeResponse({ ...validBridgeResponse(), direction: 'page-to-extension' })).toBeNull();
  });

  test('validates snapshot versions, timestamps, scores, strings, and lists', () => {
    expect(parseShikihoSnapshot(validSnapshot())).not.toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ schemaVersion: 2 }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ capturedAt: 'yesterday' }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ score: { ...validSnapshot().score, overall: 5.1 } }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ features: 'x'.repeat(4097) }))).toBeNull();
    expect(
      parseShikihoSnapshot(validSnapshot({ industries: Array.from({ length: 101 }, () => 'industry') }))
    ).toBeNull();
  });

  test('strictly validates an optional delayed quote and its exact keys', () => {
    expect(parseShikihoSnapshot(validSnapshot())).not.toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: validQuote() }))).not.toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: { ...validQuote(), code: '7203' } }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: { ...validQuote(), extra: true } }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: { ...validQuote(), delayMinutes: 10 } }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: { ...validQuote(), sourceLabel: '別ソース' } }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: { ...validQuote(), tradingDate: '2026-02-30' } }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: { ...validQuote(), observedAt: 'not-a-time' } }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: { ...validQuote(), openTime: '9:00' } }))).toBeNull();
  });

  test('requires finite positive prices, nullable nonnegative volume, nullable times, and OHLC invariants', () => {
    for (const [key, value] of [
      ['currentPrice', 0],
      ['open', -1],
      ['high', Number.POSITIVE_INFINITY],
      ['low', Number.NaN],
      ['previousClose', 0],
    ] as const) {
      expect(parseShikihoSnapshot(validSnapshot({ quote: validQuote({ [key]: value }) }))).toBeNull();
    }
    expect(parseShikihoSnapshot(validSnapshot({ quote: validQuote({ volume: null }) }))).not.toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: validQuote({ volume: -1 }) }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: validQuote({ volume: Number.POSITIVE_INFINITY }) }))).toBeNull();
    expect(
      parseShikihoSnapshot(validSnapshot({ quote: validQuote({ highTime: null, lowTime: '12:34' }) }))
    ).not.toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: validQuote({ currentPrice: 106 }) }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: validQuote({ currentPrice: 97 }) }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: validQuote({ open: 106 }) }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ quote: validQuote({ open: 97 }) }))).toBeNull();
  });

  test('rejects impossible calendar timestamps while accepting ISO offsets', () => {
    expect(parseShikihoSnapshot(validSnapshot({ capturedAt: '2026-02-31T00:00:00Z' }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ capturedAt: '2024-02-29T23:59:59+09:00' }))).not.toBeNull();
    expect(
      parseShikihoDiagnostic({
        schemaVersion: 1,
        code: '7203',
        observedAt: '2025-02-29T00:00:00-05:00',
        status: 'page_changed',
      })
    ).toBeNull();
  });

  test('limits ISO timestamp offsets to the real-world range through plus or minus fourteen hours', () => {
    expect(parseShikihoSnapshot(validSnapshot({ capturedAt: '2026-07-10T01:02:03+14:00' }))).not.toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ capturedAt: '2026-07-10T01:02:03-14:00' }))).not.toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ capturedAt: '2026-07-10T01:02:03+23:00' }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ capturedAt: '2026-07-10T01:02:03+14:01' }))).toBeNull();
    expect(
      parseShikihoSnapshot(validSnapshot({ quote: validQuote({ observedAt: '2026-07-10T14:45:00+14:01' }) }))
    ).toBeNull();
  });

  test('rejects serialized snapshots above 64 KiB', () => {
    const profile = Array.from({ length: 20 }, (_, index) => ({
      label: `label-${index}-${'x'.repeat(2000)}`,
      value: `value-${index}-${'x'.repeat(2000)}`,
    }));
    expect(parseShikihoSnapshot(validSnapshot({ profile }))).toBeNull();
  });

  test('validates capture diagnostics', () => {
    expect(
      parseShikihoDiagnostic({
        schemaVersion: 1,
        code: '7203',
        observedAt: '2026-07-10T01:02:03.000Z',
        status: 'page_changed',
      })
    ).not.toBeNull();
    expect(
      parseShikihoDiagnostic({
        schemaVersion: 1,
        code: '72030',
        observedAt: '2026-07-10T01:02:03.000Z',
        status: 'page_changed',
      })
    ).toBeNull();
  });

  test('requires response code and nested records to agree', () => {
    expect(parseShikihoBridgeResponse(validBridgeResponse({ code: '6758' }))).toBeNull();
    expect(
      parseShikihoBridgeResponse(
        validBridgeResponse({
          snapshot: null,
          diagnostic: {
            schemaVersion: 1,
            code: '6758',
            observedAt: '2026-07-10T01:02:03.000Z',
            status: 'page_changed',
          },
        })
      )
    ).toBeNull();
  });
});
