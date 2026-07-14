import { describe, expect, test } from 'bun:test';
import {
  normalizeShikihoCode,
  parseShikihoBridgeResponse,
  parseShikihoCaptureProgress,
  parseShikihoCaptureTrace,
  parseShikihoDiagnostic,
  parseShikihoSnapshot,
  SHIKIHO_BRIDGE_CHANNEL,
  type ShikihoBridgeRequestV1,
  type ShikihoBridgeResponseV1,
  type ShikihoCaptureProgressV1,
  type ShikihoCaptureTraceV1,
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
    trace: validTrace(),
    ...overrides,
  };
}

function validTrace(overrides: Record<string, unknown> = {}): ShikihoCaptureTraceV1 {
  return {
    schemaVersion: 1,
    attemptId: 'attempt-1',
    code: '7203',
    mode: 'new_owned_tab',
    phase: 'observing_dom',
    startedAt: '2026-07-14T00:00:00.000Z',
    updatedAt: '2026-07-14T00:00:03.000Z',
    outcome: null,
    waitEndReason: null,
    receiverAttempts: 3,
    receiverReadyMs: 210,
    documentReadyState: 'interactive',
    navigation: { responseStartMs: 80, domInteractiveMs: 900, domContentLoadedMs: null, loadEndMs: null },
    dom: {
      firstSampleMs: 230,
      mutationBatches: 40,
      meaningfulChanges: 2,
      samples: 4,
      presentFields: ['identity', 'features'],
      missingFields: ['consolidatedBusinesses', 'commentary'],
      firstSeenMs: {
        identity: 230,
        quote: null,
        features: 1100,
        consolidatedBusinesses: null,
        commentary: null,
        score: null,
        comparisonCompanies: null,
        industries: null,
        marketThemes: null,
        profile: null,
        editionLabel: null,
        pageUpdatedAt: null,
        coreReady: null,
      },
    },
    extraction: { samples: 4, lastMs: 3, maxMs: 5, totalMs: 14 },
    timings: {
      probeMs: 20,
      acquisitionMs: 35,
      receiverMs: 210,
      domObservationMs: 2760,
      storageMs: 0,
      totalMs: 3000,
    },
    ...overrides,
  };
}

function validProgress(overrides: Record<string, unknown> = {}): ShikihoCaptureProgressV1 {
  return {
    schemaVersion: 1,
    attemptId: 'attempt-1',
    code: '7203',
    sequence: 1,
    candidate: validSnapshot({ status: 'partial' }),
    trace: validTrace(),
    ...overrides,
  } as ShikihoCaptureProgressV1;
}

function validProgressBridgeResponse(
  overrides: Record<string, unknown> = {}
): Extract<ShikihoBridgeResponseV1, { type: 'capture_progress' }> {
  const { schemaVersion: _schemaVersion, ...progress } = validProgress();
  return {
    channel: SHIKIHO_BRIDGE_CHANNEL,
    direction: 'extension-to-page',
    type: 'capture_progress',
    requestId: 'request-1',
    ...progress,
    ...overrides,
  } as Extract<ShikihoBridgeResponseV1, { type: 'capture_progress' }>;
}

function validMismatchedCandidate() {
  return validSnapshot({
    code: '6758',
    sourceUrl: 'https://shikiho.toyokeizai.net/stocks/6758',
  });
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
    expect(parseShikihoSnapshot(validSnapshot({ capturedAt: '2026-07-10T01:02:03+09:60' }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ capturedAt: '2026-07-10T01:02:03-05:99' }))).toBeNull();
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

  test('strictly validates metadata-only capture traces', () => {
    const trace = validTrace();

    expect(parseShikihoCaptureTrace(trace)).toEqual(trace);
    expect(parseShikihoCaptureTrace({ ...trace, code: '72030' })).toBeNull();
    expect(parseShikihoCaptureTrace({ ...trace, extra: true })).toBeNull();
    expect(parseShikihoCaptureTrace({ ...trace, receiverAttempts: -1 })).toBeNull();
    expect(
      parseShikihoCaptureTrace({
        ...trace,
        dom: { ...trace.dom, presentFields: ['features', 'features'] },
      })
    ).toBeNull();
  });

  test('rejects invalid fixed trace values and nested trace keys', () => {
    const trace = validTrace();

    expect(parseShikihoCaptureTrace({ ...trace, phase: 'fetching' })).toBeNull();
    expect(parseShikihoCaptureTrace({ ...trace, mode: 'arbitrary_tab' })).toBeNull();
    expect(parseShikihoCaptureTrace({ ...trace, documentReadyState: 'ready' })).toBeNull();
    expect(parseShikihoCaptureTrace({ ...trace, receiverReadyMs: Number.POSITIVE_INFINITY })).toBeNull();
    expect(parseShikihoCaptureTrace({ ...trace, navigation: { ...trace.navigation, extra: 1 } })).toBeNull();
    expect(
      parseShikihoCaptureTrace({
        ...trace,
        dom: { ...trace.dom, firstSeenMs: { ...trace.dom.firstSeenMs, unknown: null } },
      })
    ).toBeNull();
  });

  test('rejects trace milestones or pipeline phases beyond the shared total duration', () => {
    const trace = validTrace();

    expect(
      parseShikihoCaptureTrace({
        ...trace,
        dom: { ...trace.dom, firstSeenMs: { ...trace.dom.firstSeenMs, identity: trace.timings.totalMs + 1 } },
      })
    ).toBeNull();
    expect(
      parseShikihoCaptureTrace({
        ...trace,
        timings: { ...trace.timings, probeMs: trace.timings.totalMs + 1 },
      })
    ).toBeNull();
  });

  test('strictly validates capture progress identity, sequence, candidate, and trace agreement', () => {
    const progress = validProgress();
    const mismatchedCandidate = validMismatchedCandidate();

    expect(parseShikihoCaptureProgress(progress)).toEqual(progress);
    expect(parseShikihoSnapshot(mismatchedCandidate)).not.toBeNull();
    expect(parseShikihoCaptureProgress({ ...progress, extra: true })).toBeNull();
    expect(parseShikihoCaptureProgress({ ...progress, attemptId: '' })).toBeNull();
    expect(parseShikihoCaptureProgress({ ...progress, sequence: 0 })).toBeNull();
    expect(parseShikihoCaptureProgress({ ...progress, sequence: 1.5 })).toBeNull();
    expect(parseShikihoCaptureProgress({ ...progress, sequence: Number.MAX_SAFE_INTEGER + 1 })).toBeNull();
    expect(parseShikihoCaptureProgress({ ...progress, code: '6758' })).toBeNull();
    expect(parseShikihoCaptureProgress({ ...progress, attemptId: 'attempt-2' })).toBeNull();
    expect(parseShikihoCaptureProgress({ ...progress, candidate: mismatchedCandidate })).toBeNull();
    expect(parseShikihoCaptureProgress({ ...progress, candidate: null })).not.toBeNull();
  });

  test('validates exact public capture-progress and terminal snapshot response keys', () => {
    const progressResponse = validProgressBridgeResponse();
    const mismatchedCandidate = validMismatchedCandidate();

    expect(parseShikihoBridgeResponse(progressResponse)).toEqual(progressResponse);
    expect(parseShikihoSnapshot(mismatchedCandidate)).not.toBeNull();
    expect(parseShikihoBridgeResponse({ ...progressResponse, extra: true })).toBeNull();
    expect(parseShikihoBridgeResponse({ ...progressResponse, requestId: '' })).toBeNull();
    expect(parseShikihoBridgeResponse({ ...progressResponse, attemptId: 'attempt-2' })).toBeNull();
    expect(parseShikihoBridgeResponse({ ...progressResponse, code: '6758' })).toBeNull();
    expect(parseShikihoBridgeResponse({ ...progressResponse, candidate: mismatchedCandidate })).toBeNull();
    expect(parseShikihoBridgeResponse({ ...validBridgeResponse(), extra: true })).toBeNull();
    expect(parseShikihoBridgeResponse({ ...validBridgeResponse(), trace: null })).not.toBeNull();
    const { trace: _trace, ...missingTrace } = validBridgeResponse();
    expect(parseShikihoBridgeResponse(missingTrace)).toBeNull();
  });

  test('rejects public capture progress above 64 KiB', () => {
    const profile = Array.from({ length: 15 }, (_, index) => ({
      label: `label-${index}-${'x'.repeat(2120)}`,
      value: `value-${index}-${'x'.repeat(2120)}`,
    }));
    const candidate = validSnapshot({ status: 'partial', profile });
    expect(parseShikihoSnapshot(candidate)).not.toBeNull();
    expect(parseShikihoBridgeResponse(validProgressBridgeResponse({ candidate }))).toBeNull();
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
    expect(parseShikihoBridgeResponse(validBridgeResponse({ trace: validTrace({ code: '6758' }) }))).toBeNull();
  });
});
