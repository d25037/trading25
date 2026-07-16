import { describe, expect, mock, test } from 'bun:test';
import type { ShikihoCaptureTraceV1 } from './contract';
import type { ShikihoExtractionResult } from './extractor';
import { ProgressiveCaptureCancelledError, type ProgressiveCaptureRequest } from './progressive-capture';
import { type RuntimeMessageListener, type ShikihoTabBridgeOptions, startShikihoTabBridge } from './shikiho-tab-bridge';

const success7203: ShikihoExtractionResult = {
  kind: 'success',
  snapshot: {
    schemaVersion: 1,
    extractorVersion: 'test',
    code: '7203',
    companyName: 'Toyota',
    sourceUrl: 'https://shikiho.toyokeizai.net/stocks/7203',
    capturedAt: '2026-07-14T00:00:00.000Z',
    pageUpdatedAt: null,
    editionLabel: null,
    earningsAnnouncementDate: null,
    contentHash: 'sha256:test',
    status: 'captured',
    features: null,
    consolidatedBusinesses: null,
    commentary: [],
    score: {
      overall: null,
      growth: null,
      profitability: null,
      safety: null,
      scale: null,
      value: null,
      priceMomentum: null,
    },
    comparisonCompanies: [],
    industries: [],
    marketThemes: [],
    profile: [],
    missingFields: [],
  },
};

const terminalTrace: ShikihoCaptureTraceV1 = {
  schemaVersion: 1,
  attemptId: 'attempt-1',
  code: '7203',
  mode: 'exact_user_tab',
  phase: 'complete',
  startedAt: '2026-07-14T00:00:00.000Z',
  updatedAt: '2026-07-14T00:00:01.000Z',
  outcome: 'success',
  waitEndReason: 'field_stable',
  receiverAttempts: 1,
  receiverReadyMs: 10,
  documentReadyState: 'complete',
  navigation: { responseStartMs: null, domInteractiveMs: null, domContentLoadedMs: null, loadEndMs: null },
  dom: {
    firstSampleMs: 0,
    mutationBatches: 0,
    meaningfulChanges: 1,
    samples: 2,
    presentFields: ['identity', 'features', 'consolidatedBusinesses', 'commentary', 'coreReady'],
    missingFields: [],
    firstSeenMs: {
      identity: 0,
      quote: null,
      features: 0,
      consolidatedBusinesses: 0,
      commentary: 0,
      score: null,
      comparisonCompanies: null,
      industries: null,
      marketThemes: null,
      profile: null,
      editionLabel: null,
      earningsAnnouncementDate: null,
      pageUpdatedAt: null,
      coreReady: 0,
    },
  },
  extraction: { samples: 2, lastMs: 1, maxMs: 1, totalMs: 2 },
  timings: { probeMs: 0, acquisitionMs: 0, receiverMs: 10, domObservationMs: 1000, storageMs: 0, totalMs: 1000 },
};

function captureRequest(overrides: Record<string, unknown> = {}) {
  return {
    type: 'capture_now',
    requestId: 'job-1',
    attemptId: 'attempt-1',
    code: '7203',
    mode: 'exact_user_tab',
    deadlineMs: Date.parse('2026-07-14T00:00:25.000Z'),
    receiverAttempts: 1,
    receiverReadyMs: 10,
    startedAtMs: Date.parse('2026-07-14T00:00:00.000Z'),
    probeMs: 2,
    acquisitionMs: 3,
    receiverMs: 5,
    ...overrides,
  };
}

function createHarness(overrides: Partial<ShikihoTabBridgeOptions> = {}) {
  let listener: RuntimeMessageListener | null = null;
  const removeMessageListener = mock((candidate: RuntimeMessageListener) => {
    if (listener === candidate) listener = null;
  });
  const options: ShikihoTabBridgeOptions = {
    getCode: () => '7203',
    capture: () => ({ result: success7203, trace: terminalTrace }),
    addMessageListener: (candidate) => {
      listener = candidate;
    },
    removeMessageListener,
    ...overrides,
  };
  const stop = startShikihoTabBridge(options);

  return {
    removeMessageListener,
    stop,
    request(message: unknown): Promise<unknown> {
      return new Promise((resolve) => {
        if (listener === null) {
          resolve(undefined);
          return;
        }
        let responded = false;
        const keepChannelOpen = listener(message, {}, (response) => {
          responded = true;
          resolve(response);
        });
        if (!responded && keepChannelOpen !== true) resolve(undefined);
      });
    },
  };
}

describe('Shikiho tab bridge', () => {
  test('probes only the normalized current stock code', async () => {
    const harness = createHarness({ getCode: () => '72030' });

    expect(await harness.request({ type: 'probe_shikiho_code' })).toEqual({
      type: 'shikiho_code',
      code: '7203',
    });
    harness.stop();
    expect(harness.removeMessageListener).toHaveBeenCalledTimes(1);
  });

  test.each([null, '', 'stocks', '72A3'])('returns null when the current path has no stock code: %p', async (code) => {
    const harness = createHarness({ getCode: () => code });

    expect(await harness.request({ type: 'probe_shikiho_code' })).toEqual({
      type: 'shikiho_code',
      code: null,
    });
    harness.stop();
  });

  test('passes exact attempt metadata and echoes terminal result, trace, and request identity', async () => {
    const capture = mock((_request: ProgressiveCaptureRequest) => ({ result: success7203, trace: terminalTrace }));
    const harness = createHarness({ capture });

    expect(await harness.request(captureRequest())).toEqual({
      type: 'capture_result',
      requestId: 'job-1',
      attemptId: 'attempt-1',
      code: '7203',
      result: success7203,
      trace: terminalTrace,
    });
    expect(capture).toHaveBeenCalledWith({
      attemptId: 'attempt-1',
      code: '7203',
      mode: 'exact_user_tab',
      deadlineMs: Date.parse('2026-07-14T00:00:25.000Z'),
      receiverAttempts: 1,
      receiverReadyMs: 10,
      startedAtMs: Date.parse('2026-07-14T00:00:00.000Z'),
      probeMs: 2,
      acquisitionMs: 3,
      receiverMs: 5,
    });
    harness.stop();
  });

  test('rejects symbol replacement during extraction without returning a diagnostic', async () => {
    let code = '7203';
    const capture = mock(async () => {
      code = '6758';
      throw new ProgressiveCaptureCancelledError();
    });
    const harness = createHarness({ capture, getCode: () => code });

    expect(await harness.request(captureRequest({ requestId: 'job-2' }))).toBeUndefined();
    harness.stop();
  });

  test.each([
    captureRequest({ requestId: '' }),
    captureRequest({ requestId: 'x'.repeat(257) }),
    captureRequest({ attemptId: '' }),
    captureRequest({ code: '72030' }),
    captureRequest({ mode: 'unknown' }),
    captureRequest({ deadlineMs: Number.NaN }),
    captureRequest({ receiverAttempts: -1 }),
    captureRequest({ receiverReadyMs: null }),
    captureRequest({ extra: true }),
    { type: 'capture_now', requestId: 'job-1', code: '7203' },
    { type: 'probe_shikiho_code', extra: true },
    null,
  ])('ignores malformed requests: %p', async (request) => {
    const capture = mock(() => ({ result: success7203, trace: terminalTrace }));
    const harness = createHarness({ capture });

    expect(await harness.request(request)).toBeUndefined();
    expect(capture).not.toHaveBeenCalled();
    harness.stop();
  });
});
