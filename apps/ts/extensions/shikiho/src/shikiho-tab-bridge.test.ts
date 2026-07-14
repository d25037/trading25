import { describe, expect, mock, test } from 'bun:test';
import type { ShikihoExtractionResult } from './extractor';
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

function createHarness(overrides: Partial<ShikihoTabBridgeOptions> = {}) {
  let listener: RuntimeMessageListener | null = null;
  const removeMessageListener = mock((candidate: RuntimeMessageListener) => {
    if (listener === candidate) listener = null;
  });
  const options: ShikihoTabBridgeOptions = {
    getCode: () => '7203',
    capture: () => success7203,
    waitUntilReady: async () => undefined,
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

  test.each([null, '', 'stocks', '720A'])('returns null when the current path has no stock code: %p', async (code) => {
    const harness = createHarness({ getCode: () => code });

    expect(await harness.request({ type: 'probe_shikiho_code' })).toEqual({
      type: 'shikiho_code',
      code: null,
    });
    harness.stop();
  });

  test('captures directly and echoes only the validated request identity and result', async () => {
    const waitUntilReady = mock(async () => undefined);
    const harness = createHarness({ waitUntilReady });

    expect(
      await harness.request({
        type: 'capture_now',
        requestId: 'job-1',
        code: '7203',
        waitForReady: false,
      })
    ).toEqual({
      type: 'capture_result',
      requestId: 'job-1',
      code: '7203',
      result: success7203,
    });
    expect(waitUntilReady).not.toHaveBeenCalled();
    harness.stop();
  });

  test('waits for readiness before capture and rejects a symbol change during extraction', async () => {
    const calls: string[] = [];
    let code = '7203';
    const waitUntilReady = mock(async () => {
      calls.push('wait');
    });
    const capture = mock(async () => {
      calls.push('capture');
      code = '6758';
      return success7203;
    });
    const harness = createHarness({ capture, getCode: () => code, waitUntilReady });

    expect(
      await harness.request({
        type: 'capture_now',
        requestId: 'job-2',
        code: '7203',
        waitForReady: true,
      })
    ).toBeUndefined();
    expect(calls).toEqual(['wait', 'capture']);
    harness.stop();
  });

  test.each([
    { type: 'capture_now', requestId: '', code: '7203', waitForReady: false },
    { type: 'capture_now', requestId: 'x'.repeat(257), code: '7203', waitForReady: false },
    { type: 'capture_now', requestId: 'job-1', code: '72030', waitForReady: false },
    { type: 'capture_now', requestId: 'job-1', code: '7203', waitForReady: false, extra: true },
    { type: 'capture_now', requestId: 'job-1', code: '7203' },
    { type: 'probe_shikiho_code', extra: true },
    null,
  ])('ignores malformed requests: %p', async (request) => {
    const capture = mock(() => success7203);
    const harness = createHarness({ capture });

    expect(await harness.request(request)).toBeUndefined();
    expect(capture).not.toHaveBeenCalled();
    harness.stop();
  });
});
