import { act, renderHook } from '@testing-library/react';
import {
  SHIKIHO_BRIDGE_CHANNEL,
  type ShikihoBridgeRequestV1,
  type ShikihoCaptureDiagnosticV1,
  type ShikihoCaptureTraceV1,
  type ShikihoSnapshotV1,
} from '@trading25/shikiho-extension/contract';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { mergeShikihoDisplaySnapshot, selectShikihoSnapshotState, useShikihoSnapshot } from './useShikihoSnapshot';

const CAPTURED_AT = '2026-07-10T01:00:00.000Z';

function snapshot(code: string, overrides: Partial<ShikihoSnapshotV1> = {}): ShikihoSnapshotV1 {
  return {
    schemaVersion: 1,
    extractorVersion: '1.0.0',
    code,
    companyName: code === '7203' ? 'トヨタ自動車' : 'ソニーグループ',
    sourceUrl: `https://shikiho.toyokeizai.net/stocks/${code}`,
    capturedAt: CAPTURED_AT,
    pageUpdatedAt: null,
    editionLabel: null,
    contentHash: `sha256:${code}`,
    status: 'captured',
    features: '特色',
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
    ...overrides,
  };
}

function diagnostic(
  code: string,
  status: ShikihoCaptureDiagnosticV1['status'],
  observedAt = '2026-07-10T02:00:00.000Z'
): ShikihoCaptureDiagnosticV1 {
  return { schemaVersion: 1, code, observedAt, status };
}

function response(
  requestId: string,
  code: string,
  snapshotValue: ShikihoSnapshotV1 | null,
  diagnosticValue: ShikihoCaptureDiagnosticV1 | null,
  traceValue: ShikihoCaptureTraceV1 | null = null
) {
  return {
    channel: SHIKIHO_BRIDGE_CHANNEL,
    direction: 'extension-to-page' as const,
    type: 'snapshot' as const,
    requestId,
    code,
    snapshot: snapshotValue,
    diagnostic: diagnosticValue,
    trace: traceValue,
  };
}

function trace(attemptId: string, overrides: Partial<ShikihoCaptureTraceV1> = {}): ShikihoCaptureTraceV1 {
  return {
    schemaVersion: 1,
    attemptId,
    code: '7203',
    mode: 'exact_user_tab',
    phase: 'observing_dom',
    startedAt: CAPTURED_AT,
    updatedAt: '2026-07-10T01:00:01.000Z',
    outcome: null,
    waitEndReason: null,
    receiverAttempts: 1,
    receiverReadyMs: 100,
    documentReadyState: 'interactive',
    navigation: {
      responseStartMs: 10,
      domInteractiveMs: 90,
      domContentLoadedMs: null,
      loadEndMs: null,
    },
    dom: {
      firstSampleMs: 120,
      mutationBatches: 1,
      meaningfulChanges: 1,
      samples: 1,
      presentFields: ['identity', 'features'],
      missingFields: [
        'quote',
        'consolidatedBusinesses',
        'commentary',
        'score',
        'comparisonCompanies',
        'industries',
        'marketThemes',
        'profile',
        'editionLabel',
        'pageUpdatedAt',
        'coreReady',
      ],
      firstSeenMs: {
        identity: 120,
        quote: null,
        features: 120,
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
    extraction: { samples: 1, lastMs: 4, maxMs: 4, totalMs: 4 },
    timings: { probeMs: 5, acquisitionMs: 10, receiverMs: 100, domObservationMs: 20, storageMs: 0, totalMs: 140 },
    ...overrides,
  };
}

function progress(
  requestId: string,
  attemptId: string,
  sequence: number,
  candidate: ShikihoSnapshotV1 | null,
  traceValue = trace(attemptId)
) {
  return {
    channel: SHIKIHO_BRIDGE_CHANNEL,
    direction: 'extension-to-page' as const,
    type: 'capture_progress' as const,
    requestId,
    code: '7203',
    attemptId,
    sequence,
    candidate,
    trace: traceValue,
  };
}

function pageRequests(postMessage: ReturnType<typeof vi.spyOn>): ShikihoBridgeRequestV1[] {
  return postMessage.mock.calls.map((call: unknown[]) => call[0] as ShikihoBridgeRequestV1);
}

function lastSnapshotRequest(postMessage: ReturnType<typeof vi.spyOn>) {
  const request = pageRequests(postMessage).findLast((message) => message.type === 'get_snapshot');
  if (request?.type !== 'get_snapshot') throw new Error('No get_snapshot request was posted');
  return request;
}

function emitExtensionResponse(data: unknown, source: MessageEventSource | null = window): void {
  act(() => {
    window.dispatchEvent(new MessageEvent('message', { data, source }));
  });
}

describe('useShikihoSnapshot', () => {
  let postMessage: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    postMessage = vi.spyOn(window, 'postMessage').mockImplementation(() => undefined);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  test('merges only present candidate fields for panel display and keeps the stable quote', () => {
    const stableQuote = {
      tradingDate: '2026-07-10',
      observedAt: '2026-07-10T01:00:00.000Z',
      delayMinutes: 15 as const,
      currentPrice: 100,
      open: 99,
      high: 101,
      low: 98,
      previousClose: 97,
      volume: 1000,
      openTime: null,
      highTime: null,
      lowTime: null,
      sourceLabel: '会社四季報オンライン' as const,
    };
    const stable = snapshot('7203', {
      consolidatedBusinesses: 'stable businesses',
      quote: stableQuote,
    });
    const candidate = snapshot('7203', {
      status: 'partial',
      features: 'candidate features',
      consolidatedBusinesses: null,
      quote: { ...stableQuote, currentPrice: 999 },
      missingFields: ['consolidatedBusinesses'],
    });

    const display = mergeShikihoDisplaySnapshot(stable, candidate);

    expect(display?.features).toBe('candidate features');
    expect(display?.consolidatedBusinesses).toBe('stable businesses');
    expect(display?.quote).toEqual(stable.quote);
  });

  test('accepts monotonic progress, resets for a new attempt, and rejects retired attempt races', () => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    const request = lastSnapshotRequest(postMessage);
    const first = snapshot('7203', { status: 'partial', features: 'first', missingFields: ['commentary'] });
    const second = snapshot('7203', { status: 'partial', features: 'second', missingFields: ['commentary'] });

    emitExtensionResponse(progress(request.requestId, 'attempt-a', 2, second));
    emitExtensionResponse(progress(request.requestId, 'attempt-a', 1, first));
    expect(result.current.candidate?.features).toBe('second');

    emitExtensionResponse(progress(request.requestId, 'attempt-a', 3, null));
    expect(result.current.candidate?.features).toBe('second');

    const transientlyMissing = snapshot('7203', {
      status: 'partial',
      features: null,
      commentary: [{ heading: 'new', body: 'new commentary' }],
      missingFields: ['features'],
    });
    emitExtensionResponse(progress(request.requestId, 'attempt-a', 4, transientlyMissing));
    expect(result.current.candidate?.features).toBe('second');
    expect(result.current.candidate?.commentary).toEqual(transientlyMissing.commentary);

    emitExtensionResponse(progress(request.requestId, 'attempt-b', 2, first));
    expect(result.current.candidate?.features).toBe('first');
    expect(result.current.trace?.attemptId).toBe('attempt-b');

    emitExtensionResponse(progress(request.requestId, 'attempt-b', 3, second));
    expect(result.current.candidate?.features).toBe('second');
    expect(result.current.trace?.attemptId).toBe('attempt-b');

    emitExtensionResponse(progress(request.requestId, 'attempt-a', 5, first));
    expect(result.current.candidate?.features).toBe('second');
    expect(result.current.trace?.attemptId).toBe('attempt-b');
  });

  test('ignores stale request and symbol progress and clears candidates when the symbol changes', () => {
    const { result, rerender } = renderHook(({ symbol }) => useShikihoSnapshot(symbol), {
      initialProps: { symbol: '7203' },
    });
    const request7203 = lastSnapshotRequest(postMessage);
    emitExtensionResponse(progress(request7203.requestId, 'attempt-a', 1, snapshot('7203')));
    expect(result.current.candidate?.code).toBe('7203');

    rerender({ symbol: '6758' });
    expect(result.current.candidate).toBeNull();
    emitExtensionResponse(progress(request7203.requestId, 'attempt-a', 2, snapshot('7203')));
    expect(result.current.candidate).toBeNull();
  });

  test('keeps candidate content display-only and promotes only the terminal canonical snapshot', () => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    const request = lastSnapshotRequest(postMessage);
    const candidate = snapshot('7203', { status: 'partial', features: 'candidate', missingFields: ['commentary'] });
    emitExtensionResponse(progress(request.requestId, 'attempt-a', 1, candidate));

    expect(result.current.snapshot).toBeNull();
    expect(result.current.displaySnapshot?.features).toBe('candidate');

    const canonical = snapshot('7203', { features: 'canonical' });
    const completeTrace = trace('attempt-a', {
      phase: 'complete',
      outcome: 'success',
      waitEndReason: 'field_stable',
    });
    emitExtensionResponse(response(request.requestId, '7203', canonical, null, completeTrace));

    expect(result.current.snapshot).toEqual(canonical);
    expect(result.current.displaySnapshot).toEqual(canonical);
    expect(result.current.candidate).toBeNull();
    expect(result.current.isRefreshing).toBe(false);

    emitExtensionResponse(
      progress(request.requestId, 'attempt-a', 2, snapshot('7203', { status: 'partial', features: 'late candidate' }))
    );
    expect(result.current.snapshot).toEqual(canonical);
    expect(result.current.displaySnapshot).toEqual(canonical);
    expect(result.current.candidate).toBeNull();
    expect(result.current.trace).toEqual(completeTrace);
  });

  test.each([
    ['timeout', 'deadline'],
    ['error', 'error'],
  ] as const)('discards a candidate on terminal %s while preserving the stable snapshot', (outcome, reason) => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    const initialRequest = lastSnapshotRequest(postMessage);
    const stable = snapshot('7203', { features: 'stable' });
    emitExtensionResponse(response(initialRequest.requestId, '7203', stable, null));

    act(() => result.current.refresh());
    const refreshRequest = lastSnapshotRequest(postMessage);
    emitExtensionResponse(
      progress(
        refreshRequest.requestId,
        'attempt-failure',
        1,
        snapshot('7203', { status: 'partial', features: 'candidate', missingFields: ['commentary'] })
      )
    );
    const terminalTrace = trace('attempt-failure', {
      phase: outcome,
      outcome,
      waitEndReason: reason,
    });
    emitExtensionResponse(response(refreshRequest.requestId, '7203', null, null, terminalTrace));

    expect(result.current.snapshot).toEqual(stable);
    expect(result.current.displaySnapshot).toEqual(stable);
    expect(result.current.candidate).toBeNull();
    expect(result.current.trace).toEqual(terminalTrace);
    expect(result.current.isRefreshing).toBe(false);
  });

  test('accepts the current request terminal fallback when its stored trace belongs to an older attempt', () => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    const request = lastSnapshotRequest(postMessage);
    emitExtensionResponse(
      progress(request.requestId, 'attempt-abandoned', 1, snapshot('7203', { features: 'candidate' }))
    );
    const fallback = snapshot('7203', { features: 'canonical fallback' });
    const storedTrace = trace('attempt-older', {
      phase: 'timeout',
      outcome: 'timeout',
      waitEndReason: 'deadline',
    });

    emitExtensionResponse(response(request.requestId, '7203', fallback, null, storedTrace));

    expect(result.current.snapshot).toEqual(fallback);
    expect(result.current.displaySnapshot).toEqual(fallback);
    expect(result.current.candidate).toBeNull();
    expect(result.current.trace).toEqual(storedTrace);
    expect(result.current.isRefreshing).toBe(false);
  });

  test('synchronously masks bridge data owned by a previously selected code', () => {
    const previousSnapshot = snapshot('7203');
    const previousDiagnostic = diagnostic('7203', 'page_changed');

    expect(
      selectShikihoSnapshotState('6758', 'available', {
        ownerCode: '7203',
        snapshot: previousSnapshot,
        diagnostic: previousDiagnostic,
      })
    ).toEqual({
      bridgeStatus: 'available',
      snapshot: null,
      displaySnapshot: null,
      candidate: null,
      trace: null,
      diagnostic: null,
      captureState: 'checking_extension',
    });
  });

  test('pings the extension and requests the normalized selected code', () => {
    renderHook(() => useShikihoSnapshot('72030'));

    const requests = pageRequests(postMessage);
    expect(requests).toHaveLength(2);
    expect(requests[0]).toMatchObject({
      channel: SHIKIHO_BRIDGE_CHANNEL,
      direction: 'page-to-extension',
      type: 'ping',
    });
    expect(requests[1]).toMatchObject({
      channel: SHIKIHO_BRIDGE_CHANNEL,
      direction: 'page-to-extension',
      type: 'get_snapshot',
      code: '7203',
      forceRefresh: false,
    });
    expect(requests[0]?.requestId).toBe(requests[1]?.requestId);
  });

  test('requests once per symbol change without forcing refresh', () => {
    const { result, rerender } = renderHook(({ symbol }) => useShikihoSnapshot(symbol), {
      initialProps: { symbol: '4502' },
    });
    rerender({ symbol: '285A' });

    const snapshotRequests = pageRequests(postMessage).filter((message) => message.type === 'get_snapshot');
    expect(snapshotRequests).toHaveLength(2);
    expect(snapshotRequests).toMatchObject([
      { code: '4502', forceRefresh: false },
      { code: '285A', forceRefresh: false },
    ]);

    const currentRequest = lastSnapshotRequest(postMessage);
    emitExtensionResponse(response(currentRequest.requestId, '285A', null, null));
    expect(result.current.bridgeStatus).toBe('available');
    expect(result.current.captureState).toBe('not_captured');
  });

  test('forces refresh, keeps the prior snapshot visible, and ignores old request responses', () => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    const initialRequest = lastSnapshotRequest(postMessage);
    const previousSnapshot = snapshot('7203');
    emitExtensionResponse(response(initialRequest.requestId, '7203', previousSnapshot, null));

    expect(result.current.isRefreshing).toBe(false);
    act(() => result.current.refresh());
    const refreshRequest = lastSnapshotRequest(postMessage);
    expect(refreshRequest).toMatchObject({ code: '7203', forceRefresh: true });
    expect(refreshRequest.requestId).not.toBe(initialRequest.requestId);
    expect(result.current.isRefreshing).toBe(true);
    expect(result.current.snapshot).toEqual(previousSnapshot);

    emitExtensionResponse(
      response(initialRequest.requestId, '7203', snapshot('7203', { contentHash: 'sha256:old' }), null)
    );
    expect(result.current.isRefreshing).toBe(true);
    expect(result.current.snapshot).toEqual(previousSnapshot);

    const refreshedSnapshot = snapshot('7203', { contentHash: 'sha256:refreshed' });
    emitExtensionResponse(response(refreshRequest.requestId, '7203', refreshedSnapshot, null));
    expect(result.current.isRefreshing).toBe(false);
    expect(result.current.snapshot).toEqual(refreshedSnapshot);
  });

  test('does not complete refresh for a mismatched current code', () => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    act(() => result.current.refresh());
    const refreshRequest = lastSnapshotRequest(postMessage);

    emitExtensionResponse(response(refreshRequest.requestId, '6758', snapshot('6758'), null));
    expect(result.current.isRefreshing).toBe(true);
    expect(result.current.snapshot).toBeNull();
  });

  test('recognizes the explicit ping handshake even when an earlier extension announcement was missed', () => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    const request = lastSnapshotRequest(postMessage);

    expect(result.current.bridgeStatus).toBe('checking');
    emitExtensionResponse({
      channel: SHIKIHO_BRIDGE_CHANNEL,
      direction: 'extension-to-page',
      type: 'ready',
      requestId: request.requestId,
    });
    expect(result.current.bridgeStatus).toBe('available');
  });

  test('marks the extension unavailable after the handshake timeout', () => {
    vi.useFakeTimers();
    const { result } = renderHook(() => useShikihoSnapshot('7203'));

    expect(result.current.captureState).toBe('checking_extension');
    act(() => vi.runAllTimers());
    expect(result.current.bridgeStatus).toBe('unavailable');
    expect(result.current.captureState).toBe('extension_unavailable');
    expect(result.current.isRefreshing).toBe(false);
  });

  test('requests the selected code and accepts only its matching response', () => {
    const { result, rerender } = renderHook(({ symbol }) => useShikihoSnapshot(symbol), {
      initialProps: { symbol: '7203' },
    });
    const request7203 = lastSnapshotRequest(postMessage);

    rerender({ symbol: '6758' });
    emitExtensionResponse(response(request7203.requestId, '7203', snapshot('7203'), null));
    expect(result.current.snapshot).toBeNull();

    const request6758 = lastSnapshotRequest(postMessage);
    emitExtensionResponse(response(request6758.requestId, '6758', snapshot('6758'), null));
    expect(result.current.snapshot?.code).toBe('6758');
    expect(result.current.captureState).toBe('captured');
  });

  test('retains a valid snapshot when a newer diagnostic marks it stale', () => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    const request = lastSnapshotRequest(postMessage);
    const pageChanged = diagnostic('7203', 'page_changed');

    emitExtensionResponse(response(request.requestId, '7203', snapshot('7203'), pageChanged));
    expect(result.current.snapshot?.code).toBe('7203');
    expect(result.current.diagnostic).toEqual(pageChanged);
    expect(result.current.captureState).toBe('stale');
  });

  test('uses the valid snapshot status when its diagnostic is older', () => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    const request = lastSnapshotRequest(postMessage);
    const partial = snapshot('7203', { status: 'partial' });

    emitExtensionResponse(
      response(request.requestId, '7203', partial, diagnostic('7203', 'page_changed', '2026-07-09T23:00:00.000Z'))
    );
    expect(result.current.captureState).toBe('partial');
  });

  test.each([
    'login_required',
    'page_changed',
    'storage_error',
  ] as const)('maps a diagnostic without a snapshot to %s', (status) => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    const request = lastSnapshotRequest(postMessage);
    emitExtensionResponse(response(request.requestId, '7203', null, diagnostic('7203', status)));
    expect(result.current.captureState).toBe(status);
  });

  test('reports not captured for an empty matching response', () => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    const request = lastSnapshotRequest(postMessage);
    emitExtensionResponse(response(request.requestId, '7203', null, null));
    expect(result.current.bridgeStatus).toBe('available');
    expect(result.current.captureState).toBe('not_captured');
  });

  test('requires the current window source and extension-to-page protocol direction', () => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    const request = lastSnapshotRequest(postMessage);
    const valid = response(request.requestId, '7203', snapshot('7203'), null);

    emitExtensionResponse(valid, {} as WindowProxy);
    emitExtensionResponse({ ...valid, direction: 'page-to-extension' });
    expect(result.current.snapshot).toBeNull();

    emitExtensionResponse(valid);
    expect(result.current.snapshot?.code).toBe('7203');
  });

  test.each([
    ['schema', (value: ShikihoSnapshotV1) => ({ ...value, schemaVersion: 2 })],
    ['host', (value: ShikihoSnapshotV1) => ({ ...value, sourceUrl: 'https://example.com/stocks/7203' })],
    ['path', (value: ShikihoSnapshotV1) => ({ ...value, sourceUrl: 'https://shikiho.toyokeizai.net/stocks/6758' })],
    ['payload size', (value: ShikihoSnapshotV1) => ({ ...value, features: 'x'.repeat(70 * 1024) })],
  ] as const)('rejects an invalid %s without replacing a valid snapshot', (_label, mutate) => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    const request = lastSnapshotRequest(postMessage);
    const valid = snapshot('7203');
    emitExtensionResponse(response(request.requestId, '7203', valid, null));

    emitExtensionResponse(response(request.requestId, '7203', mutate(valid) as ShikihoSnapshotV1, null));
    expect(result.current.snapshot).toEqual(valid);
  });

  test('accepts repeated matching responses as live storage notifications', () => {
    const { result } = renderHook(() => useShikihoSnapshot('7203'));
    const request = lastSnapshotRequest(postMessage);
    emitExtensionResponse(response(request.requestId, '7203', null, null));

    const liveSnapshot = snapshot('7203', { capturedAt: '2026-07-10T03:00:00.000Z' });
    emitExtensionResponse(response(request.requestId, '7203', liveSnapshot, null));
    expect(result.current.snapshot).toEqual(liveSnapshot);
    expect(result.current.captureState).toBe('captured');
  });

  test('mounts one stable listener across symbol changes and removes it on cleanup', () => {
    const addEventListener = vi.spyOn(window, 'addEventListener');
    const removeEventListener = vi.spyOn(window, 'removeEventListener');
    const { rerender, unmount } = renderHook(({ symbol }) => useShikihoSnapshot(symbol), {
      initialProps: { symbol: '7203' },
    });

    rerender({ symbol: '6758' });
    expect(addEventListener.mock.calls.filter(([type]) => type === 'message')).toHaveLength(1);
    const listener = addEventListener.mock.calls.find(([type]) => type === 'message')?.[1];
    unmount();
    expect(removeEventListener).toHaveBeenCalledWith('message', listener);
  });
});
