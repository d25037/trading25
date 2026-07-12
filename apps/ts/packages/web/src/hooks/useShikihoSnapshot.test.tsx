import { act, renderHook } from '@testing-library/react';
import {
  SHIKIHO_BRIDGE_CHANNEL,
  type ShikihoBridgeRequestV1,
  type ShikihoCaptureDiagnosticV1,
  type ShikihoSnapshotV1,
} from '@trading25/shikiho-extension/contract';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { selectShikihoSnapshotState, useShikihoSnapshot } from './useShikihoSnapshot';

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
  diagnosticValue: ShikihoCaptureDiagnosticV1 | null
) {
  return {
    channel: SHIKIHO_BRIDGE_CHANNEL,
    direction: 'extension-to-page' as const,
    type: 'snapshot' as const,
    requestId,
    code,
    snapshot: snapshotValue,
    diagnostic: diagnosticValue,
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
    const { rerender } = renderHook(({ symbol }) => useShikihoSnapshot(symbol), {
      initialProps: { symbol: '7203' },
    });
    rerender({ symbol: '6758' });

    const snapshotRequests = pageRequests(postMessage).filter((message) => message.type === 'get_snapshot');
    expect(snapshotRequests).toHaveLength(2);
    expect(snapshotRequests).toMatchObject([
      { code: '7203', forceRefresh: false },
      { code: '6758', forceRefresh: false },
    ]);
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
