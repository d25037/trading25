import { describe, expect, test } from 'bun:test';
import { createCaptureProgressBroker, type ListenerEvent, type ProgressPort } from './capture-progress';
import type { ShikihoCaptureProgressV1, ShikihoCaptureTraceV1, ShikihoSnapshotV1 } from './contract';

function trace(overrides: Partial<ShikihoCaptureTraceV1> = {}): ShikihoCaptureTraceV1 {
  return {
    schemaVersion: 1,
    attemptId: 'a1',
    code: '7203',
    mode: 'new_owned_tab',
    phase: 'observing_dom',
    startedAt: '1970-01-01T00:00:00.100Z',
    updatedAt: '2026-07-14T00:00:01.000Z',
    outcome: null,
    waitEndReason: null,
    receiverAttempts: 0,
    receiverReadyMs: null,
    documentReadyState: 'interactive',
    navigation: {
      responseStartMs: 1,
      domInteractiveMs: 2,
      domContentLoadedMs: null,
      loadEndMs: null,
    },
    dom: {
      firstSampleMs: 5,
      mutationBatches: 0,
      meaningfulChanges: 1,
      samples: 1,
      presentFields: ['identity'],
      missingFields: [],
      firstSeenMs: {
        identity: 5,
        quote: null,
        features: null,
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
    extraction: { samples: 1, lastMs: 1, maxMs: 1, totalMs: 1 },
    timings: {
      probeMs: 1,
      acquisitionMs: 2,
      receiverMs: 0,
      domObservationMs: 4,
      storageMs: 0,
      totalMs: 7,
    },
    ...overrides,
  };
}

function candidate(code = '7203'): ShikihoSnapshotV1 {
  return {
    schemaVersion: 1,
    extractorVersion: '1.0.0',
    code,
    companyName: `Company ${code}`,
    sourceUrl: `https://shikiho.toyokeizai.net/stocks/${code}`,
    capturedAt: '2026-07-14T00:00:01.000Z',
    pageUpdatedAt: null,
    editionLabel: null,
    contentHash: `sha256:${code}`,
    status: 'partial',
    features: 'provisional candidate',
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
    missingFields: ['consolidatedBusinesses'],
  };
}

function progress(overrides: Partial<ShikihoCaptureProgressV1> = {}): ShikihoCaptureProgressV1 {
  const attemptId = overrides.attemptId ?? 'a1';
  const code = overrides.code ?? '7203';
  return {
    schemaVersion: 1,
    attemptId,
    code,
    sequence: 1,
    candidate: null,
    trace: trace({ attemptId, code }),
    ...overrides,
  };
}

function listenerEvent<TListener extends (...args: never[]) => void>() {
  const listeners = new Set<TListener>();
  const event: ListenerEvent<TListener> = {
    addListener(listener) {
      listeners.add(listener);
    },
    removeListener(listener) {
      listeners.delete(listener);
    },
  };
  return { event, listeners };
}

function portHarness() {
  type MessageListener = (message: unknown) => void;
  type DisconnectListener = () => void;
  const messages = listenerEvent<MessageListener>();
  const disconnects = listenerEvent<DisconnectListener>();
  const posted: unknown[] = [];
  const port: ProgressPort = {
    postMessage(message) {
      posted.push(structuredClone(message));
    },
    onMessage: messages.event,
    onDisconnect: disconnects.event,
  };
  return {
    port,
    posted,
    send(message: unknown) {
      for (const listener of messages.listeners) listener(message);
    },
    disconnect() {
      for (const listener of [...disconnects.listeners]) listener();
    },
    listenerCounts() {
      return { messages: messages.listeners.size, disconnects: disconnects.listeners.size };
    },
  };
}

function harness() {
  const saved: ShikihoCaptureTraceV1[] = [];
  return {
    saved,
    broker: createCaptureProgressBroker({
      async saveTrace(value) {
        saved.push(structuredClone(value));
      },
    }),
  };
}

describe('capture progress broker', () => {
  test('accepts only the active tab, code, and a fresh sequence', async () => {
    const h = harness();
    h.broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });

    expect(await h.broker.acceptContentProgress(progress(), 41)).toBe(true);
    expect(await h.broker.acceptContentProgress(progress(), 41)).toBe(false);
    expect(await h.broker.acceptContentProgress(progress({ sequence: 2 }), 42)).toBe(false);
    expect(
      await h.broker.acceptContentProgress(progress({ code: '6758', sequence: 2, trace: trace({ code: '6758' }) }), 41)
    ).toBe(false);
  });

  test('merges trusted acquisition metadata and receiver attempts into accepted progress', async () => {
    const h = harness();
    const port = portHarness();
    h.broker.attachPort(port.port);
    port.send({ type: 'subscribe_capture_progress', code: '7203' });
    h.broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'exact_user_tab', startedAtMs: 100 });
    h.broker.recordReceiverAttempt('a1', 25);
    h.broker.recordReceiverAttempt('a1', 125);

    expect(await h.broker.acceptContentProgress(progress({ candidate: candidate() }), 41)).toBe(true);
    expect(port.posted).toEqual([
      {
        type: 'capture_progress',
        progress: progress({
          candidate: candidate(),
          trace: trace({
            mode: 'exact_user_tab',
            startedAt: '1970-01-01T00:00:00.100Z',
            receiverAttempts: 2,
            receiverReadyMs: 125,
            timings: { ...trace().timings, receiverMs: 125 },
          }),
        }),
      },
    ]);
  });

  test('sends progress only to ports subscribed with the exact shape and matching code', async () => {
    const h = harness();
    const matching = portHarness();
    const other = portHarness();
    const invalid = portHarness();
    h.broker.attachPort(matching.port);
    h.broker.attachPort(other.port);
    h.broker.attachPort(invalid.port);
    matching.send({ type: 'subscribe_capture_progress', code: '7203' });
    other.send({ type: 'subscribe_capture_progress', code: '6758' });
    invalid.send({ type: 'subscribe_capture_progress', code: '7203', extra: true });
    h.broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });

    await h.broker.acceptContentProgress(progress({ candidate: candidate() }), 41);

    expect(matching.posted).toHaveLength(1);
    expect(other.posted).toHaveLength(0);
    expect(invalid.posted).toHaveLength(0);
  });

  test('removes disconnected ports and explicit cleanup is idempotent', async () => {
    const h = harness();
    const port = portHarness();
    const cleanup = h.broker.attachPort(port.port);
    port.send({ type: 'subscribe_capture_progress', code: '7203' });
    port.disconnect();
    port.disconnect();
    cleanup();
    cleanup();
    h.broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });

    await h.broker.acceptContentProgress(progress(), 41);

    expect(port.posted).toHaveLength(0);
    expect(port.listenerCounts()).toEqual({ messages: 0, disconnects: 0 });
  });

  test('terminal finish persists metadata only and removes the active attempt', async () => {
    const h = harness();
    const provisional = candidate();
    h.broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });
    await h.broker.acceptContentProgress(progress({ candidate: provisional }), 41);
    const terminal = trace({ phase: 'complete', outcome: 'success', waitEndReason: 'field_stable' });

    await h.broker.finishAttempt('a1', terminal);

    expect(h.saved).toEqual([terminal]);
    expect(JSON.stringify(h.saved)).not.toContain('provisional candidate');
    expect(await h.broker.acceptContentProgress(progress({ sequence: 2 }), 41)).toBe(false);
  });

  test('abandon removes the active attempt without persisting a trace', async () => {
    const h = harness();
    h.broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });

    h.broker.abandonAttempt('a1');

    expect(await h.broker.acceptContentProgress(progress(), 41)).toBe(false);
    expect(h.saved).toHaveLength(0);
  });
});
