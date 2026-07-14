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

function acquisitionTrace(
  phase: 'queued' | 'probing_tabs' | 'acquiring_tab',
  outcome: 'timeout' | 'error' | null = null
): ShikihoCaptureTraceV1 {
  return trace({
    mode: 'acquisition_unbound',
    phase,
    outcome,
    waitEndReason: outcome === 'timeout' ? 'deadline' : outcome === 'error' ? 'error' : null,
    documentReadyState: null,
    navigation: { responseStartMs: null, domInteractiveMs: null, domContentLoadedMs: null, loadEndMs: null },
    dom: {
      firstSampleMs: null,
      mutationBatches: 0,
      meaningfulChanges: 0,
      samples: 0,
      presentFields: [],
      missingFields: [],
      firstSeenMs: Object.fromEntries(Object.keys(trace().dom.firstSeenMs).map((key) => [key, null])) as never,
    },
    extraction: { samples: 0, lastMs: null, maxMs: null, totalMs: 0 },
    timings: {
      probeMs: phase === 'probing_tabs' ? 7 : 0,
      acquisitionMs: phase === 'acquiring_tab' ? 7 : 0,
      receiverMs: 0,
      domObservationMs: 0,
      storageMs: 0,
      totalMs: 7,
    },
  });
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
  test('persists an unbound timeout and merges acquisition metadata when bound', async () => {
    const saved: ShikihoCaptureTraceV1[] = [];
    const broker = createCaptureProgressBroker({
      saveTrace: async (value) => {
        saved.push(value);
      },
    });
    broker.registerAcquisition({ attemptId: 'a1', code: '7203', startedAtMs: 100 });
    broker.updateAcquisition('a1', acquisitionTrace('probing_tabs'));
    await broker.finishAcquisition('a1', acquisitionTrace('probing_tabs', 'timeout'));
    expect(saved).toHaveLength(1);
    expect(saved[0]).toMatchObject({ mode: 'acquisition_unbound', phase: 'probing_tabs', outcome: 'timeout' });

    broker.registerAcquisition({ attemptId: 'a1', code: '7203', startedAtMs: 100 });
    broker.updateAcquisition('a1', acquisitionTrace('acquiring_tab'));
    broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });
    expect(await broker.acceptContentProgress(progress(), 41)).toBe(true);
  });
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

  test('rejects progress for an unregistered attempt ID', async () => {
    const h = harness();
    h.broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });

    expect(
      await h.broker.acceptContentProgress(progress({ attemptId: 'a2', trace: trace({ attemptId: 'a2' }) }), 41)
    ).toBe(false);
  });

  test('duplicate registration is idempotent and cannot reset sequence or trusted metadata', async () => {
    const h = harness();
    const registered = { attemptId: 'a1', tabId: 41, code: '7203', mode: 'exact_user_tab' as const, startedAtMs: 100 };
    h.broker.registerAttempt(registered);
    h.broker.recordReceiverAttempt('a1', 25);
    expect(await h.broker.acceptContentProgress(progress(), 41)).toBe(true);

    h.broker.registerAttempt(registered);
    h.broker.registerAttempt({
      attemptId: 'a1',
      tabId: 42,
      code: '6758',
      mode: 'warm_owned_navigated',
      startedAtMs: 200,
    });

    expect(await h.broker.acceptContentProgress(progress(), 41)).toBe(false);
    expect(await h.broker.acceptContentProgress(progress({ sequence: 2 }), 41)).toBe(true);
    const terminal = trace({ phase: 'complete', outcome: 'success', waitEndReason: 'field_stable' });
    await h.broker.finishAttempt('a1', terminal);
    expect(h.saved).toEqual([
      trace({
        phase: 'complete',
        outcome: 'success',
        waitEndReason: 'field_stable',
        mode: 'exact_user_tab',
        receiverAttempts: 1,
        receiverReadyMs: 25,
        timings: { ...trace().timings, receiverMs: 25, totalMs: 25 },
      }),
    ]);
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
            timings: { ...trace().timings, receiverMs: 125, totalMs: 125 },
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

  test('broadcasts the broker-monotonic trace instead of regressive raw progress metadata', async () => {
    const h = harness();
    const port = portHarness();
    h.broker.attachPort(port.port);
    port.send({ type: 'subscribe_capture_progress', code: '7203' });
    h.broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });
    const advanced = trace({
      timings: { ...trace().timings, totalMs: 25 },
      dom: {
        ...trace().dom,
        samples: 7,
        mutationBatches: 5,
        meaningfulChanges: 3,
        presentFields: ['identity', 'quote'],
        firstSeenMs: { ...trace().dom.firstSeenMs, quote: 25 },
      },
    });

    expect(await h.broker.acceptContentProgress(progress({ sequence: 1, trace: advanced }), 41)).toBe(true);
    expect(await h.broker.acceptContentProgress(progress({ sequence: 2, trace: trace() }), 41)).toBe(true);

    expect(port.posted[1]).toMatchObject({
      progress: {
        sequence: 2,
        candidate: null,
        trace: {
          dom: {
            samples: 7,
            mutationBatches: 5,
            meaningfulChanges: 3,
            presentFields: ['identity', 'quote'],
            firstSeenMs: { quote: 25 },
          },
        },
      },
    });
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

  test('mismatched finish identity leaves the attempt active for a later valid finish', async () => {
    const invalidTraces = [
      trace({ attemptId: 'a2', phase: 'complete', outcome: 'success', waitEndReason: 'field_stable' }),
      trace({ code: '6758', phase: 'complete', outcome: 'success', waitEndReason: 'field_stable' }),
    ];

    for (const invalid of invalidTraces) {
      const h = harness();
      h.broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });

      await h.broker.finishAttempt('a1', invalid);

      expect(h.saved).toHaveLength(0);
      expect(await h.broker.acceptContentProgress(progress(), 41)).toBe(true);
      const valid = trace({ phase: 'complete', outcome: 'success', waitEndReason: 'field_stable' });
      await h.broker.finishAttempt('a1', valid);
      expect(h.saved).toEqual([valid]);
    }
  });

  test('nonterminal or incompatible finish leaves the attempt active', async () => {
    const invalidTraces = [
      trace(),
      trace({ phase: 'complete', outcome: null }),
      trace({ phase: 'complete', outcome: 'timeout', waitEndReason: 'deadline' }),
      trace({ phase: 'complete', outcome: 'success', waitEndReason: 'deadline' }),
      trace({ phase: 'complete', outcome: 'partial', waitEndReason: 'field_stable' }),
      trace({ phase: 'timeout', outcome: 'success', waitEndReason: 'deadline' }),
      trace({ phase: 'timeout', outcome: 'timeout', waitEndReason: 'error' }),
      trace({ phase: 'error', outcome: 'partial', waitEndReason: 'error' }),
      trace({ phase: 'error', outcome: 'error', waitEndReason: 'deadline' }),
      trace({ phase: 'error', outcome: 'login_required', waitEndReason: 'deadline' }),
      trace({ phase: 'error', outcome: 'page_changed', waitEndReason: 'navigation_changed' }),
    ];

    for (const invalid of invalidTraces) {
      const h = harness();
      h.broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });

      await h.broker.finishAttempt('a1', invalid);

      expect(h.saved).toHaveLength(0);
      expect(await h.broker.acceptContentProgress(progress(), 41)).toBe(true);
      const valid = trace({ phase: 'complete', outcome: 'partial', waitEndReason: 'deadline' });
      await h.broker.finishAttempt('a1', valid);
      expect(h.saved).toEqual([valid]);
    }
  });

  test('persists timeout and error terminal traces and removes their attempts', async () => {
    const terminals: ShikihoCaptureTraceV1[] = [
      trace({ phase: 'timeout', outcome: 'timeout', waitEndReason: 'deadline' }),
      trace({ phase: 'error', outcome: 'error', waitEndReason: 'error' }),
      trace({ phase: 'error', outcome: 'error', waitEndReason: 'invalid_response' }),
      trace({ phase: 'error', outcome: 'login_required', waitEndReason: 'login_confirmed' }),
      trace({ phase: 'error', outcome: 'page_changed', waitEndReason: 'deadline' }),
    ];

    for (const terminal of terminals) {
      const h = harness();
      h.broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });

      await h.broker.finishAttempt('a1', terminal);

      expect(h.saved).toEqual([terminal]);
      expect(await h.broker.acceptContentProgress(progress(), 41)).toBe(false);
    }
  });

  test.each([
    ['timeout', 'timeout', 'deadline'],
    ['error', 'error', 'error'],
  ] as const)('keeps the latest accepted progress metrics when %s finishes in the background', async (phase, outcome, waitEndReason) => {
    const h = harness();
    h.broker.registerAttempt({
      attemptId: 'a1',
      tabId: 41,
      code: '7203',
      mode: 'new_owned_tab',
      startedAtMs: 100,
    });
    const latest = trace({
      updatedAt: '2026-07-14T00:00:02.000Z',
      dom: { ...trace().dom, mutationBatches: 4, meaningfulChanges: 3, samples: 5 },
      extraction: { samples: 5, lastMs: 2, maxMs: 4, totalMs: 12 },
    });
    expect(await h.broker.acceptContentProgress(progress({ trace: latest }), 41)).toBe(true);
    const terminal = trace({
      phase,
      outcome,
      waitEndReason,
      updatedAt: '2026-07-14T00:00:03.000Z',
    });

    await h.broker.finishAttempt('a1', terminal);

    expect(h.saved[0]).toMatchObject({
      phase,
      outcome,
      waitEndReason,
      updatedAt: terminal.updatedAt,
      dom: { mutationBatches: 4, meaningfulChanges: 3, samples: 5 },
      extraction: { samples: 5, lastMs: 2, maxMs: 4, totalMs: 12 },
    });
    expect(await h.broker.acceptContentProgress(progress({ sequence: 2 }), 41)).toBe(false);
  });

  test('persists a valid minimal terminal trace when no content progress was accepted', async () => {
    const h = harness();
    h.broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });
    const terminal = trace({ phase: 'timeout', outcome: 'timeout', waitEndReason: 'deadline' });

    await h.broker.finishAttempt('a1', terminal);

    expect(h.saved).toEqual([terminal]);
  });

  test.each([
    ['timeout', 'timeout', 'deadline'],
    ['error', 'error', 'error'],
  ] as const)('keeps metadata monotonic across regressive progress before %s persistence', async (phase, outcome, waitEndReason) => {
    const h = harness();
    h.broker.registerAttempt({
      attemptId: 'a1',
      tabId: 41,
      code: '7203',
      mode: 'new_owned_tab',
      startedAtMs: 100,
    });
    const firstSeenMs = { ...trace().dom.firstSeenMs, features: 20 };
    const advanced = trace({
      updatedAt: '2026-07-14T00:00:02.000Z',
      documentReadyState: 'complete',
      navigation: {
        responseStartMs: 1,
        domInteractiveMs: 2,
        domContentLoadedMs: 3,
        loadEndMs: 4,
      },
      dom: {
        ...trace().dom,
        mutationBatches: 8,
        meaningfulChanges: 6,
        samples: 9,
        presentFields: ['identity', 'features'],
        missingFields: [],
        firstSeenMs,
      },
      extraction: { samples: 9, lastMs: 7, maxMs: 8, totalMs: 40 },
      timings: {
        probeMs: 3,
        acquisitionMs: 5,
        receiverMs: 7,
        domObservationMs: 11,
        storageMs: 0,
        totalMs: 26,
      },
    });
    const regressive = trace({
      updatedAt: '2026-07-14T00:00:03.000Z',
      documentReadyState: 'loading',
      navigation: {
        responseStartMs: null,
        domInteractiveMs: null,
        domContentLoadedMs: null,
        loadEndMs: null,
      },
    });
    expect(await h.broker.acceptContentProgress(progress({ sequence: 1, trace: advanced }), 41)).toBe(true);
    expect(await h.broker.acceptContentProgress(progress({ sequence: 2, trace: regressive }), 41)).toBe(true);

    await h.broker.finishAttempt('a1', trace({ phase, outcome, waitEndReason, updatedAt: '2026-07-14T00:00:04.000Z' }));

    expect(h.saved[0]).toMatchObject({
      phase,
      outcome,
      waitEndReason,
      documentReadyState: 'complete',
      navigation: advanced.navigation,
      dom: {
        mutationBatches: 8,
        meaningfulChanges: 6,
        samples: 9,
        presentFields: ['identity', 'features'],
        firstSeenMs,
      },
      extraction: advanced.extraction,
      timings: advanced.timings,
    });
  });

  test('abandon removes the active attempt without persisting a trace', async () => {
    const h = harness();
    h.broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });

    h.broker.abandonAttempt('a1');

    expect(await h.broker.acceptContentProgress(progress(), 41)).toBe(false);
    expect(h.saved).toHaveLength(0);
  });
});
