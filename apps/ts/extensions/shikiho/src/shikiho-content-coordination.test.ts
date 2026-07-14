import { describe, expect, mock, test } from 'bun:test';
import type { ShikihoExtractionResult } from './extractor';
import * as passiveCapture from './shikiho-passive-capture';

type CaptureLanes = {
  runExplicit<T>(attemptId: string, capture: () => Promise<T>): Promise<T>;
  publishPassive(publish: () => Promise<void>): Promise<boolean>;
};

type CaptureLaneFactory = () => CaptureLanes;
type PassivePublisher = (
  lanes: CaptureLanes,
  result: ShikihoExtractionResult,
  fallbackCode: string,
  observedAt: Date,
  sendMessage: (message: unknown) => Promise<unknown>
) => Promise<boolean>;

function captureLaneFactory(): CaptureLaneFactory {
  const factory = (passiveCapture as Record<string, unknown>).createShikihoCaptureLanes;
  expect(factory).toBeFunction();
  return factory as CaptureLaneFactory;
}

function passivePublisher(): PassivePublisher {
  const publish = (passiveCapture as Record<string, unknown>).publishPassiveShikihoResult;
  expect(publish).toBeFunction();
  return publish as PassivePublisher;
}

function deferred<T>() {
  let resolve: (value: T) => void = () => undefined;
  let reject: (reason: unknown) => void = () => undefined;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

const identityAndQuoteState: ShikihoExtractionResult = { kind: 'page_changed', code: '7203' };
const partialArticleState: ShikihoExtractionResult = {
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
    contentHash: 'sha256:partial',
    status: 'partial',
    features: 'features',
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
    missingFields: ['consolidatedBusinesses', 'commentary'],
  },
};

describe('Shikiho content capture lane coordination', () => {
  test.each([
    ['identity and quote', identityAndQuoteState, 'capture_diagnostic'],
    ['partial article', partialArticleState, 'capture_success'],
  ] as const)('suspends passive %s publication until explicit terminal completion', async (_name, state, type) => {
    const lanes = captureLaneFactory()();
    const publishPassive = passivePublisher();
    const explicit = deferred<string>();
    const sendMessage = mock(async (_message: unknown) => undefined);

    const running = lanes.runExplicit('attempt-1', () => explicit.promise);
    await expect(publishPassive(lanes, state, '7203', new Date('2026-07-14T00:00:01.000Z'), sendMessage)).resolves.toBe(
      false
    );
    expect(sendMessage).not.toHaveBeenCalled();

    explicit.resolve('terminal');
    await expect(running).resolves.toBe('terminal');
    await expect(publishPassive(lanes, state, '7203', new Date('2026-07-14T00:00:01.000Z'), sendMessage)).resolves.toBe(
      true
    );
    expect(sendMessage).toHaveBeenCalledTimes(1);
    expect(sendMessage.mock.calls[0]?.[0]).toMatchObject({ type });
  });

  test('keeps passive publication suspended across replacement and resumes after cancellation cleanup', async () => {
    const lanes = captureLaneFactory()();
    const first = deferred<void>();
    const second = deferred<void>();
    const publish = mock(async () => undefined);
    const firstRun = lanes.runExplicit('attempt-1', () => first.promise);
    const secondRun = lanes.runExplicit('attempt-2', () => second.promise);

    first.reject(new Error('cancelled'));
    await expect(firstRun).rejects.toThrow('cancelled');
    await expect(lanes.publishPassive(publish)).resolves.toBe(false);
    expect(publish).not.toHaveBeenCalled();

    second.reject(new Error('cancelled'));
    await expect(secondRun).rejects.toThrow('cancelled');
    await expect(lanes.publishPassive(publish)).resolves.toBe(true);
    expect(publish).toHaveBeenCalledTimes(1);
  });
});
