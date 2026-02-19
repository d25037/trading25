import { afterEach, beforeEach, describe, expect, it, mock, spyOn } from 'bun:test';
import { CLIError } from '../../utils/error-handling.js';

const createScreeningJobMock = mock();
const getScreeningJobStatusMock = mock();
const getScreeningResultMock = mock();

mock.module('chalk', () => {
  const identity = (text: string) => text;
  const bold = Object.assign((text: string) => text, { white: identity });
  return {
    default: {
      red: identity,
      green: identity,
      yellow: identity,
      cyan: identity,
      white: identity,
      gray: identity,
      dim: identity,
      magenta: identity,
      blue: identity,
      bold,
    },
  };
});

mock.module('ora', () => {
  return {
    default: (text?: string) => {
      return {
        text: text ?? '',
        start() {
          return this;
        },
        succeed() {
          return this;
        },
        fail() {
          return this;
        },
        warn() {
          return this;
        },
        stop() {
          return this;
        },
      };
    },
  };
});

mock.module('../../utils/api-client.js', () => {
  class MockApiClient {
    analytics = {
      createScreeningJob: createScreeningJobMock,
      getScreeningJobStatus: getScreeningJobStatusMock,
      getScreeningResult: getScreeningResultMock,
    };
  }

  return {
    ApiClient: MockApiClient,
  };
});

import { buildApiParams, screeningCommand } from './screening.js';

type ScreeningCtx = Parameters<typeof screeningCommand.run>[0];

function createCtx(values: Record<string, unknown>): ScreeningCtx {
  return { values } as ScreeningCtx;
}

function createRunValues(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    markets: 'prime',
    strategies: undefined,
    recentDays: '10',
    date: undefined,
    format: 'table',
    sortBy: 'matchedDate',
    order: 'desc',
    limit: undefined,
    noWait: false,
    debug: false,
    verbose: false,
    ...overrides,
  };
}

describe('analysis screening buildApiParams', () => {
  it('builds strategy-driven query params without backtest metric', () => {
    const params = buildApiParams({
      markets: 'prime,standard',
      strategies: 'range_break_v15,forward_eps_driven',
      recentDays: '15',
      date: '2026-01-15',
      sortBy: 'matchedDate',
      order: 'asc',
      limit: '30',
    });

    expect(params).toEqual({
      markets: 'prime,standard',
      strategies: 'range_break_v15,forward_eps_driven',
      recentDays: 15,
      date: '2026-01-15',
      sortBy: 'matchedDate',
      order: 'asc',
      limit: 30,
    });
    expect('backtestMetric' in params).toBe(false);
  });

  it('defaults sortBy to matchedDate and order to desc', () => {
    const params = buildApiParams({
      recentDays: '10',
    });

    expect(params.sortBy).toBe('matchedDate');
    expect(params.order).toBe('desc');
    expect('rangeBreakFast' in params).toBe(false);
    expect('rangeBreakSlow' in params).toBe(false);
    expect('minBreakPercentage' in params).toBe(false);
    expect('minVolumeRatio' in params).toBe(false);
  });
});

describe('analysis screening command run', () => {
  beforeEach(() => {
    createScreeningJobMock.mockReset();
    getScreeningJobStatusMock.mockReset();
    getScreeningResultMock.mockReset();
  });

  afterEach(() => {
    mock.restore();
  });

  it('submits no-wait job and exits without polling', async () => {
    createScreeningJobMock.mockResolvedValueOnce({
      job_id: 'screen-001',
      status: 'pending',
    });
    const logSpy = spyOn(console, 'log').mockImplementation(() => undefined);

    await screeningCommand.run(createCtx(createRunValues({ noWait: true })));

    expect(createScreeningJobMock).toHaveBeenCalledWith({
      markets: 'prime',
      strategies: undefined,
      recentDays: 10,
      date: undefined,
      sortBy: 'matchedDate',
      order: 'desc',
      limit: undefined,
    });
    expect(getScreeningJobStatusMock).toHaveBeenCalledTimes(0);
    expect(getScreeningResultMock).toHaveBeenCalledTimes(0);
    expect(logSpy.mock.calls.some((call) => String(call[0] ?? '').includes('Job ID: screen-001'))).toBe(true);
  });

  it('polls completed job and forwards results to formatter', async () => {
    createScreeningJobMock.mockResolvedValueOnce({
      job_id: 'screen-002',
      status: 'pending',
    });
    getScreeningJobStatusMock.mockResolvedValueOnce({
      job_id: 'screen-002',
      status: 'completed',
      progress: 1,
      message: 'done',
    });
    getScreeningResultMock.mockResolvedValueOnce({
      markets: ['prime'],
      recentDays: 10,
      referenceDate: '2026-01-31',
      summary: {
        matchCount: 1,
        totalStocksScreened: 1200,
        skippedCount: 2,
        byStrategy: { range_break_v15: 1 },
        strategiesEvaluated: ['range_break_v15'],
        strategiesWithoutBacktestMetrics: [],
        warnings: ['minor warning'],
      },
      results: [
        {
          stockCode: '7203',
          companyName: 'Toyota Motor',
          scaleCategory: 'TOPIX Large70',
          sector33Name: '輸送用機器',
          matchedDate: '2026-01-31',
          bestStrategyName: 'range_break_v15',
          bestStrategyScore: 1.2,
          matchStrategyCount: 1,
          matchedStrategies: [
            {
              strategyName: 'range_break_v15',
              matchedDate: '2026-01-31',
              strategyScore: 1.2,
            },
          ],
        },
      ],
    });
    const logSpy = spyOn(console, 'log').mockImplementation(() => undefined);

    await screeningCommand.run(
      createCtx(
        createRunValues({
          strategies: 'range_break_v15',
          recentDays: '5',
          date: '2026-01-31',
          format: 'csv',
          limit: '50',
          debug: true,
          verbose: true,
        })
      )
    );

    expect(getScreeningJobStatusMock).toHaveBeenCalledTimes(1);
    expect(getScreeningResultMock).toHaveBeenCalledTimes(1);
    expect(logSpy.mock.calls.some((call) => String(call[0] ?? '').includes('StockCode'))).toBe(true);
    expect(logSpy.mock.calls.some((call) => String(call[0] ?? '').includes('Market Screening Results'))).toBe(true);
  });

  it('returns early when job is cancelled', async () => {
    createScreeningJobMock.mockResolvedValueOnce({
      job_id: 'screen-003',
      status: 'pending',
    });
    getScreeningJobStatusMock.mockResolvedValueOnce({
      job_id: 'screen-003',
      status: 'cancelled',
      progress: 1,
      message: 'cancelled by user',
    });

    await screeningCommand.run(createCtx(createRunValues()));

    expect(getScreeningResultMock).toHaveBeenCalledTimes(0);
  });

  it('wraps failed screening jobs into CLIError', async () => {
    createScreeningJobMock.mockResolvedValueOnce({
      job_id: 'screen-004',
      status: 'pending',
    });
    getScreeningJobStatusMock
      .mockResolvedValueOnce({
        job_id: 'screen-004',
        status: 'failed',
        progress: 1,
        message: 'failed',
      })
      .mockResolvedValueOnce({
        job_id: 'screen-004',
        status: 'failed',
        error: 'job exploded',
      });

    const errorSpy = spyOn(console, 'error').mockImplementation(() => undefined);

    await expect(
      screeningCommand.run(createCtx(createRunValues()))
    ).rejects.toBeInstanceOf(CLIError);

    expect(getScreeningJobStatusMock).toHaveBeenCalledTimes(2);
    expect(errorSpy.mock.calls.some((call) => String(call[0] ?? '').includes('Market screening failed'))).toBe(true);
  });
});
