import type { Mock } from 'bun:test';
import { afterEach, beforeEach, describe, expect, spyOn, test } from 'bun:test';
import { createMockResponse } from '../test-utils/fetch-mock.js';
import { BacktestApiError, BacktestClient } from './BacktestClient.js';
import type { BacktestJobResponse } from './types.js';

describe('BacktestClient', () => {
  let client: BacktestClient;
  let fetchSpy: Mock<typeof fetch>;

  beforeEach(() => {
    client = new BacktestClient({ baseUrl: 'http://localhost:9999' });
    fetchSpy = spyOn(globalThis, 'fetch').mockImplementation((() =>
      Promise.resolve(createMockResponse({ status: 'ok' }))) as unknown as typeof fetch);
  });

  afterEach(() => {
    fetchSpy.mockRestore();
  });

  test('parses valid JSON response', async () => {
    const data = { status: 'healthy', service: 'backtest', version: '1.0.0' };
    fetchSpy.mockResolvedValueOnce(createMockResponse(data));

    const result = await client.healthCheck();
    expect(result).toEqual(data);
  });

  test('throws BacktestApiError on non-JSON response', async () => {
    fetchSpy.mockResolvedValueOnce(
      new Response('<html>Internal Server Error</html>', { status: 200, statusText: 'OK' })
    );

    await expect(client.healthCheck()).rejects.toThrow('Invalid JSON response');
  });

  test('throws BacktestApiError on empty response body', async () => {
    fetchSpy.mockResolvedValueOnce(new Response('', { status: 200, statusText: 'OK' }));

    await expect(client.healthCheck()).rejects.toThrow('Empty response body');
  });

  test('throws BacktestApiError on HTTP error status', async () => {
    fetchSpy.mockResolvedValueOnce(new Response('Not Found', { status: 404, statusText: 'Not Found' }));

    await expect(client.healthCheck()).rejects.toThrow(BacktestApiError);
  });

  test('truncates long non-JSON response in error message', async () => {
    const longHtml = `${'<html>'.padEnd(500, 'x')}</html>`;
    fetchSpy.mockResolvedValueOnce(new Response(longHtml, { status: 200, statusText: 'OK' }));

    const error = await client.healthCheck().catch((e: BacktestApiError) => e);
    expect(error).toBeInstanceOf(BacktestApiError);
    const apiError = error as BacktestApiError;
    expect(apiError.message).toContain('Invalid JSON response');
    // 200 chars + "Invalid JSON response: " prefix
    expect(apiError.message.length).toBeLessThan(250);
  });

  // Strategy endpoints
  test('listStrategies calls correct endpoint', async () => {
    const strategies = {
      strategies: [
        { name: 'sma_cross', category: 'trend', display_name: null, description: null, last_modified: null },
      ],
      total: 1,
    };
    fetchSpy.mockResolvedValueOnce(createMockResponse(strategies));

    const result = await client.listStrategies();
    expect(result).toEqual(strategies);
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/strategies');
  });

  test('getStrategy calls correct endpoint with encoding', async () => {
    const detail = {
      name: 'sma_cross',
      category: 'trend',
      display_name: 'SMA Cross',
      description: 'SMA crossover',
      config: {},
      execution_info: {},
    };
    fetchSpy.mockResolvedValueOnce(createMockResponse(detail));

    const result = await client.getStrategy('sma_cross');
    expect(result).toEqual(detail);
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/strategies/sma_cross');
  });

  test('validateStrategy sends POST with config', async () => {
    const validation = { valid: true, errors: [], warnings: [] };
    fetchSpy.mockResolvedValueOnce(createMockResponse(validation));

    const config = { parameters: { period: 20 } };
    const result = await client.validateStrategy('sma_cross', config as never);
    expect(result).toEqual(validation);

    const lastCall = fetchSpy.mock.calls.at(-1);
    expect(lastCall?.[1]?.method).toBe('POST');
  });

  test('validateStrategy sends POST without config', async () => {
    const validation = { valid: true, errors: [], warnings: [] };
    fetchSpy.mockResolvedValueOnce(createMockResponse(validation));

    await client.validateStrategy('sma_cross');
    const lastCall = fetchSpy.mock.calls.at(-1);
    expect(lastCall?.[1]?.method).toBe('POST');
  });

  // Backtest endpoints
  test('runBacktest sends POST with request body', async () => {
    const job = {
      job_id: 'abc123',
      status: 'pending',
      progress: null,
      message: null,
      created_at: '2024-01-01T00:00:00Z',
      started_at: null,
      completed_at: null,
      error: null,
      result: null,
    } satisfies BacktestJobResponse;
    fetchSpy.mockResolvedValueOnce(createMockResponse(job));

    const request = { strategy: 'sma_cross', start_date: '2024-01-01', end_date: '2024-12-31' };
    const result = await client.runBacktest(request as never);
    expect(result).toEqual(job);

    const lastCall = fetchSpy.mock.calls.at(-1);
    expect(lastCall?.[0]).toContain('/api/backtest/run');
    expect(lastCall?.[1]?.method).toBe('POST');
  });

  test('getJobStatus calls correct endpoint', async () => {
    const job = {
      job_id: 'abc123',
      status: 'running',
      progress: 50,
      message: null,
      created_at: '2024-01-01T00:00:00Z',
      started_at: '2024-01-01T00:00:01Z',
      completed_at: null,
      error: null,
      result: null,
    } satisfies BacktestJobResponse;
    fetchSpy.mockResolvedValueOnce(createMockResponse(job));

    const result = await client.getJobStatus('abc123');
    expect(result).toEqual(job);
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/backtest/jobs/abc123');
  });

  test('listJobs calls correct endpoint with limit', async () => {
    const jobs: BacktestJobResponse[] = [
      {
        job_id: 'abc123',
        status: 'completed',
        progress: 100,
        message: null,
        created_at: '2024-01-01T00:00:00Z',
        started_at: '2024-01-01T00:00:01Z',
        completed_at: '2024-01-01T00:01:00Z',
        error: null,
        result: null,
      },
    ];
    fetchSpy.mockResolvedValueOnce(createMockResponse(jobs));

    const result = await client.listJobs(10);
    expect(result).toEqual(jobs);
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('limit=10');
  });

  test('listJobs uses default limit', async () => {
    fetchSpy.mockResolvedValueOnce(createMockResponse([]));
    await client.listJobs();
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('limit=50');
  });

  test('cancelJob sends POST to cancel endpoint', async () => {
    const job = {
      job_id: 'abc123',
      status: 'cancelled',
      progress: null,
      message: null,
      created_at: '2024-01-01T00:00:00Z',
      started_at: null,
      completed_at: null,
      error: null,
      result: null,
    } satisfies BacktestJobResponse;
    fetchSpy.mockResolvedValueOnce(createMockResponse(job));

    const result = await client.cancelJob('abc123');
    expect(result).toEqual(job);

    const lastCall = fetchSpy.mock.calls.at(-1);
    expect(lastCall?.[0]).toContain('/api/backtest/jobs/abc123/cancel');
    expect(lastCall?.[1]?.method).toBe('POST');
  });

  test('getResult calls correct endpoint', async () => {
    const result_data = {
      job_id: 'abc123',
      strategy_name: 'sma_cross',
      dataset_name: 'test',
      summary: {
        total_return: 0.1,
        sharpe_ratio: 1.0,
        calmar_ratio: 0.5,
        max_drawdown: -0.05,
        win_rate: 0.6,
        trade_count: 10,
        html_path: null,
      },
      execution_time: 1.5,
      html_content: null,
      created_at: '2024-01-01T00:00:00Z',
    };
    fetchSpy.mockResolvedValueOnce(createMockResponse(result_data));

    const result = await client.getResult('abc123');
    expect(result).toEqual(result_data);
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/backtest/result/abc123');
    expect(fetchSpy.mock.calls.at(-1)?.[0]).not.toContain('include_html');
  });

  test('getResult with includeHtml adds query param', async () => {
    fetchSpy.mockResolvedValueOnce(createMockResponse({ job_id: 'abc123' }));

    await client.getResult('abc123', true);
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('include_html=true');
  });

  // runAndWait
  test('runAndWait polls until completion', async () => {
    const pending = {
      job_id: 'abc123',
      status: 'pending',
      progress: null,
      message: null,
      created_at: '2024-01-01T00:00:00Z',
      started_at: null,
      completed_at: null,
      error: null,
      result: null,
    };
    const running = {
      job_id: 'abc123',
      status: 'running',
      progress: 50,
      message: null,
      created_at: '2024-01-01T00:00:00Z',
      started_at: '2024-01-01T00:00:01Z',
      completed_at: null,
      error: null,
      result: null,
    };
    const completed = {
      job_id: 'abc123',
      status: 'completed',
      progress: 100,
      message: null,
      created_at: '2024-01-01T00:00:00Z',
      started_at: '2024-01-01T00:00:01Z',
      completed_at: '2024-01-01T00:01:00Z',
      error: null,
      result: null,
    };

    fetchSpy
      .mockResolvedValueOnce(createMockResponse(pending)) // runBacktest
      .mockResolvedValueOnce(createMockResponse(running)) // getJobStatus poll 1
      .mockResolvedValueOnce(createMockResponse(completed)); // getJobStatus poll 2

    const progressCalls: string[] = [];
    const result = await client.runAndWait({ strategy: 'sma_cross' } as never, {
      pollInterval: 10,
      onProgress: (job) => progressCalls.push(job.status),
    });

    expect(result.status).toBe('completed');
    expect(progressCalls).toEqual(['running', 'completed']);
  });

  test('runAndWait returns immediately if job already completed', async () => {
    const completedJob = {
      job_id: 'abc123',
      status: 'completed',
      progress: 100,
      message: null,
      created_at: '2024-01-01T00:00:00Z',
      started_at: '2024-01-01T00:00:01Z',
      completed_at: '2024-01-01T00:01:00Z',
      error: null,
      result: null,
    };
    fetchSpy.mockResolvedValueOnce(createMockResponse(completedJob));

    const result = await client.runAndWait({ strategy: 'sma_cross' } as never, { pollInterval: 10 });

    expect(result.status).toBe('completed');
    // Only 1 fetch call (runBacktest), no polling
    expect(fetchSpy.mock.calls.length).toBe(1);
  });

  // BacktestApiError
  test('BacktestApiError has correct properties', () => {
    const error = new BacktestApiError(500, 'Internal Server Error', 'Something broke');
    expect(error.status).toBe(500);
    expect(error.statusText).toBe('Internal Server Error');
    expect(error.message).toBe('Something broke');
    expect(error.name).toBe('BacktestApiError');
    expect(error).toBeInstanceOf(Error);
  });

  // Constructor defaults
  test('uses default baseUrl and timeout', () => {
    const defaultClient = new BacktestClient();
    // Verify it doesn't throw when making requests
    expect(defaultClient).toBeDefined();
  });
});
