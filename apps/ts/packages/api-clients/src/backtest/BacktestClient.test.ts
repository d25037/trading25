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

  test('moveStrategy sends POST with target category', async () => {
    const moved = {
      success: true,
      old_strategy_name: 'experimental/sma_cross',
      new_strategy_name: 'production/sma_cross',
      target_category: 'production',
      new_path: '/tmp/production/sma_cross.yaml',
    };
    fetchSpy.mockResolvedValueOnce(createMockResponse(moved));

    const result = await client.moveStrategy('experimental/sma_cross', {
      target_category: 'production',
    });
    expect(result).toEqual(moved);

    const lastCall = fetchSpy.mock.calls.at(-1);
    expect(lastCall?.[0]).toContain('/api/strategies/experimental%2Fsma_cross/move');
    expect(lastCall?.[1]?.method).toBe('POST');
  });

  test('strategy CRUD methods call expected endpoints', async () => {
    fetchSpy.mockResolvedValueOnce(createMockResponse({ success: true, strategy_name: 'sma_cross' }));
    fetchSpy.mockResolvedValueOnce(createMockResponse({ success: true, strategy_name: 'sma_cross' }));
    fetchSpy.mockResolvedValueOnce(createMockResponse({ success: true, new_strategy_name: 'sma_cross_copy' }));
    fetchSpy.mockResolvedValueOnce(createMockResponse({ success: true, new_name: 'sma_cross_new' }));

    await client.updateStrategy('sma_cross', { config: { foo: 'bar' } });
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/strategies/sma_cross');
    expect(fetchSpy.mock.calls.at(-1)?.[1]?.method).toBe('PUT');

    await client.deleteStrategy('sma_cross');
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/strategies/sma_cross');
    expect(fetchSpy.mock.calls.at(-1)?.[1]?.method).toBe('DELETE');

    await client.duplicateStrategy('sma_cross', { new_name: 'sma_cross_copy' });
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/strategies/sma_cross/duplicate');
    expect(fetchSpy.mock.calls.at(-1)?.[1]?.method).toBe('POST');

    await client.renameStrategy('sma_cross', { new_name: 'sma_cross_new' });
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/strategies/sma_cross/rename');
    expect(fetchSpy.mock.calls.at(-1)?.[1]?.method).toBe('POST');
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

    const request = { strategy_name: 'sma_cross', engine_family: 'vectorbt' };
    const result = await client.runBacktest(request as never);
    expect(result).toEqual(job);

    const lastCall = fetchSpy.mock.calls.at(-1);
    expect(lastCall?.[0]).toContain('/api/backtest/run');
    expect(lastCall?.[1]?.method).toBe('POST');
    expect(JSON.parse(String(lastCall?.[1]?.body))).toEqual(request);
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

  test('runOptimization sends POST with request body', async () => {
    const job = {
      job_id: 'opt-1',
      status: 'pending',
      progress: null,
      message: null,
      created_at: '2024-01-01T00:00:00Z',
      started_at: null,
      completed_at: null,
      error: null,
      best_score: null,
      best_params: null,
      worst_score: null,
      worst_params: null,
      total_combinations: null,
      html_path: null,
    };
    fetchSpy.mockResolvedValueOnce(createMockResponse(job));

    const result = await client.runOptimization({ strategy_name: 'sma_cross' });
    expect(result).toEqual(job);

    const lastCall = fetchSpy.mock.calls.at(-1);
    expect(lastCall?.[0]).toContain('/api/optimize/run');
    expect(lastCall?.[1]?.method).toBe('POST');
  });

  test('getOptimizationJobStatus calls correct endpoint', async () => {
    const job = {
      job_id: 'opt-1',
      status: 'running',
      progress: 0.5,
      message: 'running',
      created_at: '2024-01-01T00:00:00Z',
      started_at: '2024-01-01T00:00:01Z',
      completed_at: null,
      error: null,
      best_score: null,
      best_params: null,
      worst_score: null,
      worst_params: null,
      total_combinations: null,
      html_path: null,
    };
    fetchSpy.mockResolvedValueOnce(createMockResponse(job));

    const result = await client.getOptimizationJobStatus('opt-1');
    expect(result).toEqual(job);
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/optimize/jobs/opt-1');
  });

  test('cancelOptimizationJob calls correct endpoint', async () => {
    const job = {
      job_id: 'opt-1',
      status: 'cancelled',
      progress: 1,
      message: 'cancelled',
      created_at: '2024-01-01T00:00:00Z',
      started_at: '2024-01-01T00:00:01Z',
      completed_at: '2024-01-01T00:00:02Z',
      error: null,
      best_score: null,
      best_params: null,
      worst_score: null,
      worst_params: null,
      total_combinations: null,
      html_path: null,
    };
    fetchSpy.mockResolvedValueOnce(createMockResponse(job));

    const result = await client.cancelOptimizationJob('opt-1');
    expect(result).toEqual(job);

    const lastCall = fetchSpy.mock.calls.at(-1);
    expect(lastCall?.[0]).toContain('/api/optimize/jobs/opt-1/cancel');
    expect(lastCall?.[1]?.method).toBe('POST');
  });

  test('optimization grid config methods call expected endpoints', async () => {
    fetchSpy.mockResolvedValueOnce(createMockResponse({ configs: [], total: 0 }));
    fetchSpy.mockResolvedValueOnce(
      createMockResponse({
        strategy_name: 'Alpha',
        content: 'params: {}',
        param_count: 1,
        combinations: 10,
      })
    );
    fetchSpy.mockResolvedValueOnce(
      createMockResponse({
        success: true,
        strategy_name: 'Alpha',
        param_count: 1,
        combinations: 10,
      })
    );
    fetchSpy.mockResolvedValueOnce(createMockResponse({ success: true, strategy_name: 'Alpha' }));

    await client.getOptimizationGridConfigs();
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/optimize/grid-configs');

    await client.getOptimizationGridConfig('Alpha Strategy');
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/optimize/grid-configs/Alpha%20Strategy');

    await client.saveOptimizationGridConfig('Alpha', { content: 'param1: [1, 2, 3]' });
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/optimize/grid-configs/Alpha');
    expect(fetchSpy.mock.calls.at(-1)?.[1]?.method).toBe('PUT');

    await client.deleteOptimizationGridConfig('Alpha');
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/optimize/grid-configs/Alpha');
    expect(fetchSpy.mock.calls.at(-1)?.[1]?.method).toBe('DELETE');
  });

  test('optimization HTML file methods call expected endpoints', async () => {
    fetchSpy.mockResolvedValueOnce(createMockResponse({ files: [], total: 0 }));
    fetchSpy.mockResolvedValueOnce(
      createMockResponse({
        strategy_name: 'strat',
        filename: 'opt.html',
        html_content: '<html></html>',
      })
    );
    fetchSpy.mockResolvedValueOnce(
      createMockResponse({
        success: true,
        strategy_name: 'strat',
        old_filename: 'old.html',
        new_filename: 'new.html',
      })
    );
    fetchSpy.mockResolvedValueOnce(
      createMockResponse({
        success: true,
        strategy_name: 'strat',
        filename: 'opt.html',
      })
    );

    await client.listOptimizationHtmlFiles({ strategy: 'strat', limit: 10 });
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/optimize/html-files?strategy=strat&limit=10');

    await client.getOptimizationHtmlFileContent('strat', 'opt.html');
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/optimize/html-files/strat/opt.html');

    await client.renameOptimizationHtmlFile('strat', 'old.html', { new_filename: 'new.html' });
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/optimize/html-files/strat/old.html/rename');
    expect(fetchSpy.mock.calls.at(-1)?.[1]?.method).toBe('POST');

    await client.deleteOptimizationHtmlFile('strat', 'opt.html');
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/optimize/html-files/strat/opt.html');
    expect(fetchSpy.mock.calls.at(-1)?.[1]?.method).toBe('DELETE');
  });

  test('runSignalAttribution sends POST with request body', async () => {
    const job = {
      job_id: 'attr-1',
      status: 'pending',
      progress: null,
      message: null,
      created_at: '2024-01-01T00:00:00Z',
      started_at: null,
      completed_at: null,
      error: null,
      result_data: null,
    };
    fetchSpy.mockResolvedValueOnce(createMockResponse(job));

    const request = {
      strategy_name: 'sma_cross',
      shapley_top_n: 5,
      shapley_permutations: 128,
      random_seed: 42,
    };
    const result = await client.runSignalAttribution(request);
    expect(result).toEqual(job);
    const lastCall = fetchSpy.mock.calls.at(-1);
    expect(lastCall?.[0]).toContain('/api/backtest/attribution/run');
    expect(lastCall?.[1]?.method).toBe('POST');
  });

  test('getSignalAttributionJob calls correct endpoint', async () => {
    const job = {
      job_id: 'attr-1',
      status: 'running',
      progress: 0.5,
      message: 'running',
      created_at: '2024-01-01T00:00:00Z',
      started_at: '2024-01-01T00:00:01Z',
      completed_at: null,
      error: null,
      result_data: null,
    };
    fetchSpy.mockResolvedValueOnce(createMockResponse(job));

    const result = await client.getSignalAttributionJob('attr-1');
    expect(result).toEqual(job);
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/backtest/attribution/jobs/attr-1');
  });

  test('cancelSignalAttributionJob sends POST to cancel endpoint', async () => {
    const job = {
      job_id: 'attr-1',
      status: 'cancelled',
      progress: null,
      message: null,
      created_at: '2024-01-01T00:00:00Z',
      started_at: null,
      completed_at: null,
      error: null,
      result_data: null,
    };
    fetchSpy.mockResolvedValueOnce(createMockResponse(job));

    const result = await client.cancelSignalAttributionJob('attr-1');
    expect(result).toEqual(job);
    const lastCall = fetchSpy.mock.calls.at(-1);
    expect(lastCall?.[0]).toContain('/api/backtest/attribution/jobs/attr-1/cancel');
    expect(lastCall?.[1]?.method).toBe('POST');
  });

  test('getSignalAttributionResult calls correct endpoint', async () => {
    const resultData = {
      job_id: 'attr-1',
      strategy_name: 'sma_cross',
      result: {
        baseline_metrics: { total_return: 0.2, sharpe_ratio: 1.1 },
        signals: [],
        top_n_selection: {
          top_n_requested: 5,
          top_n_effective: 0,
          selected_signal_ids: [],
          scores: [],
        },
        timing: {
          total_seconds: 1,
          baseline_seconds: 0.2,
          loo_seconds: 0.4,
          shapley_seconds: 0.4,
        },
        shapley: {
          method: null,
          sample_size: null,
          error: null,
          evaluations: null,
        },
      },
      created_at: '2024-01-01T00:00:00Z',
    };
    fetchSpy.mockResolvedValueOnce(createMockResponse(resultData));

    const result = await client.getSignalAttributionResult('attr-1');
    expect(result).toEqual(resultData);
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/backtest/attribution/result/attr-1');
  });

  test('attribution artifact methods call expected endpoints', async () => {
    fetchSpy.mockResolvedValueOnce(createMockResponse({ files: [], total: 0 }));
    fetchSpy.mockResolvedValueOnce(
      createMockResponse({
        strategy_name: 'experimental/range_break_v18',
        filename: 'attribution_20260112_120000_job-1.json',
        artifact: {},
      })
    );

    await client.listAttributionArtifactFiles({ strategy: 'experimental/range_break_v18', limit: 50 });
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain(
      '/api/backtest/attribution-files?strategy=experimental%2Frange_break_v18&limit=50'
    );

    await client.getAttributionArtifactContent('experimental/range_break_v18', 'attribution_20260112_120000_job-1.json');
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain(
      '/api/backtest/attribution-files/content?strategy=experimental%2Frange_break_v18&filename=attribution_20260112_120000_job-1.json'
    );
  });

  test('backtest HTML/config/reference methods call expected endpoints', async () => {
    fetchSpy.mockResolvedValueOnce(createMockResponse({ files: [], total: 0 }));
    fetchSpy.mockResolvedValueOnce(
      createMockResponse({
        strategy_name: 'strat',
        filename: 'report.html',
        html_content: '<html></html>',
        metrics: null,
      })
    );
    fetchSpy.mockResolvedValueOnce(
      createMockResponse({
        success: true,
        strategy_name: 'strat',
        old_filename: 'old.html',
        new_filename: 'new.html',
      })
    );
    fetchSpy.mockResolvedValueOnce(
      createMockResponse({
        success: true,
        strategy_name: 'strat',
        filename: 'report.html',
      })
    );
    fetchSpy.mockResolvedValueOnce(createMockResponse({ content: 'default: true' }));
    fetchSpy.mockResolvedValueOnce(createMockResponse({ success: true }));
    fetchSpy.mockResolvedValueOnce(createMockResponse({ signals: [], categories: [], total: 0 }));

    await client.listHtmlFiles({ strategy: 'strat', limit: 10 });
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/backtest/html-files?strategy=strat&limit=10');

    await client.getHtmlFileContent('strat', 'report.html');
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/backtest/html-files/strat/report.html');

    await client.renameHtmlFile('strat', 'old.html', { new_filename: 'new.html' });
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/backtest/html-files/strat/old.html/rename');
    expect(fetchSpy.mock.calls.at(-1)?.[1]?.method).toBe('POST');

    await client.deleteHtmlFile('strat', 'report.html');
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/backtest/html-files/strat/report.html');
    expect(fetchSpy.mock.calls.at(-1)?.[1]?.method).toBe('DELETE');

    await client.getDefaultConfig();
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/config/default');

    await client.updateDefaultConfig({ content: 'default: false' });
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/config/default');
    expect(fetchSpy.mock.calls.at(-1)?.[1]?.method).toBe('PUT');

    await client.getSignalReference();
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toContain('/api/signals/reference');
  });

  test('runSignalAttributionAndWait polls until completion', async () => {
    const pending = {
      job_id: 'attr-1',
      status: 'pending',
      progress: null,
      message: null,
      created_at: '2024-01-01T00:00:00Z',
      started_at: null,
      completed_at: null,
      error: null,
      result_data: null,
    };
    const running = {
      job_id: 'attr-1',
      status: 'running',
      progress: 0.5,
      message: 'running',
      created_at: '2024-01-01T00:00:00Z',
      started_at: '2024-01-01T00:00:01Z',
      completed_at: null,
      error: null,
      result_data: null,
    };
    const completed = {
      job_id: 'attr-1',
      status: 'completed',
      progress: 1,
      message: null,
      created_at: '2024-01-01T00:00:00Z',
      started_at: '2024-01-01T00:00:01Z',
      completed_at: '2024-01-01T00:01:00Z',
      error: null,
      result_data: {
        baseline_metrics: { total_return: 0.2, sharpe_ratio: 1.1 },
        signals: [],
        top_n_selection: {
          top_n_requested: 5,
          top_n_effective: 0,
          selected_signal_ids: [],
          scores: [],
        },
        timing: {
          total_seconds: 1,
          baseline_seconds: 0.2,
          loo_seconds: 0.4,
          shapley_seconds: 0.4,
        },
        shapley: {
          method: null,
          sample_size: null,
          error: null,
          evaluations: null,
        },
      },
    };

    fetchSpy
      .mockResolvedValueOnce(createMockResponse(pending))
      .mockResolvedValueOnce(createMockResponse(running))
      .mockResolvedValueOnce(createMockResponse(completed));

    const progressCalls: string[] = [];
    const result = await client.runSignalAttributionAndWait(
      { strategy_name: 'sma_cross' },
      {
        pollInterval: 10,
        onProgress: (job) => progressCalls.push(job.status),
      }
    );

    expect(result.status).toBe('completed');
    expect(progressCalls).toEqual(['running', 'completed']);
  });

  test('runSignalAttributionAndWait returns immediately if job already completed', async () => {
    const completed = {
      job_id: 'attr-1',
      status: 'completed',
      progress: 1,
      message: null,
      created_at: '2024-01-01T00:00:00Z',
      started_at: null,
      completed_at: '2024-01-01T00:01:00Z',
      error: null,
      result_data: null,
    };
    fetchSpy.mockResolvedValueOnce(createMockResponse(completed));

    const result = await client.runSignalAttributionAndWait({ strategy_name: 'sma_cross' }, { pollInterval: 10 });

    expect(result.status).toBe('completed');
    expect(fetchSpy.mock.calls.length).toBe(1);
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

  test('uses relative endpoint when baseUrl is empty', async () => {
    const relativeClient = new BacktestClient({ baseUrl: '', timeout: 1000 });
    fetchSpy.mockResolvedValueOnce(createMockResponse({ status: 'ok', service: 'bt', version: 'test' }));

    await relativeClient.healthCheck();
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toBe('/api/health');
  });
});
