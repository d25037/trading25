/**
 * Backtest API Client
 *
 * trading25-bt FastAPI サーバーと通信するクライアント
 */

import { HttpRequestError, requestJson } from '../base/http-client.js';
import { isActiveJobStatus } from '../base/job-status.js';
import type {
  AttributionArtifactContentResponse,
  AttributionArtifactContentQuery,
  AttributionArtifactFilesQuery,
  AttributionArtifactListResponse,
  BacktestClientConfig,
  BacktestJobCancelResponse,
  BacktestJobCancelPathParams,
  BacktestJobStatusResponse,
  BacktestJobStatusPathParams,
  BacktestJobsQuery,
  BacktestJobsResponse,
  BacktestHtmlFilesQuery,
  BacktestRequest,
  BacktestResultResponse,
  BacktestResultPathParams,
  BacktestResultQuery,
  BacktestRunResponse,
  DefaultConfigEditorContextResponse,
  DefaultConfigResponse,
  DefaultConfigStructuredUpdateRequest,
  DefaultConfigStructuredUpdateResponse,
  DefaultConfigUpdateRequest,
  DefaultConfigUpdateResponse,
  FundamentalsComputeRequest,
  FundamentalsComputeResponse,
  HealthResponse,
  HtmlFileContentResponse,
  HtmlFileContentPathParams,
  HtmlFileDeleteResponse,
  HtmlFileDeletePathParams,
  HtmlFileListResponse,
  HtmlFileRenameRequest,
  HtmlFileRenamePathParams,
  HtmlFileRenameResponse,
  JobStatus,
  OHLCVResampleRequest,
  OHLCVResampleResponse,
  OptimizationHtmlFileContentResponse,
  OptimizationHtmlFileContentPathParams,
  OptimizationHtmlFileDeletePathParams,
  OptimizationHtmlFileDeleteResponse,
  OptimizationHtmlFileRenamePathParams,
  OptimizationHtmlFileRenameRequest,
  OptimizationHtmlFileRenameResponse,
  OptimizationHtmlFilesQuery,
  OptimizationHtmlFileListResponse,
  OptimizationJobCancelResponse,
  OptimizationJobCancelPathParams,
  OptimizationJobStatusResponse,
  OptimizationJobStatusPathParams,
  OptimizationRequest,
  OptimizationRunResponse,
  SignalAttributionJobCancelResponse,
  SignalAttributionJobCancelPathParams,
  SignalAttributionJobStatusResponse,
  SignalAttributionJobStatusPathParams,
  SignalAttributionRequest,
  SignalAttributionResultResponse,
  SignalAttributionResultPathParams,
  SignalAttributionRunResponse,
  SignalReferenceResponse,
  StrategyDeleteResponse,
  StrategyDeletePathParams,
  StrategyDetailResponse,
  StrategyDetailPathParams,
  StrategyDuplicateRequest,
  StrategyDuplicatePathParams,
  StrategyDuplicateResponse,
  StrategyEditorContextResponse,
  StrategyEditorContextPathParams,
  StrategyEditorReferenceResponse,
  StrategyListResponse,
  StrategyMoveRequest,
  StrategyMovePathParams,
  StrategyMoveResponse,
  StrategyOptimizationDeleteResponse,
  StrategyOptimizationDeletePathParams,
  StrategyOptimizationDraftPathParams,
  StrategyOptimizationDraftResponse,
  StrategyOptimizationSaveRequest,
  StrategyOptimizationSavePathParams,
  StrategyOptimizationSaveResponse,
  StrategyOptimizationStateResponse,
  StrategyOptimizationStatePathParams,
  StrategyRenameRequest,
  StrategyRenamePathParams,
  StrategyRenameResponse,
  StrategyUpdateRequest,
  StrategyUpdatePathParams,
  StrategyUpdateResponse,
  StrategyValidationRequest,
  StrategyValidationPathParams,
  StrategyValidationResponse,
} from './types.js';

type PollableJob = {
  job_id: string;
  status: JobStatus;
};

type WaitForJobOptions<TJob> = {
  pollInterval?: number;
  onProgress?: (job: TJob) => void;
};

export class BacktestApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly statusText: string,
    message: string
  ) {
    super(message);
    this.name = 'BacktestApiError';
  }
}

function resolveProcessEnv(name: string): string | undefined {
  if (typeof process === 'undefined') {
    return undefined;
  }
  return process.env?.[name];
}

function resolveDefaultBaseUrl(): string | undefined {
  const hasWindow = typeof (globalThis as { window?: unknown }).window !== 'undefined';
  if (hasWindow) {
    return undefined;
  }
  return 'http://localhost:3002';
}

function toBacktestApiError(error: unknown): never {
  if (error instanceof HttpRequestError) {
    throw new BacktestApiError(error.status ?? 0, error.statusText ?? 'Unknown', error.message);
  }
  throw error;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildStrategyLimitQuery(
  params?: BacktestHtmlFilesQuery | AttributionArtifactFilesQuery | OptimizationHtmlFilesQuery
): string {
  const query = new URLSearchParams();
  if (params?.strategy) query.set('strategy', params.strategy);
  query.set('limit', String(params?.limit ?? 100));
  return query.toString();
}

export class BacktestClient {
  private readonly baseUrl?: string;
  private readonly timeout: number;

  constructor(config?: Partial<BacktestClientConfig>) {
    this.baseUrl = config?.baseUrl ?? resolveProcessEnv('BT_API_URL') ?? resolveDefaultBaseUrl();
    this.timeout = config?.timeout ?? Number(resolveProcessEnv('BT_API_TIMEOUT') ?? 600000);
  }

  private async request<T>(endpoint: string, options?: RequestInit): Promise<T> {
    try {
      return await requestJson<T>(endpoint, {
        ...options,
        baseUrl: this.baseUrl,
        timeoutMs: this.timeout,
        headers: {
          'Content-Type': 'application/json',
          ...options?.headers,
        },
      });
    } catch (error) {
      toBacktestApiError(error);
    }
  }

  private async waitForJob<TJob extends PollableJob>(
    initialJob: TJob,
    fetchJob: (jobId: string) => Promise<TJob>,
    options?: WaitForJobOptions<TJob>
  ): Promise<TJob> {
    const pollInterval = options?.pollInterval ?? 2000;

    let job = initialJob;
    while (isActiveJobStatus(job.status)) {
      await sleep(pollInterval);
      job = await fetchJob(job.job_id);
      options?.onProgress?.(job);
    }

    return job;
  }

  // Health
  async healthCheck(): Promise<HealthResponse> {
    return this.request<HealthResponse>('/api/health');
  }

  // Strategies
  async listStrategies(): Promise<StrategyListResponse> {
    return this.request<StrategyListResponse>('/api/strategies');
  }

  async getStrategy(strategyName: StrategyDetailPathParams['strategy_name']): Promise<StrategyDetailResponse> {
    return this.request<StrategyDetailResponse>(`/api/strategies/${encodeURIComponent(strategyName)}`);
  }

  async getStrategyEditorReference(): Promise<StrategyEditorReferenceResponse> {
    return this.request<StrategyEditorReferenceResponse>('/api/strategies/editor/reference');
  }

  async getStrategyEditorContext(
    strategyName: StrategyEditorContextPathParams['strategy_name']
  ): Promise<StrategyEditorContextResponse> {
    return this.request<StrategyEditorContextResponse>(
      `/api/strategies/${encodeURIComponent(strategyName)}/editor-context`
    );
  }

  async validateStrategy(
    strategyName: StrategyValidationPathParams['strategy_name'],
    config?: StrategyValidationRequest
  ): Promise<StrategyValidationResponse> {
    return this.request<StrategyValidationResponse>(`/api/strategies/${encodeURIComponent(strategyName)}/validate`, {
      method: 'POST',
      body: config ? JSON.stringify(config) : undefined,
    });
  }

  async moveStrategy(
    strategyName: StrategyMovePathParams['strategy_name'],
    request: StrategyMoveRequest
  ): Promise<StrategyMoveResponse> {
    return this.request<StrategyMoveResponse>(`/api/strategies/${encodeURIComponent(strategyName)}/move`, {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async updateStrategy(
    strategyName: StrategyUpdatePathParams['strategy_name'],
    request: StrategyUpdateRequest
  ): Promise<StrategyUpdateResponse> {
    return this.request<StrategyUpdateResponse>(`/api/strategies/${encodeURIComponent(strategyName)}`, {
      method: 'PUT',
      body: JSON.stringify(request),
    });
  }

  async deleteStrategy(strategyName: StrategyDeletePathParams['strategy_name']): Promise<StrategyDeleteResponse> {
    return this.request<StrategyDeleteResponse>(`/api/strategies/${encodeURIComponent(strategyName)}`, {
      method: 'DELETE',
    });
  }

  async duplicateStrategy(
    strategyName: StrategyDuplicatePathParams['strategy_name'],
    request: StrategyDuplicateRequest
  ): Promise<StrategyDuplicateResponse> {
    return this.request<StrategyDuplicateResponse>(`/api/strategies/${encodeURIComponent(strategyName)}/duplicate`, {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async renameStrategy(
    strategyName: StrategyRenamePathParams['strategy_name'],
    request: StrategyRenameRequest
  ): Promise<StrategyRenameResponse> {
    return this.request<StrategyRenameResponse>(`/api/strategies/${encodeURIComponent(strategyName)}/rename`, {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  // Backtest
  async runBacktest(request: BacktestRequest): Promise<BacktestRunResponse> {
    return this.request<BacktestRunResponse>('/api/backtest/run', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async getJobStatus(jobId: BacktestJobStatusPathParams['job_id']): Promise<BacktestJobStatusResponse> {
    return this.request<BacktestJobStatusResponse>(`/api/backtest/jobs/${encodeURIComponent(jobId)}`);
  }

  async listJobs(limit: NonNullable<BacktestJobsQuery['limit']> = 50): Promise<BacktestJobsResponse> {
    return this.request<BacktestJobsResponse>(`/api/backtest/jobs?limit=${limit}`);
  }

  async cancelJob(jobId: BacktestJobCancelPathParams['job_id']): Promise<BacktestJobCancelResponse> {
    return this.request<BacktestJobCancelResponse>(`/api/backtest/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: 'POST',
    });
  }

  async getResult(
    jobId: BacktestResultPathParams['job_id'],
    includeHtml: NonNullable<BacktestResultQuery['include_html']> = false
  ): Promise<BacktestResultResponse> {
    const params = includeHtml ? '?include_html=true' : '';
    return this.request<BacktestResultResponse>(`/api/backtest/result/${encodeURIComponent(jobId)}${params}`);
  }

  async runSignalAttribution(request: SignalAttributionRequest): Promise<SignalAttributionRunResponse> {
    return this.request<SignalAttributionRunResponse>('/api/backtest/attribution/run', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async getSignalAttributionJob(
    jobId: SignalAttributionJobStatusPathParams['job_id']
  ): Promise<SignalAttributionJobStatusResponse> {
    return this.request<SignalAttributionJobStatusResponse>(
      `/api/backtest/attribution/jobs/${encodeURIComponent(jobId)}`
    );
  }

  async cancelSignalAttributionJob(
    jobId: SignalAttributionJobCancelPathParams['job_id']
  ): Promise<SignalAttributionJobCancelResponse> {
    return this.request<SignalAttributionJobCancelResponse>(
      `/api/backtest/attribution/jobs/${encodeURIComponent(jobId)}/cancel`,
      { method: 'POST' }
    );
  }

  async getSignalAttributionResult(
    jobId: SignalAttributionResultPathParams['job_id']
  ): Promise<SignalAttributionResultResponse> {
    return this.request<SignalAttributionResultResponse>(
      `/api/backtest/attribution/result/${encodeURIComponent(jobId)}`
    );
  }

  async listAttributionArtifactFiles(
    params?: AttributionArtifactFilesQuery
  ): Promise<AttributionArtifactListResponse> {
    return this.request<AttributionArtifactListResponse>(
      `/api/backtest/attribution-files?${buildStrategyLimitQuery(params)}`
    );
  }

  async getAttributionArtifactContent(
    strategy: AttributionArtifactContentQuery['strategy'],
    filename: AttributionArtifactContentQuery['filename']
  ): Promise<AttributionArtifactContentResponse> {
    const query = new URLSearchParams({
      strategy,
      filename,
    });
    return this.request<AttributionArtifactContentResponse>(
      `/api/backtest/attribution-files/content?${query.toString()}`
    );
  }

  async listHtmlFiles(params?: BacktestHtmlFilesQuery): Promise<HtmlFileListResponse> {
    return this.request<HtmlFileListResponse>(`/api/backtest/html-files?${buildStrategyLimitQuery(params)}`);
  }

  async getHtmlFileContent(
    strategy: HtmlFileContentPathParams['strategy'],
    filename: HtmlFileContentPathParams['filename']
  ): Promise<HtmlFileContentResponse> {
    return this.request<HtmlFileContentResponse>(
      `/api/backtest/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}`
    );
  }

  async renameHtmlFile(
    strategy: HtmlFileRenamePathParams['strategy'],
    filename: HtmlFileRenamePathParams['filename'],
    request: HtmlFileRenameRequest
  ): Promise<HtmlFileRenameResponse> {
    return this.request<HtmlFileRenameResponse>(
      `/api/backtest/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}/rename`,
      {
        method: 'POST',
        body: JSON.stringify(request),
      }
    );
  }

  async deleteHtmlFile(
    strategy: HtmlFileDeletePathParams['strategy'],
    filename: HtmlFileDeletePathParams['filename']
  ): Promise<HtmlFileDeleteResponse> {
    return this.request<HtmlFileDeleteResponse>(
      `/api/backtest/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}`,
      {
        method: 'DELETE',
      }
    );
  }

  async getDefaultConfig(): Promise<DefaultConfigResponse> {
    return this.request<DefaultConfigResponse>('/api/config/default');
  }

  async getDefaultConfigEditorContext(): Promise<DefaultConfigEditorContextResponse> {
    return this.request<DefaultConfigEditorContextResponse>('/api/config/default/editor-context');
  }

  async updateDefaultConfig(request: DefaultConfigUpdateRequest): Promise<DefaultConfigUpdateResponse> {
    return this.request<DefaultConfigUpdateResponse>('/api/config/default', {
      method: 'PUT',
      body: JSON.stringify(request),
    });
  }

  async updateDefaultConfigStructured(
    request: DefaultConfigStructuredUpdateRequest
  ): Promise<DefaultConfigStructuredUpdateResponse> {
    return this.request<DefaultConfigStructuredUpdateResponse>('/api/config/default/structured', {
      method: 'PUT',
      body: JSON.stringify(request),
    });
  }

  async getSignalReference(): Promise<SignalReferenceResponse> {
    return this.request<SignalReferenceResponse>('/api/signals/reference');
  }

  // Optimization
  async runOptimization(request: OptimizationRequest): Promise<OptimizationRunResponse> {
    return this.request<OptimizationRunResponse>('/api/optimize/run', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async getOptimizationJobStatus(
    jobId: OptimizationJobStatusPathParams['job_id']
  ): Promise<OptimizationJobStatusResponse> {
    return this.request<OptimizationJobStatusResponse>(`/api/optimize/jobs/${encodeURIComponent(jobId)}`);
  }

  async cancelOptimizationJob(
    jobId: OptimizationJobCancelPathParams['job_id']
  ): Promise<OptimizationJobCancelResponse> {
    return this.request<OptimizationJobCancelResponse>(`/api/optimize/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: 'POST',
    });
  }

  async getStrategyOptimization(
    strategyName: StrategyOptimizationStatePathParams['strategy_name']
  ): Promise<StrategyOptimizationStateResponse> {
    return this.request<StrategyOptimizationStateResponse>(
      `/api/strategies/${encodeURIComponent(strategyName)}/optimization`
    );
  }

  async generateStrategyOptimizationDraft(
    strategyName: StrategyOptimizationDraftPathParams['strategy_name']
  ): Promise<StrategyOptimizationDraftResponse> {
    return this.request<StrategyOptimizationDraftResponse>(
      `/api/strategies/${encodeURIComponent(strategyName)}/optimization/draft`,
      {
        method: 'POST',
      }
    );
  }

  async saveStrategyOptimization(
    strategyName: StrategyOptimizationSavePathParams['strategy_name'],
    request: StrategyOptimizationSaveRequest
  ): Promise<StrategyOptimizationSaveResponse> {
    return this.request<StrategyOptimizationSaveResponse>(
      `/api/strategies/${encodeURIComponent(strategyName)}/optimization`,
      {
        method: 'PUT',
        body: JSON.stringify(request),
      }
    );
  }

  async deleteStrategyOptimization(
    strategyName: StrategyOptimizationDeletePathParams['strategy_name']
  ): Promise<StrategyOptimizationDeleteResponse> {
    return this.request<StrategyOptimizationDeleteResponse>(
      `/api/strategies/${encodeURIComponent(strategyName)}/optimization`,
      {
        method: 'DELETE',
      }
    );
  }

  async listOptimizationHtmlFiles(params?: OptimizationHtmlFilesQuery): Promise<OptimizationHtmlFileListResponse> {
    return this.request<OptimizationHtmlFileListResponse>(
      `/api/optimize/html-files?${buildStrategyLimitQuery(params)}`
    );
  }

  async getOptimizationHtmlFileContent(
    strategy: OptimizationHtmlFileContentPathParams['strategy'],
    filename: OptimizationHtmlFileContentPathParams['filename']
  ): Promise<OptimizationHtmlFileContentResponse> {
    return this.request<OptimizationHtmlFileContentResponse>(
      `/api/optimize/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}`
    );
  }

  async renameOptimizationHtmlFile(
    strategy: OptimizationHtmlFileRenamePathParams['strategy'],
    filename: OptimizationHtmlFileRenamePathParams['filename'],
    request: OptimizationHtmlFileRenameRequest
  ): Promise<OptimizationHtmlFileRenameResponse> {
    return this.request<OptimizationHtmlFileRenameResponse>(
      `/api/optimize/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}/rename`,
      {
        method: 'POST',
        body: JSON.stringify(request),
      }
    );
  }

  async deleteOptimizationHtmlFile(
    strategy: OptimizationHtmlFileDeletePathParams['strategy'],
    filename: OptimizationHtmlFileDeletePathParams['filename']
  ): Promise<OptimizationHtmlFileDeleteResponse> {
    return this.request<OptimizationHtmlFileDeleteResponse>(
      `/api/optimize/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}`,
      {
        method: 'DELETE',
      }
    );
  }

  async runSignalAttributionAndWait(
    request: SignalAttributionRequest,
    options?: WaitForJobOptions<SignalAttributionJobStatusResponse>
  ): Promise<SignalAttributionJobStatusResponse> {
    const initialJob = await this.runSignalAttribution(request);
    return this.waitForJob(initialJob, (jobId) => this.getSignalAttributionJob(jobId), options);
  }

  /**
   * バックテストを実行し、完了まで待機
   * @param request バックテストリクエスト
   * @param options ポーリングオプション
   * @returns 完了したジョブレスポンス
   */
  async runAndWait(
    request: BacktestRequest,
    options?: WaitForJobOptions<BacktestJobStatusResponse>
  ): Promise<BacktestJobStatusResponse> {
    const initialJob = await this.runBacktest(request);
    return this.waitForJob(initialJob, (jobId) => this.getJobStatus(jobId), options);
  }

  // OHLCV Resample
  /**
   * OHLCVデータをリサンプル（週足/月足変換）
   * @param request リサンプルリクエスト
   * @returns リサンプルされたOHLCVデータ
   */
  async resampleOHLCV(request: OHLCVResampleRequest): Promise<OHLCVResampleResponse> {
    return this.request<OHLCVResampleResponse>('/api/ohlcv/resample', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  // Fundamentals
  /**
   * ファンダメンタル指標を計算
   * @param request 計算リクエスト
   * @returns ファンダメンタル分析結果
   */
  async computeFundamentals(request: FundamentalsComputeRequest): Promise<FundamentalsComputeResponse> {
    return this.request<FundamentalsComputeResponse>('/api/fundamentals/compute', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }
}
