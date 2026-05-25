/**
 * Backtest API Client
 *
 * trading25-bt FastAPI サーバーと通信するクライアント
 */

import { isActiveJobStatus } from '../base/job-status.js';
import { HttpRequestError, requestJson } from '../base/http-client.js';
import type {
  AttributionArtifactContentResponse,
  AttributionArtifactListResponse,
  BacktestClientConfig,
  BacktestJobResponse,
  BacktestRequest,
  BacktestResultResponse,
  DefaultConfigEditorContextResponse,
  DefaultConfigResponse,
  DefaultConfigStructuredUpdateRequest,
  DefaultConfigUpdateRequest,
  DefaultConfigUpdateResponse,
  FundamentalsComputeRequest,
  FundamentalsComputeResponse,
  HealthResponse,
  HtmlFileContentResponse,
  HtmlFileDeleteResponse,
  HtmlFileListResponse,
  HtmlFileRenameRequest,
  HtmlFileRenameResponse,
  JobStatus,
  OHLCVResampleRequest,
  OHLCVResampleResponse,
  OptimizationHtmlFileContentResponse,
  OptimizationHtmlFileListResponse,
  OptimizationJobResponse,
  OptimizationRequest,
  SignalAttributionJobResponse,
  SignalAttributionRequest,
  SignalAttributionResultResponse,
  SignalReferenceResponse,
  StrategyDeleteResponse,
  StrategyDetailResponse,
  StrategyDuplicateRequest,
  StrategyDuplicateResponse,
  StrategyEditorContextResponse,
  StrategyEditorReferenceResponse,
  StrategyListResponse,
  StrategyMoveRequest,
  StrategyMoveResponse,
  StrategyOptimizationDeleteResponse,
  StrategyOptimizationSaveRequest,
  StrategyOptimizationSaveResponse,
  StrategyOptimizationStateResponse,
  StrategyRenameRequest,
  StrategyRenameResponse,
  StrategyUpdateRequest,
  StrategyUpdateResponse,
  StrategyValidationRequest,
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

type StrategyLimitParams = {
  strategy?: string;
  limit?: number;
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

function buildStrategyLimitQuery(params?: StrategyLimitParams): string {
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

  async getStrategy(strategyName: string): Promise<StrategyDetailResponse> {
    return this.request<StrategyDetailResponse>(`/api/strategies/${encodeURIComponent(strategyName)}`);
  }

  async getStrategyEditorReference(): Promise<StrategyEditorReferenceResponse> {
    return this.request<StrategyEditorReferenceResponse>('/api/strategies/editor/reference');
  }

  async getStrategyEditorContext(strategyName: string): Promise<StrategyEditorContextResponse> {
    return this.request<StrategyEditorContextResponse>(
      `/api/strategies/${encodeURIComponent(strategyName)}/editor-context`
    );
  }

  async validateStrategy(
    strategyName: string,
    config?: StrategyValidationRequest
  ): Promise<StrategyValidationResponse> {
    return this.request<StrategyValidationResponse>(`/api/strategies/${encodeURIComponent(strategyName)}/validate`, {
      method: 'POST',
      body: config ? JSON.stringify(config) : undefined,
    });
  }

  async moveStrategy(strategyName: string, request: StrategyMoveRequest): Promise<StrategyMoveResponse> {
    return this.request<StrategyMoveResponse>(`/api/strategies/${encodeURIComponent(strategyName)}/move`, {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async updateStrategy(strategyName: string, request: StrategyUpdateRequest): Promise<StrategyUpdateResponse> {
    return this.request<StrategyUpdateResponse>(`/api/strategies/${encodeURIComponent(strategyName)}`, {
      method: 'PUT',
      body: JSON.stringify(request),
    });
  }

  async deleteStrategy(strategyName: string): Promise<StrategyDeleteResponse> {
    return this.request<StrategyDeleteResponse>(`/api/strategies/${encodeURIComponent(strategyName)}`, {
      method: 'DELETE',
    });
  }

  async duplicateStrategy(strategyName: string, request: StrategyDuplicateRequest): Promise<StrategyDuplicateResponse> {
    return this.request<StrategyDuplicateResponse>(`/api/strategies/${encodeURIComponent(strategyName)}/duplicate`, {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async renameStrategy(strategyName: string, request: StrategyRenameRequest): Promise<StrategyRenameResponse> {
    return this.request<StrategyRenameResponse>(`/api/strategies/${encodeURIComponent(strategyName)}/rename`, {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  // Backtest
  async runBacktest(request: BacktestRequest): Promise<BacktestJobResponse> {
    return this.request<BacktestJobResponse>('/api/backtest/run', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async getJobStatus(jobId: string): Promise<BacktestJobResponse> {
    return this.request<BacktestJobResponse>(`/api/backtest/jobs/${encodeURIComponent(jobId)}`);
  }

  async listJobs(limit = 50): Promise<BacktestJobResponse[]> {
    return this.request<BacktestJobResponse[]>(`/api/backtest/jobs?limit=${limit}`);
  }

  async cancelJob(jobId: string): Promise<BacktestJobResponse> {
    return this.request<BacktestJobResponse>(`/api/backtest/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: 'POST',
    });
  }

  async getResult(jobId: string, includeHtml = false): Promise<BacktestResultResponse> {
    const params = includeHtml ? '?include_html=true' : '';
    return this.request<BacktestResultResponse>(`/api/backtest/result/${encodeURIComponent(jobId)}${params}`);
  }

  async runSignalAttribution(request: SignalAttributionRequest): Promise<SignalAttributionJobResponse> {
    return this.request<SignalAttributionJobResponse>('/api/backtest/attribution/run', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async getSignalAttributionJob(jobId: string): Promise<SignalAttributionJobResponse> {
    return this.request<SignalAttributionJobResponse>(`/api/backtest/attribution/jobs/${encodeURIComponent(jobId)}`);
  }

  async cancelSignalAttributionJob(jobId: string): Promise<SignalAttributionJobResponse> {
    return this.request<SignalAttributionJobResponse>(
      `/api/backtest/attribution/jobs/${encodeURIComponent(jobId)}/cancel`,
      { method: 'POST' }
    );
  }

  async getSignalAttributionResult(jobId: string): Promise<SignalAttributionResultResponse> {
    return this.request<SignalAttributionResultResponse>(
      `/api/backtest/attribution/result/${encodeURIComponent(jobId)}`
    );
  }

  async listAttributionArtifactFiles(params?: StrategyLimitParams): Promise<AttributionArtifactListResponse> {
    return this.request<AttributionArtifactListResponse>(
      `/api/backtest/attribution-files?${buildStrategyLimitQuery(params)}`
    );
  }

  async getAttributionArtifactContent(strategy: string, filename: string): Promise<AttributionArtifactContentResponse> {
    const query = new URLSearchParams({
      strategy,
      filename,
    });
    return this.request<AttributionArtifactContentResponse>(
      `/api/backtest/attribution-files/content?${query.toString()}`
    );
  }

  async listHtmlFiles(params?: { strategy?: string; limit?: number }): Promise<HtmlFileListResponse> {
    return this.request<HtmlFileListResponse>(`/api/backtest/html-files?${buildStrategyLimitQuery(params)}`);
  }

  async getHtmlFileContent(strategy: string, filename: string): Promise<HtmlFileContentResponse> {
    return this.request<HtmlFileContentResponse>(
      `/api/backtest/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}`
    );
  }

  async renameHtmlFile(
    strategy: string,
    filename: string,
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

  async deleteHtmlFile(strategy: string, filename: string): Promise<HtmlFileDeleteResponse> {
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
  ): Promise<DefaultConfigUpdateResponse> {
    return this.request<DefaultConfigUpdateResponse>('/api/config/default/structured', {
      method: 'PUT',
      body: JSON.stringify(request),
    });
  }

  async getSignalReference(): Promise<SignalReferenceResponse> {
    return this.request<SignalReferenceResponse>('/api/signals/reference');
  }

  // Optimization
  async runOptimization(request: OptimizationRequest): Promise<OptimizationJobResponse> {
    return this.request<OptimizationJobResponse>('/api/optimize/run', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async getOptimizationJobStatus(jobId: string): Promise<OptimizationJobResponse> {
    return this.request<OptimizationJobResponse>(`/api/optimize/jobs/${encodeURIComponent(jobId)}`);
  }

  async cancelOptimizationJob(jobId: string): Promise<OptimizationJobResponse> {
    return this.request<OptimizationJobResponse>(`/api/optimize/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: 'POST',
    });
  }

  async getStrategyOptimization(strategyName: string): Promise<StrategyOptimizationStateResponse> {
    return this.request<StrategyOptimizationStateResponse>(
      `/api/strategies/${encodeURIComponent(strategyName)}/optimization`
    );
  }

  async generateStrategyOptimizationDraft(strategyName: string): Promise<StrategyOptimizationStateResponse> {
    return this.request<StrategyOptimizationStateResponse>(
      `/api/strategies/${encodeURIComponent(strategyName)}/optimization/draft`,
      {
        method: 'POST',
      }
    );
  }

  async saveStrategyOptimization(
    strategyName: string,
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

  async deleteStrategyOptimization(strategyName: string): Promise<StrategyOptimizationDeleteResponse> {
    return this.request<StrategyOptimizationDeleteResponse>(
      `/api/strategies/${encodeURIComponent(strategyName)}/optimization`,
      {
        method: 'DELETE',
      }
    );
  }

  async listOptimizationHtmlFiles(params?: StrategyLimitParams): Promise<OptimizationHtmlFileListResponse> {
    return this.request<OptimizationHtmlFileListResponse>(
      `/api/optimize/html-files?${buildStrategyLimitQuery(params)}`
    );
  }

  async getOptimizationHtmlFileContent(
    strategy: string,
    filename: string
  ): Promise<OptimizationHtmlFileContentResponse> {
    return this.request<OptimizationHtmlFileContentResponse>(
      `/api/optimize/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}`
    );
  }

  async renameOptimizationHtmlFile(
    strategy: string,
    filename: string,
    request: HtmlFileRenameRequest
  ): Promise<HtmlFileRenameResponse> {
    return this.request<HtmlFileRenameResponse>(
      `/api/optimize/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}/rename`,
      {
        method: 'POST',
        body: JSON.stringify(request),
      }
    );
  }

  async deleteOptimizationHtmlFile(strategy: string, filename: string): Promise<HtmlFileDeleteResponse> {
    return this.request<HtmlFileDeleteResponse>(
      `/api/optimize/html-files/${encodeURIComponent(strategy)}/${encodeURIComponent(filename)}`,
      {
        method: 'DELETE',
      }
    );
  }

  async runSignalAttributionAndWait(
    request: SignalAttributionRequest,
    options?: WaitForJobOptions<SignalAttributionJobResponse>
  ): Promise<SignalAttributionJobResponse> {
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
    options?: WaitForJobOptions<BacktestJobResponse>
  ): Promise<BacktestJobResponse> {
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
