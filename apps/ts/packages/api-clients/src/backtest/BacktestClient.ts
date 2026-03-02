/**
 * Backtest API Client
 *
 * trading25-bt FastAPI サーバーと通信するクライアント
 */

import type {
  AttributionArtifactContentResponse,
  AttributionArtifactListResponse,
  BacktestClientConfig,
  BacktestJobResponse,
  BacktestRequest,
  BacktestResultResponse,
  FundamentalsComputeRequest,
  FundamentalsComputeResponse,
  HealthResponse,
  HtmlFileContentResponse,
  HtmlFileListResponse,
  HtmlFileDeleteResponse,
  HtmlFileRenameRequest,
  HtmlFileRenameResponse,
  OHLCVResampleRequest,
  OHLCVResampleResponse,
  OptimizationGridConfig,
  OptimizationGridListResponse,
  OptimizationGridSaveRequest,
  OptimizationGridSaveResponse,
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
  StrategyListResponse,
  StrategyMoveRequest,
  StrategyMoveResponse,
  StrategyRenameRequest,
  StrategyRenameResponse,
  StrategyUpdateRequest,
  StrategyUpdateResponse,
  StrategyValidationRequest,
  StrategyValidationResponse,
  DefaultConfigResponse,
  DefaultConfigUpdateRequest,
  DefaultConfigUpdateResponse,
} from './types.js';

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

export class BacktestClient {
  private readonly baseUrl?: string;
  private readonly timeout: number;

  constructor(config?: Partial<BacktestClientConfig>) {
    this.baseUrl = config?.baseUrl ?? resolveProcessEnv('BT_API_URL') ?? resolveDefaultBaseUrl();
    this.timeout = config?.timeout ?? Number(resolveProcessEnv('BT_API_TIMEOUT') ?? 600000);
  }

  private async request<T>(endpoint: string, options?: RequestInit): Promise<T> {
    const url = this.baseUrl ? `${this.baseUrl}${endpoint}` : endpoint;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);

    try {
      const response = await fetch(url, {
        ...options,
        signal: controller.signal,
        headers: {
          'Content-Type': 'application/json',
          ...options?.headers,
        },
      });

      if (!response.ok) {
        const errorBody = await response.text();
        throw new BacktestApiError(response.status, response.statusText, errorBody);
      }

      const text = await response.text();
      if (!text) {
        throw new BacktestApiError(response.status, response.statusText || 'Unknown', 'Empty response body');
      }
      try {
        return JSON.parse(text) as T;
      } catch (parseError) {
        const parseMessage = parseError instanceof Error ? parseError.message : String(parseError);
        const parseReason = parseMessage.slice(0, 80);
        const bodyPreview = text.slice(0, 150);
        throw new BacktestApiError(
          response.status,
          response.statusText || 'Unknown',
          `Invalid JSON response (${parseReason}): ${bodyPreview}`
        );
      }
    } finally {
      clearTimeout(timeoutId);
    }
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

  async duplicateStrategy(
    strategyName: string,
    request: StrategyDuplicateRequest
  ): Promise<StrategyDuplicateResponse> {
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
    return this.request<SignalAttributionResultResponse>(`/api/backtest/attribution/result/${encodeURIComponent(jobId)}`);
  }

  async listAttributionArtifactFiles(params?: {
    strategy?: string;
    limit?: number;
  }): Promise<AttributionArtifactListResponse> {
    const query = new URLSearchParams();
    if (params?.strategy) query.set('strategy', params.strategy);
    query.set('limit', String(params?.limit ?? 100));

    return this.request<AttributionArtifactListResponse>(`/api/backtest/attribution-files?${query.toString()}`);
  }

  async getAttributionArtifactContent(
    strategy: string,
    filename: string
  ): Promise<AttributionArtifactContentResponse> {
    const query = new URLSearchParams({
      strategy,
      filename,
    });
    return this.request<AttributionArtifactContentResponse>(`/api/backtest/attribution-files/content?${query.toString()}`);
  }

  async listHtmlFiles(params?: {
    strategy?: string;
    limit?: number;
  }): Promise<HtmlFileListResponse> {
    const query = new URLSearchParams();
    if (params?.strategy) query.set('strategy', params.strategy);
    query.set('limit', String(params?.limit ?? 100));
    return this.request<HtmlFileListResponse>(`/api/backtest/html-files?${query.toString()}`);
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

  async updateDefaultConfig(request: DefaultConfigUpdateRequest): Promise<DefaultConfigUpdateResponse> {
    return this.request<DefaultConfigUpdateResponse>('/api/config/default', {
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

  async getOptimizationGridConfigs(): Promise<OptimizationGridListResponse> {
    return this.request<OptimizationGridListResponse>('/api/optimize/grid-configs');
  }

  async getOptimizationGridConfig(strategy: string): Promise<OptimizationGridConfig> {
    return this.request<OptimizationGridConfig>(`/api/optimize/grid-configs/${encodeURIComponent(strategy)}`);
  }

  async saveOptimizationGridConfig(
    strategy: string,
    request: OptimizationGridSaveRequest
  ): Promise<OptimizationGridSaveResponse> {
    return this.request<OptimizationGridSaveResponse>(`/api/optimize/grid-configs/${encodeURIComponent(strategy)}`, {
      method: 'PUT',
      body: JSON.stringify(request),
    });
  }

  async deleteOptimizationGridConfig(strategy: string): Promise<{ success: boolean; strategy_name: string }> {
    return this.request<{ success: boolean; strategy_name: string }>(
      `/api/optimize/grid-configs/${encodeURIComponent(strategy)}`,
      {
        method: 'DELETE',
      }
    );
  }

  async listOptimizationHtmlFiles(params?: {
    strategy?: string;
    limit?: number;
  }): Promise<OptimizationHtmlFileListResponse> {
    const query = new URLSearchParams();
    if (params?.strategy) query.set('strategy', params.strategy);
    query.set('limit', String(params?.limit ?? 100));

    return this.request<OptimizationHtmlFileListResponse>(`/api/optimize/html-files?${query.toString()}`);
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
    options?: {
      pollInterval?: number;
      onProgress?: (job: SignalAttributionJobResponse) => void;
    }
  ): Promise<SignalAttributionJobResponse> {
    const pollInterval = options?.pollInterval ?? 2000;

    const initialJob = await this.runSignalAttribution(request);

    let job = initialJob;
    while (job.status === 'pending' || job.status === 'running') {
      await new Promise((resolve) => setTimeout(resolve, pollInterval));
      job = await this.getSignalAttributionJob(job.job_id);
      options?.onProgress?.(job);
    }

    return job;
  }

  /**
   * バックテストを実行し、完了まで待機
   * @param request バックテストリクエスト
   * @param options ポーリングオプション
   * @returns 完了したジョブレスポンス
   */
  async runAndWait(
    request: BacktestRequest,
    options?: {
      pollInterval?: number;
      onProgress?: (job: BacktestJobResponse) => void;
    }
  ): Promise<BacktestJobResponse> {
    const pollInterval = options?.pollInterval ?? 2000;

    // ジョブを開始
    const initialJob = await this.runBacktest(request);

    // 完了までポーリング
    let job = initialJob;
    while (job.status === 'pending' || job.status === 'running') {
      await new Promise((resolve) => setTimeout(resolve, pollInterval));
      job = await this.getJobStatus(job.job_id);
      options?.onProgress?.(job);
    }

    return job;
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
