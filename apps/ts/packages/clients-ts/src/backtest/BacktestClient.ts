/**
 * Backtest API Client
 *
 * trading25-bt FastAPI サーバーと通信するクライアント
 */

import type {
  BacktestClientConfig,
  BacktestJobResponse,
  BacktestRequest,
  BacktestResultResponse,
  FundamentalsComputeRequest,
  FundamentalsComputeResponse,
  HealthResponse,
  OHLCVResampleRequest,
  OHLCVResampleResponse,
  SignalAttributionJobResponse,
  SignalAttributionRequest,
  SignalAttributionResultResponse,
  StrategyDetailResponse,
  StrategyListResponse,
  StrategyValidationRequest,
  StrategyValidationResponse,
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

export class BacktestClient {
  private baseUrl: string;
  private timeout: number;

  constructor(config?: Partial<BacktestClientConfig>) {
    this.baseUrl = config?.baseUrl ?? process.env.BT_API_URL ?? 'http://localhost:3002';
    this.timeout = config?.timeout ?? Number(process.env.BT_API_TIMEOUT ?? 600000);
  }

  private async request<T>(endpoint: string, options?: RequestInit): Promise<T> {
    const url = `${this.baseUrl}${endpoint}`;
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
