import { buildUrl, requestJson, type QueryParams } from '../base/http-client.js';
import type {
  AnalyticsClientConfig,
  FactorRegressionParams,
  FactorRegressionResponse,
  FundamentalRankingParams,
  MarketFundamentalRankingResponse,
  MarketRankingParams,
  MarketRankingResponse,
  MarketScreeningResponse,
  PortfolioFactorRegressionParams,
  PortfolioFactorRegressionResponse,
  ROEParams,
  ROEResponse,
  ScreeningJobRequest,
  ScreeningJobResponse,
} from './types.js';

function normalizeConfig(config?: string | Partial<AnalyticsClientConfig>): AnalyticsClientConfig {
  if (typeof config === 'string') {
    return { baseUrl: config };
  }
  return {
    baseUrl: config?.baseUrl,
    timeoutMs: config?.timeoutMs,
  };
}

export class AnalyticsClient {
  private readonly baseUrl?: string;
  private readonly timeoutMs?: number;

  constructor(config?: string | Partial<AnalyticsClientConfig>) {
    const normalized = normalizeConfig(config);
    this.baseUrl = normalized.baseUrl;
    this.timeoutMs = normalized.timeoutMs;
  }

  private request<T>(path: string, options?: RequestInit, query?: QueryParams): Promise<T> {
    const url = buildUrl(path, query);
    const hasBody = options?.body != null;
    const headers = hasBody
      ? {
          'Content-Type': 'application/json',
          ...options?.headers,
        }
      : options?.headers;

    return requestJson<T>(url, {
      ...options,
      headers,
      baseUrl: this.baseUrl,
      timeoutMs: this.timeoutMs,
    });
  }

  async getMarketRanking(params: MarketRankingParams = {}): Promise<MarketRankingResponse> {
    return this.request<MarketRankingResponse>('/api/analytics/ranking', undefined, {
      date: params.date,
      limit: params.limit,
      markets: params.markets,
      lookbackDays: params.lookbackDays,
      periodDays: params.periodDays,
    });
  }

  async getFundamentalRanking(params: FundamentalRankingParams = {}): Promise<MarketFundamentalRankingResponse> {
    return this.request<MarketFundamentalRankingResponse>('/api/analytics/fundamental-ranking', undefined, {
      limit: params.limit,
      markets: params.markets,
      forecastAboveRecentFyActuals: params.forecastAboveRecentFyActuals,
      forecastLookbackFyCount: params.forecastLookbackFyCount,
      forecastAboveAllActuals: params.forecastAboveAllActuals,
    });
  }

  async createScreeningJob(params: ScreeningJobRequest): Promise<ScreeningJobResponse> {
    return this.request<ScreeningJobResponse>('/api/analytics/screening/jobs', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  }

  async getScreeningJobStatus(jobId: string): Promise<ScreeningJobResponse> {
    return this.request<ScreeningJobResponse>(`/api/analytics/screening/jobs/${encodeURIComponent(jobId)}`);
  }

  async cancelScreeningJob(jobId: string): Promise<ScreeningJobResponse> {
    return this.request<ScreeningJobResponse>(`/api/analytics/screening/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: 'POST',
    });
  }

  async getScreeningResult(jobId: string): Promise<MarketScreeningResponse> {
    return this.request<MarketScreeningResponse>(`/api/analytics/screening/result/${encodeURIComponent(jobId)}`);
  }

  async getROE(params: ROEParams): Promise<ROEResponse> {
    return this.request<ROEResponse>('/api/analytics/roe', undefined, {
      code: params.code,
      date: params.date,
      annualize: params.annualize,
      preferConsolidated: params.preferConsolidated,
      minEquity: params.minEquity,
      sortBy: params.sortBy,
      limit: params.limit,
    });
  }

  async getFactorRegression(params: FactorRegressionParams): Promise<FactorRegressionResponse> {
    return this.request<FactorRegressionResponse>(
      `/api/analytics/factor-regression/${encodeURIComponent(params.symbol)}`,
      undefined,
      {
        lookbackDays: params.lookbackDays,
      }
    );
  }

  async getPortfolioFactorRegression(
    params: PortfolioFactorRegressionParams
  ): Promise<PortfolioFactorRegressionResponse> {
    return this.request<PortfolioFactorRegressionResponse>(
      `/api/analytics/portfolio-factor-regression/${params.portfolioId}`,
      undefined,
      {
        lookbackDays: params.lookbackDays,
      }
    );
  }
}
