import { buildUrl, type QueryParams, requestJson } from '../base/http-client.js';
import type {
  AnalyticsClientConfig,
  CostStructureAnalysisParams,
  CostStructureResponse,
  FactorRegressionParams,
  FactorRegressionResponse,
  FundamentalRankingParams,
  FundamentalsParams,
  MarginPressureIndicatorsParams,
  MarginVolumeRatioParams,
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
  SectorStocksParams,
  Topix100RankingParams,
  Topix100RankingResponse,
  ValueCompositeRankingParams,
  ValueCompositeRankingResponse,
  ValueCompositeScoreParams,
  ValueCompositeScoreResponse,
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

  async getTopix100Ranking(params: Topix100RankingParams = {}): Promise<Topix100RankingResponse> {
    return this.request<Topix100RankingResponse>('/api/analytics/topix100-ranking', undefined, {
      date: params.date,
      studyMode: params.studyMode,
      metric: params.metric,
      smaWindow: params.smaWindow,
    });
  }

  async getValueCompositeRanking(params: ValueCompositeRankingParams = {}): Promise<ValueCompositeRankingResponse> {
    return this.request<ValueCompositeRankingResponse>('/api/analytics/value-composite-ranking', undefined, {
      date: params.date,
      limit: params.limit,
      markets: params.markets,
      scoreMethod: params.scoreMethod,
      forwardEpsMode: params.forwardEpsMode,
    });
  }

  async getValueCompositeScore(params: ValueCompositeScoreParams): Promise<ValueCompositeScoreResponse> {
    return this.request<ValueCompositeScoreResponse>(
      `/api/analytics/value-composite-score/${encodeURIComponent(params.symbol)}`,
      undefined,
      {
        date: params.date,
        forwardEpsMode: params.forwardEpsMode,
      }
    );
  }

  async getFundamentals<T>(params: FundamentalsParams): Promise<T> {
    return this.request<T>(`/api/analytics/fundamentals/${encodeURIComponent(params.symbol)}`, undefined, {
      tradingValuePeriod: params.tradingValuePeriod,
      forecastEpsLookbackFyCount: params.forecastEpsLookbackFyCount,
    });
  }

  async getMarginPressureIndicators<T>(params: MarginPressureIndicatorsParams): Promise<T> {
    return this.request<T>(`/api/analytics/stocks/${encodeURIComponent(params.symbol)}/margin-pressure`, undefined, {
      period: params.period,
    });
  }

  async getMarginVolumeRatio<T>(params: MarginVolumeRatioParams): Promise<T> {
    return this.request<T>(`/api/analytics/stocks/${encodeURIComponent(params.symbol)}/margin-ratio`);
  }

  async getCostStructureAnalysis(params: CostStructureAnalysisParams): Promise<CostStructureResponse> {
    return this.request<CostStructureResponse>(
      `/api/analytics/stocks/${encodeURIComponent(params.symbol)}/cost-structure`,
      undefined,
      {
        view: params.view,
        windowQuarters: params.windowQuarters,
      }
    );
  }

  async getSectorStocks<T>(params: SectorStocksParams = {}): Promise<T> {
    return this.request<T>('/api/analytics/sector-stocks', undefined, {
      sector33Name: params.sector33Name,
      sector17Name: params.sector17Name,
      markets: params.markets,
      lookbackDays: params.lookbackDays,
      sortBy: params.sortBy,
      sortOrder: params.sortOrder,
      limit: params.limit,
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
