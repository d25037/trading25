import { BaseApiClient, toQueryString } from './base-client.js';
import type {
  FactorRegressionResponse,
  MarketRankingResponse,
  MarketScreeningResponse,
  PortfolioFactorRegressionResponse,
  ROEResponse,
  ScreeningJobResponse,
} from './types.js';

export class AnalyticsClient extends BaseApiClient {
  /**
   * Get market rankings (trading value, gainers, losers)
   */
  async getMarketRanking(params: {
    date?: string;
    limit?: number;
    markets?: string;
    lookbackDays?: number;
  }): Promise<MarketRankingResponse> {
    const query = toQueryString({
      date: params.date,
      limit: params.limit,
      markets: params.markets,
      lookbackDays: params.lookbackDays,
    });
    const url = `/api/analytics/ranking${query ? `?${query}` : ''}`;
    return this.request<MarketRankingResponse>(url);
  }

  /**
   * Create screening job
   */
  async createScreeningJob(params: {
    markets?: string;
    strategies?: string;
    recentDays?: number;
    date?: string;
    sortBy?: 'bestStrategyScore' | 'matchedDate' | 'stockCode' | 'matchStrategyCount';
    order?: 'asc' | 'desc';
    limit?: number;
  }): Promise<ScreeningJobResponse> {
    return this.request<ScreeningJobResponse>('/api/analytics/screening/jobs', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  }

  /**
   * Get screening job status
   */
  async getScreeningJobStatus(jobId: string): Promise<ScreeningJobResponse> {
    return this.request<ScreeningJobResponse>(`/api/analytics/screening/jobs/${encodeURIComponent(jobId)}`);
  }

  /**
   * Cancel screening job
   */
  async cancelScreeningJob(jobId: string): Promise<ScreeningJobResponse> {
    return this.request<ScreeningJobResponse>(`/api/analytics/screening/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: 'POST',
    });
  }

  /**
   * Get completed screening result
   */
  async getScreeningResult(jobId: string): Promise<MarketScreeningResponse> {
    return this.request<MarketScreeningResponse>(`/api/analytics/screening/result/${encodeURIComponent(jobId)}`);
  }

  /**
   * Calculate ROE from financial statements
   */
  async getROE(params: {
    code?: string;
    date?: string;
    annualize?: boolean;
    preferConsolidated?: boolean;
    minEquity?: number;
    sortBy?: 'roe' | 'code' | 'date';
    limit?: number;
  }): Promise<ROEResponse> {
    const query = toQueryString({
      code: params.code,
      date: params.date,
      annualize: params.annualize,
      preferConsolidated: params.preferConsolidated,
      minEquity: params.minEquity,
      sortBy: params.sortBy,
      limit: params.limit,
    });
    const url = `/api/analytics/roe${query ? `?${query}` : ''}`;
    return this.request<ROEResponse>(url);
  }

  /**
   * Perform factor regression analysis for risk decomposition
   */
  async getFactorRegression(params: { symbol: string; lookbackDays?: number }): Promise<FactorRegressionResponse> {
    const query = toQueryString({
      lookbackDays: params.lookbackDays,
    });
    const url = `/api/analytics/factor-regression/${params.symbol}${query ? `?${query}` : ''}`;
    return this.request<FactorRegressionResponse>(url);
  }

  /**
   * Perform factor regression analysis for a portfolio
   */
  async getPortfolioFactorRegression(params: {
    portfolioId: number;
    lookbackDays?: number;
  }): Promise<PortfolioFactorRegressionResponse> {
    const query = toQueryString({
      lookbackDays: params.lookbackDays,
    });
    const url = `/api/analytics/portfolio-factor-regression/${params.portfolioId}${query ? `?${query}` : ''}`;
    return this.request<PortfolioFactorRegressionResponse>(url);
  }
}
