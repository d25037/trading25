/**
 * API Client for CLI commands
 * Provides methods to call the backend API server
 */

import type { DatasetPreset } from '@trading25/shared/dataset';
import type {
  DeleteResponse,
  ListPortfoliosResponse,
  PortfolioItemResponse,
  PortfolioResponse,
  PortfolioWithItemsResponse,
} from '@trading25/shared/portfolio';
import type {
  AdjustmentEvent,
  CancelDatasetJobResponse,
  CancelJobResponse,
  CreateSyncJobResponse,
  DatasetCreateJobResponse,
  DatasetInfoResponse,
  DatasetJobResponse,
  IntegrityIssue,
  MarketRankingResponse,
  MarketScreeningResponse,
  MarketValidationResponse,
  RankingItem,
  Rankings,
  ScreeningResultItem,
  SyncJobResponse,
  SyncMode,
} from '@trading25/shared/types/api-response-types';
import type {
  ListWatchlistsResponse,
  WatchlistDeleteResponse,
  WatchlistItemResponse,
  WatchlistResponse,
  WatchlistWithItemsResponse,
} from '@trading25/shared/watchlist';

// Re-export shared types used by CLI commands
export type {
  AdjustmentEvent,
  IntegrityIssue,
  MarketRankingResponse,
  MarketScreeningResponse,
  MarketValidationResponse,
  ScreeningResultItem,
  RankingItem as MarketRankingItem,
  Rankings as MarketRankings,
};
export type { DatasetPreset } from '@trading25/shared/dataset';
export type {
  CancelDatasetJobResponse,
  CancelJobResponse,
  CreateSyncJobResponse,
  DatasetCreateJobResponse,
  DatasetCreateJobResponse as CreateDatasetJobResponse,
  DatasetInfoResponse,
  DatasetJobProgress,
  DatasetJobResponse,
  JobProgress,
  JobStatus,
  SyncJobResponse,
  SyncJobResult,
  SyncMode,
} from '@trading25/shared/types/api-response-types';

/**
 * ROE API response types
 */
export interface ROEMetadata {
  code: string;
  periodType: string;
  periodEnd: string;
  isConsolidated: boolean;
  accountingStandard: string | null;
  isAnnualized?: boolean;
}

export interface ROEResultItem {
  roe: number;
  netProfit: number;
  equity: number;
  metadata: ROEMetadata;
}

export interface ROESummary {
  averageROE: number;
  maxROE: number;
  minROE: number;
  totalCompanies: number;
}

export interface ROEResponse {
  results: ROEResultItem[];
  summary: ROESummary;
  lastUpdated: string;
}

/**
 * Factor Regression API response types
 */
export interface IndexMatch {
  indexCode: string;
  indexName: string;
  category: string;
  rSquared: number;
  beta: number;
}

export interface FactorRegressionResponse {
  stockCode: string;
  companyName?: string;
  marketBeta: number;
  marketRSquared: number;
  sector17Matches: IndexMatch[];
  sector33Matches: IndexMatch[];
  topixStyleMatches: IndexMatch[];
  analysisDate: string;
  dataPoints: number;
  dateRange: {
    from: string;
    to: string;
  };
}

/**
 * Portfolio Weight in factor regression response
 */
export interface PortfolioWeight {
  code: string;
  companyName: string;
  weight: number;
  latestPrice: number;
  marketValue: number;
  quantity: number;
}

/**
 * Excluded stock from portfolio analysis
 */
export interface ExcludedStock {
  code: string;
  companyName: string;
  reason: string;
}

/**
 * Portfolio Factor Regression API response types
 */
export interface PortfolioFactorRegressionResponse {
  portfolioId: number;
  portfolioName: string;
  weights: PortfolioWeight[];
  totalValue: number;
  stockCount: number;
  includedStockCount: number;
  marketBeta: number;
  marketRSquared: number;
  sector17Matches: IndexMatch[];
  sector33Matches: IndexMatch[];
  topixStyleMatches: IndexMatch[];
  analysisDate: string;
  dataPoints: number;
  dateRange: {
    from: string;
    to: string;
  };
  excludedStocks: ExcludedStock[];
}

export class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string = process.env.API_BASE_URL || 'http://localhost:3001') {
    this.baseUrl = baseUrl;
  }

  private async request<T>(endpoint: string, options?: RequestInit): Promise<T> {
    const url = `${this.baseUrl}${endpoint}`;

    try {
      const response = await fetch(url, {
        ...options,
        headers: {
          'Content-Type': 'application/json',
          ...options?.headers,
        },
      });

      if (!response.ok) {
        const error = (await response.json()) as { message?: string };
        throw new Error(error.message || `HTTP error! status: ${response.status}`);
      }

      return (await response.json()) as T;
    } catch (error) {
      if (error instanceof Error) {
        if (error.message.includes('fetch failed') || error.message.includes('ECONNREFUSED')) {
          throw new Error(
            'Cannot connect to API server. Please ensure the API server is running with "bun run dev" in packages/api'
          );
        }
        throw error;
      }
      throw new Error('Unknown error occurred');
    }
  }

  /**
   * Get daily stock quotes for chart display
   */
  async getDailyQuotes(symbol: string, params?: { from?: string; to?: string; date?: string }) {
    const queryParams = new URLSearchParams();
    if (params?.from) queryParams.append('from', params.from);
    if (params?.to) queryParams.append('to', params.to);
    if (params?.date) queryParams.append('date', params.date);

    const query = queryParams.toString();
    const url = `/api/chart/stocks/${symbol}${query ? `?${query}` : ''}`;

    return this.request<{
      data: Array<{
        time: string;
        open: number;
        high: number;
        low: number;
        close: number;
        volume?: number;
      }>;
      symbol: string;
      companyName?: string;
      timeframe: string;
      lastUpdated: string;
    }>(url);
  }

  /**
   * Get listed stock information
   */
  async getListedInfo(params?: { code?: string; date?: string }) {
    const queryParams = new URLSearchParams();
    if (params?.code) queryParams.append('code', params.code);
    if (params?.date) queryParams.append('date', params.date);

    const query = queryParams.toString();
    const url = `/api/jquants/listed-info${query ? `?${query}` : ''}`;

    return this.request<{
      info: Array<{
        code: string;
        companyName: string;
        companyNameEnglish?: string;
        marketCode?: string;
        marketCodeName?: string;
        sector33Code?: string;
        sector33CodeName?: string;
        scaleCategory?: string;
      }>;
      lastUpdated: string;
    }>(url);
  }

  /**
   * Get weekly margin interest data
   */
  async getMarginInterest(symbol: string, params?: { from?: string; to?: string; date?: string }) {
    const queryParams = new URLSearchParams();
    if (params?.from) queryParams.append('from', params.from);
    if (params?.to) queryParams.append('to', params.to);
    if (params?.date) queryParams.append('date', params.date);

    const query = queryParams.toString();
    const url = `/api/jquants/stocks/${symbol}/margin-interest${query ? `?${query}` : ''}`;

    return this.request<{
      symbol: string;
      marginInterest: Array<{
        date: string;
        code: string;
        shortMarginTradeVolume: number;
        longMarginTradeVolume: number;
      }>;
      lastUpdated: string;
    }>(url);
  }

  /**
   * Get index data
   */
  async getIndices(params?: { code?: string; from?: string; to?: string; date?: string }) {
    const queryParams = new URLSearchParams();
    if (params?.code) queryParams.append('code', params.code);
    if (params?.from) queryParams.append('from', params.from);
    if (params?.to) queryParams.append('to', params.to);
    if (params?.date) queryParams.append('date', params.date);

    const query = queryParams.toString();
    const url = `/api/jquants/indices${query ? `?${query}` : ''}`;

    return this.request<{
      indices: Array<{
        date: string;
        code?: string;
        open: number;
        high: number;
        low: number;
        close: number;
      }>;
      lastUpdated: string;
    }>(url);
  }

  /**
   * Get TOPIX index data for chart display
   */
  async getTOPIX(params?: { from?: string; to?: string; date?: string }) {
    const queryParams = new URLSearchParams();
    if (params?.from) queryParams.append('from', params.from);
    if (params?.to) queryParams.append('to', params.to);
    if (params?.date) queryParams.append('date', params.date);

    const query = queryParams.toString();
    const url = `/api/chart/indices/topix${query ? `?${query}` : ''}`;

    return this.request<{
      topix: Array<{
        date: string;
        open: number;
        high: number;
        low: number;
        close: number;
        volume: number;
      }>;
      lastUpdated: string;
    }>(url);
  }

  // ===== MARKET ANALYTICS =====

  /**
   * Get market rankings (trading value, gainers, losers)
   */
  async getMarketRanking(params: {
    date?: string;
    limit?: number;
    markets?: string;
    lookbackDays?: number;
  }): Promise<MarketRankingResponse> {
    const queryParams = new URLSearchParams();
    if (params.date) queryParams.append('date', params.date);
    if (params.limit !== undefined) queryParams.append('limit', String(params.limit));
    if (params.markets) queryParams.append('markets', params.markets);
    if (params.lookbackDays !== undefined) queryParams.append('lookbackDays', String(params.lookbackDays));

    const query = queryParams.toString();
    const url = `/api/analytics/ranking${query ? `?${query}` : ''}`;

    return this.request<MarketRankingResponse>(url);
  }

  /**
   * Validate market database
   */
  async validateMarketDatabase(): Promise<MarketValidationResponse> {
    return this.request<MarketValidationResponse>('/api/db/validate');
  }

  /**
   * Run stock screening
   */
  async runMarketScreening(params: {
    markets?: string;
    rangeBreakFast?: boolean;
    rangeBreakSlow?: boolean;
    recentDays?: number;
    date?: string;
    minBreakPercentage?: number;
    minVolumeRatio?: number;
    sortBy?: 'date' | 'stockCode' | 'volumeRatio' | 'breakPercentage';
    order?: 'asc' | 'desc';
    limit?: number;
  }): Promise<MarketScreeningResponse> {
    const queryParams = new URLSearchParams();
    if (params.markets) queryParams.append('markets', params.markets);
    if (params.rangeBreakFast !== undefined) queryParams.append('rangeBreakFast', String(params.rangeBreakFast));
    if (params.rangeBreakSlow !== undefined) queryParams.append('rangeBreakSlow', String(params.rangeBreakSlow));
    if (params.recentDays !== undefined) queryParams.append('recentDays', String(params.recentDays));
    if (params.date) queryParams.append('date', params.date);
    if (params.minBreakPercentage !== undefined)
      queryParams.append('minBreakPercentage', String(params.minBreakPercentage));
    if (params.minVolumeRatio !== undefined) queryParams.append('minVolumeRatio', String(params.minVolumeRatio));
    if (params.sortBy) queryParams.append('sortBy', params.sortBy);
    if (params.order) queryParams.append('order', params.order);
    if (params.limit !== undefined) queryParams.append('limit', String(params.limit));

    const query = queryParams.toString();
    const url = `/api/analytics/screening${query ? `?${query}` : ''}`;

    return this.request<MarketScreeningResponse>(url);
  }

  // ===== ROE ANALYSIS =====

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
    const queryParams = new URLSearchParams();
    if (params.code) queryParams.append('code', params.code);
    if (params.date) queryParams.append('date', params.date);
    if (params.annualize !== undefined) queryParams.append('annualize', String(params.annualize));
    if (params.preferConsolidated !== undefined)
      queryParams.append('preferConsolidated', String(params.preferConsolidated));
    if (params.minEquity !== undefined) queryParams.append('minEquity', String(params.minEquity));
    if (params.sortBy) queryParams.append('sortBy', params.sortBy);
    if (params.limit !== undefined) queryParams.append('limit', String(params.limit));

    const query = queryParams.toString();
    const url = `/api/analytics/roe${query ? `?${query}` : ''}`;

    return this.request<ROEResponse>(url);
  }

  // ===== FACTOR REGRESSION ANALYSIS =====

  /**
   * Perform factor regression analysis for risk decomposition
   */
  async getFactorRegression(params: { symbol: string; lookbackDays?: number }): Promise<FactorRegressionResponse> {
    const queryParams = new URLSearchParams();
    if (params.lookbackDays !== undefined) queryParams.append('lookbackDays', String(params.lookbackDays));

    const query = queryParams.toString();
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
    const queryParams = new URLSearchParams();
    if (params.lookbackDays !== undefined) queryParams.append('lookbackDays', String(params.lookbackDays));

    const query = queryParams.toString();
    const url = `/api/analytics/portfolio-factor-regression/${params.portfolioId}${query ? `?${query}` : ''}`;

    return this.request<PortfolioFactorRegressionResponse>(url);
  }

  // ===== PORTFOLIO MANAGEMENT =====

  /**
   * List all portfolios with summary statistics
   */
  async listPortfolios(): Promise<ListPortfoliosResponse> {
    return this.request<ListPortfoliosResponse>('/api/portfolio');
  }

  /**
   * Create a new portfolio
   */
  async createPortfolio(data: { name: string; description?: string }): Promise<PortfolioResponse> {
    return this.request<PortfolioResponse>('/api/portfolio', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  /**
   * Get portfolio with all items
   */
  async getPortfolio(id: number): Promise<PortfolioWithItemsResponse> {
    return this.request<PortfolioWithItemsResponse>(`/api/portfolio/${id}`);
  }

  /**
   * Update portfolio
   */
  async updatePortfolio(id: number, data: { name?: string; description?: string }): Promise<PortfolioResponse> {
    return this.request<PortfolioResponse>(`/api/portfolio/${id}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  }

  /**
   * Delete portfolio
   */
  async deletePortfolio(id: number): Promise<DeleteResponse> {
    return this.request<DeleteResponse>(`/api/portfolio/${id}`, {
      method: 'DELETE',
    });
  }

  /**
   * Add item to portfolio
   */
  async addPortfolioItem(
    portfolioId: number,
    data: {
      code: string;
      companyName?: string;
      quantity: number;
      purchasePrice: number;
      purchaseDate: string;
      account?: string;
      notes?: string;
    }
  ): Promise<PortfolioItemResponse> {
    return this.request<PortfolioItemResponse>(`/api/portfolio/${portfolioId}/items`, {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  // ===== ID-BASED ITEM METHODS (for programmatic access) =====

  /**
   * Update portfolio item by ID
   */
  async updatePortfolioItem(
    portfolioId: number,
    itemId: number,
    data: {
      quantity?: number;
      purchasePrice?: number;
      purchaseDate?: string;
      account?: string;
      notes?: string;
    }
  ): Promise<PortfolioItemResponse> {
    return this.request<PortfolioItemResponse>(`/api/portfolio/${portfolioId}/items/${itemId}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  }

  /**
   * Delete portfolio item by ID
   */
  async deletePortfolioItem(portfolioId: number, itemId: number): Promise<DeleteResponse> {
    return this.request<DeleteResponse>(`/api/portfolio/${portfolioId}/items/${itemId}`, {
      method: 'DELETE',
    });
  }

  // ===== WATCHLIST MANAGEMENT =====

  /**
   * List all watchlists with summary statistics
   */
  async listWatchlists(): Promise<ListWatchlistsResponse> {
    return this.request<ListWatchlistsResponse>('/api/watchlist');
  }

  /**
   * Create a new watchlist
   */
  async createWatchlist(data: { name: string; description?: string }): Promise<WatchlistResponse> {
    return this.request<WatchlistResponse>('/api/watchlist', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  /**
   * Get watchlist with all items
   */
  async getWatchlist(id: number): Promise<WatchlistWithItemsResponse> {
    return this.request<WatchlistWithItemsResponse>(`/api/watchlist/${id}`);
  }

  /**
   * Delete watchlist
   */
  async deleteWatchlist(id: number): Promise<WatchlistDeleteResponse> {
    return this.request<WatchlistDeleteResponse>(`/api/watchlist/${id}`, {
      method: 'DELETE',
    });
  }

  /**
   * Add item to watchlist
   */
  async addWatchlistItem(watchlistId: number, data: { code: string; memo?: string }): Promise<WatchlistItemResponse> {
    return this.request<WatchlistItemResponse>(`/api/watchlist/${watchlistId}/items`, {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  /**
   * Delete item from watchlist
   */
  async deleteWatchlistItem(watchlistId: number, itemId: number): Promise<WatchlistDeleteResponse> {
    return this.request<WatchlistDeleteResponse>(`/api/watchlist/${watchlistId}/items/${itemId}`, {
      method: 'DELETE',
    });
  }

  // ===== NAME+CODE-BASED STOCK METHODS (for CLI/human access) =====

  /**
   * Update stock in portfolio by name and code
   */
  async updatePortfolioStock(
    portfolioName: string,
    code: string,
    data: {
      quantity?: number;
      purchasePrice?: number;
      purchaseDate?: string;
      account?: string;
      notes?: string;
    }
  ): Promise<PortfolioItemResponse> {
    return this.request<PortfolioItemResponse>(`/api/portfolio/${encodeURIComponent(portfolioName)}/stocks/${code}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  }

  /**
   * Delete stock from portfolio by name and code
   */
  async deletePortfolioStock(portfolioName: string, code: string): Promise<PortfolioItemDeletedResponse> {
    return this.request<PortfolioItemDeletedResponse>(
      `/api/portfolio/${encodeURIComponent(portfolioName)}/stocks/${code}`,
      {
        method: 'DELETE',
      }
    );
  }

  // ===== JQUANTS AUTHENTICATION =====

  /**
   * Get JQuants authentication status
   */
  async getAuthStatus(): Promise<AuthStatusResponse> {
    return this.request<AuthStatusResponse>('/api/jquants/auth/status');
  }

  /**
   * Refresh JQuants authentication tokens
   */
  async refreshTokens(params: {
    mailAddress?: string;
    password?: string;
    refreshToken?: string;
  }): Promise<RefreshTokenResponse> {
    return this.request<RefreshTokenResponse>('/api/jquants/auth/refresh', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  }

  // ===== DATABASE MANAGEMENT =====

  /**
   * Get market database statistics
   */
  async getMarketStats(): Promise<MarketStatsResponse> {
    return this.request<MarketStatsResponse>('/api/db/stats');
  }

  /**
   * Refresh historical data for specific stocks
   */
  async refreshStocks(codes: string[]): Promise<MarketRefreshResponse> {
    return this.request<MarketRefreshResponse>('/api/db/stocks/refresh', {
      method: 'POST',
      body: JSON.stringify({ codes }),
    });
  }

  /**
   * Start a database sync job
   */
  async startSync(mode: SyncMode = 'auto'): Promise<CreateSyncJobResponse> {
    return this.request<CreateSyncJobResponse>('/api/db/sync', {
      method: 'POST',
      body: JSON.stringify({ mode }),
    });
  }

  /**
   * Get sync job status
   */
  async getSyncJobStatus(jobId: string): Promise<SyncJobResponse> {
    return this.request<SyncJobResponse>(`/api/db/sync/jobs/${jobId}`);
  }

  /**
   * Cancel a sync job
   */
  async cancelSyncJob(jobId: string): Promise<CancelJobResponse> {
    return this.request<CancelJobResponse>(`/api/db/sync/jobs/${jobId}`, {
      method: 'DELETE',
    });
  }

  // ===== DATASET MANAGEMENT =====

  /**
   * Start a dataset creation job
   */
  async startDatasetCreate(name: string, preset: DatasetPreset, overwrite = false): Promise<DatasetCreateJobResponse> {
    return this.request<DatasetCreateJobResponse>('/api/dataset', {
      method: 'POST',
      body: JSON.stringify({ name, preset, overwrite }),
    });
  }

  /**
   * Start a dataset resume job (fetch missing data for existing dataset)
   */
  async startDatasetResume(name: string, preset: DatasetPreset): Promise<DatasetCreateJobResponse> {
    return this.request<DatasetCreateJobResponse>('/api/dataset/resume', {
      method: 'POST',
      body: JSON.stringify({ name, preset }),
    });
  }

  /**
   * Get dataset job status
   */
  async getDatasetJobStatus(jobId: string): Promise<DatasetJobResponse> {
    return this.request<DatasetJobResponse>(`/api/dataset/jobs/${jobId}`);
  }

  /**
   * Cancel a dataset job
   */
  async cancelDatasetJob(jobId: string): Promise<CancelDatasetJobResponse> {
    return this.request<CancelDatasetJobResponse>(`/api/dataset/jobs/${jobId}`, {
      method: 'DELETE',
    });
  }

  /**
   * Get dataset information (includes validation)
   */
  async getDatasetInfo(name: string): Promise<DatasetInfoResponse> {
    return this.request<DatasetInfoResponse>(`/api/dataset/${encodeURIComponent(name)}/info`);
  }

  /**
   * Sample stocks from a dataset
   */
  async sampleDataset(
    name: string,
    params: {
      size?: number;
      byMarket?: boolean;
      bySector?: boolean;
      seed?: number;
    } = {}
  ): Promise<DatasetSampleResponse> {
    const queryParams = new URLSearchParams();
    if (params.size !== undefined) queryParams.append('size', String(params.size));
    if (params.byMarket !== undefined) queryParams.append('byMarket', String(params.byMarket));
    if (params.bySector !== undefined) queryParams.append('bySector', String(params.bySector));
    if (params.seed !== undefined) queryParams.append('seed', String(params.seed));

    const query = queryParams.toString();
    const url = `/api/dataset/${encodeURIComponent(name)}/sample${query ? `?${query}` : ''}`;

    return this.request<DatasetSampleResponse>(url);
  }

  /**
   * Search stocks in a dataset
   */
  async searchDataset(
    name: string,
    term: string,
    params: {
      limit?: number;
      exact?: boolean;
    } = {}
  ): Promise<DatasetSearchResponse> {
    const queryParams = new URLSearchParams();
    queryParams.append('term', term);
    if (params.limit !== undefined) queryParams.append('limit', String(params.limit));
    if (params.exact !== undefined) queryParams.append('exact', String(params.exact));

    const url = `/api/dataset/${encodeURIComponent(name)}/search?${queryParams.toString()}`;

    return this.request<DatasetSearchResponse>(url);
  }
}

/**
 * JQuants auth status response
 */
export interface AuthStatusResponse {
  authenticated: boolean;
  hasRefreshToken: boolean;
  hasIdToken: boolean;
  tokenExpiry: string | null;
  hoursRemaining: number | null;
}

/**
 * JQuants token refresh response
 */
export interface RefreshTokenResponse {
  refreshToken: string;
  idToken: string;
  expiresAt: string;
  success: boolean;
}

/**
 * Stock refetch result
 */
export interface StockRefetchResult {
  code: string;
  success: boolean;
  recordsFetched: number;
  recordsStored: number;
  error?: string;
}

/**
 * Market refresh response
 */
export interface MarketRefreshResponse {
  totalStocks: number;
  successCount: number;
  failedCount: number;
  totalApiCalls: number;
  totalRecordsStored: number;
  results: StockRefetchResult[];
  errors: string[];
  lastUpdated: string;
}

/**
 * Dataset sample response
 */
export interface DatasetSampleResponse {
  codes: string[];
  metadata: {
    totalAvailable: number;
    sampleSize: number;
    stratificationUsed: boolean;
    marketDistribution?: Record<string, number>;
    sectorDistribution?: Record<string, number>;
  };
}

/**
 * Dataset search result item
 */
export interface DatasetSearchResultItem {
  code: string;
  companyName: string;
  companyNameEnglish?: string;
  marketName: string;
  sectorName: string;
  matchType: 'code' | 'name' | 'english_name';
}

/**
 * Dataset search response
 */
export interface DatasetSearchResponse {
  results: DatasetSearchResultItem[];
  totalFound: number;
}

/**
 * Portfolio item deleted response (with item details)
 */
export interface PortfolioItemDeletedResponse {
  success: boolean;
  message: string;
  deletedItem: PortfolioItemResponse;
}

/**
 * Market stats response
 */
export interface MarketStatsResponse {
  initialized: boolean;
  lastSync: string | null;
  databaseSize: number;
  topix: {
    count: number;
    dateRange: { min: string; max: string } | null;
  };
  stocks: {
    total: number;
    byMarket: Record<string, number>;
  };
  stockData: {
    count: number;
    dateCount: number;
    dateRange: { min: string; max: string } | null;
    averageStocksPerDay: number;
  };
  indices: {
    masterCount: number;
    dataCount: number;
    dateCount: number;
    dateRange: { min: string; max: string } | null;
    byCategory: Record<string, number>;
  };
  lastUpdated: string;
}
