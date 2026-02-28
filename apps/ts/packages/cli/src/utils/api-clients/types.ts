import type { PortfolioItemResponse } from '@trading25/shared/portfolio';

// Re-export shared types used by CLI commands
export type {
  AdjustmentEvent,
  IntegrityIssue,
  MarketRankingResponse,
  MarketScreeningResponse,
  MarketValidationResponse,
  ScreeningResultItem,
  ScreeningJobResponse,
  RankingItem as MarketRankingItem,
  Rankings as MarketRankings,
} from '@trading25/shared/types/api-response-types';
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
  StartSyncRequest,
  SyncDataBackend,
  SyncDataPlaneOptions,
  SyncJobResponse,
  SyncJobResult,
  SyncMode,
} from '@trading25/shared/types/api-response-types';
export type {
  DeleteResponse,
  ListPortfoliosResponse,
  PortfolioItemResponse,
  PortfolioResponse,
  PortfolioWithItemsResponse,
} from '@trading25/shared/portfolio';
export type {
  ListWatchlistsResponse,
  WatchlistDeleteResponse,
  WatchlistItemResponse,
  WatchlistResponse,
  WatchlistWithItemsResponse,
} from '@trading25/shared/watchlist';

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

/**
 * JQuants auth status response
 */
export interface AuthStatusResponse {
  authenticated: boolean;
  hasApiKey: boolean;
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
