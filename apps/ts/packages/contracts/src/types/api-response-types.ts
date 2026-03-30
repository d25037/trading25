/**
 * Centralized API Response Types
 *
 * Single source of truth for API response types shared between packages/web and packages/api-clients.
 */

import type { DataProvenance, ResponseDiagnostics } from './api-types';

// ===== RANKING TYPES =====

export interface RankingItem {
  rank: number;
  code: string;
  companyName: string;
  marketCode: string;
  sector33Name: string;
  currentPrice: number;
  volume: number;
  tradingValue?: number;
  tradingValueAverage?: number;
  previousPrice?: number;
  basePrice?: number;
  changeAmount?: number;
  changePercentage?: number;
  lookbackDays?: number;
}

export interface Rankings {
  tradingValue: RankingItem[];
  gainers: RankingItem[];
  losers: RankingItem[];
  periodHigh: RankingItem[];
  periodLow: RankingItem[];
}

export interface IndexPerformanceItem {
  code: string;
  name: string;
  category: string;
  currentDate: string;
  baseDate: string;
  currentClose: number;
  baseClose: number;
  changeAmount: number;
  changePercentage: number;
  lookbackDays: number;
}

export interface MarketRankingResponse {
  date: string;
  markets: string[];
  lookbackDays: number;
  periodDays: number;
  rankings: Rankings;
  indexPerformance: IndexPerformanceItem[];
  lastUpdated: string;
}

export type RankingType = 'tradingValue' | 'gainers' | 'losers' | 'periodHigh' | 'periodLow';
export type Topix100PriceBucket = 'q1' | 'q10' | 'q456' | 'other';
export type Topix100VolumeBucket = 'high' | 'low' | null;

export interface Topix100RankingItem {
  rank: number;
  code: string;
  companyName: string;
  marketCode: string;
  sector33Name: string;
  scaleCategory: string;
  currentPrice: number;
  volume: number;
  priceSma20_80: number;
  volumeSma20_80: number;
  priceDecile: number;
  priceBucket: Topix100PriceBucket;
  volumeBucket: Topix100VolumeBucket;
}

export interface Topix100RankingResponse {
  date: string;
  itemCount: number;
  items: Topix100RankingItem[];
  lastUpdated: string;
}

export type FundamentalRankingSource = 'revised' | 'fy';
export type FundamentalRankingMetricKey = string;

export interface FundamentalRankingItem {
  rank: number;
  code: string;
  companyName: string;
  marketCode: string;
  sector33Name: string;
  currentPrice: number;
  volume: number;
  epsValue: number; // latest forecast EPS / latest actual EPS
  disclosedDate: string;
  periodType: string;
  source: FundamentalRankingSource;
}

export interface FundamentalRankings {
  ratioHigh: FundamentalRankingItem[];
  ratioLow: FundamentalRankingItem[];
}

export interface MarketFundamentalRankingResponse {
  date: string;
  markets: string[];
  metricKey: FundamentalRankingMetricKey;
  rankings: FundamentalRankings;
  lastUpdated: string;
}

// ===== SCREENING TYPES =====

export type ScreeningSortBy = 'bestStrategyScore' | 'matchedDate' | 'stockCode' | 'matchStrategyCount';
export type SortOrder = 'asc' | 'desc';
export type ScreeningDataSource = 'market' | 'dataset';
export type EntryDecidability = 'pre_open_decidable' | 'requires_same_session_observation';
export type ScreeningSupport = 'supported' | 'unsupported';

export interface MatchedStrategyItem {
  strategyName: string;
  matchedDate: string;
  strategyScore: number | null;
}

export interface ScreeningResultItem {
  stockCode: string;
  companyName: string;
  scaleCategory?: string;
  sector33Name?: string;
  matchedDate: string;
  bestStrategyName: string;
  bestStrategyScore: number | null;
  matchStrategyCount: number;
  matchedStrategies: MatchedStrategyItem[];
}

export interface ScreeningSummary {
  totalStocksScreened: number;
  matchCount: number;
  skippedCount: number;
  byStrategy: Record<string, number>;
  strategiesEvaluated: string[];
  strategiesWithoutBacktestMetrics: string[];
  warnings: string[];
}

export interface MarketScreeningResponse {
  results: ScreeningResultItem[];
  summary: ScreeningSummary;
  entry_decidability?: EntryDecidability;
  markets: string[];
  scopeLabel?: string | null;
  recentDays: number;
  referenceDate?: string;
  sortBy: ScreeningSortBy;
  order: SortOrder;
  lastUpdated: string;
  provenance: DataProvenance;
  diagnostics: ResponseDiagnostics;
}

export interface ScreeningJobRequest {
  entry_decidability?: EntryDecidability;
  markets?: string;
  strategies?: string;
  recentDays?: number;
  date?: string;
  sortBy?: ScreeningSortBy;
  order?: SortOrder;
  limit?: number;
}

export interface ScreeningJobResponse {
  job_id: string;
  status: JobStatus;
  progress?: number | null;
  message?: string | null;
  created_at: string;
  started_at?: string | null;
  completed_at?: string | null;
  error?: string | null;
  entry_decidability?: EntryDecidability;
  markets: string;
  scopeLabel?: string | null;
  strategies?: string | null;
  recentDays: number;
  referenceDate?: string | null;
  sortBy: ScreeningSortBy;
  order: SortOrder;
  limit?: number | null;
}

// ===== INDICES TYPES =====

export interface IndexItem {
  code: string;
  name: string;
  nameEnglish: string | null;
  category: string;
  dataStartDate: string | null;
}

export interface IndicesListResponse {
  indices: IndexItem[];
  lastUpdated: string;
}

export interface IndexDataPoint {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
}

export interface IndexDataResponse {
  code: string;
  name: string;
  data: IndexDataPoint[];
  lastUpdated: string;
}

// ===== N225 OPTIONS TYPES =====

export type Options225PutCallFilter = 'all' | 'put' | 'call';
export type Options225SortBy = 'openInterest' | 'volume' | 'strikePrice' | 'impliedVolatility' | 'wholeDayClose';

export interface N225OptionsNumericRange {
  min: number | null;
  max: number | null;
}

export interface N225OptionItem {
  date: string;
  code: string;
  wholeDayOpen: number | null;
  wholeDayHigh: number | null;
  wholeDayLow: number | null;
  wholeDayClose: number | null;
  nightSessionOpen: number | null;
  nightSessionHigh: number | null;
  nightSessionLow: number | null;
  nightSessionClose: number | null;
  daySessionOpen: number | null;
  daySessionHigh: number | null;
  daySessionLow: number | null;
  daySessionClose: number | null;
  volume: number | null;
  openInterest: number | null;
  turnoverValue: number | null;
  contractMonth: string | null;
  strikePrice: number | null;
  onlyAuctionVolume: number | null;
  emergencyMarginTriggerDivision: string | null;
  emergencyMarginTriggerLabel: string | null;
  putCallDivision: string | null;
  putCallLabel: string | null;
  lastTradingDay: string | null;
  specialQuotationDay: string | null;
  settlementPrice: number | null;
  theoreticalPrice: number | null;
  baseVolatility: number | null;
  underlyingPrice: number | null;
  impliedVolatility: number | null;
  interestRate: number | null;
}

export interface N225OptionsSummary {
  totalCount: number;
  putCount: number;
  callCount: number;
  totalVolume: number;
  totalOpenInterest: number;
  strikePriceRange: N225OptionsNumericRange;
  underlyingPriceRange: N225OptionsNumericRange;
  settlementPriceRange: N225OptionsNumericRange;
}

export interface N225OptionsExplorerResponse {
  requestedDate: string | null;
  resolvedDate: string;
  lastUpdated: string;
  sourceCallCount: number;
  availableContractMonths: string[];
  items: N225OptionItem[];
  summary: N225OptionsSummary;
}

// ===== SYNC TYPES =====

export type SyncMode = 'auto' | 'initial' | 'incremental' | 'repair';
export type SyncDataBackend = 'duckdb-parquet';
export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

export interface JobProgress {
  stage: string;
  current: number;
  total: number;
  percentage: number;
  message: string;
}

export interface SyncFetchDetail {
  eventType: 'strategy' | 'execution';
  stage: string;
  endpoint: string;
  method: 'rest' | 'bulk';
  targetLabel?: string;
  reason?: string;
  reasonDetail?: string;
  estimatedRestCalls?: number;
  estimatedBulkCalls?: number;
  plannerApiCalls?: number;
  fallback?: boolean;
  fallbackReason?: string;
  timestamp: string;
}

export interface SyncFetchDetailsResponse {
  jobId: string;
  status: JobStatus;
  mode: SyncMode;
  latest?: SyncFetchDetail;
  items: SyncFetchDetail[];
}

export interface SyncJobResult {
  success: boolean;
  totalApiCalls: number;
  stocksUpdated: number;
  datesProcessed: number;
  fundamentalsUpdated: number;
  fundamentalsDatesProcessed: number;
  failedDates?: string[];
  errors?: string[];
}

export interface CreateSyncJobResponse {
  jobId: string;
  status: JobStatus;
  mode: SyncMode;
  estimatedApiCalls: number;
  message: string;
}

export interface SyncJobResponse {
  jobId: string;
  status: JobStatus;
  mode: SyncMode;
  enforceBulkForStockData: boolean;
  progress?: JobProgress;
  result?: SyncJobResult;
  startedAt: string;
  completedAt?: string;
  error?: string;
}

export interface CancelJobResponse {
  success: boolean;
  jobId: string;
  message: string;
}

export interface SyncDataPlaneOptions {
  backend?: SyncDataBackend;
}

export interface StartSyncRequest {
  mode: SyncMode;
  dataPlane?: SyncDataPlaneOptions;
  enforceBulkForStockData?: boolean;
  resetBeforeSync?: boolean;
}

// ===== DATASET TYPES =====

export interface DatasetListItem {
  name: string;
  fileSize: number;
  lastModified: string;
  preset: string | null;
  createdAt: string | null;
  backend: 'duckdb-parquet' | 'sqlite-compatibility' | 'sqlite-legacy';
  hasCompatibilityArtifact: boolean;
}

export type DatasetListResponse = DatasetListItem[];

export interface DatasetDeleteResponse {
  success: boolean;
  name: string;
  message: string;
}

export interface DatasetCreateRequest {
  name: string;
  preset: string;
  overwrite?: boolean;
}

export interface DatasetCreateJobResponse {
  jobId: string;
  status: string;
  name: string;
  preset: string;
  message: string;
  estimatedTime?: string;
}

export interface DatasetJobProgress {
  stage: string;
  current: number;
  total: number;
  percentage: number;
  message: string;
}

export interface DatasetJobResponse {
  jobId: string;
  status: JobStatus;
  preset: string;
  name: string;
  progress?: DatasetJobProgress;
  result?: {
    success: boolean;
    totalStocks: number;
    processedStocks: number;
    warnings: string[];
    errors: string[];
    outputPath: string;
  };
  startedAt: string;
  completedAt?: string;
  error?: string;
}

export interface DatasetInfoResponse {
  name: string;
  path: string;
  fileSize: number;
  lastModified: string;
  storage: {
    backend: 'duckdb-parquet' | 'sqlite-compatibility' | 'sqlite-legacy';
    primaryPath: string;
    duckdbPath: string | null;
    compatibilityDbPath: string | null;
    manifestPath: string | null;
    hasCompatibilityArtifact: boolean;
  };
  snapshot: {
    preset: string | null;
    createdAt: string | null;
  };
  stats: {
    totalStocks: number;
    totalQuotes: number;
    dateRange: { from: string; to: string };
    hasMarginData: boolean;
    hasTOPIXData: boolean;
    hasSectorData: boolean;
    hasStatementsData: boolean;
    statementsFieldCoverage: {
      total: number;
      totalFY: number;
      totalHalf: number;
      hasExtendedFields: boolean;
      hasCashFlowFields: boolean;
      earningsPerShare: number;
      profit: number;
      equity: number;
      nextYearForecastEps: number;
      bps: number;
      sales: number;
      operatingProfit: number;
      ordinaryProfit: number;
      operatingCashFlow: number;
      dividendFY: number;
      forecastEps: number;
      investingCashFlow: number;
      financingCashFlow: number;
      cashAndEquivalents: number;
      totalAssets: number;
      sharesOutstanding: number;
      treasuryShares: number;
    } | null;
  };
  validation: {
    isValid: boolean;
    errors: string[];
    warnings: string[];
    details?: {
      dateGapsCount?: number;
      fkIntegrity?: {
        stockDataOrphans: number;
        marginDataOrphans: number;
        statementsOrphans: number;
      };
      orphanStocksCount?: number;
      stockCountValidation?: {
        preset: string | null;
        expected: { min: number; max: number } | null;
        actual: number;
        isWithinRange: boolean;
      };
      dataCoverage?: {
        totalStocks: number;
        stocksWithQuotes: number;
        stocksWithStatements: number;
        stocksWithMargin: number;
      };
    };
  };
}

// ===== PORTFOLIO / WATCHLIST TYPES =====

export interface DeleteResponse {
  success: boolean;
  message: string;
}

export type WatchlistDeleteResponse = DeleteResponse;

export interface PortfolioResponse {
  id: number;
  name: string;
  description?: string;
  createdAt: string;
  updatedAt: string;
}

export interface PortfolioItemResponse {
  id: number;
  portfolioId: number;
  code: string;
  companyName: string;
  quantity: number;
  purchasePrice: number;
  purchaseDate: string;
  account?: string;
  notes?: string;
  createdAt: string;
  updatedAt: string;
}

export interface PortfolioWithItemsResponse extends PortfolioResponse {
  items: PortfolioItemResponse[];
}

export interface PortfolioSummaryResponse {
  id: number;
  name: string;
  description?: string;
  stockCount: number;
  totalShares: number;
  createdAt: string;
  updatedAt: string;
}

export interface ListPortfoliosResponse {
  portfolios: PortfolioSummaryResponse[];
}

export interface WatchlistResponse {
  id: number;
  name: string;
  description?: string;
  createdAt: string;
  updatedAt: string;
}

export interface WatchlistItemResponse {
  id: number;
  watchlistId: number;
  code: string;
  companyName: string;
  memo?: string;
  createdAt: string;
}

export interface WatchlistWithItemsResponse extends WatchlistResponse {
  items: WatchlistItemResponse[];
}

export interface WatchlistSummaryResponse {
  id: number;
  name: string;
  description?: string;
  stockCount: number;
  createdAt: string;
  updatedAt: string;
}

export interface ListWatchlistsResponse {
  watchlists: WatchlistSummaryResponse[];
}

export interface WatchlistStockPrice {
  code: string;
  close: number;
  prevClose: number | null;
  changePercent: number | null;
  volume: number;
  date: string;
}

export interface WatchlistPricesResponse {
  prices: WatchlistStockPrice[];
}

export interface CancelDatasetJobResponse {
  success: boolean;
  jobId: string;
  message: string;
}

export interface PresetInfo {
  value: string;
  label: string;
  description: string;
  estimatedTime: string;
}

export const DATASET_PRESETS: PresetInfo[] = [
  { value: 'quickTesting', label: 'Quick Testing', description: 'テスト用小規模データセット', estimatedTime: '1-3分' },
  { value: 'topix100', label: 'TOPIX 100', description: 'TOPIX 100構成銘柄', estimatedTime: '10-20分' },
  { value: 'mid400', label: 'Mid400', description: 'TOPIX Mid400構成銘柄', estimatedTime: '10-20分' },
  { value: 'topix500', label: 'TOPIX 500', description: 'TOPIX 500構成銘柄', estimatedTime: '10-20分' },
  { value: 'growthMarket', label: 'Growth Market', description: 'グロース市場銘柄', estimatedTime: '10-25分' },
  { value: 'standardMarket', label: 'Standard Market', description: 'スタンダード市場銘柄', estimatedTime: '15-30分' },
  { value: 'primeMarket', label: 'Prime Market', description: 'プライム市場銘柄', estimatedTime: '20-40分' },
  {
    value: 'primeExTopix500',
    label: 'Prime ex TOPIX500',
    description: 'プライム市場（TOPIX500除く）',
    estimatedTime: '10-30分',
  },
  { value: 'fullMarket', label: 'Full Market', description: '全TSE上場銘柄', estimatedTime: '30-60分' },
];

// ===== VALIDATION TYPES =====

export interface AdjustmentEvent {
  code: string;
  date: string;
  adjustmentFactor: number;
  close: number;
  eventType: string;
}

export interface IntegrityIssue {
  code: string;
  count: number;
}

export interface MarketStatsResponse {
  initialized: boolean;
  lastSync: string | null;
  timeSeriesSource: string;
  databaseSize: number;
  storage: {
    duckdbBytes: number;
    parquetBytes: number;
    totalBytes: number;
  };
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
  options225: {
    count: number;
    dateCount: number;
    dateRange: { min: string; max: string } | null;
  };
  margin: {
    count: number;
    uniqueStockCount: number;
    dateCount: number;
    dateRange: { min: string; max: string } | null;
  };
  fundamentals: {
    count: number;
    uniqueStockCount: number;
    latestDisclosedDate: string | null;
    listedMarketCoverage: {
      listedMarketStocks: number;
      coveredStocks: number;
      missingStocks: number;
      coverageRatio: number;
      issuerAliasCoveredCount: number;
      emptySkippedCount: number;
    };
  };
  lastUpdated: string;
}

export interface MarketValidationResponse {
  status: 'healthy' | 'warning' | 'error';
  initialized: boolean;
  lastSync: string | null;
  lastStocksRefresh: string | null;
  timeSeriesSource: string;
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
    dateRange: { min: string; max: string } | null;
    missingDates: string[];
    missingDatesCount: number;
  };
  options225: {
    count: number;
    dateCount: number;
    dateRange: { min: string; max: string } | null;
    missingTopixCoverageDatesCount: number;
    missingTopixCoverageDates: string[];
    missingUnderlyingPriceDatesCount: number;
    missingUnderlyingPriceDates: string[];
    conflictingUnderlyingPriceDatesCount: number;
    conflictingUnderlyingPriceDates: string[];
  };
  margin: {
    count: number;
    uniqueStockCount: number;
    dateCount: number;
    dateRange: { min: string; max: string } | null;
    orphanCount: number;
    emptySkippedCount: number;
    emptySkippedCodes: string[];
  };
  fundamentals: {
    count: number;
    uniqueStockCount: number;
    latestDisclosedDate: string | null;
    missingListedMarketStocksCount: number;
    missingListedMarketStocks: string[];
    issuerAliasCoveredCount: number;
    emptySkippedCount: number;
    emptySkippedCodes: string[];
    failedDatesCount: number;
    failedCodesCount: number;
  };
  failedDates: string[];
  failedDatesCount: number;
  adjustmentEvents: AdjustmentEvent[];
  adjustmentEventsCount: number;
  stocksNeedingRefresh: string[];
  stocksNeedingRefreshCount: number;
  integrityIssues: IntegrityIssue[];
  integrityIssuesCount: number;
  sampleWindows: {
    stockDataMissingDates: ValidationSampleWindow;
    failedDates: ValidationSampleWindow;
    adjustmentEvents: ValidationSampleWindow;
    stocksNeedingRefresh: ValidationSampleWindow;
    options225MissingTopixCoverageDates: ValidationSampleWindow;
    options225MissingUnderlyingPriceDates: ValidationSampleWindow;
    options225ConflictingUnderlyingPriceDates: ValidationSampleWindow;
    missingListedMarketStocks: ValidationSampleWindow;
    fundamentalsEmptySkippedCodes: ValidationSampleWindow;
    marginEmptySkippedCodes: ValidationSampleWindow;
  };
  recommendations: string[];
  lastUpdated: string;
}

export interface ValidationSampleWindow {
  returnedCount: number;
  totalCount: number;
  limit: number;
  truncated: boolean;
}

export interface RefreshStockResult {
  code: string;
  success: boolean;
  recordsFetched: number;
  recordsStored: number;
  error?: string | null;
}

export interface MarketRefreshResponse {
  totalStocks: number;
  successCount: number;
  failedCount: number;
  totalApiCalls: number;
  totalRecordsStored: number;
  results: RefreshStockResult[];
  errors: string[];
  lastUpdated: string;
}
