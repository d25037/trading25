/**
 * Centralized API Response Types
 *
 * Single source of truth for API response types shared between packages/web and packages/api-clients.
 */

import type { components as BtApiComponents } from '../clients/backtest/generated/bt-api-types';
import type { DataProvenance, ResponseDiagnostics } from './api-types';

type BtApiSchemas = BtApiComponents['schemas'];

// ===== BT SIGNAL / INDICATOR CONTRACT TYPES =====

export type IndicatorSpec = BtApiSchemas['IndicatorSpec'];
export type IndicatorComputeRequest = BtApiSchemas['IndicatorComputeRequest'];
export type IndicatorComputeResponse = BtApiSchemas['IndicatorComputeResponse'];
export type MarginIndicatorRequest = BtApiSchemas['MarginIndicatorRequest'];
export type MarginIndicatorResponse = BtApiSchemas['MarginIndicatorResponse'];
export type SignalSpec = BtApiSchemas['SignalSpec'];
export type SignalResult = BtApiSchemas['SignalResult'];
export type SignalComputeRequest = BtApiSchemas['SignalComputeRequest'];
export type SignalComputeResponse = BtApiSchemas['SignalComputeResponse'];

// ===== RESEARCH API CONTRACT TYPES =====

export type ResearchHighlightTone = BtApiSchemas['ResearchHighlight']['tone'];
export type ResearchDecisionStatus = BtApiSchemas['ResearchCatalogItem']['status'];
export type ResearchLabelValueContract = BtApiSchemas['ResearchLabelValue'];
export type ResearchHighlightContract = BtApiSchemas['ResearchHighlight'];
export type ResearchTableHighlightContract = BtApiSchemas['ResearchTableHighlight'];
export type PublishedReadoutSectionContract = BtApiSchemas['PublishedReadoutSection'];
export type PublishedResearchSummaryContract = BtApiSchemas['PublishedResearchSummary'];
export type ResearchCatalogItemContract = BtApiSchemas['ResearchCatalogItem'];
export type ResearchRunReferenceContract = BtApiSchemas['ResearchRunReference'];
export type ResearchCatalogResponseContract = BtApiSchemas['ResearchCatalogResponse'];
export type ResearchDetailResponseContract = BtApiSchemas['ResearchDetailResponse'];

// ===== RANKING TYPES =====

export type RankingRiskFlag = 'overheat' | 'stale_rally_fade';

export interface RankingItem {
  rank: number;
  code: string;
  companyName: string;
  marketCode: string;
  sector33Name: string;
  sectorStrengthScore?: number | null;
  sectorStrengthBucket?: 'sector_strong' | 'sector_neutral' | 'sector_weak' | null;
  currentPrice: number;
  volume: number;
  tradingValue?: number;
  tradingValueAverage?: number;
  previousPrice?: number;
  basePrice?: number;
  changeAmount?: number;
  changePercentage?: number;
  lookbackDays?: number;
  per?: number | null;
  perPercentile?: number | null;
  forwardPer?: number | null;
  forwardPerPercentile?: number | null;
  pOp?: number | null;
  forwardPOp?: number | null;
  forwardPOpPercentile?: number | null;
  forwardEpsDisclosedDate?: string | null;
  forwardEpsSource?: FundamentalRankingSource | null;
  pbr?: number | null;
  pbrPercentile?: number | null;
  marketCap?: number | null;
  liquidityResidualZ?: number | null;
  liquidityRegime?:
    | 'neutral_rerating'
    | 'crowded_rerating'
    | 'distribution_stress'
    | 'stale_liquidity'
    | 'neutral'
    | null;
  adv60ToFreeFloatPct?: number | null;
  riskFlags?: RankingRiskFlag[];
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
  sectorStrengthScore?: number | null;
  sectorStrengthBucket?: 'sector_strong' | 'sector_neutral' | 'sector_weak' | null;
  sector20dTopixExcessPct?: number | null;
  sector60dTopixExcessPct?: number | null;
  sectorBreadth20dPct?: number | null;
  sectorStockCount?: number | null;
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

export type ValueCompositeScoreMethod =
  | 'standard_pbr_tilt'
  | 'prime_size_tilt'
  | 'prime_size75_forward_per25'
  | 'equal_weight';
export type ValueCompositeProfileId = 'standard_breakout_120d20' | 'prime_size75_forward_per25';
export type ValueCompositeForwardEpsMode = 'latest' | 'fy';
export type ValueCompositeScoreUnavailableReason =
  | 'not_found'
  | 'unsupported_market'
  | 'forward_eps_missing'
  | 'bps_missing'
  | 'not_rankable';

export interface ValueCompositeTechnicalMetrics {
  featureDate?: string | null;
  breakoutFeatureDate?: string | null;
  reboundFrom252dLowPct?: number | null;
  return252dPct?: number | null;
  volatility20dPct?: number | null;
  volatility60dPct?: number | null;
  downsideVolatility60dPct?: number | null;
  avgTradingValue60dMilJpy?: number | null;
  avgTradingValue60dSourceSessions?: number | null;
  newHigh20d?: boolean | null;
  daysSinceNewHigh20d?: number | null;
  closeToPriorHigh20dPct?: number | null;
  newHigh120d?: boolean | null;
  daysSinceNewHigh120d?: number | null;
  closeToPriorHigh120dPct?: number | null;
}

export interface ValueCompositeRankingItem {
  rank: number;
  code: string;
  companyName: string;
  marketCode: string;
  sector33Name: string;
  currentPrice: number;
  volume: number;
  score: number;
  scoreBeforeBoost?: number | null;
  breakoutBoost?: number | null;
  liquidityEligible?: boolean | null;
  avgTradingValue60dMilJpy?: number | null;
  lowPbrScore: number;
  smallMarketCapScore: number;
  lowForwardPerScore: number;
  pbr: number;
  forwardPer: number;
  marketCapBilJpy: number;
  bps?: number | null;
  forwardEps?: number | null;
  latestFyDisclosedDate?: string | null;
  forwardEpsDisclosedDate?: string | null;
  forwardEpsSource?: FundamentalRankingSource | null;
  technicalMetrics?: ValueCompositeTechnicalMetrics | null;
}

export interface ValueCompositeRankingResponse {
  date: string;
  markets: string[];
  metricKey: 'standard_value_composite';
  profileId?: ValueCompositeProfileId | null;
  profileLabel?: string | null;
  scoreMethod: ValueCompositeScoreMethod;
  forwardEpsMode: ValueCompositeForwardEpsMode;
  rebalanceMonths?: number | null;
  breakoutWindow?: number | null;
  breakoutLookbackSessions?: number | null;
  breakoutScoreBoost?: number | null;
  applyLiquidityFilter: boolean;
  scorePolicy: string;
  weights: Record<string, number>;
  itemCount: number;
  items: ValueCompositeRankingItem[];
  lastUpdated: string;
}

export interface ValueCompositeScoreResponse {
  date: string;
  code: string;
  companyName?: string | null;
  marketCode?: string | null;
  market?: string | null;
  metricKey: 'standard_value_composite';
  scoreMethod?: ValueCompositeScoreMethod | null;
  forwardEpsMode: ValueCompositeForwardEpsMode;
  scorePolicy?: string | null;
  weights: Record<string, number>;
  universeCount: number;
  scoreAvailable: boolean;
  unsupportedReason?: ValueCompositeScoreUnavailableReason | null;
  item?: ValueCompositeRankingItem | null;
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

export interface AdjustedMetricsMaterializeResult {
  success: boolean;
  statementRows: number;
  dailyValuationRows: number;
  priceBasisDate?: string;
  basisVersion?: string;
  errors?: string[];
}

export interface CreateAdjustedMetricsMaterializeJobResponse {
  jobId: string;
  status: JobStatus;
  mode: 'full';
  message: string;
}

export interface AdjustedMetricsMaterializeJobResponse {
  jobId: string;
  status: JobStatus;
  mode: 'full';
  progress?: JobProgress;
  result?: AdjustedMetricsMaterializeResult;
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
  path: string;
  fileSize: number;
  lastModified: string;
  preset: string | null;
  createdAt: string | null;
  backend: 'duckdb-parquet';
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
    backend: 'duckdb-parquet';
    primaryPath: string;
    duckdbPath: string | null;
    manifestPath: string | null;
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

export interface PortfolioCreateRequest {
  name: string;
  description?: string | null;
}

export interface PortfolioUpdateRequest {
  name?: string | null;
  description?: string | null;
}

export interface PortfolioItemCreateRequest {
  code: string;
  companyName: string;
  quantity: number;
  purchasePrice: number;
  purchaseDate: string;
  account?: string | null;
  notes?: string | null;
}

export interface PortfolioItemUpdateRequest {
  quantity?: number | null;
  purchasePrice?: number | null;
  purchaseDate?: string | null;
  account?: string | null;
  notes?: string | null;
}

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

export interface PortfolioPerformanceSummary {
  currentValue: number;
  returnRate: number;
  totalCost: number;
  totalPnL: number;
}

export interface PortfolioHoldingPerformance {
  code: string;
  companyName: string;
  quantity: number;
  purchasePrice: number;
  purchaseDate: string;
  currentPrice: number;
  cost: number;
  marketValue: number;
  pnl: number;
  returnRate: number;
  weight: number;
  account?: string | null;
}

export interface PortfolioPerformancePoint {
  date: string;
  dailyReturn: number;
  cumulativeReturn: number;
}

export interface PortfolioPerformanceDateRange {
  from: string;
  to: string;
}

export interface PortfolioBenchmarkMetrics {
  code: string;
  name: string;
  benchmarkReturn: number;
  relativeReturn: number;
  beta: number;
  alpha: number;
  correlation: number;
  rSquared: number;
}

export interface PortfolioBenchmarkPoint {
  date: string;
  portfolioReturn: number;
  benchmarkReturn: number;
}

export interface PortfolioPerformanceResponse {
  portfolioId: number;
  portfolioName: string;
  portfolioDescription?: string | null;
  dateRange?: PortfolioPerformanceDateRange | null;
  dataPoints: number;
  summary: PortfolioPerformanceSummary;
  holdings: PortfolioHoldingPerformance[];
  timeSeries: PortfolioPerformancePoint[];
  benchmark?: PortfolioBenchmarkMetrics | null;
  benchmarkTimeSeries?: PortfolioBenchmarkPoint[] | null;
  warnings: string[];
}

export interface WatchlistCreateRequest {
  name: string;
  description?: string | null;
}

export interface WatchlistUpdateRequest {
  name?: string | null;
  description?: string | null;
}

export interface WatchlistItemCreateRequest {
  code: string;
  companyName: string;
  memo?: string | null;
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

// ===== STOCK LOOKUP TYPES =====

export interface StockInfoResponse {
  code: string;
  companyName: string;
  companyNameEnglish: string;
  listedDate: string;
  marketCode: string;
  marketName: string;
  scaleCategory: string;
  sector17Code: string;
  sector17Name: string;
  sector33Code: string;
  sector33Name: string;
}

export interface StockSearchResultItem {
  code: string;
  companyName: string;
  companyNameEnglish?: string | null;
  marketCode: string;
  marketName: string;
  sector33Name: string;
}

export interface StockSearchResponse {
  count: number;
  query: string;
  results: StockSearchResultItem[];
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
  adjustedMetrics?: {
    statementRows: number;
    dailyValuationRows: number;
    priceBasisDate: string | null;
    basisVersion: string | null;
    status: 'ready' | 'missing' | 'stale' | 'empty_source';
  };
  lastUpdated: string;
}

export interface MarketValidationResponse {
  status: 'healthy' | 'warning' | 'error';
  healthDomains?: {
    coreDailyStatus: 'healthy' | 'info' | 'warning' | 'error';
    derivativesStatus: 'healthy' | 'info' | 'warning' | 'error';
    intradayStatus: 'healthy' | 'info' | 'warning' | 'error';
    sourceQualityStatus: 'healthy' | 'info' | 'warning' | 'error';
  };
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
    coverageStatus?: 'in_sync' | 'missing' | 'pending' | 'stale' | 'partial';
    allowedTopixLagDates?: number;
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
