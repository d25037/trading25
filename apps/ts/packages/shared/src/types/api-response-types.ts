/**
 * Centralized API Response Types
 *
 * Single source of truth for API response types shared between packages/web, packages/cli, and packages/clients-ts.
 */

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

export interface MarketRankingResponse {
  date: string;
  markets: string[];
  lookbackDays: number;
  periodDays: number;
  rankings: Rankings;
  lastUpdated: string;
}

export type RankingType = 'tradingValue' | 'gainers' | 'losers' | 'periodHigh' | 'periodLow';

// ===== SCREENING TYPES =====

export type ScreeningSortBy = 'bestStrategyScore' | 'matchedDate' | 'stockCode' | 'matchStrategyCount';
export type SortOrder = 'asc' | 'desc';
export type ScreeningDataSource = 'market' | 'dataset';

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
  markets: string[];
  recentDays: number;
  referenceDate?: string;
  sortBy: ScreeningSortBy;
  order: SortOrder;
  lastUpdated: string;
}

export interface ScreeningJobRequest {
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
  markets: string;
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

// ===== SYNC TYPES =====

export type SyncMode = 'auto' | 'initial' | 'incremental' | 'indices-only';
export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

export interface JobProgress {
  stage: string;
  current: number;
  total: number;
  percentage: number;
  message: string;
}

export interface SyncJobResult {
  success: boolean;
  totalApiCalls: number;
  stocksUpdated: number;
  datesProcessed: number;
  fundamentalsUpdated: number;
  fundamentalsDatesProcessed: number;
  failedDates: string[];
  errors: string[];
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

// ===== DATASET TYPES =====

export interface DatasetListItem {
  name: string;
  fileSize: number;
  lastModified: string;
  preset: string | null;
  createdAt: string | null;
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
  timeoutMinutes?: number;
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

export interface MarketValidationResponse {
  status: 'healthy' | 'warning' | 'error';
  initialized: boolean;
  lastSync: string | null;
  lastStocksRefresh: string | null;
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
  failedDates: string[];
  failedDatesCount: number;
  adjustmentEvents: AdjustmentEvent[];
  adjustmentEventsCount: number;
  stocksNeedingRefresh: string[];
  stocksNeedingRefreshCount: number;
  integrityIssues: IntegrityIssue[];
  integrityIssuesCount: number;
  recommendations: string[];
  lastUpdated: string;
}
