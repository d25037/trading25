/**
 * Analytics API Types
 */

export interface AnalyticsClientConfig {
  baseUrl?: string;
  timeoutMs?: number;
}

export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

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

export interface MarketRankingParams {
  date?: string;
  limit?: number;
  markets?: string;
  lookbackDays?: number;
  periodDays?: number;
}

// ===== FUNDAMENTAL RANKING TYPES =====

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
  epsValue: number;
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

export interface FundamentalRankingParams {
  limit?: number;
  markets?: string;
  forecastAboveRecentFyActuals?: boolean;
  forecastLookbackFyCount?: number;
  /** @deprecated Use forecastAboveRecentFyActuals */
  forecastAboveAllActuals?: boolean;
}

// ===== SCREENING TYPES =====

export type ScreeningSortBy = 'bestStrategyScore' | 'matchedDate' | 'stockCode' | 'matchStrategyCount';
export type SortOrder = 'asc' | 'desc';

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

// ===== ROE TYPES =====

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

export interface ROEParams {
  code?: string;
  date?: string;
  annualize?: boolean;
  preferConsolidated?: boolean;
  minEquity?: number;
  sortBy?: 'roe' | 'code' | 'date';
  limit?: number;
}

// ===== FACTOR REGRESSION TYPES =====

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

export interface FactorRegressionParams {
  symbol: string;
  lookbackDays?: number;
}

export interface PortfolioWeight {
  code: string;
  companyName: string;
  weight: number;
  latestPrice: number;
  marketValue: number;
  quantity: number;
}

export interface ExcludedStock {
  code: string;
  companyName: string;
  reason: string;
}

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

export interface PortfolioFactorRegressionParams {
  portfolioId: number;
  lookbackDays?: number;
}
