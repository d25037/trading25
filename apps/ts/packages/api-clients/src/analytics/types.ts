/**
 * Analytics API Types
 */

import type {
  RankingRegimeState,
  RankingRiskFlag,
  RankingTechnicalFlag,
  ValueCompositeForwardEpsMode,
  ValueCompositeProfileId,
  ValueCompositeScoreMethod,
} from '@trading25/contracts/types/api-response-types';
import type { JobStatus } from '../base/job-status.js';

export type {
  MarketRankingResponse,
  MarketRankingSymbolResponse,
  MarketFundamentalRankingResponse,
  FundamentalRankingItem,
  FundamentalRankingMetricKey,
  FundamentalRankingSource,
  FundamentalRankings,
  RankingItem,
  RankingRegimeState,
  RankingRiskFlag,
  Rankings,
  RankingTechnicalFlag,
  ValueCompositeForwardEpsMode,
  ValueCompositeProfileId,
  ValueCompositeRankingItem,
  ValueCompositeRankingResponse,
  ValueCompositeScoreMethod,
  ValueCompositeScoreResponse,
  ValueCompositeScoreUnavailableReason,
  ValueCompositeTechnicalMetrics,
} from '@trading25/contracts/types/api-response-types';

export interface AnalyticsClientConfig {
  baseUrl?: string;
  timeoutMs?: number;
}

export type { JobStatus };
export type SectorStrengthFamily = 'balanced_sector_strength' | 'long_hybrid_leadership';
export type DailyRankingValuationSignalFilter =
  | 'deep_value'
  | 'value_confirmed'
  | 'undervalued'
  | 'expensive_or'
  | 'overvalued'
  | 'very_overvalued'
  | 'no_earnings';
export interface ResponseDiagnostics {
  missing_required_data?: string[];
  used_fields?: string[];
  effective_period_type?: string | null;
  warnings?: string[];
}

export interface DataProvenance {
  source_kind: 'market' | 'dataset';
  market_snapshot_id?: string | null;
  dataset_snapshot_id?: string | null;
  reference_date?: string | null;
  loaded_domains?: string[];
  strategy_name?: string | null;
  strategy_fingerprint?: string | null;
  warnings?: string[];
}

export interface MarketRankingParams {
  date?: string;
  limit?: number;
  markets?: string;
  lookbackDays?: number;
  periodDays?: number;
  sector33Name?: string;
  sector17Name?: string;
  includeValuation?: boolean;
  includeSectorStrength?: boolean;
  sectorStrengthFamily?: SectorStrengthFamily;
  forwardEpsDisclosedWithinDays?: number;
  regimeState?: RankingRegimeState;
  fundamentalState?: DailyRankingValuationSignalFilter;
  riskState?: RankingRiskFlag;
  technicalState?: RankingTechnicalFlag;
}

export interface FundamentalsParams {
  symbol: string;
  tradingValuePeriod?: number;
  forecastEpsLookbackFyCount?: number;
}

export interface MarginPressureIndicatorsParams {
  symbol: string;
  period?: number;
}

export interface MarginVolumeRatioParams {
  symbol: string;
}

export interface MarginVolumeRatioData {
  date: string;
  ratio: number;
  weeklyAvgVolume: number;
  marginVolume: number;
}

export interface MarginVolumeRatioResponse {
  symbol: string;
  longRatio: MarginVolumeRatioData[];
  shortRatio: MarginVolumeRatioData[];
  lastUpdated: string;
  provenance: DataProvenance;
  diagnostics: ResponseDiagnostics;
}

export interface MarginLongPressureData {
  date: string;
  pressure: number;
  longVol: number;
  shortVol: number;
  avgVolume: number;
}

export interface MarginFlowPressureData {
  date: string;
  flowPressure: number;
  currentNetMargin: number;
  previousNetMargin: number | null;
  avgVolume: number;
}

export interface MarginTurnoverDaysData {
  date: string;
  turnoverDays: number;
  longVol: number;
  avgVolume: number;
}

export interface MarginPressureIndicatorsResponse {
  symbol: string;
  averagePeriod: number;
  longPressure: MarginLongPressureData[];
  flowPressure: MarginFlowPressureData[];
  turnoverDays: MarginTurnoverDaysData[];
  lastUpdated: string;
  provenance: DataProvenance;
  diagnostics: ResponseDiagnostics;
}

export interface SectorStocksParams {
  sector33Name?: string;
  sector17Name?: string;
  markets?: string;
  lookbackDays?: number;
  sortBy?: 'tradingValue' | 'changePercentage' | 'code' | 'per' | 'forwardPer' | 'forwardPOp' | 'pbr' | 'marketCap';
  sortOrder?: 'asc' | 'desc';
  limit?: number;
}

export interface SectorStockItem {
  basePrice?: number | null;
  changeAmount?: number | null;
  changePercentage?: number | null;
  code: string;
  companyName: string;
  currentPrice: number;
  forwardPer?: number | null;
  lookbackDays?: number | null;
  marketCap?: number | null;
  marketCode: string;
  pbr?: number | null;
  per?: number | null;
  rank: number;
  sector33Name: string;
  tradingValue?: number | null;
  tradingValueAverage?: number | null;
  volume: number;
}

export interface SectorStocksResponse {
  lastUpdated: string;
  lookbackDays: number;
  markets: string[];
  sector17Name?: string | null;
  sector33Name?: string | null;
  sortBy: string;
  sortOrder: string;
  stocks: SectorStockItem[];
}

// ===== FUNDAMENTAL RANKING TYPES =====

export interface FundamentalRankingParams {
  limit?: number;
  markets?: string;
  forecastAboveRecentFyActuals?: boolean;
  forecastLookbackFyCount?: number;
}

export interface ValueCompositeRankingParams {
  date?: string;
  limit?: number;
  markets?: string;
  profileId?: ValueCompositeProfileId;
  scoreMethod?: ValueCompositeScoreMethod;
  applyLiquidityFilter?: boolean;
  forwardEpsMode?: ValueCompositeForwardEpsMode;
}

export interface ValueCompositeScoreParams {
  symbol: string;
  date?: string;
  forwardEpsMode?: ValueCompositeForwardEpsMode;
}

// ===== SCREENING TYPES =====

export type ScreeningSortBy = 'bestStrategyScore' | 'matchedDate' | 'stockCode' | 'matchStrategyCount';
export type SortOrder = 'asc' | 'desc';
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
  provenance: DataProvenance;
  diagnostics: ResponseDiagnostics;
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
