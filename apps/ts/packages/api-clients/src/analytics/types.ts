/**
 * Analytics API Types
 */

import type { JobStatus } from '../base/job-status.js';

export interface AnalyticsClientConfig {
  baseUrl?: string;
  timeoutMs?: number;
}

export type { JobStatus };
export type FundamentalRankingSource = 'revised' | 'fy';
export type RankingRiskFlag = 'overheat' | 'stale_rally_fade';
export type RankingTechnicalFlag = 'atr20_acceleration' | 'momentum_20_60_top20';
export type SectorStrengthFamily = 'balanced_sector_strength' | 'long_hybrid_leadership';
export type RankingRegimeState =
  | 'neutral_rerating'
  | 'crowded_rerating'
  | 'distribution_stress'
  | 'stale_liquidity'
  | 'neutral';
export type DailyRankingValuationSignalFilter =
  | 'deep_value'
  | 'value_confirmed'
  | 'undervalued'
  | 'expensive_or'
  | 'overvalued'
  | 'very_overvalued'
  | 'no_earnings';
export type RankingLiquidityState =
  | 'neutral_rerating'
  | 'crowded_rerating'
  | 'distribution_stress'
  | 'stale_liquidity'
  | 'neutral'
  | 'overheat'
  | 'stale_rally_fade';

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

// ===== RANKING TYPES =====

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
  forecastOperatingProfitGrowthRatio?: number | null;
  forecastOperatingProfitGrowthPct?: number | null;
  psr?: number | null;
  psrPercentile?: number | null;
  forwardPsr?: number | null;
  forwardPsrPercentile?: number | null;
  forwardEpsDisclosedDate?: string | null;
  forwardEpsSource?: FundamentalRankingSource | null;
  pbr?: number | null;
  pbrPercentile?: number | null;
  valueCompositeScore?: number | null;
  overvaluationCompositeScore?: number | null;
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
  technicalFlags?: RankingTechnicalFlag[];
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
  sectorStrengthFamily?: SectorStrengthFamily;
  rankings: Rankings;
  indexPerformance: IndexPerformanceItem[];
  lastUpdated: string;
}

export interface MarketRankingSymbolResponse {
  date: string | null;
  item: RankingItem | null;
  lastUpdated: string;
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
  /** @deprecated Use regimeState for liquidity regimes and riskState for warning/risk flags. */
  liquidityState?: RankingLiquidityState;
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
