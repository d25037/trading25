/**
 * Analytics API Types
 */

export interface AnalyticsClientConfig {
  baseUrl?: string;
  timeoutMs?: number;
}

export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
export type FundamentalRankingSource = 'revised' | 'fy';
export type RankingRiskFlag = 'overheat';
export type RankingLiquidityState =
  | 'rerating_participation'
  | 'distribution_stress'
  | 'stale_liquidity'
  | 'neutral'
  | 'overheat';

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
  forwardPer?: number | null;
  pOp?: number | null;
  forwardPOp?: number | null;
  forwardEpsDisclosedDate?: string | null;
  forwardEpsSource?: FundamentalRankingSource | null;
  pbr?: number | null;
  marketCap?: number | null;
  liquidityResidualZ?: number | null;
  liquidityRegime?: 'rerating_participation' | 'distribution_stress' | 'stale_liquidity' | 'neutral' | null;
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

export interface MarketRankingParams {
  date?: string;
  limit?: number;
  markets?: string;
  lookbackDays?: number;
  periodDays?: number;
  sector33Name?: string;
  sector17Name?: string;
  includeValuation?: boolean;
  forwardEpsDisclosedWithinDays?: number;
  liquidityState?: RankingLiquidityState;
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

export type CostStructureAnalysisPeriodType = '1Q' | '2Q' | '3Q' | '4Q' | 'FY';
export type CostStructureAnalysisView = 'recent' | 'same_quarter' | 'fiscal_year_only' | 'all';

export interface CostStructureAnalysisParams {
  symbol: string;
  view?: CostStructureAnalysisView;
  windowQuarters?: number;
}

export interface CostStructureDateRange {
  from: string;
  to: string;
}

export interface CostStructurePoint {
  analysisPeriodType: CostStructureAnalysisPeriodType;
  disclosedDate: string;
  fiscalYear: string;
  isDerived: boolean;
  operatingMargin?: number | null;
  operatingProfit: number;
  periodEnd: string;
  sales: number;
}

export interface CostStructureRegressionSummary {
  breakEvenSales?: number | null;
  contributionMarginRatio: number;
  fixedCost?: number | null;
  intercept: number;
  rSquared: number;
  sampleCount: number;
  slope: number;
  variableCostRatio: number;
}

export interface CostStructureResponse {
  symbol: string;
  companyName?: string | null;
  lastUpdated: string;
  dateRange: CostStructureDateRange;
  provenance: DataProvenance;
  diagnostics: ResponseDiagnostics;
  points: CostStructurePoint[];
  latestPoint: CostStructurePoint;
  regression: CostStructureRegressionSummary;
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
  /** @deprecated Use forecastAboveRecentFyActuals */
  forecastAboveAllActuals?: boolean;
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
