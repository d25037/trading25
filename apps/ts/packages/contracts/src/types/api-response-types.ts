/**
 * Centralized API Response Types
 *
 * Single source of truth for API response types shared between packages/web and packages/api-clients.
 */

import type { components as BtApiComponents } from '../clients/backtest/generated/bt-api-types';
import type { ApiJsonResponse } from './endpoint-types';

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

// ===== MARKET REGIME CONTRACT TYPES =====

export type MarketBubbleFootprintHorizonContract = BtApiSchemas['MarketBubbleFootprintHorizon'];
export type MarketBubbleFootprintLatestResponseContract = BtApiSchemas['MarketBubbleFootprintLatestResponse'];

// ===== RANKING TYPES =====

export type RankingItem = BtApiSchemas['RankingItem'];
export type Rankings = BtApiSchemas['Rankings'];
export type IndexPerformanceItem = BtApiSchemas['IndexPerformanceItem'];
export type MarketRankingResponse = BtApiSchemas['MarketRankingResponse'];
export type MarketRankingSymbolResponse = BtApiSchemas['MarketRankingSymbolResponse'];
export type RankingRiskFlag = NonNullable<RankingItem['riskFlags']>[number];
export type RankingTechnicalFlag = NonNullable<RankingItem['technicalFlags']>[number];
export type RankingRegimeState = NonNullable<RankingItem['liquidityRegime']>;

export type RankingType = 'tradingValue' | 'gainers' | 'losers' | 'periodHigh' | 'periodLow';

export type FundamentalRankingItem = BtApiSchemas['FundamentalRankingItem'];
export type FundamentalRankings = BtApiSchemas['FundamentalRankings'];
export type MarketFundamentalRankingResponse = BtApiSchemas['MarketFundamentalRankingResponse'];
export type FundamentalRankingSource = FundamentalRankingItem['source'];
export type FundamentalRankingMetricKey = MarketFundamentalRankingResponse['metricKey'];
export type ValueCompositeTechnicalMetrics = BtApiSchemas['ValueCompositeTechnicalMetrics'];
export type ValueCompositeRankingItem = BtApiSchemas['ValueCompositeRankingItem'];
export type ValueCompositeRankingResponse = BtApiSchemas['ValueCompositeRankingResponse'];
export type ValueCompositeScoreResponse = BtApiSchemas['ValueCompositeScoreResponse'];
export type ValueCompositeScoreMethod = ValueCompositeRankingResponse['scoreMethod'];
export type ValueCompositeProfileId = NonNullable<ValueCompositeRankingResponse['profileId']>;
export type ValueCompositeForwardEpsMode = ValueCompositeRankingResponse['forwardEpsMode'];
export type ValueCompositeScoreUnavailableReason = NonNullable<ValueCompositeScoreResponse['unsupportedReason']>;

// ===== FACTOR REGRESSION TYPES =====

export type FactorRegressionResponse = BtApiSchemas['FactorRegressionResponse'];
export type FactorRegressionDateRange = FactorRegressionResponse['dateRange'];
export type FactorRegressionIndexMatch = FactorRegressionResponse['sector17Matches'][number];
export type PortfolioFactorRegressionResponse = BtApiSchemas['PortfolioFactorRegressionResponse'];
export type PortfolioFactorRegressionDateRange = PortfolioFactorRegressionResponse['dateRange'];
export type PortfolioFactorRegressionIndexMatch = PortfolioFactorRegressionResponse['sector17Matches'][number];
export type PortfolioFactorRegressionStockWeight = PortfolioFactorRegressionResponse['weights'][number];
export type PortfolioFactorRegressionExcludedStock = PortfolioFactorRegressionResponse['excludedStocks'][number];

// ===== SCREENING TYPES =====

export type ScreeningSortBy = BtApiSchemas['MarketScreeningResponse']['sortBy'];
export type SortOrder = BtApiSchemas['MarketScreeningResponse']['order'];
export type ScreeningDataSource = 'market' | 'dataset';
export type EntryDecidability = BtApiSchemas['MarketScreeningResponse']['entry_decidability'];
export type ScreeningSupport = BtApiSchemas['StrategyMetadataResponse']['screening_support'];

export type MatchedStrategyItem = BtApiSchemas['MatchedStrategyItem'];
export type ScreeningResultItem = BtApiSchemas['ScreeningResultItem'];
export type ScreeningSummary = BtApiSchemas['ScreeningSummary'];
export type MarketScreeningResponse = BtApiSchemas['MarketScreeningResponse'];
export type ScreeningJobRequest = BtApiSchemas['ScreeningJobRequest'];
export type ScreeningJobResponse = BtApiSchemas['ScreeningJobResponse'];

// ===== INDICES TYPES =====

export type IndexItem = BtApiSchemas['IndexInfo'];
export type IndicesListResponse = BtApiSchemas['IndicesListResponse'];
export type IndexDataPoint = BtApiSchemas['IndexOHLCRecord'];
export type IndexDataResponse = BtApiSchemas['IndexDataResponse'];

// ===== N225 OPTIONS TYPES =====

export type Options225PutCallFilter = 'all' | 'put' | 'call';
export type Options225SortBy = 'openInterest' | 'volume' | 'strikePrice' | 'impliedVolatility' | 'wholeDayClose';

export type N225OptionsNumericRange = BtApiSchemas['N225OptionsNumericRange'];
export type N225OptionItem = BtApiSchemas['N225OptionItem'];
export type N225OptionsSummary = BtApiSchemas['N225OptionsSummary'];
export type N225OptionsExplorerResponse = BtApiSchemas['N225OptionsExplorerResponse'];

// ===== SYNC TYPES =====

export type SyncMode = BtApiSchemas['SyncRequest']['mode'];
export type SyncDataBackend = BtApiSchemas['SyncDataPlaneRequest']['backend'];
export type JobStatus = BtApiSchemas['JobStatus'];
export type JobProgress = BtApiSchemas['SyncProgress'];
export type SyncFetchDetail = BtApiSchemas['SyncFetchDetail'];
export type SyncFetchDetailsResponse = BtApiSchemas['SyncFetchDetailsResponse'];
export type SyncJobResult = BtApiSchemas['SyncResult'];
export type CreateSyncJobResponse = BtApiSchemas['CreateSyncJobResponse'];
export type SyncJobResponse = BtApiSchemas['SyncJobResponse'];
export type AdjustedMetricsMaterializeResult = BtApiSchemas['AdjustedMetricsMaterializeResult'];
export type CreateAdjustedMetricsMaterializeJobResponse = BtApiSchemas['CreateAdjustedMetricsMaterializeJobResponse'];
export type AdjustedMetricsMaterializeJobResponse = BtApiSchemas['AdjustedMetricsMaterializeJobResponse'];
export type CancelJobResponse = BtApiSchemas['CancelJobResponse'];
export type SyncDataPlaneOptions = BtApiSchemas['SyncDataPlaneRequest'];
export type StartSyncRequest = BtApiSchemas['SyncRequest'];

// ===== LAB TYPES =====

export type LabGenerateRequest = BtApiSchemas['LabGenerateRequest'];
export type LabEvolveRequest = BtApiSchemas['LabEvolveRequest'];
export type LabOptimizeRequest = BtApiSchemas['LabOptimizeRequest'];
export type LabImproveRequest = BtApiSchemas['LabImproveRequest'];
export type LabOptimizeTrialRecommendationResponse = BtApiSchemas['LabOptimizeRecommendationResponse'];
export type LabGenerateResult = BtApiSchemas['LabGenerateResult'];
export type LabEvolveResult = BtApiSchemas['LabEvolveResult'];
export type LabOptimizeResult = BtApiSchemas['LabOptimizeResult'];
export type LabImproveResult = BtApiSchemas['LabImproveResult'];
export type LabResultData = NonNullable<BtApiSchemas['LabJobResponse']['result_data']>;
export type LabJobResponse = BtApiSchemas['LabJobResponse'];

// ===== DATASET TYPES =====

export type DatasetListItem = BtApiSchemas['DatasetListItem'];

export type DatasetListResponse = ApiJsonResponse<'/api/dataset', 'get', 200>;

export type DatasetDeleteResponse = ApiJsonResponse<'/api/dataset/{name}', 'delete', 200>;
export type DatasetCreateRequest = BtApiSchemas['DatasetCreateRequest'];
export type DatasetCreateJobResponse = BtApiSchemas['DatasetCreateResponse'];
export type DatasetJobResponse = BtApiSchemas['DatasetJobResponse'];
export type DatasetJobProgress = NonNullable<DatasetJobResponse['progress']>;
export type DatasetInfoResponse = BtApiSchemas['DatasetInfoResponse'];

// ===== PORTFOLIO / WATCHLIST TYPES =====

export type DeleteResponse = BtApiSchemas['DeleteResponse'];

export type WatchlistDeleteResponse = DeleteResponse;

export type PortfolioCreateRequest = BtApiSchemas['PortfolioCreateRequest'];
export type PortfolioUpdateRequest = BtApiSchemas['PortfolioUpdateRequest'];
export type PortfolioItemCreateRequest = BtApiSchemas['PortfolioItemCreateRequest'];
export type PortfolioItemUpdateRequest = BtApiSchemas['PortfolioItemUpdateRequest'];
export type PortfolioResponse = BtApiSchemas['PortfolioResponse'];
export type PortfolioItemResponse = BtApiSchemas['PortfolioItemResponse'];
export type PortfolioWithItemsResponse = BtApiSchemas['PortfolioDetailResponse'];
export type PortfolioSummaryResponse = BtApiSchemas['PortfolioSummaryResponse'];
export type ListPortfoliosResponse = ApiJsonResponse<'/api/portfolio', 'get', 200>;
export type PortfolioPerformanceResponse = BtApiSchemas['PortfolioPerformanceResponse'];
export type PortfolioPerformanceSummary = PortfolioPerformanceResponse['summary'];
export type PortfolioHoldingPerformance = PortfolioPerformanceResponse['holdings'][number];
export type PortfolioPerformancePoint = PortfolioPerformanceResponse['timeSeries'][number];
export type PortfolioPerformanceDateRange = NonNullable<PortfolioPerformanceResponse['dateRange']>;
export type PortfolioBenchmarkMetrics = NonNullable<PortfolioPerformanceResponse['benchmark']>;
export type PortfolioBenchmarkPoint = NonNullable<PortfolioPerformanceResponse['benchmarkTimeSeries']>[number];

export type WatchlistCreateRequest = BtApiSchemas['WatchlistCreateRequest'];
export type WatchlistUpdateRequest = BtApiSchemas['WatchlistUpdateRequest'];
export type WatchlistItemCreateRequest = BtApiSchemas['WatchlistItemCreateRequest'];
export type WatchlistItemUpdateRequest = BtApiSchemas['WatchlistItemUpdateRequest'];
export type WatchlistResponse = BtApiSchemas['WatchlistResponse'];
export type WatchlistItemResponse = BtApiSchemas['WatchlistItemResponse'];
export type WatchlistWithItemsResponse = BtApiSchemas['WatchlistDetailResponse'];
export type WatchlistSummaryResponse = BtApiSchemas['WatchlistSummaryResponse'];
export type ListWatchlistsResponse = ApiJsonResponse<'/api/watchlist', 'get', 200>;
export type WatchlistStockPrice = BtApiSchemas['WatchlistStockPrice'];
export type WatchlistPricesResponse = BtApiSchemas['WatchlistPricesResponse'];

// ===== STOCK LOOKUP TYPES =====

export type StockInfoResponse = BtApiSchemas['StockInfo'];
export type StockSearchResultItem = BtApiSchemas['StockSearchResultItem'];
export type StockSearchResponse = BtApiSchemas['StockSearchResponse'];
export type CancelDatasetJobResponse = BtApiSchemas['CancelJobResponse'];

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

export type AdjustmentEvent = BtApiSchemas['AdjustmentEvent'];
export type IntegrityIssue = BtApiSchemas['IntegrityIssue'];
export type MarketStatsResponse = BtApiSchemas['MarketStatsResponse'];
export type MarketValidationResponse = BtApiSchemas['MarketValidationResponse'];
export type ValidationSampleWindow = BtApiSchemas['ValidationSampleWindow'];
export type RefreshStockResult = BtApiSchemas['RefreshStockResult'];
export type MarketRefreshResponse = BtApiSchemas['RefreshResponse'];
