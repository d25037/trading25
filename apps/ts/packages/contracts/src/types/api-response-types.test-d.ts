import type { components as BtApiComponents } from '../clients/backtest/generated/bt-api-types';
import type {
  ApiJsonResponse,
} from './endpoint-types';
import type {
  ApiFundamentalDataPoint,
  ApiFundamentalsResponse,
  ApiMarginFlowPressureData,
  ApiMarginLongPressureData,
  ApiMarginPressureIndicatorsResponse,
  ApiMarginTurnoverDaysData,
  ApiMarginVolumeRatioData,
  ApiMarginVolumeRatioResponse,
  DataProvenance,
  ResponseDiagnostics,
} from './api-types';
import type {
  AdjustedMetricsMaterializeJobResponse,
  AdjustedMetricsMaterializeResult,
  CancelDatasetJobResponse,
  CreateAdjustedMetricsMaterializeJobResponse,
  CreateSyncJobResponse,
  DatasetCreateJobResponse,
  DatasetCreateRequest,
  DatasetInfoResponse,
  DatasetJobProgress,
  DatasetJobResponse,
  DatasetListItem,
  DatasetListResponse,
  DeleteResponse,
  IndexDataPoint,
  IndexDataResponse,
  IndexItem,
  IndicesListResponse,
  JobProgress,
  JobStatus,
  LabEvolveRequest,
  LabEvolveResult,
  LabGenerateRequest,
  LabGenerateResult,
  LabImproveRequest,
  LabImproveResult,
  LabJobResponse,
  LabOptimizeRequest,
  LabOptimizeResult,
  LabOptimizeTrialRecommendationResponse,
  LabResultData,
  ListPortfoliosResponse,
  ListWatchlistsResponse,
  MarketRefreshResponse,
  MarketScreeningResponse,
  MarketStatsResponse,
  MarketValidationResponse,
  MatchedStrategyItem,
  N225OptionItem,
  N225OptionsExplorerResponse,
  N225OptionsNumericRange,
  N225OptionsSummary,
  PortfolioCreateRequest,
  PortfolioItemCreateRequest,
  PortfolioItemResponse,
  PortfolioItemUpdateRequest,
  PortfolioPerformanceResponse,
  PortfolioResponse,
  PortfolioSummaryResponse,
  PortfolioUpdateRequest,
  PortfolioWithItemsResponse,
  RefreshStockResult,
  ScreeningJobRequest,
  ScreeningJobResponse,
  ScreeningResultItem,
  ScreeningSupport,
  ScreeningSummary,
  StartSyncRequest,
  StockInfoResponse,
  StockSearchResponse,
  StockSearchResultItem,
  SyncFetchDetail,
  SyncFetchDetailsResponse,
  SyncJobResponse,
  SyncJobResult,
  WatchlistCreateRequest,
  WatchlistItemCreateRequest,
  WatchlistItemResponse,
  WatchlistItemUpdateRequest,
  WatchlistPricesResponse,
  WatchlistResponse,
  WatchlistStockPrice,
  WatchlistSummaryResponse,
  WatchlistUpdateRequest,
  WatchlistWithItemsResponse,
} from './api-response-types';

type Schemas = BtApiComponents['schemas'];
type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Expect<Value extends true> = Value;

type ScreeningContracts = [
  Expect<Equal<MatchedStrategyItem, Schemas['MatchedStrategyItem']>>,
  Expect<Equal<ScreeningResultItem, Schemas['ScreeningResultItem']>>,
  Expect<Equal<ScreeningSupport, Schemas['StrategyMetadataResponse']['screening_support']>>,
  Expect<Equal<ScreeningSummary, Schemas['ScreeningSummary']>>,
  Expect<Equal<MarketScreeningResponse, Schemas['MarketScreeningResponse']>>,
  Expect<Equal<ScreeningJobRequest, Schemas['ScreeningJobRequest']>>,
  Expect<Equal<ScreeningJobResponse, Schemas['ScreeningJobResponse']>>,
];

type IndexContracts = [
  Expect<Equal<IndexItem, Schemas['IndexInfo']>>,
  Expect<Equal<IndicesListResponse, Schemas['IndicesListResponse']>>,
  Expect<Equal<IndexDataPoint, Schemas['IndexOHLCRecord']>>,
  Expect<Equal<IndexDataResponse, Schemas['IndexDataResponse']>>,
];

type OptionsContracts = [
  Expect<Equal<N225OptionsNumericRange, Schemas['N225OptionsNumericRange']>>,
  Expect<Equal<N225OptionItem, Schemas['N225OptionItem']>>,
  Expect<Equal<N225OptionsSummary, Schemas['N225OptionsSummary']>>,
  Expect<Equal<N225OptionsExplorerResponse, Schemas['N225OptionsExplorerResponse']>>,
];

type SyncContracts = [
  Expect<Equal<JobProgress, Schemas['SyncProgress']>>,
  Expect<Equal<SyncFetchDetail, Schemas['SyncFetchDetail']>>,
  Expect<Equal<SyncFetchDetailsResponse, Schemas['SyncFetchDetailsResponse']>>,
  Expect<Equal<SyncJobResult, Schemas['SyncResult']>>,
  Expect<Equal<CreateSyncJobResponse, Schemas['CreateSyncJobResponse']>>,
  Expect<Equal<SyncJobResponse, Schemas['SyncJobResponse']>>,
  Expect<Equal<AdjustedMetricsMaterializeResult, Schemas['AdjustedMetricsMaterializeResult']>>,
  Expect<
    Equal<CreateAdjustedMetricsMaterializeJobResponse, Schemas['CreateAdjustedMetricsMaterializeJobResponse']>
  >,
  Expect<Equal<AdjustedMetricsMaterializeJobResponse, Schemas['AdjustedMetricsMaterializeJobResponse']>>,
  Expect<Equal<StartSyncRequest, Schemas['SyncRequest']>>,
  Expect<Equal<JobStatus, Schemas['JobStatus']>>,
];

type DatasetContracts = [
  Expect<Equal<DatasetListItem, Schemas['DatasetListItem']>>,
  Expect<Equal<DatasetListResponse, ApiJsonResponse<'/api/dataset', 'get', 200>>>,
  Expect<Equal<DatasetCreateRequest, Schemas['DatasetCreateRequest']>>,
  Expect<Equal<DatasetCreateJobResponse, Schemas['DatasetCreateResponse']>>,
  Expect<Equal<DatasetJobProgress, NonNullable<Schemas['DatasetJobResponse']['progress']>>>,
  Expect<Equal<DatasetJobResponse, Schemas['DatasetJobResponse']>>,
  Expect<Equal<DatasetInfoResponse, Schemas['DatasetInfoResponse']>>,
  Expect<Equal<CancelDatasetJobResponse, Schemas['CancelJobResponse']>>,
];

type PortfolioContracts = [
  Expect<Equal<DeleteResponse, Schemas['DeleteResponse']>>,
  Expect<Equal<PortfolioCreateRequest, Schemas['PortfolioCreateRequest']>>,
  Expect<Equal<PortfolioUpdateRequest, Schemas['PortfolioUpdateRequest']>>,
  Expect<Equal<PortfolioItemCreateRequest, Schemas['PortfolioItemCreateRequest']>>,
  Expect<Equal<PortfolioItemUpdateRequest, Schemas['PortfolioItemUpdateRequest']>>,
  Expect<Equal<PortfolioResponse, Schemas['PortfolioResponse']>>,
  Expect<Equal<PortfolioItemResponse, Schemas['PortfolioItemResponse']>>,
  Expect<Equal<PortfolioWithItemsResponse, Schemas['PortfolioDetailResponse']>>,
  Expect<Equal<PortfolioSummaryResponse, Schemas['PortfolioSummaryResponse']>>,
  Expect<Equal<ListPortfoliosResponse, ApiJsonResponse<'/api/portfolio', 'get', 200>>>,
  Expect<Equal<PortfolioPerformanceResponse, Schemas['PortfolioPerformanceResponse']>>,
];

type WatchlistContracts = [
  Expect<Equal<WatchlistCreateRequest, Schemas['WatchlistCreateRequest']>>,
  Expect<Equal<WatchlistUpdateRequest, Schemas['WatchlistUpdateRequest']>>,
  Expect<Equal<WatchlistItemCreateRequest, Schemas['WatchlistItemCreateRequest']>>,
  Expect<Equal<WatchlistItemUpdateRequest, Schemas['WatchlistItemUpdateRequest']>>,
  Expect<Equal<WatchlistResponse, Schemas['WatchlistResponse']>>,
  Expect<Equal<WatchlistItemResponse, Schemas['WatchlistItemResponse']>>,
  Expect<Equal<WatchlistWithItemsResponse, Schemas['WatchlistDetailResponse']>>,
  Expect<Equal<WatchlistSummaryResponse, Schemas['WatchlistSummaryResponse']>>,
  Expect<Equal<ListWatchlistsResponse, ApiJsonResponse<'/api/watchlist', 'get', 200>>>,
  Expect<Equal<WatchlistStockPrice, Schemas['WatchlistStockPrice']>>,
  Expect<Equal<WatchlistPricesResponse, Schemas['WatchlistPricesResponse']>>,
];

type StockLookupContracts = [
  Expect<Equal<StockInfoResponse, Schemas['StockInfo']>>,
  Expect<Equal<StockSearchResultItem, Schemas['StockSearchResultItem']>>,
  Expect<Equal<StockSearchResponse, Schemas['StockSearchResponse']>>,
];

type MarketDatabaseContracts = [
  Expect<Equal<MarketStatsResponse, Schemas['MarketStatsResponse']>>,
  Expect<Equal<MarketValidationResponse, Schemas['MarketValidationResponse']>>,
  Expect<Equal<RefreshStockResult, Schemas['RefreshStockResult']>>,
  Expect<Equal<MarketRefreshResponse, Schemas['RefreshResponse']>>,
];

type FundamentalsContracts = [
  Expect<Equal<ApiFundamentalDataPoint, Schemas['FundamentalDataPoint']>>,
  Expect<Equal<ApiFundamentalsResponse, Schemas['FundamentalsComputeResponse']>>,
];

type MarginContracts = [
  Expect<Equal<ApiMarginVolumeRatioData, Schemas['MarginVolumeRatioData']>>,
  Expect<Equal<ApiMarginVolumeRatioResponse, Schemas['MarginVolumeRatioResponse']>>,
  Expect<Equal<ApiMarginLongPressureData, Schemas['MarginLongPressureData']>>,
  Expect<Equal<ApiMarginFlowPressureData, Schemas['MarginFlowPressureData']>>,
  Expect<Equal<ApiMarginTurnoverDaysData, Schemas['MarginTurnoverDaysData']>>,
  Expect<Equal<ApiMarginPressureIndicatorsResponse, Schemas['MarginPressureIndicatorsResponse']>>,
];

type ProvenanceContracts = [
  Expect<Equal<DataProvenance, Schemas['DataProvenance']>>,
  Expect<Equal<ResponseDiagnostics, Schemas['ResponseDiagnostics']>>,
];

type LabContracts = [
  Expect<Equal<LabGenerateRequest, Schemas['LabGenerateRequest']>>,
  Expect<Equal<LabEvolveRequest, Schemas['LabEvolveRequest']>>,
  Expect<Equal<LabOptimizeRequest, Schemas['LabOptimizeRequest']>>,
  Expect<Equal<LabImproveRequest, Schemas['LabImproveRequest']>>,
  Expect<Equal<LabOptimizeTrialRecommendationResponse, Schemas['LabOptimizeRecommendationResponse']>>,
  Expect<Equal<LabGenerateResult, Schemas['LabGenerateResult']>>,
  Expect<Equal<LabEvolveResult, Schemas['LabEvolveResult']>>,
  Expect<Equal<LabOptimizeResult, Schemas['LabOptimizeResult']>>,
  Expect<Equal<LabImproveResult, Schemas['LabImproveResult']>>,
  Expect<Equal<LabResultData, NonNullable<Schemas['LabJobResponse']['result_data']>>>,
  Expect<Equal<LabJobResponse, Schemas['LabJobResponse']>>,
];

export type StableGeneratedContractAssertions = [
  ScreeningContracts,
  IndexContracts,
  OptionsContracts,
  SyncContracts,
  DatasetContracts,
  PortfolioContracts,
  WatchlistContracts,
  StockLookupContracts,
  MarketDatabaseContracts,
  FundamentalsContracts,
  MarginContracts,
  ProvenanceContracts,
  LabContracts,
];
