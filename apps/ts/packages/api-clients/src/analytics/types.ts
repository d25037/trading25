/** Analytics API client types bound to the generated FastAPI contract. */

import type {
  ApiJsonBody,
  ApiJsonResponse,
  ApiPathParams,
  ApiQuery,
  DataProvenance,
  JobStatus,
  ResponseDiagnostics,
} from '@trading25/contracts';
import type { components } from '@trading25/contracts/clients/backtest/generated/bt-api-types';

type Schemas = components['schemas'];
export interface AnalyticsClientConfig {
  baseUrl?: string;
  timeoutMs?: number;
}

export type { DataProvenance, JobStatus, ResponseDiagnostics };

export type RankingItem = Schemas['RankingItem'];
export type Rankings = Schemas['Rankings'];
export type RankingRegimeState = NonNullable<RankingItem['liquidityRegime']>;
export type RankingRiskFlag = NonNullable<RankingItem['riskFlags']>[number];
export type RankingTechnicalFlag = NonNullable<RankingItem['technicalFlags']>[number];
export type MarketRankingParams = ApiQuery<'/api/analytics/ranking', 'get'>;
export type MarketRankingResponse = ApiJsonResponse<'/api/analytics/ranking', 'get', 200>;
export type MarketRankingSymbolPathParams = ApiPathParams<'/api/analytics/ranking/symbol/{code}', 'get'>;
export type MarketRankingSymbolResponse = ApiJsonResponse<'/api/analytics/ranking/symbol/{code}', 'get', 200>;
export type SectorStrengthFamily = NonNullable<MarketRankingParams['sectorStrengthFamily']>;
export type DailyRankingValuationSignalFilter = NonNullable<MarketRankingParams['fundamentalState']>;

export type FundamentalsPathParams = ApiPathParams<'/api/analytics/fundamentals/{symbol}', 'get'>;
export type FundamentalsQuery = ApiQuery<'/api/analytics/fundamentals/{symbol}', 'get'>;
export type FundamentalsParams = FundamentalsPathParams & FundamentalsQuery;
export type FundamentalsResponse = ApiJsonResponse<'/api/analytics/fundamentals/{symbol}', 'get', 200>;

export type MarginPressureIndicatorsPathParams = ApiPathParams<
  '/api/analytics/stocks/{symbol}/margin-pressure',
  'get'
>;
export type MarginPressureIndicatorsQuery = ApiQuery<'/api/analytics/stocks/{symbol}/margin-pressure', 'get'>;
export type MarginPressureIndicatorsParams = MarginPressureIndicatorsPathParams & MarginPressureIndicatorsQuery;
export type MarginPressureIndicatorsResponse = ApiJsonResponse<
  '/api/analytics/stocks/{symbol}/margin-pressure',
  'get',
  200
>;
export type MarginLongPressureData = Schemas['MarginLongPressureData'];
export type MarginFlowPressureData = Schemas['MarginFlowPressureData'];
export type MarginTurnoverDaysData = Schemas['MarginTurnoverDaysData'];

export type MarginVolumeRatioParams = ApiPathParams<'/api/analytics/stocks/{symbol}/margin-ratio', 'get'>;
export type MarginVolumeRatioResponse = ApiJsonResponse<'/api/analytics/stocks/{symbol}/margin-ratio', 'get', 200>;
export type MarginVolumeRatioData = Schemas['MarginVolumeRatioData'];

export type SectorStocksParams = ApiQuery<'/api/analytics/sector-stocks', 'get'>;
export type SectorStocksResponse = ApiJsonResponse<'/api/analytics/sector-stocks', 'get', 200>;
export type SectorStockItem = Schemas['SectorStockItem'];

export type FundamentalRankingParams = ApiQuery<'/api/analytics/fundamental-ranking', 'get'>;
export type MarketFundamentalRankingResponse = ApiJsonResponse<'/api/analytics/fundamental-ranking', 'get', 200>;
export type FundamentalRankingItem = Schemas['FundamentalRankingItem'];
export type FundamentalRankings = Schemas['FundamentalRankings'];
export type FundamentalRankingMetricKey = MarketFundamentalRankingResponse['metricKey'];
export type FundamentalRankingSource = FundamentalRankingItem['source'];

export type ValueCompositeRankingParams = ApiQuery<'/api/analytics/value-composite-ranking', 'get'>;
export type ValueCompositeRankingResponse = ApiJsonResponse<'/api/analytics/value-composite-ranking', 'get', 200>;
export type ValueCompositeRankingItem = Schemas['ValueCompositeRankingItem'];
export type ValueCompositeTechnicalMetrics = Schemas['ValueCompositeTechnicalMetrics'];
export type ValueCompositeScorePathParams = ApiPathParams<'/api/analytics/value-composite-score/{code}', 'get'>;
export type ValueCompositeScoreQuery = ApiQuery<'/api/analytics/value-composite-score/{code}', 'get'>;
export type ValueCompositeScoreParams = {
  symbol: ValueCompositeScorePathParams['code'];
} & ValueCompositeScoreQuery;
export type ValueCompositeScoreResponse = ApiJsonResponse<'/api/analytics/value-composite-score/{code}', 'get', 200>;
export type ValueCompositeProfileId = NonNullable<ValueCompositeRankingResponse['profileId']>;
export type ValueCompositeScoreMethod = ValueCompositeRankingResponse['scoreMethod'];
export type ValueCompositeForwardEpsMode = ValueCompositeRankingResponse['forwardEpsMode'];
export type ValueCompositeScoreUnavailableReason = NonNullable<ValueCompositeScoreResponse['unsupportedReason']>;

export type ScreeningSortBy = Schemas['MarketScreeningResponse']['sortBy'];
export type SortOrder = Schemas['MarketScreeningResponse']['order'];
export type EntryDecidability = Schemas['MarketScreeningResponse']['entry_decidability'];
export type ScreeningSupport = Schemas['StrategyMetadataResponse']['screening_support'];
export type MatchedStrategyItem = Schemas['MatchedStrategyItem'];
export type ScreeningResultItem = Schemas['ScreeningResultItem'];
export type ScreeningSummary = Schemas['ScreeningSummary'];
export type MarketScreeningResponse = ApiJsonResponse<'/api/analytics/screening/result/{job_id}', 'get', 200>;
export type ScreeningJobRequest = ApiJsonBody<'/api/analytics/screening/jobs', 'post'>;
export type ScreeningJobCreateResponse = ApiJsonResponse<'/api/analytics/screening/jobs', 'post', 202>;
export type ScreeningJobStatusPathParams = ApiPathParams<'/api/analytics/screening/jobs/{job_id}', 'get'>;
export type ScreeningJobStatusResponse = ApiJsonResponse<'/api/analytics/screening/jobs/{job_id}', 'get', 200>;
export type ScreeningJobCancelPathParams = ApiPathParams<'/api/analytics/screening/jobs/{job_id}/cancel', 'post'>;
export type ScreeningJobCancelResponse = ApiJsonResponse<'/api/analytics/screening/jobs/{job_id}/cancel', 'post', 200>;
export type ScreeningJobResultPathParams = ApiPathParams<'/api/analytics/screening/result/{job_id}', 'get'>;
/** @deprecated Use the operation-specific screening response alias. */
export type ScreeningJobResponse = ScreeningJobCreateResponse;

export type ROEParams = ApiQuery<'/api/analytics/roe', 'get'>;
export type ROEResponse = ApiJsonResponse<'/api/analytics/roe', 'get', 200>;
export type ROEMetadata = Schemas['ROEMetadata'];
export type ROEResultItem = Schemas['ROEResultItem'];
export type ROESummary = Schemas['ROESummary'];

export type FactorRegressionPathParams = ApiPathParams<'/api/analytics/factor-regression/{symbol}', 'get'>;
export type FactorRegressionQuery = ApiQuery<'/api/analytics/factor-regression/{symbol}', 'get'>;
export type FactorRegressionParams = FactorRegressionPathParams & FactorRegressionQuery;
export type FactorRegressionResponse = ApiJsonResponse<'/api/analytics/factor-regression/{symbol}', 'get', 200>;
export type FactorRegressionDateRange = FactorRegressionResponse['dateRange'];
export type FactorRegressionIndexMatch = FactorRegressionResponse['sector17Matches'][number];

export type PortfolioFactorRegressionPathParams = ApiPathParams<
  '/api/analytics/portfolio-factor-regression/{portfolioId}',
  'get'
>;
export type PortfolioFactorRegressionQuery = ApiQuery<
  '/api/analytics/portfolio-factor-regression/{portfolioId}',
  'get'
>;
export type PortfolioFactorRegressionParams = PortfolioFactorRegressionPathParams & PortfolioFactorRegressionQuery;
export type PortfolioFactorRegressionResponse = ApiJsonResponse<
  '/api/analytics/portfolio-factor-regression/{portfolioId}',
  'get',
  200
>;
export type PortfolioFactorRegressionDateRange = PortfolioFactorRegressionResponse['dateRange'];
export type PortfolioFactorRegressionIndexMatch = PortfolioFactorRegressionResponse['sector17Matches'][number];
export type PortfolioFactorRegressionStockWeight = PortfolioFactorRegressionResponse['weights'][number];
export type PortfolioFactorRegressionExcludedStock = PortfolioFactorRegressionResponse['excludedStocks'][number];
