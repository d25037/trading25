import type { ApiJsonBody, ApiJsonResponse, ApiPathParams, ApiQuery } from '@trading25/contracts';
import type { AnalyticsClient } from '../src/analytics/AnalyticsClient.js';
import type {
  FactorRegressionPathParams,
  FactorRegressionQuery,
  FundamentalsPathParams,
  FundamentalsQuery,
  MarginPressureIndicatorsPathParams,
  MarginPressureIndicatorsQuery,
  MarketRankingSymbolPathParams,
  PortfolioFactorRegressionPathParams,
  PortfolioFactorRegressionQuery,
  ScreeningJobCancelResponse,
  ScreeningJobCancelPathParams,
  ScreeningJobCreateResponse,
  ScreeningJobResultPathParams,
  ScreeningJobStatusPathParams,
  ScreeningJobStatusResponse,
  ValueCompositeScorePathParams,
  ValueCompositeScoreQuery,
} from '../src/analytics/types.js';
import type { BacktestClient } from '../src/backtest/BacktestClient.js';
import type {
  BacktestJobCancelResponse,
  BacktestJobCancelPathParams,
  BacktestJobStatusPathParams,
  BacktestJobStatusResponse,
  BacktestJobsQuery,
  BacktestResultPathParams,
  BacktestResultQuery,
  BacktestHtmlFilesQuery,
  AttributionArtifactFilesQuery,
  AttributionArtifactContentQuery,
  HtmlFileContentPathParams,
  HtmlFileDeletePathParams,
  HtmlFileRenamePathParams,
  OptimizationHtmlFilesQuery,
  OptimizationHtmlFileContentPathParams,
  OptimizationHtmlFileDeletePathParams,
  OptimizationHtmlFileDeleteResponse,
  OptimizationHtmlFileRenamePathParams,
  OptimizationHtmlFileRenameRequest,
  OptimizationHtmlFileRenameResponse,
  BacktestRunResponse,
  DefaultConfigStructuredUpdateRequest,
  DefaultConfigStructuredUpdateResponse,
  LabEvolveRequest,
  LabEvolveResponse,
  LabGenerateRequest,
  LabGenerateResponse,
  LabImproveRequest,
  LabImproveResponse,
  LabJobCancelResponse,
  LabJobCancelPathParams,
  LabJobStatusPathParams,
  LabJobStatusResponse,
  LabJobsQuery,
  LabJobsResponse,
  LabOptimizeRecommendationQuery,
  LabOptimizeRecommendationResponse,
  LabOptimizeRequest,
  LabOptimizeResponse,
  OptimizationJobCancelResponse,
  OptimizationJobCancelPathParams,
  OptimizationJobStatusPathParams,
  OptimizationJobStatusResponse,
  OptimizationRunResponse,
  SignalAttributionJobCancelResponse,
  SignalAttributionJobCancelPathParams,
  SignalAttributionJobStatusPathParams,
  SignalAttributionJobStatusResponse,
  SignalAttributionResultPathParams,
  SignalAttributionRunResponse,
  StrategyDeletePathParams,
  StrategyDetailPathParams,
  StrategyDuplicatePathParams,
  StrategyEditorContextPathParams,
  StrategyMovePathParams,
  StrategyOptimizationDeletePathParams,
  StrategyOptimizationDraftPathParams,
  StrategyOptimizationDraftResponse,
  StrategyOptimizationSavePathParams,
  StrategyOptimizationStatePathParams,
  StrategyRenamePathParams,
  StrategyUpdatePathParams,
  StrategyValidationPathParams,
} from '../src/backtest/types.js';
import type {
  FundamentalsComputeRequest,
  FundamentalsComputeResponse,
  OHLCVResampleRequest,
  OHLCVResampleResponse,
} from '../src/backtest/fundamentals-types.js';

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Expect<Value extends true> = Value;

type ScreeningOperations = [
  Expect<Equal<ScreeningJobCreateResponse, ApiJsonResponse<'/api/analytics/screening/jobs', 'post', 202>>>,
  Expect<Equal<ScreeningJobStatusResponse, ApiJsonResponse<'/api/analytics/screening/jobs/{job_id}', 'get', 200>>>,
  Expect<
    Equal<ScreeningJobCancelResponse, ApiJsonResponse<'/api/analytics/screening/jobs/{job_id}/cancel', 'post', 200>>
  >,
  Expect<Equal<Awaited<ReturnType<AnalyticsClient['createScreeningJob']>>, ScreeningJobCreateResponse>>,
  Expect<Equal<Awaited<ReturnType<AnalyticsClient['getScreeningJobStatus']>>, ScreeningJobStatusResponse>>,
  Expect<Equal<Awaited<ReturnType<AnalyticsClient['cancelScreeningJob']>>, ScreeningJobCancelResponse>>,
  Expect<
    Equal<ScreeningJobStatusPathParams, ApiPathParams<'/api/analytics/screening/jobs/{job_id}', 'get'>>
  >,
  Expect<
    Equal<ScreeningJobCancelPathParams, ApiPathParams<'/api/analytics/screening/jobs/{job_id}/cancel', 'post'>>
  >,
  Expect<
    Equal<ScreeningJobResultPathParams, ApiPathParams<'/api/analytics/screening/result/{job_id}', 'get'>>
  >,
  Expect<Equal<Parameters<AnalyticsClient['getScreeningJobStatus']>[0], ScreeningJobStatusPathParams['job_id']>>,
  Expect<Equal<Parameters<AnalyticsClient['cancelScreeningJob']>[0], ScreeningJobCancelPathParams['job_id']>>,
  Expect<Equal<Parameters<AnalyticsClient['getScreeningResult']>[0], ScreeningJobResultPathParams['job_id']>>,
];

type AnalyticsPathQueryOperations = [
  Expect<Equal<MarketRankingSymbolPathParams, ApiPathParams<'/api/analytics/ranking/symbol/{code}', 'get'>>>,
  Expect<Equal<FundamentalsPathParams, ApiPathParams<'/api/analytics/fundamentals/{symbol}', 'get'>>>,
  Expect<Equal<FundamentalsQuery, ApiQuery<'/api/analytics/fundamentals/{symbol}', 'get'>>>,
  Expect<
    Equal<
      MarginPressureIndicatorsPathParams,
      ApiPathParams<'/api/analytics/stocks/{symbol}/margin-pressure', 'get'>
    >
  >,
  Expect<
    Equal<MarginPressureIndicatorsQuery, ApiQuery<'/api/analytics/stocks/{symbol}/margin-pressure', 'get'>>
  >,
  Expect<
    Equal<ValueCompositeScorePathParams, ApiPathParams<'/api/analytics/value-composite-score/{code}', 'get'>>
  >,
  Expect<Equal<ValueCompositeScoreQuery, ApiQuery<'/api/analytics/value-composite-score/{code}', 'get'>>>,
  Expect<
    Equal<FactorRegressionPathParams, ApiPathParams<'/api/analytics/factor-regression/{symbol}', 'get'>>
  >,
  Expect<Equal<FactorRegressionQuery, ApiQuery<'/api/analytics/factor-regression/{symbol}', 'get'>>>,
  Expect<
    Equal<
      PortfolioFactorRegressionPathParams,
      ApiPathParams<'/api/analytics/portfolio-factor-regression/{portfolioId}', 'get'>
    >
  >,
  Expect<
    Equal<
      PortfolioFactorRegressionQuery,
      ApiQuery<'/api/analytics/portfolio-factor-regression/{portfolioId}', 'get'>
    >
  >,
  Expect<
    Equal<Parameters<AnalyticsClient['getMarketRankingSymbol']>[0], MarketRankingSymbolPathParams['code']>
  >,
];

type BacktestOperations = [
  Expect<Equal<BacktestRunResponse, ApiJsonResponse<'/api/backtest/run', 'post', 200>>>,
  Expect<Equal<BacktestJobStatusResponse, ApiJsonResponse<'/api/backtest/jobs/{job_id}', 'get', 200>>>,
  Expect<Equal<BacktestJobCancelResponse, ApiJsonResponse<'/api/backtest/jobs/{job_id}/cancel', 'post', 200>>>,
  Expect<Equal<Awaited<ReturnType<BacktestClient['runBacktest']>>, BacktestRunResponse>>,
  Expect<Equal<Awaited<ReturnType<BacktestClient['getJobStatus']>>, BacktestJobStatusResponse>>,
  Expect<Equal<Awaited<ReturnType<BacktestClient['cancelJob']>>, BacktestJobCancelResponse>>,
  Expect<Equal<BacktestJobStatusPathParams, ApiPathParams<'/api/backtest/jobs/{job_id}', 'get'>>>,
  Expect<Equal<BacktestJobCancelPathParams, ApiPathParams<'/api/backtest/jobs/{job_id}/cancel', 'post'>>>,
  Expect<Equal<BacktestResultPathParams, ApiPathParams<'/api/backtest/result/{job_id}', 'get'>>>,
  Expect<Equal<BacktestResultQuery, ApiQuery<'/api/backtest/result/{job_id}', 'get'>>>,
  Expect<Equal<BacktestJobsQuery, ApiQuery<'/api/backtest/jobs', 'get'>>>,
  Expect<Equal<BacktestHtmlFilesQuery, ApiQuery<'/api/backtest/html-files', 'get'>>>,
  Expect<Equal<Parameters<BacktestClient['getJobStatus']>[0], BacktestJobStatusPathParams['job_id']>>,
  Expect<Equal<Parameters<BacktestClient['cancelJob']>[0], BacktestJobCancelPathParams['job_id']>>,
  Expect<Equal<Parameters<BacktestClient['listHtmlFiles']>[0], BacktestHtmlFilesQuery | undefined>>,
];

type StrategyPathOperations = [
  Expect<Equal<StrategyDetailPathParams, ApiPathParams<'/api/strategies/{strategy_name}', 'get'>>>,
  Expect<
    Equal<StrategyEditorContextPathParams, ApiPathParams<'/api/strategies/{strategy_name}/editor-context', 'get'>>
  >,
  Expect<
    Equal<StrategyValidationPathParams, ApiPathParams<'/api/strategies/{strategy_name}/validate', 'post'>>
  >,
  Expect<Equal<StrategyMovePathParams, ApiPathParams<'/api/strategies/{strategy_name}/move', 'post'>>>,
  Expect<Equal<StrategyUpdatePathParams, ApiPathParams<'/api/strategies/{strategy_name}', 'put'>>>,
  Expect<Equal<StrategyDeletePathParams, ApiPathParams<'/api/strategies/{strategy_name}', 'delete'>>>,
  Expect<
    Equal<StrategyDuplicatePathParams, ApiPathParams<'/api/strategies/{strategy_name}/duplicate', 'post'>>
  >,
  Expect<Equal<StrategyRenamePathParams, ApiPathParams<'/api/strategies/{strategy_name}/rename', 'post'>>>,
  Expect<
    Equal<
      StrategyOptimizationStatePathParams,
      ApiPathParams<'/api/strategies/{strategy_name}/optimization', 'get'>
    >
  >,
  Expect<
    Equal<
      StrategyOptimizationDraftPathParams,
      ApiPathParams<'/api/strategies/{strategy_name}/optimization/draft', 'post'>
    >
  >,
  Expect<
    Equal<
      StrategyOptimizationSavePathParams,
      ApiPathParams<'/api/strategies/{strategy_name}/optimization', 'put'>
    >
  >,
  Expect<
    Equal<
      StrategyOptimizationDeletePathParams,
      ApiPathParams<'/api/strategies/{strategy_name}/optimization', 'delete'>
    >
  >,
  Expect<Equal<Parameters<BacktestClient['getStrategy']>[0], StrategyDetailPathParams['strategy_name']>>,
  Expect<
    Equal<Parameters<BacktestClient['validateStrategy']>[0], StrategyValidationPathParams['strategy_name']>
  >,
  Expect<
    Equal<Parameters<BacktestClient['getStrategyOptimization']>[0], StrategyOptimizationStatePathParams['strategy_name']>
  >,
  Expect<
    Equal<
      StrategyOptimizationDraftResponse,
      ApiJsonResponse<'/api/strategies/{strategy_name}/optimization/draft', 'post', 200>
    >
  >,
  Expect<
    Equal<
      Awaited<ReturnType<BacktestClient['generateStrategyOptimizationDraft']>>,
      StrategyOptimizationDraftResponse
    >
  >,
];

type DefaultConfigOperations = [
  Expect<
    Equal<DefaultConfigStructuredUpdateRequest, ApiJsonBody<'/api/config/default/structured', 'put'>>
  >,
  Expect<
    Equal<
      DefaultConfigStructuredUpdateResponse,
      ApiJsonResponse<'/api/config/default/structured', 'put', 200>
    >
  >,
  Expect<
    Equal<
      Parameters<BacktestClient['updateDefaultConfigStructured']>[0],
      DefaultConfigStructuredUpdateRequest
    >
  >,
  Expect<
    Equal<
      Awaited<ReturnType<BacktestClient['updateDefaultConfigStructured']>>,
      DefaultConfigStructuredUpdateResponse
    >
  >,
];

type AttributionOperations = [
  Expect<Equal<SignalAttributionRunResponse, ApiJsonResponse<'/api/backtest/attribution/run', 'post', 200>>>,
  Expect<
    Equal<SignalAttributionJobStatusResponse, ApiJsonResponse<'/api/backtest/attribution/jobs/{job_id}', 'get', 200>>
  >,
  Expect<
    Equal<
      SignalAttributionJobCancelResponse,
      ApiJsonResponse<'/api/backtest/attribution/jobs/{job_id}/cancel', 'post', 200>
    >
  >,
  Expect<Equal<Awaited<ReturnType<BacktestClient['runSignalAttribution']>>, SignalAttributionRunResponse>>,
  Expect<Equal<Awaited<ReturnType<BacktestClient['getSignalAttributionJob']>>, SignalAttributionJobStatusResponse>>,
  Expect<Equal<Awaited<ReturnType<BacktestClient['cancelSignalAttributionJob']>>, SignalAttributionJobCancelResponse>>,
  Expect<
    Equal<SignalAttributionJobStatusPathParams, ApiPathParams<'/api/backtest/attribution/jobs/{job_id}', 'get'>>
  >,
  Expect<
    Equal<
      SignalAttributionJobCancelPathParams,
      ApiPathParams<'/api/backtest/attribution/jobs/{job_id}/cancel', 'post'>
    >
  >,
  Expect<
    Equal<SignalAttributionResultPathParams, ApiPathParams<'/api/backtest/attribution/result/{job_id}', 'get'>>
  >,
  Expect<Equal<AttributionArtifactFilesQuery, ApiQuery<'/api/backtest/attribution-files', 'get'>>>,
  Expect<
    Equal<AttributionArtifactContentQuery, ApiQuery<'/api/backtest/attribution-files/content', 'get'>>
  >,
  Expect<
    Equal<Parameters<BacktestClient['getSignalAttributionJob']>[0], SignalAttributionJobStatusPathParams['job_id']>
  >,
  Expect<
    Equal<Parameters<BacktestClient['cancelSignalAttributionJob']>[0], SignalAttributionJobCancelPathParams['job_id']>
  >,
  Expect<
    Equal<Parameters<BacktestClient['listAttributionArtifactFiles']>[0], AttributionArtifactFilesQuery | undefined>
  >,
];

type BacktestArtifactOperations = [
  Expect<
    Equal<HtmlFileContentPathParams, ApiPathParams<'/api/backtest/html-files/{strategy}/{filename}', 'get'>>
  >,
  Expect<
    Equal<
      HtmlFileRenamePathParams,
      ApiPathParams<'/api/backtest/html-files/{strategy}/{filename}/rename', 'post'>
    >
  >,
  Expect<
    Equal<HtmlFileDeletePathParams, ApiPathParams<'/api/backtest/html-files/{strategy}/{filename}', 'delete'>>
  >,
  Expect<Equal<Parameters<BacktestClient['getHtmlFileContent']>[0], HtmlFileContentPathParams['strategy']>>,
  Expect<Equal<Parameters<BacktestClient['getHtmlFileContent']>[1], HtmlFileContentPathParams['filename']>>,
];

type OptimizationOperations = [
  Expect<Equal<OptimizationRunResponse, ApiJsonResponse<'/api/optimize/run', 'post', 200>>>,
  Expect<Equal<OptimizationJobStatusResponse, ApiJsonResponse<'/api/optimize/jobs/{job_id}', 'get', 200>>>,
  Expect<
    Equal<OptimizationJobCancelResponse, ApiJsonResponse<'/api/optimize/jobs/{job_id}/cancel', 'post', 200>>
  >,
  Expect<Equal<Awaited<ReturnType<BacktestClient['runOptimization']>>, OptimizationRunResponse>>,
  Expect<Equal<Awaited<ReturnType<BacktestClient['getOptimizationJobStatus']>>, OptimizationJobStatusResponse>>,
  Expect<Equal<Awaited<ReturnType<BacktestClient['cancelOptimizationJob']>>, OptimizationJobCancelResponse>>,
  Expect<Equal<OptimizationJobStatusPathParams, ApiPathParams<'/api/optimize/jobs/{job_id}', 'get'>>>,
  Expect<Equal<OptimizationJobCancelPathParams, ApiPathParams<'/api/optimize/jobs/{job_id}/cancel', 'post'>>>,
  Expect<Equal<OptimizationHtmlFilesQuery, ApiQuery<'/api/optimize/html-files', 'get'>>>,
  Expect<
    Equal<
      OptimizationHtmlFileContentPathParams,
      ApiPathParams<'/api/optimize/html-files/{strategy}/{filename}', 'get'>
    >
  >,
  Expect<
    Equal<
      OptimizationHtmlFileRenamePathParams,
      ApiPathParams<'/api/optimize/html-files/{strategy}/{filename}/rename', 'post'>
    >
  >,
  Expect<
    Equal<
      OptimizationHtmlFileDeletePathParams,
      ApiPathParams<'/api/optimize/html-files/{strategy}/{filename}', 'delete'>
    >
  >,
  Expect<
    Equal<
      OptimizationHtmlFileRenameRequest,
      ApiJsonBody<'/api/optimize/html-files/{strategy}/{filename}/rename', 'post'>
    >
  >,
  Expect<
    Equal<
      OptimizationHtmlFileRenameResponse,
      ApiJsonResponse<'/api/optimize/html-files/{strategy}/{filename}/rename', 'post', 200>
    >
  >,
  Expect<
    Equal<
      OptimizationHtmlFileDeleteResponse,
      ApiJsonResponse<'/api/optimize/html-files/{strategy}/{filename}', 'delete', 200>
    >
  >,
  Expect<Equal<Parameters<BacktestClient['renameOptimizationHtmlFile']>[2], OptimizationHtmlFileRenameRequest>>,
  Expect<
    Equal<
      Awaited<ReturnType<BacktestClient['renameOptimizationHtmlFile']>>,
      OptimizationHtmlFileRenameResponse
    >
  >,
  Expect<
    Equal<
      Awaited<ReturnType<BacktestClient['deleteOptimizationHtmlFile']>>,
      OptimizationHtmlFileDeleteResponse
    >
  >,
  Expect<
    Equal<Parameters<BacktestClient['getOptimizationJobStatus']>[0], OptimizationJobStatusPathParams['job_id']>
  >,
  Expect<
    Equal<Parameters<BacktestClient['cancelOptimizationJob']>[0], OptimizationJobCancelPathParams['job_id']>
  >,
  Expect<
    Equal<Parameters<BacktestClient['listOptimizationHtmlFiles']>[0], OptimizationHtmlFilesQuery | undefined>
  >,
];

type LabOperations = [
  Expect<Equal<LabGenerateRequest, ApiJsonBody<'/api/lab/generate', 'post'>>>,
  Expect<Equal<LabGenerateResponse, ApiJsonResponse<'/api/lab/generate', 'post', 200>>>,
  Expect<Equal<LabEvolveRequest, ApiJsonBody<'/api/lab/evolve', 'post'>>>,
  Expect<Equal<LabEvolveResponse, ApiJsonResponse<'/api/lab/evolve', 'post', 200>>>,
  Expect<Equal<LabOptimizeRequest, ApiJsonBody<'/api/lab/optimize', 'post'>>>,
  Expect<Equal<LabOptimizeResponse, ApiJsonResponse<'/api/lab/optimize', 'post', 200>>>,
  Expect<Equal<LabImproveRequest, ApiJsonBody<'/api/lab/improve', 'post'>>>,
  Expect<Equal<LabImproveResponse, ApiJsonResponse<'/api/lab/improve', 'post', 200>>>,
  Expect<Equal<LabJobsResponse, ApiJsonResponse<'/api/lab/jobs', 'get', 200>>>,
  Expect<Equal<LabJobStatusResponse, ApiJsonResponse<'/api/lab/jobs/{job_id}', 'get', 200>>>,
  Expect<Equal<LabJobCancelResponse, ApiJsonResponse<'/api/lab/jobs/{job_id}/cancel', 'post', 200>>>,
  Expect<Equal<LabJobsQuery, ApiQuery<'/api/lab/jobs', 'get'>>>,
  Expect<Equal<LabJobStatusPathParams, ApiPathParams<'/api/lab/jobs/{job_id}', 'get'>>>,
  Expect<Equal<LabJobCancelPathParams, ApiPathParams<'/api/lab/jobs/{job_id}/cancel', 'post'>>>,
  Expect<Equal<LabOptimizeRecommendationQuery, ApiQuery<'/api/lab/optimize/recommendation', 'get'>>>,
  Expect<
    Equal<
      LabOptimizeRecommendationResponse,
      ApiJsonResponse<'/api/lab/optimize/recommendation', 'get', 200>
    >
  >,
];

type FundamentalsCompatibilityModule = [
  Expect<Equal<FundamentalsComputeRequest, ApiJsonBody<'/api/fundamentals/compute', 'post'>>>,
  Expect<Equal<FundamentalsComputeResponse, ApiJsonResponse<'/api/fundamentals/compute', 'post', 200>>>,
  Expect<Equal<OHLCVResampleRequest, ApiJsonBody<'/api/ohlcv/resample', 'post'>>>,
  Expect<Equal<OHLCVResampleResponse, ApiJsonResponse<'/api/ohlcv/resample', 'post', 200>>>,
];

export type OperationContractAssertions = [
  AnalyticsPathQueryOperations,
  ScreeningOperations,
  StrategyPathOperations,
  DefaultConfigOperations,
  BacktestOperations,
  AttributionOperations,
  BacktestArtifactOperations,
  OptimizationOperations,
  LabOperations,
  FundamentalsCompatibilityModule,
];
