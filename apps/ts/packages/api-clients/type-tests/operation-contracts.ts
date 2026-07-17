import type { ApiJsonBody, ApiJsonResponse } from '@trading25/contracts';
import type { AnalyticsClient } from '../src/analytics/AnalyticsClient.js';
import type {
  ScreeningJobCancelResponse,
  ScreeningJobCreateResponse,
  ScreeningJobStatusResponse,
} from '../src/analytics/types.js';
import type { BacktestClient } from '../src/backtest/BacktestClient.js';
import type {
  BacktestJobCancelResponse,
  BacktestJobStatusResponse,
  BacktestRunResponse,
  LabEvolveRequest,
  LabEvolveResponse,
  LabGenerateRequest,
  LabGenerateResponse,
  LabImproveRequest,
  LabImproveResponse,
  LabJobCancelResponse,
  LabJobStatusResponse,
  LabJobsResponse,
  LabOptimizeRecommendationResponse,
  LabOptimizeRequest,
  LabOptimizeResponse,
  OptimizationJobCancelResponse,
  OptimizationJobStatusResponse,
  OptimizationRunResponse,
  SignalAttributionJobCancelResponse,
  SignalAttributionJobStatusResponse,
  SignalAttributionRunResponse,
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
];

type BacktestOperations = [
  Expect<Equal<BacktestRunResponse, ApiJsonResponse<'/api/backtest/run', 'post', 200>>>,
  Expect<Equal<BacktestJobStatusResponse, ApiJsonResponse<'/api/backtest/jobs/{job_id}', 'get', 200>>>,
  Expect<Equal<BacktestJobCancelResponse, ApiJsonResponse<'/api/backtest/jobs/{job_id}/cancel', 'post', 200>>>,
  Expect<Equal<Awaited<ReturnType<BacktestClient['runBacktest']>>, BacktestRunResponse>>,
  Expect<Equal<Awaited<ReturnType<BacktestClient['getJobStatus']>>, BacktestJobStatusResponse>>,
  Expect<Equal<Awaited<ReturnType<BacktestClient['cancelJob']>>, BacktestJobCancelResponse>>,
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
  ScreeningOperations,
  BacktestOperations,
  AttributionOperations,
  OptimizationOperations,
  LabOperations,
  FundamentalsCompatibilityModule,
];
