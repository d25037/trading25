/** Backtest API client types bound to the generated FastAPI schemas. */

import type { ApiJsonBody, ApiJsonResponse, ApiPathParams, ApiQuery, JobStatus } from '@trading25/contracts';
import type { components } from '@trading25/contracts/clients/backtest/generated/bt-api-types';

type Schemas = components['schemas'];

export type { JobStatus };
export interface BacktestClientConfig {
  baseUrl?: string;
  timeout?: number;
}

export type EngineFamily = Schemas['RunSpec']['engine_family'];
export type RunType = Schemas['RunSpec']['run_type'];
export type ArtifactKind = Schemas['ArtifactRecord']['kind'];
export type ArtifactStorage = Schemas['ArtifactRecord']['storage'];
export type CompiledStrategyInputRequirements = Schemas['CompiledStrategyInputRequirements'];
export type RunSpec = Schemas['RunSpec'];
export type RunMetadata = Schemas['RunMetadata'];
export type ArtifactRecord = Schemas['ArtifactRecord'];
export type ArtifactIndex = Schemas['ArtifactIndex'];
export type JobExecutionControl = Schemas['JobExecutionControl'];
export type EnginePolicy = Schemas['EnginePolicy'];
export type EnginePolicyMode = EnginePolicy['mode'];
export type FastCandidateSummary = Schemas['FastCandidateSummary'];
export type VerificationDelta = Schemas['VerificationDelta'];
export type VerificationCandidate = Schemas['VerificationCandidateSummary'];
export type VerificationStatus = VerificationCandidate['verification_status'];
export type VerificationSummary = Schemas['VerificationSummary'];
export type VerificationOverallStatus = VerificationSummary['overall_status'];
export type CanonicalExecutionMetrics = Schemas['CanonicalExecutionMetrics'];
export type CanonicalExecutionResult = Schemas['CanonicalExecutionResult'];

export type BacktestRequest = ApiJsonBody<'/api/backtest/run', 'post'>;
export type BacktestResultSummary = Schemas['BacktestResultSummary'];
export type BacktestRunResponse = ApiJsonResponse<'/api/backtest/run', 'post', 200>;
export type BacktestJobStatusPathParams = ApiPathParams<'/api/backtest/jobs/{job_id}', 'get'>;
export type BacktestJobStatusResponse = ApiJsonResponse<'/api/backtest/jobs/{job_id}', 'get', 200>;
export type BacktestJobCancelPathParams = ApiPathParams<'/api/backtest/jobs/{job_id}/cancel', 'post'>;
export type BacktestJobCancelResponse = ApiJsonResponse<'/api/backtest/jobs/{job_id}/cancel', 'post', 200>;
export type BacktestJobsQuery = ApiQuery<'/api/backtest/jobs', 'get'>;
export type BacktestJobsResponse = ApiJsonResponse<'/api/backtest/jobs', 'get', 200>;
/** @deprecated Use the operation-specific backtest response alias. */
export type BacktestJobResponse = BacktestRunResponse;
export type BacktestResultPathParams = ApiPathParams<'/api/backtest/result/{job_id}', 'get'>;
export type BacktestResultQuery = ApiQuery<'/api/backtest/result/{job_id}', 'get'>;
export type BacktestResultResponse = ApiJsonResponse<'/api/backtest/result/{job_id}', 'get', 200>;
export type SignalAttributionRequest = ApiJsonBody<'/api/backtest/attribution/run', 'post'>;
export type SignalAttributionMetrics = Schemas['SignalAttributionMetrics'];
export type SignalAttributionLooResult = Schemas['SignalAttributionLooResult'];
export type SignalAttributionShapleyResult = Schemas['SignalAttributionShapleyResult'];
export type SignalAttributionSignalResult = Schemas['SignalAttributionSignalResult'];
export type SignalAttributionTopNScore = Schemas['SignalAttributionTopNScore'];
export type SignalAttributionTopNSelection = Schemas['SignalAttributionTopNSelection'];
export type SignalAttributionTiming = Schemas['SignalAttributionTiming'];
export type SignalAttributionShapleyMeta = Schemas['SignalAttributionShapleyMeta'];
export type SignalAttributionResult = Schemas['SignalAttributionResult'];
export type SignalAttributionRunResponse = ApiJsonResponse<'/api/backtest/attribution/run', 'post', 200>;
export type SignalAttributionJobStatusPathParams = ApiPathParams<
  '/api/backtest/attribution/jobs/{job_id}',
  'get'
>;
export type SignalAttributionJobStatusResponse = ApiJsonResponse<'/api/backtest/attribution/jobs/{job_id}', 'get', 200>;
export type SignalAttributionJobCancelPathParams = ApiPathParams<
  '/api/backtest/attribution/jobs/{job_id}/cancel',
  'post'
>;
export type SignalAttributionJobCancelResponse = ApiJsonResponse<
  '/api/backtest/attribution/jobs/{job_id}/cancel',
  'post',
  200
>;
/** @deprecated Use the operation-specific attribution response alias. */
export type SignalAttributionJobResponse = SignalAttributionRunResponse;
export type SignalAttributionResultPathParams = ApiPathParams<
  '/api/backtest/attribution/result/{job_id}',
  'get'
>;
export type SignalAttributionResultResponse = ApiJsonResponse<'/api/backtest/attribution/result/{job_id}', 'get', 200>;
export type AttributionArtifactInfo = Schemas['AttributionArtifactInfo'];
export type AttributionArtifactFilesQuery = ApiQuery<'/api/backtest/attribution-files', 'get'>;
export type AttributionArtifactListResponse = ApiJsonResponse<'/api/backtest/attribution-files', 'get', 200>;
export type AttributionArtifactContentQuery = ApiQuery<'/api/backtest/attribution-files/content', 'get'>;
export type AttributionArtifactContentResponse = ApiJsonResponse<'/api/backtest/attribution-files/content', 'get', 200>;

export type EntryDecidability = Schemas['StrategyMetadataResponse']['entry_decidability'];
export type ScreeningSupport = Schemas['StrategyMetadataResponse']['screening_support'];
export type StrategyMetadata = Schemas['StrategyMetadataResponse'];
export type StrategyListResponse = ApiJsonResponse<'/api/strategies', 'get', 200>;
export type StrategyDetailPathParams = ApiPathParams<'/api/strategies/{strategy_name}', 'get'>;
export type StrategyDetailResponse = ApiJsonResponse<'/api/strategies/{strategy_name}', 'get', 200>;
export type StrategyValidationPathParams = ApiPathParams<'/api/strategies/{strategy_name}/validate', 'post'>;
export type StrategyValidationRequest = ApiJsonBody<'/api/strategies/{strategy_name}/validate', 'post'>;
export type CompiledAvailabilityPoint = Schemas['CompiledSignalAvailability']['observation_time'];
export type CompiledExecutionSession = Schemas['CompiledSignalAvailability']['execution_session'];
export type CompiledSignalScope = Schemas['CompiledSignalIR']['scope'];
export type CompiledSignalAvailability = Schemas['CompiledSignalAvailability'];
export type CompiledSignalIR = Schemas['CompiledSignalIR'];
export type CompiledStrategyIR = Schemas['CompiledStrategyIR'];
export type StrategyValidationResponse = ApiJsonResponse<'/api/strategies/{strategy_name}/validate', 'post', 200>;
export type HealthResponse = ApiJsonResponse<'/api/health', 'get', 200>;
export type StrategyUpdatePathParams = ApiPathParams<'/api/strategies/{strategy_name}', 'put'>;
export type StrategyUpdateRequest = ApiJsonBody<'/api/strategies/{strategy_name}', 'put'>;
export type StrategyUpdateResponse = ApiJsonResponse<'/api/strategies/{strategy_name}', 'put', 200>;
export type StrategyDeletePathParams = ApiPathParams<'/api/strategies/{strategy_name}', 'delete'>;
export type StrategyDeleteResponse = ApiJsonResponse<'/api/strategies/{strategy_name}', 'delete', 200>;
export type StrategyDuplicatePathParams = ApiPathParams<'/api/strategies/{strategy_name}/duplicate', 'post'>;
export type StrategyDuplicateRequest = ApiJsonBody<'/api/strategies/{strategy_name}/duplicate', 'post'>;
export type StrategyDuplicateResponse = ApiJsonResponse<'/api/strategies/{strategy_name}/duplicate', 'post', 200>;
export type StrategyRenamePathParams = ApiPathParams<'/api/strategies/{strategy_name}/rename', 'post'>;
export type StrategyRenameRequest = ApiJsonBody<'/api/strategies/{strategy_name}/rename', 'post'>;
export type StrategyRenameResponse = ApiJsonResponse<'/api/strategies/{strategy_name}/rename', 'post', 200>;
export type StrategyMovePathParams = ApiPathParams<'/api/strategies/{strategy_name}/move', 'post'>;
export type StrategyMoveRequest = ApiJsonBody<'/api/strategies/{strategy_name}/move', 'post'>;
export type StrategyMoveTargetCategory = StrategyMoveRequest['target_category'];
export type StrategyMoveResponse = ApiJsonResponse<'/api/strategies/{strategy_name}/move', 'post', 200>;

export type HtmlFileInfo = Schemas['HtmlFileInfo'];
export type BacktestHtmlFilesQuery = ApiQuery<'/api/backtest/html-files', 'get'>;
export type HtmlFileListResponse = ApiJsonResponse<'/api/backtest/html-files', 'get', 200>;
export type HtmlFileMetrics = Schemas['HtmlFileMetrics'];
export type HtmlFileContentPathParams = ApiPathParams<'/api/backtest/html-files/{strategy}/{filename}', 'get'>;
export type HtmlFileContentResponse = ApiJsonResponse<'/api/backtest/html-files/{strategy}/{filename}', 'get', 200>;
export type HtmlFileRenamePathParams = ApiPathParams<
  '/api/backtest/html-files/{strategy}/{filename}/rename',
  'post'
>;
export type HtmlFileRenameRequest = ApiJsonBody<'/api/backtest/html-files/{strategy}/{filename}/rename', 'post'>;
export type HtmlFileRenameResponse = ApiJsonResponse<
  '/api/backtest/html-files/{strategy}/{filename}/rename',
  'post',
  200
>;
export type HtmlFileDeletePathParams = ApiPathParams<'/api/backtest/html-files/{strategy}/{filename}', 'delete'>;
export type HtmlFileDeleteResponse = ApiJsonResponse<'/api/backtest/html-files/{strategy}/{filename}', 'delete', 200>;
export type OptimizationRequest = ApiJsonBody<'/api/optimize/run', 'post'>;
export type OptimizationRunResponse = ApiJsonResponse<'/api/optimize/run', 'post', 200>;
export type OptimizationJobStatusPathParams = ApiPathParams<'/api/optimize/jobs/{job_id}', 'get'>;
export type OptimizationJobStatusResponse = ApiJsonResponse<'/api/optimize/jobs/{job_id}', 'get', 200>;
export type OptimizationJobCancelPathParams = ApiPathParams<'/api/optimize/jobs/{job_id}/cancel', 'post'>;
export type OptimizationJobCancelResponse = ApiJsonResponse<'/api/optimize/jobs/{job_id}/cancel', 'post', 200>;
/** @deprecated Use the operation-specific optimization response alias. */
export type OptimizationJobResponse = OptimizationRunResponse;
export type OptimizationDiagnosticResponse = Schemas['OptimizationDiagnosticResponse'];
export type StrategyOptimizationStateResponse = ApiJsonResponse<
  '/api/strategies/{strategy_name}/optimization',
  'get',
  200
>;
export type StrategyOptimizationStatePathParams = ApiPathParams<
  '/api/strategies/{strategy_name}/optimization',
  'get'
>;
export type StrategyOptimizationDraftPathParams = ApiPathParams<
  '/api/strategies/{strategy_name}/optimization/draft',
  'post'
>;
export type StrategyOptimizationSavePathParams = ApiPathParams<
  '/api/strategies/{strategy_name}/optimization',
  'put'
>;
export type StrategyOptimizationSaveRequest = ApiJsonBody<'/api/strategies/{strategy_name}/optimization', 'put'>;
export type StrategyOptimizationSaveResponse = ApiJsonResponse<
  '/api/strategies/{strategy_name}/optimization',
  'put',
  200
>;
export type StrategyOptimizationDeleteResponse = ApiJsonResponse<
  '/api/strategies/{strategy_name}/optimization',
  'delete',
  200
>;
export type StrategyOptimizationDeletePathParams = ApiPathParams<
  '/api/strategies/{strategy_name}/optimization',
  'delete'
>;
export type OptimizationHtmlFileInfo = Schemas['OptimizationHtmlFileInfo'];
export type OptimizationHtmlFilesQuery = ApiQuery<'/api/optimize/html-files', 'get'>;
export type OptimizationHtmlFileListResponse = ApiJsonResponse<'/api/optimize/html-files', 'get', 200>;
export type OptimizationHtmlFileContentPathParams = ApiPathParams<
  '/api/optimize/html-files/{strategy}/{filename}',
  'get'
>;
export type OptimizationHtmlFileContentResponse = ApiJsonResponse<
  '/api/optimize/html-files/{strategy}/{filename}',
  'get',
  200
>;
export type OptimizationHtmlFileRenamePathParams = ApiPathParams<
  '/api/optimize/html-files/{strategy}/{filename}/rename',
  'post'
>;
export type OptimizationHtmlFileDeletePathParams = ApiPathParams<
  '/api/optimize/html-files/{strategy}/{filename}',
  'delete'
>;

export type FieldConstraints = Schemas['FieldConstraints'];
export type SignalFieldDefinition = Schemas['SignalFieldSchema'];
export type SignalChartCapability = Schemas['SignalChartCapability'];
export type SignalExecutionSemantics = Schemas['SignalAvailabilityProfile']['execution_semantics'];
export type SignalAvailabilityProfile = Schemas['SignalAvailabilityProfile'];
export type SignalDefinition = Schemas['SignalReferenceSchema'];
export type SignalCategory = Schemas['SignalCategorySchema'];
export type SignalReferenceResponse = ApiJsonResponse<'/api/signals/reference', 'get', 200>;
export type AuthoringFieldSchema = Schemas['AuthoringFieldSchema'];
export type AuthoringFieldType = AuthoringFieldSchema['type'];
export type AuthoringWidgetType = AuthoringFieldSchema['widget'];
export type AuthoringFieldSection = AuthoringFieldSchema['section'];
export type AuthoringFieldSource = Schemas['AuthoringFieldProvenance']['source'];
export type AuthoringFieldGroupSchema = Schemas['AuthoringFieldGroupSchema'];
export type StrategyEditorCapabilities = Schemas['StrategyEditorCapabilities'];
export type StrategyEditorReferenceResponse = ApiJsonResponse<'/api/strategies/editor/reference', 'get', 200>;
export type AuthoringFieldProvenance = Schemas['AuthoringFieldProvenance'];
export type StrategyEditorContextResponse = ApiJsonResponse<
  '/api/strategies/{strategy_name}/editor-context',
  'get',
  200
>;
export type StrategyEditorContextPathParams = ApiPathParams<
  '/api/strategies/{strategy_name}/editor-context',
  'get'
>;
export type DefaultConfigEditorContextResponse = ApiJsonResponse<'/api/config/default/editor-context', 'get', 200>;
export type DefaultConfigResponse = ApiJsonResponse<'/api/config/default', 'get', 200>;
export type DefaultConfigUpdateRequest = ApiJsonBody<'/api/config/default', 'put'>;
export type DefaultConfigUpdateResponse = ApiJsonResponse<'/api/config/default', 'put', 200>;
export type DefaultConfigStructuredUpdateRequest = ApiJsonBody<'/api/config/default/structured', 'put'>;

export type LabType = NonNullable<Schemas['LabJobResponse']['lab_type']>;
export type LabGenerateRequest = ApiJsonBody<'/api/lab/generate', 'post'>;
export type LabGenerateResponse = ApiJsonResponse<'/api/lab/generate', 'post', 200>;
export type LabEvolveRequest = ApiJsonBody<'/api/lab/evolve', 'post'>;
export type LabEvolveResponse = ApiJsonResponse<'/api/lab/evolve', 'post', 200>;
export type LabOptimizeRequest = ApiJsonBody<'/api/lab/optimize', 'post'>;
export type LabOptimizeResponse = ApiJsonResponse<'/api/lab/optimize', 'post', 200>;
export type LabImproveRequest = ApiJsonBody<'/api/lab/improve', 'post'>;
export type LabImproveResponse = ApiJsonResponse<'/api/lab/improve', 'post', 200>;
export type LabSignalCategory = NonNullable<LabGenerateRequest['allowed_categories']>[number];
export type LabTargetScope = NonNullable<LabEvolveRequest['target_scope']>;
export type LabOptimizeRecommendationResponse = ApiJsonResponse<'/api/lab/optimize/recommendation', 'get', 200>;
export type LabOptimizeRecommendationQuery = ApiQuery<'/api/lab/optimize/recommendation', 'get'>;
export type LabOptimizeTrialRecommendationResponse = LabOptimizeRecommendationResponse;
export type GenerateResultItem = Schemas['GenerateResultItem'];
export type EvolutionHistoryItem = Schemas['EvolutionHistoryItem'];
export type OptimizeTrialItem = Schemas['OptimizeTrialItem'];
export type ImprovementItem = Schemas['ImprovementItem'];
export type LabGenerateResult = Schemas['LabGenerateResult'];
export type LabEvolveResult = Schemas['LabEvolveResult'];
export type LabOptimizeResult = Schemas['LabOptimizeResult'];
export type LabImproveResult = Schemas['LabImproveResult'];
export type LabResultData = NonNullable<Schemas['LabJobResponse']['result_data']>;
export type LabJobsQuery = ApiQuery<'/api/lab/jobs', 'get'>;
export type LabJobsResponse = ApiJsonResponse<'/api/lab/jobs', 'get', 200>;
export type LabJobStatusPathParams = ApiPathParams<'/api/lab/jobs/{job_id}', 'get'>;
export type LabJobStatusResponse = ApiJsonResponse<'/api/lab/jobs/{job_id}', 'get', 200>;
export type LabJobCancelPathParams = ApiPathParams<'/api/lab/jobs/{job_id}/cancel', 'post'>;
export type LabJobCancelResponse = ApiJsonResponse<'/api/lab/jobs/{job_id}/cancel', 'post', 200>;
/** @deprecated Use the operation-specific Lab response alias. */
export type LabJobResponse = LabJobStatusResponse;

/** Runtime-only shape for legacy SSE consumers; this is not an HTTP wire DTO. */
export interface LabSSEEvent {
  job_id: string;
  status: JobStatus;
  progress?: number;
  message?: string;
  data?: Record<string, unknown>;
}

export type FundamentalsComputeRequest = ApiJsonBody<'/api/fundamentals/compute', 'post'>;
export type FundamentalDataPoint = Schemas['FundamentalDataPoint'];
export type DailyValuationDataPoint = Schemas['DailyValuationDataPoint'];
export type FundamentalsComputeResponse = ApiJsonResponse<'/api/fundamentals/compute', 'post', 200>;
export type FundamentalsPeriodType = NonNullable<FundamentalsComputeRequest['period_type']>;
export type OHLCVResampleRequest = ApiJsonBody<'/api/ohlcv/resample', 'post'>;
export type OHLCVRecord = Schemas['OHLCVRecord'];
export type OHLCVResampleResponse = ApiJsonResponse<'/api/ohlcv/resample', 'post', 200>;
export type Timeframe = NonNullable<OHLCVResampleRequest['timeframe']>;
export type RelativeOHLCOptions = Schemas['RelativeOHLCOptions'];
export type HandleZeroDivision = NonNullable<RelativeOHLCOptions['handle_zero_division']>;
