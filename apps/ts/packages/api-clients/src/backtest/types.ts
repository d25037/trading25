/** Backtest API client types bound to the generated FastAPI schemas. */

import type { ApiJsonBody, ApiJsonResponse, JobStatus } from '@trading25/contracts';
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
export type BacktestJobResponse = ApiJsonResponse<'/api/backtest/run', 'post', 200>;
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
export type SignalAttributionJobResponse = ApiJsonResponse<'/api/backtest/attribution/run', 'post', 200>;
export type SignalAttributionResultResponse = ApiJsonResponse<'/api/backtest/attribution/result/{job_id}', 'get', 200>;
export type AttributionArtifactInfo = Schemas['AttributionArtifactInfo'];
export type AttributionArtifactListResponse = ApiJsonResponse<'/api/backtest/attribution-files', 'get', 200>;
export type AttributionArtifactContentResponse = ApiJsonResponse<'/api/backtest/attribution-files/content', 'get', 200>;

export type EntryDecidability = Schemas['StrategyMetadataResponse']['entry_decidability'];
export type ScreeningSupport = Schemas['StrategyMetadataResponse']['screening_support'];
export type StrategyMetadata = Schemas['StrategyMetadataResponse'];
export type StrategyListResponse = ApiJsonResponse<'/api/strategies', 'get', 200>;
export type StrategyDetailResponse = ApiJsonResponse<'/api/strategies/{strategy_name}', 'get', 200>;
export type StrategyValidationRequest = ApiJsonBody<'/api/strategies/{strategy_name}/validate', 'post'>;
export type CompiledAvailabilityPoint = Schemas['CompiledSignalAvailability']['observation_time'];
export type CompiledExecutionSession = Schemas['CompiledSignalAvailability']['execution_session'];
export type CompiledSignalScope = Schemas['CompiledSignalIR']['scope'];
export type CompiledSignalAvailability = Schemas['CompiledSignalAvailability'];
export type CompiledSignalIR = Schemas['CompiledSignalIR'];
export type CompiledStrategyIR = Schemas['CompiledStrategyIR'];
export type StrategyValidationResponse = ApiJsonResponse<'/api/strategies/{strategy_name}/validate', 'post', 200>;
export type HealthResponse = ApiJsonResponse<'/api/health', 'get', 200>;
export type StrategyUpdateRequest = ApiJsonBody<'/api/strategies/{strategy_name}', 'put'>;
export type StrategyUpdateResponse = ApiJsonResponse<'/api/strategies/{strategy_name}', 'put', 200>;
export type StrategyDeleteResponse = ApiJsonResponse<'/api/strategies/{strategy_name}', 'delete', 200>;
export type StrategyDuplicateRequest = ApiJsonBody<'/api/strategies/{strategy_name}/duplicate', 'post'>;
export type StrategyDuplicateResponse = ApiJsonResponse<'/api/strategies/{strategy_name}/duplicate', 'post', 200>;
export type StrategyRenameRequest = ApiJsonBody<'/api/strategies/{strategy_name}/rename', 'post'>;
export type StrategyRenameResponse = ApiJsonResponse<'/api/strategies/{strategy_name}/rename', 'post', 200>;
export type StrategyMoveRequest = ApiJsonBody<'/api/strategies/{strategy_name}/move', 'post'>;
export type StrategyMoveTargetCategory = StrategyMoveRequest['target_category'];
export type StrategyMoveResponse = ApiJsonResponse<'/api/strategies/{strategy_name}/move', 'post', 200>;

export type HtmlFileInfo = Schemas['HtmlFileInfo'];
export type HtmlFileListResponse = ApiJsonResponse<'/api/backtest/html-files', 'get', 200>;
export type HtmlFileMetrics = Schemas['HtmlFileMetrics'];
export type HtmlFileContentResponse = ApiJsonResponse<'/api/backtest/html-files/{strategy}/{filename}', 'get', 200>;
export type HtmlFileRenameRequest = ApiJsonBody<'/api/backtest/html-files/{strategy}/{filename}/rename', 'post'>;
export type HtmlFileRenameResponse = ApiJsonResponse<
  '/api/backtest/html-files/{strategy}/{filename}/rename',
  'post',
  200
>;
export type HtmlFileDeleteResponse = ApiJsonResponse<'/api/backtest/html-files/{strategy}/{filename}', 'delete', 200>;
export type OptimizationRequest = ApiJsonBody<'/api/optimize/run', 'post'>;
export type OptimizationJobResponse = ApiJsonResponse<'/api/optimize/run', 'post', 200>;
export type OptimizationDiagnosticResponse = Schemas['OptimizationDiagnosticResponse'];
export type StrategyOptimizationStateResponse = ApiJsonResponse<
  '/api/strategies/{strategy_name}/optimization',
  'get',
  200
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
export type OptimizationHtmlFileInfo = Schemas['OptimizationHtmlFileInfo'];
export type OptimizationHtmlFileListResponse = ApiJsonResponse<'/api/optimize/html-files', 'get', 200>;
export type OptimizationHtmlFileContentResponse = ApiJsonResponse<
  '/api/optimize/html-files/{strategy}/{filename}',
  'get',
  200
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
export type DefaultConfigEditorContextResponse = ApiJsonResponse<'/api/config/default/editor-context', 'get', 200>;
export type DefaultConfigResponse = ApiJsonResponse<'/api/config/default', 'get', 200>;
export type DefaultConfigUpdateRequest = ApiJsonBody<'/api/config/default', 'put'>;
export type DefaultConfigUpdateResponse = ApiJsonResponse<'/api/config/default', 'put', 200>;
export type DefaultConfigStructuredUpdateRequest = ApiJsonBody<'/api/config/default/structured', 'put'>;

export type LabType = NonNullable<Schemas['LabJobResponse']['lab_type']>;
export type LabGenerateRequest = Schemas['LabGenerateRequest'];
export type LabEvolveRequest = Schemas['LabEvolveRequest'];
export type LabOptimizeRequest = Schemas['LabOptimizeRequest'];
export type LabImproveRequest = Schemas['LabImproveRequest'];
export type LabSignalCategory = NonNullable<LabGenerateRequest['allowed_categories']>[number];
export type LabTargetScope = NonNullable<LabEvolveRequest['target_scope']>;
export type LabOptimizeTrialRecommendationResponse = Schemas['LabOptimizeRecommendationResponse'];
export type GenerateResultItem = Schemas['GenerateResultItem'];
export type EvolutionHistoryItem = Schemas['EvolutionHistoryItem'];
export type OptimizeTrialItem = Schemas['OptimizeTrialItem'];
export type ImprovementItem = Schemas['ImprovementItem'];
export type LabGenerateResult = Schemas['LabGenerateResult'];
export type LabEvolveResult = Schemas['LabEvolveResult'];
export type LabOptimizeResult = Schemas['LabOptimizeResult'];
export type LabImproveResult = Schemas['LabImproveResult'];
export type LabResultData = NonNullable<Schemas['LabJobResponse']['result_data']>;
export type LabJobResponse = Schemas['LabJobResponse'];

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
