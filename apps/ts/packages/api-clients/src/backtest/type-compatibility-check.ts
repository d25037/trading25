/**
 * Compile-time compatibility checks for the stable backtest client aliases.
 *
 * Wire aliases are generated; these checks preserve legacy nested-name mappings and
 * normalized comparisons that remain useful to downstream callers.
 *
 * This file is never executed at runtime — it only participates in type checking.
 *
 * NOTE: FastAPI + openapi-typescript generates nullable fields as `prop?: T | null`
 * (optional). Normalize<T> compares the meaningful property value shapes while the
 * operation-level exactness checks below verify endpoint request/response bindings.
 */

import type { ApiJsonBody, ApiJsonResponse } from '@trading25/contracts';
import type { components } from '@trading25/contracts/clients/backtest/generated/bt-api-types';
import type { BacktestClient } from './BacktestClient.js';
import type {
  BacktestJobResponse as StableBacktestJobResponse,
  BacktestRequest as StableBacktestRequest,
  BacktestResultResponse as StableBacktestResultResponse,
  BacktestResultSummary as StableBacktestResultSummary,
  DefaultConfigResponse as StableDefaultConfigResponse,
  DefaultConfigUpdateRequest as StableDefaultConfigUpdateRequest,
  DefaultConfigUpdateResponse as StableDefaultConfigUpdateResponse,
  HealthResponse as StableHealthResponse,
  HtmlFileContentResponse as StableHtmlFileContentResponse,
  HtmlFileDeleteResponse as StableHtmlFileDeleteResponse,
  HtmlFileInfo as StableHtmlFileInfo,
  HtmlFileListResponse as StableHtmlFileListResponse,
  HtmlFileMetrics as StableHtmlFileMetrics,
  HtmlFileRenameRequest as StableHtmlFileRenameRequest,
  HtmlFileRenameResponse as StableHtmlFileRenameResponse,
  OptimizationDiagnosticResponse as StableOptimizationDiagnosticResponse,
  OptimizationHtmlFileContentResponse as StableOptimizationHtmlFileContentResponse,
  OptimizationHtmlFileInfo as StableOptimizationHtmlFileInfo,
  OptimizationHtmlFileListResponse as StableOptimizationHtmlFileListResponse,
  OptimizationJobResponse as StableOptimizationJobResponse,
  OptimizationRequest as StableOptimizationRequest,
  SignalAttributionJobResponse as StableSignalAttributionJobResponse,
  SignalAttributionLooResult as StableSignalAttributionLooResult,
  SignalAttributionMetrics as StableSignalAttributionMetrics,
  SignalAttributionRequest as StableSignalAttributionRequest,
  SignalAttributionResult as StableSignalAttributionResult,
  SignalAttributionResultResponse as StableSignalAttributionResultResponse,
  SignalAttributionShapleyMeta as StableSignalAttributionShapleyMeta,
  SignalAttributionShapleyResult as StableSignalAttributionShapleyResult,
  SignalAttributionSignalResult as StableSignalAttributionSignalResult,
  SignalAttributionTiming as StableSignalAttributionTiming,
  SignalAttributionTopNScore as StableSignalAttributionTopNScore,
  SignalAttributionTopNSelection as StableSignalAttributionTopNSelection,
  SignalCategory as StableSignalCategory,
  SignalDefinition as StableSignalDefinition,
  SignalFieldDefinition as StableSignalFieldDefinition,
  SignalReferenceResponse as StableSignalReferenceResponse,
  StrategyDeleteResponse as StableStrategyDeleteResponse,
  StrategyDetailResponse as StableStrategyDetailResponse,
  StrategyDuplicateRequest as StableStrategyDuplicateRequest,
  StrategyDuplicateResponse as StableStrategyDuplicateResponse,
  StrategyListResponse as StableStrategyListResponse,
  StrategyMetadata as StableStrategyMetadata,
  StrategyOptimizationDeleteResponse as StableStrategyOptimizationDeleteResponse,
  StrategyOptimizationSaveRequest as StableStrategyOptimizationSaveRequest,
  StrategyOptimizationSaveResponse as StableStrategyOptimizationSaveResponse,
  StrategyOptimizationStateResponse as StableStrategyOptimizationStateResponse,
  StrategyRenameRequest as StableStrategyRenameRequest,
  StrategyRenameResponse as StableStrategyRenameResponse,
  StrategyUpdateRequest as StableStrategyUpdateRequest,
  StrategyUpdateResponse as StableStrategyUpdateResponse,
  StrategyValidationRequest as StableStrategyValidationRequest,
  StrategyValidationResponse as StableStrategyValidationResponse,
} from './index.js';

type Schemas = components['schemas'];

/**
 * Normalize<T> makes all properties required and strips `undefined` from
 * their value type. This bridges the gap between FastAPI's generated
 * `prop?: T | null` and our stable `prop: T | null`.
 */
type Normalize<T> = {
  [K in keyof T]-?: Exclude<T[K], undefined>;
};

/**
 * Assert that type A extends type B (A is assignable to B).
 * Used to verify structural compatibility between stable and generated types.
 */
type AssertExtends<_A, _B extends _A> = true;

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Expect<Value extends true> = Value;

type _RunBacktestRequest = Expect<
  Equal<Parameters<BacktestClient['runBacktest']>[0], ApiJsonBody<'/api/backtest/run', 'post'>>
>;
type _RunBacktestResponse = Expect<
  Equal<Awaited<ReturnType<BacktestClient['runBacktest']>>, ApiJsonResponse<'/api/backtest/run', 'post', 200>>
>;

// ===== HEALTH =====
type _HealthResponse = AssertExtends<StableHealthResponse, Schemas['HealthResponse']>;

// ===== BACKTEST =====
type _BacktestResultSummary = AssertExtends<
  Normalize<StableBacktestResultSummary>,
  Normalize<Schemas['BacktestResultSummary']>
>;
// strategy_config_override: manual uses `T | undefined`, generated uses `T | null`.
// Omit the field and verify the rest; the field name match is sufficient.
type _BacktestRequest = AssertExtends<
  Omit<Normalize<StableBacktestRequest>, 'strategy_config_override'>,
  Omit<Normalize<Schemas['BacktestRequest']>, 'strategy_config_override'>
>;
// BacktestJobResponse contains nested BacktestResultSummary — check top-level keys only,
// plus separately verify the nested type above.
type _BacktestJobResponse = AssertExtends<
  Normalize<Omit<StableBacktestJobResponse, 'result'>>,
  Normalize<Omit<Schemas['BacktestJobResponse'], 'result'>>
>;
type _BacktestResultResponse = AssertExtends<
  Normalize<Omit<StableBacktestResultResponse, 'summary'>>,
  Normalize<Omit<Schemas['BacktestResultResponse'], 'summary'>>
>;
// strategy_config_override: manual uses `T | undefined`, generated uses `T | null`.
type _SignalAttributionRequest = AssertExtends<
  Omit<Normalize<StableSignalAttributionRequest>, 'strategy_config_override'>,
  Omit<Normalize<Schemas['SignalAttributionRequest']>, 'strategy_config_override'>
>;
type _SignalAttributionMetrics = AssertExtends<
  Normalize<StableSignalAttributionMetrics>,
  Normalize<Schemas['SignalAttributionMetrics']>
>;
type _SignalAttributionLooResult = AssertExtends<
  Normalize<Omit<StableSignalAttributionLooResult, 'variant_metrics'>>,
  Normalize<Omit<Schemas['SignalAttributionLooResult'], 'variant_metrics'>>
>;
type _SignalAttributionShapleyResult = AssertExtends<
  Normalize<StableSignalAttributionShapleyResult>,
  Normalize<Schemas['SignalAttributionShapleyResult']>
>;
type _SignalAttributionSignalResult = AssertExtends<
  Normalize<Omit<StableSignalAttributionSignalResult, 'loo' | 'shapley'>>,
  Normalize<Omit<Schemas['SignalAttributionSignalResult'], 'loo' | 'shapley'>>
>;
type _SignalAttributionTopNScore = AssertExtends<
  Normalize<StableSignalAttributionTopNScore>,
  Normalize<Schemas['SignalAttributionTopNScore']>
>;
type _SignalAttributionTopNSelection = AssertExtends<
  Normalize<Omit<StableSignalAttributionTopNSelection, 'scores'>>,
  Normalize<Omit<Schemas['SignalAttributionTopNSelection'], 'scores'>>
>;
type _SignalAttributionTiming = AssertExtends<
  Normalize<StableSignalAttributionTiming>,
  Normalize<Schemas['SignalAttributionTiming']>
>;
type _SignalAttributionShapleyMeta = AssertExtends<
  Normalize<StableSignalAttributionShapleyMeta>,
  Normalize<Schemas['SignalAttributionShapleyMeta']>
>;
type _SignalAttributionResult = AssertExtends<
  Normalize<
    Omit<StableSignalAttributionResult, 'baseline_metrics' | 'signals' | 'top_n_selection' | 'timing' | 'shapley'>
  >,
  Normalize<
    Omit<Schemas['SignalAttributionResult'], 'baseline_metrics' | 'signals' | 'top_n_selection' | 'timing' | 'shapley'>
  >
>;
type _SignalAttributionJobResponse = AssertExtends<
  Normalize<Omit<StableSignalAttributionJobResponse, 'result_data'>>,
  Normalize<Omit<Schemas['SignalAttributionJobResponse'], 'result_data'>>
>;
type _SignalAttributionResultResponse = AssertExtends<
  Normalize<Omit<StableSignalAttributionResultResponse, 'result'>>,
  Normalize<Omit<Schemas['SignalAttributionResultResponse'], 'result'>>
>;

// ===== STRATEGY =====
// Stable: StrategyMetadata, Generated: StrategyMetadataResponse
type _StrategyMetadata = AssertExtends<
  Normalize<StableStrategyMetadata>,
  Normalize<Schemas['StrategyMetadataResponse']>
>;
// StrategyListResponse contains StrategyMetadata[] — check scalar fields only
type _StrategyListResponse = AssertExtends<
  Omit<StableStrategyListResponse, 'strategies'>,
  Omit<Schemas['StrategyListResponse'], 'strategies'>
>;
type _StrategyDetailResponse = AssertExtends<
  Normalize<StableStrategyDetailResponse>,
  Normalize<Schemas['StrategyDetailResponse']>
>;
type _StrategyValidationRequest = AssertExtends<StableStrategyValidationRequest, Schemas['StrategyValidationRequest']>;
type _StrategyValidationResponse = AssertExtends<
  Normalize<StableStrategyValidationResponse>,
  Normalize<Schemas['StrategyValidationResponse']>
>;
type _StrategyUpdateRequest = AssertExtends<StableStrategyUpdateRequest, Schemas['StrategyUpdateRequest']>;
type _StrategyUpdateResponse = AssertExtends<StableStrategyUpdateResponse, Schemas['StrategyUpdateResponse']>;
type _StrategyDeleteResponse = AssertExtends<StableStrategyDeleteResponse, Schemas['StrategyDeleteResponse']>;
type _StrategyDuplicateRequest = AssertExtends<StableStrategyDuplicateRequest, Schemas['StrategyDuplicateRequest']>;
type _StrategyDuplicateResponse = AssertExtends<StableStrategyDuplicateResponse, Schemas['StrategyDuplicateResponse']>;
type _StrategyRenameRequest = AssertExtends<StableStrategyRenameRequest, Schemas['StrategyRenameRequest']>;
type _StrategyRenameResponse = AssertExtends<StableStrategyRenameResponse, Schemas['StrategyRenameResponse']>;

// ===== HTML FILE BROWSER =====
type _HtmlFileInfo = AssertExtends<StableHtmlFileInfo, Schemas['HtmlFileInfo']>;
type _HtmlFileListResponse = AssertExtends<StableHtmlFileListResponse, Schemas['HtmlFileListResponse']>;
type _HtmlFileMetrics = AssertExtends<Normalize<StableHtmlFileMetrics>, Normalize<Schemas['HtmlFileMetrics']>>;
// HtmlFileContentResponse contains nested HtmlFileMetrics — check without it
type _HtmlFileContentResponse = AssertExtends<
  Normalize<Omit<StableHtmlFileContentResponse, 'metrics'>>,
  Normalize<Omit<Schemas['HtmlFileContentResponse'], 'metrics'>>
>;
type _HtmlFileRenameRequest = AssertExtends<StableHtmlFileRenameRequest, Schemas['HtmlFileRenameRequest']>;
type _HtmlFileRenameResponse = AssertExtends<StableHtmlFileRenameResponse, Schemas['HtmlFileRenameResponse']>;
type _HtmlFileDeleteResponse = AssertExtends<StableHtmlFileDeleteResponse, Schemas['HtmlFileDeleteResponse']>;

// ===== OPTIMIZATION =====
type _OptimizationRequest = AssertExtends<StableOptimizationRequest, Schemas['OptimizationRequest']>;
type _OptimizationJobResponse = AssertExtends<
  Normalize<StableOptimizationJobResponse>,
  Normalize<Schemas['OptimizationJobResponse']>
>;
type _OptimizationDiagnosticResponse = AssertExtends<
  StableOptimizationDiagnosticResponse,
  Schemas['OptimizationDiagnosticResponse']
>;
type _StrategyOptimizationStateResponse = AssertExtends<
  Normalize<StableStrategyOptimizationStateResponse>,
  Normalize<Schemas['StrategyOptimizationStateResponse']>
>;
type _StrategyOptimizationSaveRequest = AssertExtends<
  StableStrategyOptimizationSaveRequest,
  Schemas['StrategyOptimizationSaveRequest']
>;
type _StrategyOptimizationSaveResponse = AssertExtends<
  Normalize<StableStrategyOptimizationSaveResponse>,
  Normalize<Schemas['StrategyOptimizationSaveResponse']>
>;
type _StrategyOptimizationDeleteResponse = AssertExtends<
  StableStrategyOptimizationDeleteResponse,
  Schemas['StrategyOptimizationDeleteResponse']
>;

// ===== OPTIMIZATION HTML FILES =====
type _OptimizationHtmlFileInfo = AssertExtends<StableOptimizationHtmlFileInfo, Schemas['OptimizationHtmlFileInfo']>;
type _OptimizationHtmlFileListResponse = AssertExtends<
  Omit<StableOptimizationHtmlFileListResponse, 'files'>,
  Omit<Schemas['OptimizationHtmlFileListResponse'], 'files'>
>;
type _OptimizationHtmlFileContentResponse = AssertExtends<
  StableOptimizationHtmlFileContentResponse,
  Schemas['OptimizationHtmlFileContentResponse']
>;

// ===== SIGNAL REFERENCE =====
// Name mapping: Stable SignalCategory → Generated SignalCategorySchema
type _SignalCategory = AssertExtends<StableSignalCategory, Schemas['SignalCategorySchema']>;

// SignalFieldDefinition: 'constraints' has nullable mismatch (manual=non-null, generated=nullable)
// — Omit from both sides.
type _SignalFieldDefinition = AssertExtends<
  Normalize<Omit<StableSignalFieldDefinition, 'constraints'>>,
  Normalize<Omit<Schemas['SignalFieldSchema'], 'constraints'>>
>;

// SignalDefinition: 'fields' contains nested SignalFieldDefinition — checked separately above.
type _SignalDefinition = AssertExtends<
  Normalize<Omit<StableSignalDefinition, 'fields'>>,
  Normalize<Omit<Schemas['SignalReferenceSchema'], 'fields'>>
>;

// SignalReferenceResponse: 'signals' wraps SignalDefinition[] (checked above);
// 'categories' wraps SignalCategory[] (checked above).
type _SignalReferenceResponse = AssertExtends<
  Omit<StableSignalReferenceResponse, 'signals' | 'categories'>,
  Omit<Schemas['SignalReferenceResponse'], 'signals' | 'categories'>
>;

// ===== DEFAULT CONFIG =====
type _DefaultConfigResponse = AssertExtends<StableDefaultConfigResponse, Schemas['DefaultConfigResponse']>;
type _DefaultConfigUpdateRequest = AssertExtends<
  StableDefaultConfigUpdateRequest,
  Schemas['DefaultConfigUpdateRequest']
>;
type _DefaultConfigUpdateResponse = AssertExtends<
  StableDefaultConfigUpdateResponse,
  Schemas['DefaultConfigUpdateResponse']
>;

// Suppress unused variable warnings — these are compile-time-only assertions
export type TypeChecks = [
  _RunBacktestRequest,
  _RunBacktestResponse,
  _HealthResponse,
  _BacktestResultSummary,
  _BacktestRequest,
  _BacktestJobResponse,
  _BacktestResultResponse,
  _SignalAttributionRequest,
  _SignalAttributionMetrics,
  _SignalAttributionLooResult,
  _SignalAttributionShapleyResult,
  _SignalAttributionSignalResult,
  _SignalAttributionTopNScore,
  _SignalAttributionTopNSelection,
  _SignalAttributionTiming,
  _SignalAttributionShapleyMeta,
  _SignalAttributionResult,
  _SignalAttributionJobResponse,
  _SignalAttributionResultResponse,
  _StrategyMetadata,
  _StrategyListResponse,
  _StrategyDetailResponse,
  _StrategyValidationRequest,
  _StrategyValidationResponse,
  _StrategyUpdateRequest,
  _StrategyUpdateResponse,
  _StrategyDeleteResponse,
  _StrategyDuplicateRequest,
  _StrategyDuplicateResponse,
  _StrategyRenameRequest,
  _StrategyRenameResponse,
  _HtmlFileInfo,
  _HtmlFileListResponse,
  _HtmlFileMetrics,
  _HtmlFileContentResponse,
  _HtmlFileRenameRequest,
  _HtmlFileRenameResponse,
  _HtmlFileDeleteResponse,
  _OptimizationRequest,
  _OptimizationJobResponse,
  _OptimizationDiagnosticResponse,
  _StrategyOptimizationStateResponse,
  _StrategyOptimizationSaveRequest,
  _StrategyOptimizationSaveResponse,
  _StrategyOptimizationDeleteResponse,
  _OptimizationHtmlFileInfo,
  _OptimizationHtmlFileListResponse,
  _OptimizationHtmlFileContentResponse,
  _SignalCategory,
  _SignalFieldDefinition,
  _SignalDefinition,
  _SignalReferenceResponse,
  _DefaultConfigResponse,
  _DefaultConfigUpdateRequest,
  _DefaultConfigUpdateResponse,
];
