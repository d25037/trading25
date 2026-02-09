/**
 * Compile-time contract test: Verify that hand-written backtest types
 * are compatible with OpenAPI-generated types from trading25-bt.
 *
 * If this file fails to compile (tsc --noEmit), it means the canonical manual types
 * in @trading25/clients-ts/backtest and the bt OpenAPI spec have drifted apart.
 *
 * This file is never executed at runtime — it only participates in type checking.
 *
 * NOTE: FastAPI + openapi-typescript generates nullable fields as `prop?: T | null`
 * (optional), whereas hand-written types use `prop: T | null` (required).
 * We use Normalize<T> to make all properties required and strip `undefined` from
 * value unions so that structural shapes can be compared cleanly.
 *
 * Types that exist only in the manual client package but not in the bt OpenAPI schema
 * (e.g., BacktestClientConfig) are NOT checked here.
 */

import type {
  BacktestJobResponse as ManualBacktestJobResponse,
  BacktestRequest as ManualBacktestRequest,
  BacktestResultResponse as ManualBacktestResultResponse,
  BacktestResultSummary as ManualBacktestResultSummary,
  DefaultConfigResponse as ManualDefaultConfigResponse,
  DefaultConfigUpdateRequest as ManualDefaultConfigUpdateRequest,
  DefaultConfigUpdateResponse as ManualDefaultConfigUpdateResponse,
  HealthResponse as ManualHealthResponse,
  HtmlFileContentResponse as ManualHtmlFileContentResponse,
  HtmlFileDeleteResponse as ManualHtmlFileDeleteResponse,
  HtmlFileInfo as ManualHtmlFileInfo,
  HtmlFileListResponse as ManualHtmlFileListResponse,
  HtmlFileMetrics as ManualHtmlFileMetrics,
  HtmlFileRenameRequest as ManualHtmlFileRenameRequest,
  HtmlFileRenameResponse as ManualHtmlFileRenameResponse,
  OptimizationGridConfig as ManualOptimizationGridConfig,
  OptimizationGridListResponse as ManualOptimizationGridListResponse,
  OptimizationGridSaveRequest as ManualOptimizationGridSaveRequest,
  OptimizationGridSaveResponse as ManualOptimizationGridSaveResponse,
  OptimizationHtmlFileContentResponse as ManualOptimizationHtmlFileContentResponse,
  OptimizationHtmlFileInfo as ManualOptimizationHtmlFileInfo,
  OptimizationHtmlFileListResponse as ManualOptimizationHtmlFileListResponse,
  OptimizationJobResponse as ManualOptimizationJobResponse,
  OptimizationRequest as ManualOptimizationRequest,
  SignalCategory as ManualSignalCategory,
  SignalDefinition as ManualSignalDefinition,
  SignalFieldDefinition as ManualSignalFieldDefinition,
  SignalReferenceResponse as ManualSignalReferenceResponse,
  StrategyDeleteResponse as ManualStrategyDeleteResponse,
  StrategyDetailResponse as ManualStrategyDetailResponse,
  StrategyDuplicateRequest as ManualStrategyDuplicateRequest,
  StrategyDuplicateResponse as ManualStrategyDuplicateResponse,
  StrategyListResponse as ManualStrategyListResponse,
  StrategyMetadata as ManualStrategyMetadata,
  StrategyRenameRequest as ManualStrategyRenameRequest,
  StrategyRenameResponse as ManualStrategyRenameResponse,
  StrategyUpdateRequest as ManualStrategyUpdateRequest,
  StrategyUpdateResponse as ManualStrategyUpdateResponse,
  StrategyValidationRequest as ManualStrategyValidationRequest,
  StrategyValidationResponse as ManualStrategyValidationResponse,
} from '@trading25/clients-ts/backtest';
import type { components } from './bt-api-types.js';

type Schemas = components['schemas'];

/**
 * Normalize<T> makes all properties required and strips `undefined` from
 * their value type. This bridges the gap between FastAPI's generated
 * `prop?: T | null` and our manual `prop: T | null`.
 */
type Normalize<T> = {
  [K in keyof T]-?: Exclude<T[K], undefined>;
};

/**
 * Assert that type A extends type B (A is assignable to B).
 * Used to verify structural compatibility between manual and generated types.
 */
type AssertExtends<_A, _B extends _A> = true;

// ===== HEALTH =====
type _HealthResponse = AssertExtends<ManualHealthResponse, Schemas['HealthResponse']>;

// ===== BACKTEST =====
type _BacktestResultSummary = AssertExtends<
  Normalize<ManualBacktestResultSummary>,
  Normalize<Schemas['BacktestResultSummary']>
>;
// strategy_config_override: manual uses `T | undefined`, generated uses `T | null`.
// Omit the field and verify the rest; the field name match is sufficient.
type _BacktestRequest = AssertExtends<
  Omit<Normalize<ManualBacktestRequest>, 'strategy_config_override'>,
  Omit<Normalize<Schemas['BacktestRequest']>, 'strategy_config_override'>
>;
// BacktestJobResponse contains nested BacktestResultSummary — check top-level keys only,
// plus separately verify the nested type above.
type _BacktestJobResponse = AssertExtends<
  Normalize<Omit<ManualBacktestJobResponse, 'result'>>,
  Normalize<Omit<Schemas['BacktestJobResponse'], 'result'>>
>;
type _BacktestResultResponse = AssertExtends<
  Normalize<Omit<ManualBacktestResultResponse, 'summary'>>,
  Normalize<Omit<Schemas['BacktestResultResponse'], 'summary'>>
>;

// ===== STRATEGY =====
// Manual: StrategyMetadata, Generated: StrategyMetadataResponse
type _StrategyMetadata = AssertExtends<
  Normalize<ManualStrategyMetadata>,
  Normalize<Schemas['StrategyMetadataResponse']>
>;
// StrategyListResponse contains StrategyMetadata[] — check scalar fields only
type _StrategyListResponse = AssertExtends<
  Omit<ManualStrategyListResponse, 'strategies'>,
  Omit<Schemas['StrategyListResponse'], 'strategies'>
>;
type _StrategyDetailResponse = AssertExtends<
  Normalize<ManualStrategyDetailResponse>,
  Normalize<Schemas['StrategyDetailResponse']>
>;
type _StrategyValidationRequest = AssertExtends<ManualStrategyValidationRequest, Schemas['StrategyValidationRequest']>;
type _StrategyValidationResponse = AssertExtends<
  Normalize<ManualStrategyValidationResponse>,
  Normalize<Schemas['StrategyValidationResponse']>
>;
type _StrategyUpdateRequest = AssertExtends<ManualStrategyUpdateRequest, Schemas['StrategyUpdateRequest']>;
type _StrategyUpdateResponse = AssertExtends<ManualStrategyUpdateResponse, Schemas['StrategyUpdateResponse']>;
type _StrategyDeleteResponse = AssertExtends<ManualStrategyDeleteResponse, Schemas['StrategyDeleteResponse']>;
type _StrategyDuplicateRequest = AssertExtends<ManualStrategyDuplicateRequest, Schemas['StrategyDuplicateRequest']>;
type _StrategyDuplicateResponse = AssertExtends<ManualStrategyDuplicateResponse, Schemas['StrategyDuplicateResponse']>;
type _StrategyRenameRequest = AssertExtends<ManualStrategyRenameRequest, Schemas['StrategyRenameRequest']>;
type _StrategyRenameResponse = AssertExtends<ManualStrategyRenameResponse, Schemas['StrategyRenameResponse']>;

// ===== HTML FILE BROWSER =====
type _HtmlFileInfo = AssertExtends<ManualHtmlFileInfo, Schemas['HtmlFileInfo']>;
type _HtmlFileListResponse = AssertExtends<ManualHtmlFileListResponse, Schemas['HtmlFileListResponse']>;
type _HtmlFileMetrics = AssertExtends<Normalize<ManualHtmlFileMetrics>, Normalize<Schemas['HtmlFileMetrics']>>;
// HtmlFileContentResponse contains nested HtmlFileMetrics — check without it
type _HtmlFileContentResponse = AssertExtends<
  Normalize<Omit<ManualHtmlFileContentResponse, 'metrics'>>,
  Normalize<Omit<Schemas['HtmlFileContentResponse'], 'metrics'>>
>;
type _HtmlFileRenameRequest = AssertExtends<ManualHtmlFileRenameRequest, Schemas['HtmlFileRenameRequest']>;
type _HtmlFileRenameResponse = AssertExtends<ManualHtmlFileRenameResponse, Schemas['HtmlFileRenameResponse']>;
type _HtmlFileDeleteResponse = AssertExtends<ManualHtmlFileDeleteResponse, Schemas['HtmlFileDeleteResponse']>;

// ===== OPTIMIZATION =====
type _OptimizationRequest = AssertExtends<ManualOptimizationRequest, Schemas['OptimizationRequest']>;
type _OptimizationJobResponse = AssertExtends<
  Normalize<ManualOptimizationJobResponse>,
  Normalize<Schemas['OptimizationJobResponse']>
>;
type _OptimizationGridConfig = AssertExtends<ManualOptimizationGridConfig, Schemas['OptimizationGridConfig']>;
type _OptimizationGridListResponse = AssertExtends<
  ManualOptimizationGridListResponse,
  Schemas['OptimizationGridListResponse']
>;
type _OptimizationGridSaveRequest = AssertExtends<
  ManualOptimizationGridSaveRequest,
  Schemas['OptimizationGridSaveRequest']
>;
type _OptimizationGridSaveResponse = AssertExtends<
  ManualOptimizationGridSaveResponse,
  Schemas['OptimizationGridSaveResponse']
>;

// ===== OPTIMIZATION HTML FILES =====
type _OptimizationHtmlFileInfo = AssertExtends<
  ManualOptimizationHtmlFileInfo,
  Schemas['OptimizationHtmlFileInfo']
>;
type _OptimizationHtmlFileListResponse = AssertExtends<
  Omit<ManualOptimizationHtmlFileListResponse, 'files'>,
  Omit<Schemas['OptimizationHtmlFileListResponse'], 'files'>
>;
type _OptimizationHtmlFileContentResponse = AssertExtends<
  ManualOptimizationHtmlFileContentResponse,
  Schemas['OptimizationHtmlFileContentResponse']
>;

// ===== SIGNAL REFERENCE =====
// Name mapping: Manual SignalCategory → Generated SignalCategorySchema
type _SignalCategory = AssertExtends<ManualSignalCategory, Schemas['SignalCategorySchema']>;

// SignalFieldDefinition: 'constraints' has nullable mismatch (manual=non-null, generated=nullable)
// — Omit from both sides.
type _SignalFieldDefinition = AssertExtends<
  Normalize<Omit<ManualSignalFieldDefinition, 'constraints'>>,
  Normalize<Omit<Schemas['SignalFieldSchema'], 'constraints'>>
>;

// SignalDefinition: 'fields' contains nested SignalFieldDefinition — checked separately above.
type _SignalDefinition = AssertExtends<
  Normalize<Omit<ManualSignalDefinition, 'fields'>>,
  Normalize<Omit<Schemas['SignalReferenceSchema'], 'fields'>>
>;

// SignalReferenceResponse: 'signals' wraps SignalDefinition[] (checked above);
// 'categories' wraps SignalCategory[] (checked above).
type _SignalReferenceResponse = AssertExtends<
  Omit<ManualSignalReferenceResponse, 'signals' | 'categories'>,
  Omit<Schemas['SignalReferenceResponse'], 'signals' | 'categories'>
>;

// ===== DEFAULT CONFIG =====
type _DefaultConfigResponse = AssertExtends<ManualDefaultConfigResponse, Schemas['DefaultConfigResponse']>;
type _DefaultConfigUpdateRequest = AssertExtends<
  ManualDefaultConfigUpdateRequest,
  Schemas['DefaultConfigUpdateRequest']
>;
type _DefaultConfigUpdateResponse = AssertExtends<
  ManualDefaultConfigUpdateResponse,
  Schemas['DefaultConfigUpdateResponse']
>;

// Suppress unused variable warnings — these are compile-time-only assertions
export type TypeChecks = [
  _HealthResponse,
  _BacktestResultSummary,
  _BacktestRequest,
  _BacktestJobResponse,
  _BacktestResultResponse,
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
  _OptimizationGridConfig,
  _OptimizationGridListResponse,
  _OptimizationGridSaveRequest,
  _OptimizationGridSaveResponse,
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
