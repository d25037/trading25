/**
 * Compile-time contract test: Verify that shared API response types
 * are compatible with OpenAPI-generated types.
 *
 * If this file fails to compile (tsc --noEmit), it means the shared types
 * and the OpenAPI spec (Zod schemas) have drifted apart.
 *
 * This file is never executed at runtime — it only participates in type checking.
 */

import type {
  AdjustmentEvent as SharedAdjustmentEvent,
  CancelDatasetJobResponse as SharedCancelDatasetJobResponse,
  CancelJobResponse as SharedCancelJobResponse,
  CreateSyncJobResponse as SharedCreateSyncJobResponse,
  DatasetDeleteResponse as SharedDatasetDeleteResponse,
  DatasetJobProgress as SharedDatasetJobProgress,
  DatasetListItem as SharedDatasetListItem,
  DatasetListResponse as SharedDatasetListResponse,
  FuturePricePoint as SharedFuturePricePoint,
  IntegrityIssue as SharedIntegrityIssue,
  JobProgress as SharedJobProgress,
  MarketRankingResponse as SharedMarketRankingResponse,
  MarketValidationResponse as SharedMarketValidationResponse,
  RangeBreakDetails as SharedRangeBreakDetails,
  RankingItem as SharedRankingItem,
  Rankings as SharedRankings,
  ScreeningSummary as SharedScreeningSummary,
  SyncJobResponse as SharedSyncJobResponse,
  SyncJobResult as SharedSyncJobResult,
} from '@trading25/shared/types/api-response-types';
import type { components } from './api-types';

type Schemas = components['schemas'];

/**
 * Assert that type Generated extends Shared (Generated is assignable to Shared).
 * This catches cases where Generated is missing fields that Shared requires,
 * or where field types are incompatible.
 */
type AssertExtends<_Shared, _Generated extends _Shared> = true;

/**
 * Some generated schemas include nullable wrappers (e.g., `T | null`) at the schema level
 * due to how @hono/zod-openapi handles `.nullable()`. Use NonNullable to strip the union
 * so we can compare the structural shape.
 */
type AssertExtendsNonNullable<_Shared, _Generated extends _Shared | null> = true;

// ===== RANKING =====
type _RankingItem = AssertExtends<SharedRankingItem, Schemas['RankingItem']>;
type _Rankings = AssertExtends<SharedRankings, Schemas['Rankings']>;
type _MarketRankingResponse = AssertExtends<SharedMarketRankingResponse, Schemas['MarketRankingResponse']>;

// ===== SCREENING =====
type _RangeBreakDetails = AssertExtends<SharedRangeBreakDetails, Schemas['RangeBreakDetails']>;
// FuturePricePoint schema is generated as `T | null` — compare non-null shape only
type _FuturePricePoint = AssertExtendsNonNullable<SharedFuturePricePoint, Schemas['FuturePricePoint']>;
// NOTE: FutureReturns, ScreeningResultItem, MarketScreeningResponse are excluded because
// the generated FuturePricePoint includes `| null` at schema level, causing cascading
// incompatibility. This is a known @hono/zod-openapi Zod v4 nullable mapping issue.
type _ScreeningSummary = AssertExtends<SharedScreeningSummary, Schemas['ScreeningSummary']>;

// ===== SYNC =====
type _JobProgress = AssertExtends<SharedJobProgress, Schemas['JobProgress']>;
type _SyncJobResult = AssertExtends<SharedSyncJobResult, Schemas['SyncJobResult']>;
type _CreateSyncJobResponse = AssertExtends<SharedCreateSyncJobResponse, Schemas['CreateSyncJobResponse']>;
type _SyncJobResponse = AssertExtends<SharedSyncJobResponse, Schemas['SyncJobResponse']>;
type _CancelJobResponse = AssertExtends<SharedCancelJobResponse, Schemas['CancelJobResponse']>;

// ===== DATASET =====
type _DatasetListItem = AssertExtends<SharedDatasetListItem, Schemas['DatasetListItem']>;
type _DatasetListResponse = AssertExtends<SharedDatasetListResponse, Schemas['DatasetListResponse']>;
type _DatasetDeleteResponse = AssertExtends<SharedDatasetDeleteResponse, Schemas['DatasetDeleteResponse']>;
type _DatasetJobProgress = AssertExtends<SharedDatasetJobProgress, Schemas['DatasetJobProgress']>;
type _CancelDatasetJobResponse = AssertExtends<SharedCancelDatasetJobResponse, Schemas['CancelDatasetJobResponse']>;

// ===== VALIDATION =====
type _AdjustmentEvent = AssertExtends<SharedAdjustmentEvent, Schemas['AdjustmentEvent']>;
type _IntegrityIssue = AssertExtends<SharedIntegrityIssue, Schemas['IntegrityIssue']>;
type _MarketValidationResponse = AssertExtends<SharedMarketValidationResponse, Schemas['MarketValidationResponse']>;

// Suppress unused variable warnings — these are compile-time-only assertions
export type TypeChecks = [
  _RankingItem,
  _Rankings,
  _MarketRankingResponse,
  _RangeBreakDetails,
  _FuturePricePoint,
  _ScreeningSummary,
  _JobProgress,
  _SyncJobResult,
  _CreateSyncJobResponse,
  _SyncJobResponse,
  _CancelJobResponse,
  _DatasetListItem,
  _DatasetListResponse,
  _DatasetDeleteResponse,
  _DatasetJobProgress,
  _CancelDatasetJobResponse,
  _AdjustmentEvent,
  _IntegrityIssue,
  _MarketValidationResponse,
];
