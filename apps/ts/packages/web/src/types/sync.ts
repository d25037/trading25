/**
 * Sync-related types for frontend
 * Re-exports from @trading25/shared
 */

export type {
  CancelJobResponse,
  CreateSyncJobResponse,
  JobProgress,
  MarketRefreshResponse,
  JobStatus,
  MarketStatsResponse,
  MarketValidationResponse,
  RefreshStockResult,
  StartSyncRequest,
  SyncDataBackend,
  SyncDataPlaneOptions,
  SyncJobResponse,
  SyncJobResult,
  SyncMode,
} from '@trading25/shared/types/api-response-types';

export interface RefreshStocksRequest {
  codes: string[];
}
