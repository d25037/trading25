/**
 * Sync-related types for frontend
 * Re-exports from @trading25/contracts
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
  SyncFetchDetail,
  SyncFetchDetailsResponse,
  SyncDataBackend,
  SyncDataPlaneOptions,
  SyncJobResponse,
  SyncJobResult,
  SyncMode,
} from '@trading25/contracts/types/api-response-types';

export interface RefreshStocksRequest {
  codes: string[];
}
