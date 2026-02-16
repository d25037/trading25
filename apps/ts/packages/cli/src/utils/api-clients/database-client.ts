import { BaseApiClient } from './base-client.js';
import type {
  CancelJobResponse,
  CreateSyncJobResponse,
  MarketRefreshResponse,
  MarketStatsResponse,
  MarketValidationResponse,
  SyncJobResponse,
  SyncMode,
} from './types.js';

export class DatabaseClient extends BaseApiClient {
  /**
   * Validate market database
   */
  async validateMarketDatabase(): Promise<MarketValidationResponse> {
    return this.request<MarketValidationResponse>('/api/db/validate');
  }

  /**
   * Get market database statistics
   */
  async getMarketStats(): Promise<MarketStatsResponse> {
    return this.request<MarketStatsResponse>('/api/db/stats');
  }

  /**
   * Refresh historical data for specific stocks
   */
  async refreshStocks(codes: string[]): Promise<MarketRefreshResponse> {
    return this.request<MarketRefreshResponse>('/api/db/stocks/refresh', {
      method: 'POST',
      body: JSON.stringify({ codes }),
    });
  }

  /**
   * Start a database sync job
   */
  async startSync(mode: SyncMode = 'auto'): Promise<CreateSyncJobResponse> {
    return this.request<CreateSyncJobResponse>('/api/db/sync', {
      method: 'POST',
      body: JSON.stringify({ mode }),
    });
  }

  /**
   * Get sync job status
   */
  async getSyncJobStatus(jobId: string): Promise<SyncJobResponse> {
    return this.request<SyncJobResponse>(`/api/db/sync/jobs/${jobId}`);
  }

  /**
   * Cancel a sync job
   */
  async cancelSyncJob(jobId: string): Promise<CancelJobResponse> {
    return this.request<CancelJobResponse>(`/api/db/sync/jobs/${jobId}`, {
      method: 'DELETE',
    });
  }
}
