import { BaseApiClient, toQueryString } from './base-client.js';
import type {
  CancelDatasetJobResponse,
  DatasetCreateJobResponse,
  DatasetInfoResponse,
  DatasetJobResponse,
  DatasetPreset,
  DatasetSampleResponse,
  DatasetSearchResponse,
} from './types.js';

export class DatasetClient extends BaseApiClient {
  /**
   * Start a dataset creation job
   */
  async startDatasetCreate(name: string, preset: DatasetPreset, overwrite = false): Promise<DatasetCreateJobResponse> {
    return this.request<DatasetCreateJobResponse>('/api/dataset', {
      method: 'POST',
      body: JSON.stringify({ name, preset, overwrite }),
    });
  }

  /**
   * Start a dataset resume job (fetch missing data for existing dataset)
   */
  async startDatasetResume(name: string, preset: DatasetPreset): Promise<DatasetCreateJobResponse> {
    return this.request<DatasetCreateJobResponse>('/api/dataset/resume', {
      method: 'POST',
      body: JSON.stringify({ name, preset }),
    });
  }

  /**
   * Get dataset job status
   */
  async getDatasetJobStatus(jobId: string): Promise<DatasetJobResponse> {
    return this.request<DatasetJobResponse>(`/api/dataset/jobs/${jobId}`);
  }

  /**
   * Cancel a dataset job
   */
  async cancelDatasetJob(jobId: string): Promise<CancelDatasetJobResponse> {
    return this.request<CancelDatasetJobResponse>(`/api/dataset/jobs/${jobId}`, {
      method: 'DELETE',
    });
  }

  /**
   * Get dataset information (includes validation)
   */
  async getDatasetInfo(name: string): Promise<DatasetInfoResponse> {
    return this.request<DatasetInfoResponse>(`/api/dataset/${encodeURIComponent(name)}/info`);
  }

  /**
   * Sample stocks from a dataset
   */
  async sampleDataset(
    name: string,
    params: {
      size?: number;
      byMarket?: boolean;
      bySector?: boolean;
      seed?: number;
    } = {}
  ): Promise<DatasetSampleResponse> {
    const query = toQueryString({
      size: params.size,
      byMarket: params.byMarket,
      bySector: params.bySector,
      seed: params.seed,
    });
    const url = `/api/dataset/${encodeURIComponent(name)}/sample${query ? `?${query}` : ''}`;
    return this.request<DatasetSampleResponse>(url);
  }

  /**
   * Search stocks in a dataset
   */
  async searchDataset(
    name: string,
    term: string,
    params: {
      limit?: number;
      exact?: boolean;
    } = {}
  ): Promise<DatasetSearchResponse> {
    const query = toQueryString({
      term,
      limit: params.limit,
      exact: params.exact,
    });
    const url = `/api/dataset/${encodeURIComponent(name)}/search?${query}`;
    return this.request<DatasetSearchResponse>(url);
  }
}
