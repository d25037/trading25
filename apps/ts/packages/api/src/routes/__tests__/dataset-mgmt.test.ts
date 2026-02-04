import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockListDatasets = mock();
const mockDeleteDataset = mock();
const mockStartCreateJob = mock();
const mockStartResumeJob = mock();
const mockGetJobStatus = mock();
const mockCancelJob = mock();
const mockGetDatasetInfo = mock();
const mockSampleDataset = mock();
const mockSearchDataset = mock();

mock.module('../../services/dataset/dataset-service', () => ({
  datasetService: {
    listDatasets: mockListDatasets,
    deleteDataset: mockDeleteDataset,
    startCreateJob: mockStartCreateJob,
    startResumeJob: mockStartResumeJob,
    getJobStatus: mockGetJobStatus,
    cancelJob: mockCancelJob,
    getDatasetInfo: mockGetDatasetInfo,
    sampleDataset: mockSampleDataset,
    searchDataset: mockSearchDataset,
  },
}));

let datasetApp: typeof import('../dataset/index').default;

describe('Dataset Management Routes', () => {
  beforeEach(async () => {
    mockListDatasets.mockReset();
    mockDeleteDataset.mockReset();
    mockStartCreateJob.mockReset();
    mockStartResumeJob.mockReset();
    mockGetJobStatus.mockReset();
    mockCancelJob.mockReset();
    mockGetDatasetInfo.mockReset();
    mockSampleDataset.mockReset();
    mockSearchDataset.mockReset();
    datasetApp = (await import('../dataset/index')).default;
  });

  describe('GET /api/dataset', () => {
    it('returns dataset list', async () => {
      mockListDatasets.mockReturnValue({
        datasets: [{ name: 'prime.db', size: 1024000, stockCount: 100, createdAt: '2024-01-01' }],
      });

      const res = await datasetApp.request('/api/dataset');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { datasets: unknown[] };
      expect(body.datasets).toHaveLength(1);
    });

    it('returns 500 on error', async () => {
      mockListDatasets.mockImplementation(() => {
        throw new Error('disk error');
      });

      const res = await datasetApp.request('/api/dataset');

      expect(res.status).toBe(500);
    });
  });

  describe('DELETE /api/dataset/{name}', () => {
    it('deletes a dataset', async () => {
      mockDeleteDataset.mockReturnValue({ success: true, message: 'Deleted' });

      const res = await datasetApp.request('/api/dataset/prime.db', { method: 'DELETE' });

      expect(res.status).toBe(200);
    });

    it('returns 404 when not found', async () => {
      mockDeleteDataset.mockReturnValue({ success: false, message: 'Not found' });

      const res = await datasetApp.request('/api/dataset/missing.db', { method: 'DELETE' });

      expect(res.status).toBe(404);
    });

    it('returns 500 on error', async () => {
      mockDeleteDataset.mockImplementation(() => {
        throw new Error('permission denied');
      });

      const res = await datasetApp.request('/api/dataset/prime.db', { method: 'DELETE' });

      expect(res.status).toBe(500);
    });
  });

  describe('POST /api/dataset', () => {
    it('starts creation job', async () => {
      mockStartCreateJob.mockReturnValue({
        jobId: '123e4567-e89b-12d3-a456-426614174000',
        status: 'running',
        message: 'Job started',
      });

      const res = await datasetApp.request('/api/dataset', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'prime.db', preset: 'primeMarket' }),
      });

      expect(res.status).toBe(202);
    });

    it('returns 409 when job already running', async () => {
      mockStartCreateJob.mockReturnValue(null);

      const res = await datasetApp.request('/api/dataset', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'prime.db', preset: 'primeMarket' }),
      });

      expect(res.status).toBe(409);
    });

    it('returns 500 on error', async () => {
      mockStartCreateJob.mockImplementation(() => {
        throw new Error('unexpected');
      });

      const res = await datasetApp.request('/api/dataset', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'prime.db', preset: 'primeMarket' }),
      });

      expect(res.status).toBe(500);
    });
  });

  describe('POST /api/dataset/resume', () => {
    it('starts resume job', async () => {
      mockStartResumeJob.mockReturnValue({
        jobId: '123e4567-e89b-12d3-a456-426614174000',
        status: 'running',
        message: 'Resume started',
      });

      const res = await datasetApp.request('/api/dataset/resume', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'prime.db', preset: 'primeMarket' }),
      });

      expect(res.status).toBe(202);
    });

    it('returns 409 when job already running', async () => {
      mockStartResumeJob.mockReturnValue(null);

      const res = await datasetApp.request('/api/dataset/resume', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'prime.db', preset: 'primeMarket' }),
      });

      expect(res.status).toBe(409);
    });

    it('returns 500 on error', async () => {
      mockStartResumeJob.mockImplementation(() => {
        throw new Error('unexpected');
      });

      const res = await datasetApp.request('/api/dataset/resume', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'prime.db', preset: 'primeMarket' }),
      });

      expect(res.status).toBe(500);
    });
  });

  describe('GET /api/dataset/jobs/{jobId}', () => {
    it('returns job status', async () => {
      mockGetJobStatus.mockReturnValue({
        jobId: '123e4567-e89b-12d3-a456-426614174000',
        status: 'running',
        progress: { current: 50, total: 100 },
      });

      const res = await datasetApp.request('/api/dataset/jobs/123e4567-e89b-12d3-a456-426614174000');

      expect(res.status).toBe(200);
    });

    it('returns 404 when job not found', async () => {
      mockGetJobStatus.mockReturnValue(null);

      const res = await datasetApp.request('/api/dataset/jobs/123e4567-e89b-12d3-a456-426614174000');

      expect(res.status).toBe(404);
    });
  });

  describe('DELETE /api/dataset/jobs/{jobId}', () => {
    it('cancels a job', async () => {
      mockCancelJob.mockReturnValue({ success: true, message: 'Job cancelled' });

      const res = await datasetApp.request('/api/dataset/jobs/123e4567-e89b-12d3-a456-426614174000', {
        method: 'DELETE',
      });

      expect(res.status).toBe(200);
    });

    it('returns 404 when job not found', async () => {
      mockCancelJob.mockReturnValue({ success: false, message: 'Cannot cancel' });
      mockGetJobStatus.mockReturnValue(null);

      const res = await datasetApp.request('/api/dataset/jobs/123e4567-e89b-12d3-a456-426614174000', {
        method: 'DELETE',
      });

      expect(res.status).toBe(404);
    });

    it('returns 400 when job cannot be cancelled', async () => {
      mockCancelJob.mockReturnValue({ success: false, message: 'Job already completed' });
      mockGetJobStatus.mockReturnValue({ jobId: '123e4567-e89b-12d3-a456-426614174000', status: 'completed' });

      const res = await datasetApp.request('/api/dataset/jobs/123e4567-e89b-12d3-a456-426614174000', {
        method: 'DELETE',
      });

      expect(res.status).toBe(400);
    });
  });

  describe('GET /api/dataset/{name}/info', () => {
    it('returns dataset info', async () => {
      mockGetDatasetInfo.mockResolvedValue({
        name: 'prime.db',
        size: 1024000,
        stockCount: 100,
        dateRange: { from: '2020-01-01', to: '2024-12-31' },
      });

      const res = await datasetApp.request('/api/dataset/prime.db/info');

      expect(res.status).toBe(200);
    });

    it('returns 404 when not found', async () => {
      mockGetDatasetInfo.mockResolvedValue(null);

      const res = await datasetApp.request('/api/dataset/missing.db/info');

      expect(res.status).toBe(404);
    });

    it('returns 500 on error', async () => {
      mockGetDatasetInfo.mockRejectedValue(new Error('read error'));

      const res = await datasetApp.request('/api/dataset/prime.db/info');

      expect(res.status).toBe(500);
    });
  });

  describe('GET /api/dataset/{name}/sample', () => {
    it('returns sampled stocks', async () => {
      mockSampleDataset.mockResolvedValue({
        codes: ['7203', '9984', '6758'],
        total: 100,
      });

      const res = await datasetApp.request('/api/dataset/prime.db/sample');

      expect(res.status).toBe(200);
    });

    it('returns 404 when not found', async () => {
      mockSampleDataset.mockResolvedValue(null);

      const res = await datasetApp.request('/api/dataset/missing.db/sample');

      expect(res.status).toBe(404);
    });

    it('returns 500 on error', async () => {
      mockSampleDataset.mockRejectedValue(new Error('read error'));

      const res = await datasetApp.request('/api/dataset/prime.db/sample');

      expect(res.status).toBe(500);
    });
  });

  describe('GET /api/dataset/{name}/search', () => {
    it('returns search results', async () => {
      mockSearchDataset.mockResolvedValue({
        results: [{ code: '7203', name: 'トヨタ自動車' }],
        total: 1,
      });

      const res = await datasetApp.request('/api/dataset/prime.db/search?term=toyota');

      expect(res.status).toBe(200);
    });

    it('returns 404 when not found', async () => {
      mockSearchDataset.mockResolvedValue(null);

      const res = await datasetApp.request('/api/dataset/missing.db/search?term=test');

      expect(res.status).toBe(404);
    });

    it('returns 500 on error', async () => {
      mockSearchDataset.mockRejectedValue(new Error('search error'));

      const res = await datasetApp.request('/api/dataset/prime.db/search?term=test');

      expect(res.status).toBe(500);
    });
  });
});
