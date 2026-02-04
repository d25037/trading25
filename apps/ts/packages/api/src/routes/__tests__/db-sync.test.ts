import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockStartSync = mock();
const mockGetJobStatus = mock();
const mockCancelJob = mock();

mock.module('../../services/market/market-sync-service', () => ({
  marketSyncService: {
    startSync: mockStartSync,
    getJobStatus: mockGetJobStatus,
    cancelJob: mockCancelJob,
  },
}));

let marketSyncApp: typeof import('../db/sync').default;

const validJobId = '123e4567-e89b-12d3-a456-426614174000';

describe('DB Sync Routes', () => {
  beforeEach(async () => {
    mockStartSync.mockReset();
    mockGetJobStatus.mockReset();
    mockCancelJob.mockReset();
    marketSyncApp = (await import('../db/sync')).default;
  });

  describe('POST /api/db/sync', () => {
    it('starts sync job and returns 202', async () => {
      mockStartSync.mockReturnValue({
        jobId: validJobId,
        status: 'running',
        message: 'Sync job started',
      });

      const res = await marketSyncApp.request('/api/db/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode: 'incremental' }),
      });

      expect(res.status).toBe(202);
    });

    it('returns 409 when another job is running', async () => {
      mockStartSync.mockReturnValue(null);

      const res = await marketSyncApp.request('/api/db/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode: 'incremental' }),
      });

      expect(res.status).toBe(409);
    });

    it('returns 500 on error', async () => {
      mockStartSync.mockImplementation(() => {
        throw new Error('sync failure');
      });

      const res = await marketSyncApp.request('/api/db/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode: 'incremental' }),
      });

      expect(res.status).toBe(500);
    });
  });

  describe('GET /api/db/sync/jobs/{jobId}', () => {
    it('returns job status', async () => {
      mockGetJobStatus.mockReturnValue({
        jobId: validJobId,
        status: 'running',
        progress: { current: 50, total: 100 },
      });

      const res = await marketSyncApp.request(`/api/db/sync/jobs/${validJobId}`);

      expect(res.status).toBe(200);
    });

    it('returns 404 when job not found', async () => {
      mockGetJobStatus.mockReturnValue(null);

      const res = await marketSyncApp.request(`/api/db/sync/jobs/${validJobId}`);

      expect(res.status).toBe(404);
    });
  });

  describe('DELETE /api/db/sync/jobs/{jobId}', () => {
    it('cancels job successfully', async () => {
      mockCancelJob.mockReturnValue({ success: true, message: 'Job cancelled' });

      const res = await marketSyncApp.request(`/api/db/sync/jobs/${validJobId}`, { method: 'DELETE' });

      expect(res.status).toBe(200);
    });

    it('returns 404 when job not found for cancel', async () => {
      mockCancelJob.mockReturnValue({ success: false, message: 'Cannot cancel' });
      mockGetJobStatus.mockReturnValue(null);

      const res = await marketSyncApp.request(`/api/db/sync/jobs/${validJobId}`, { method: 'DELETE' });

      expect(res.status).toBe(404);
    });

    it('returns 400 when job cannot be cancelled', async () => {
      mockCancelJob.mockReturnValue({ success: false, message: 'Job already completed' });
      mockGetJobStatus.mockReturnValue({ jobId: validJobId, status: 'completed' });

      const res = await marketSyncApp.request(`/api/db/sync/jobs/${validJobId}`, { method: 'DELETE' });

      expect(res.status).toBe(400);
    });
  });
});
