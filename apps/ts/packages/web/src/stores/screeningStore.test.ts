import { beforeEach, describe, expect, it } from 'vitest';
import { SCREENING_STORE_STORAGE_KEY } from '@/lib/persistedState';
import { createInitialScreeningState, useScreeningStore } from './screeningStore';

const resetScreeningStore = () => {
  useScreeningStore.setState(createInitialScreeningState());
};

describe('screeningStore', () => {
  beforeEach(() => {
    useScreeningStore.persist?.clearStorage?.();
    resetScreeningStore();
  });

  it('updates active screening job ids independently', () => {
    const { setActivePreOpenScreeningJobId, setActiveInSessionScreeningJobId } = useScreeningStore.getState();

    setActivePreOpenScreeningJobId('job-1');
    setActiveInSessionScreeningJobId('same-day-job-1');

    const state = useScreeningStore.getState();
    expect(state.activePreOpenScreeningJobId).toBe('job-1');
    expect(state.activeInSessionScreeningJobId).toBe('same-day-job-1');
  });

  it('upserts screening job history and keeps latest first', () => {
    const { upsertPreOpenScreeningJobHistory } = useScreeningStore.getState();
    upsertPreOpenScreeningJobHistory({
      job_id: 'job-1',
      status: 'pending',
      created_at: '2026-02-18T09:00:00Z',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });
    upsertPreOpenScreeningJobHistory({
      job_id: 'job-2',
      status: 'completed',
      created_at: '2026-02-18T10:00:00Z',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });
    upsertPreOpenScreeningJobHistory({
      job_id: 'job-1',
      status: 'running',
      created_at: '2026-02-18T09:00:00Z',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });

    const state = useScreeningStore.getState();
    expect(state.preOpenScreeningJobHistory).toHaveLength(2);
    expect(state.preOpenScreeningJobHistory[0]?.job_id).toBe('job-2');
    expect(state.preOpenScreeningJobHistory[1]?.job_id).toBe('job-1');
    expect(state.preOpenScreeningJobHistory[1]?.status).toBe('running');
  });

  it('persists active job ids and history without screening results', () => {
    const { setActivePreOpenScreeningJobId, upsertPreOpenScreeningJobHistory } = useScreeningStore.getState();

    setActivePreOpenScreeningJobId('job-1');
    upsertPreOpenScreeningJobHistory({
      job_id: 'job-1',
      status: 'completed',
      created_at: '2026-02-18T10:00:00Z',
      entry_decidability: 'pre_open_decidable',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });

    const raw = sessionStorage.getItem(SCREENING_STORE_STORAGE_KEY);
    expect(raw).not.toBeNull();

    const persisted = JSON.parse(raw ?? '{}') as {
      state?: Record<string, unknown>;
    };

    expect(persisted.state?.activePreOpenScreeningJobId).toBe('job-1');
    expect(persisted.state?.preOpenScreeningJobHistory).toEqual(
      expect.arrayContaining([expect.objectContaining({ job_id: 'job-1', status: 'completed' })])
    );
    expect(persisted.state).not.toHaveProperty('preOpenScreeningResult');
    expect(persisted.state).not.toHaveProperty('inSessionScreeningResult');
  });
});
