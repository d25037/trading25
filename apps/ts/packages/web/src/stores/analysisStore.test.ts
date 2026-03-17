import { beforeEach, describe, expect, it } from 'vitest';
import { createInitialAnalysisState, useAnalysisStore } from './analysisStore';

const resetAnalysisStore = () => {
  useAnalysisStore.setState(createInitialAnalysisState());
};

describe('analysisStore', () => {
  beforeEach(() => {
    useAnalysisStore.persist?.clearStorage?.();
    resetAnalysisStore();
  });

  it('updates screening state', () => {
    const { setActiveScreeningJobId, setScreeningResult } = useAnalysisStore.getState();

    setActiveScreeningJobId('job-1');
    setScreeningResult({
      summary: {
        totalStocksScreened: 1,
        matchCount: 1,
        skippedCount: 0,
        byStrategy: { 'production/range_break_v15': 1 },
        strategiesEvaluated: ['production/range_break_v15'],
        strategiesWithoutBacktestMetrics: [],
        warnings: [],
      },
      markets: ['growth'],
      recentDays: 10,
      referenceDate: '2026-02-18',
      sortBy: 'matchedDate',
      order: 'desc',
      lastUpdated: '2026-02-18T00:00:00Z',
      provenance: {
        source_kind: 'market',
        loaded_domains: ['stock_data', 'statements'],
        reference_date: '2026-02-18',
      },
      diagnostics: {
        missing_required_data: [],
        used_fields: [],
        warnings: [],
      },
      results: [
        {
          stockCode: '7203',
          companyName: 'トヨタ自動車',
          matchedDate: '2026-02-18',
          bestStrategyName: 'production/range_break_v15',
          matchStrategyCount: 1,
          bestStrategyScore: 1.2,
          matchedStrategies: [
            {
              strategyName: 'production/range_break_v15',
              matchedDate: '2026-02-18',
              strategyScore: 1.2,
            },
          ],
        },
      ],
    });

    const state = useAnalysisStore.getState();
    expect(state.activeScreeningJobId).toBe('job-1');
    expect(state.screeningResult?.results[0]?.stockCode).toBe('7203');
  });

  it('updates same-day screening state independently', () => {
    const { setActiveSameDayScreeningJobId, setSameDayScreeningResult } = useAnalysisStore.getState();

    setActiveSameDayScreeningJobId('same-day-job-1');
    setSameDayScreeningResult({
      mode: 'same_day',
      summary: {
        totalStocksScreened: 1,
        matchCount: 1,
        skippedCount: 0,
        byStrategy: { 'production/topix_gap_down_intraday_same_day': 1 },
        strategiesEvaluated: ['production/topix_gap_down_intraday_same_day'],
        strategiesWithoutBacktestMetrics: [],
        warnings: [],
      },
      markets: ['prime'],
      recentDays: 10,
      referenceDate: '2026-02-18',
      sortBy: 'matchedDate',
      order: 'desc',
      lastUpdated: '2026-02-18T00:00:00Z',
      provenance: {
        source_kind: 'market',
        loaded_domains: ['stock_data', 'statements'],
        reference_date: '2026-02-18',
      },
      diagnostics: {
        missing_required_data: [],
        used_fields: [],
        warnings: [],
      },
      results: [],
    });

    const state = useAnalysisStore.getState();
    expect(state.activeSameDayScreeningJobId).toBe('same-day-job-1');
    expect(state.sameDayScreeningResult?.mode).toBe('same_day');
  });

  it('upserts screening job history and keeps latest first', () => {
    const { upsertScreeningJobHistory } = useAnalysisStore.getState();
    upsertScreeningJobHistory({
      job_id: 'job-1',
      status: 'pending',
      created_at: '2026-02-18T09:00:00Z',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });
    upsertScreeningJobHistory({
      job_id: 'job-2',
      status: 'completed',
      created_at: '2026-02-18T10:00:00Z',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });
    upsertScreeningJobHistory({
      job_id: 'job-1',
      status: 'running',
      created_at: '2026-02-18T09:00:00Z',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });

    const state = useAnalysisStore.getState();
    expect(state.screeningJobHistory).toHaveLength(2);
    expect(state.screeningJobHistory[0]?.job_id).toBe('job-2');
    expect(state.screeningJobHistory[1]?.job_id).toBe('job-1');
    expect(state.screeningJobHistory[1]?.status).toBe('running');
  });
});
