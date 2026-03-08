import { beforeEach, describe, expect, it } from 'vitest';
import {
  createInitialAnalysisState,
  DEFAULT_FUNDAMENTAL_RANKING_PARAMS,
  DEFAULT_ORACLE_SCREENING_PARAMS,
  DEFAULT_RANKING_PARAMS,
  DEFAULT_SCREENING_PARAMS,
  useAnalysisStore,
} from './analysisStore';

const resetAnalysisStore = () => {
  useAnalysisStore.setState(createInitialAnalysisState());
};

describe('analysisStore', () => {
  beforeEach(() => {
    useAnalysisStore.persist?.clearStorage?.();
    resetAnalysisStore();
  });

  it('updates screening state', () => {
    const { setActiveSubTab, setScreeningParams, setActiveScreeningJobId, setScreeningResult } =
      useAnalysisStore.getState();

    setActiveSubTab('ranking');
    setScreeningParams({
      ...DEFAULT_SCREENING_PARAMS,
      markets: 'growth',
      limit: 100,
    });
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
    expect(state.activeSubTab).toBe('ranking');
    expect(state.screeningParams.mode).toBe('standard');
    expect(state.screeningParams.markets).toBe('growth');
    expect(state.screeningParams.limit).toBe(100);
    expect(state.activeScreeningJobId).toBe('job-1');
    expect(state.screeningResult?.results[0]?.stockCode).toBe('7203');
  });

  it('updates oracle screening state independently', () => {
    const { setOracleScreeningParams, setActiveOracleScreeningJobId, setOracleScreeningResult } =
      useAnalysisStore.getState();

    setOracleScreeningParams({
      ...DEFAULT_ORACLE_SCREENING_PARAMS,
      strategies: 'production/topix_gap_down_intraday_oracle',
    });
    setActiveOracleScreeningJobId('oracle-job-1');
    setOracleScreeningResult({
      mode: 'oracle',
      summary: {
        totalStocksScreened: 1,
        matchCount: 1,
        skippedCount: 0,
        byStrategy: { 'production/topix_gap_down_intraday_oracle': 1 },
        strategiesEvaluated: ['production/topix_gap_down_intraday_oracle'],
        strategiesWithoutBacktestMetrics: [],
        warnings: [],
      },
      markets: ['prime'],
      recentDays: 10,
      referenceDate: '2026-02-18',
      sortBy: 'matchedDate',
      order: 'desc',
      lastUpdated: '2026-02-18T00:00:00Z',
      results: [],
    });

    const state = useAnalysisStore.getState();
    expect(state.oracleScreeningParams.mode).toBe('oracle');
    expect(state.oracleScreeningParams.strategies).toBe('production/topix_gap_down_intraday_oracle');
    expect(state.activeOracleScreeningJobId).toBe('oracle-job-1');
    expect(state.oracleScreeningResult?.mode).toBe('oracle');
  });

  it('updates ranking params', () => {
    const { setRankingParams } = useAnalysisStore.getState();
    setRankingParams({
      ...DEFAULT_RANKING_PARAMS,
      markets: 'growth',
      lookbackDays: 5,
    });

    const state = useAnalysisStore.getState();
    expect(state.rankingParams.markets).toBe('growth');
    expect(state.rankingParams.lookbackDays).toBe(5);
  });

  it('updates fundamental ranking params', () => {
    const { setFundamentalRankingParams } = useAnalysisStore.getState();
    setFundamentalRankingParams({
      ...DEFAULT_FUNDAMENTAL_RANKING_PARAMS,
      markets: 'prime,standard',
      limit: 50,
      forecastAboveRecentFyActuals: true,
      forecastLookbackFyCount: 5,
    });

    const state = useAnalysisStore.getState();
    expect(state.fundamentalRankingParams.markets).toBe('prime,standard');
    expect(state.fundamentalRankingParams.limit).toBe(50);
    expect(state.fundamentalRankingParams.forecastAboveRecentFyActuals).toBe(true);
    expect(state.fundamentalRankingParams.forecastLookbackFyCount).toBe(5);
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
