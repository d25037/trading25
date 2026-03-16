import { create } from 'zustand';
import { createJSONStorage, persist } from 'zustand/middleware';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import type { RankingParams } from '@/types/ranking';
import type { MarketScreeningResponse, ScreeningJobResponse, ScreeningParams } from '@/types/screening';

export type AnalysisSubTab = 'screening' | 'oracleScreening' | 'ranking' | 'fundamentalRanking';

export const DEFAULT_SCREENING_PARAMS: ScreeningParams = {
  mode: 'standard',
  markets: 'prime',
  recentDays: 10,
  sortBy: 'matchedDate',
  order: 'desc',
  limit: 50,
};

export const DEFAULT_ORACLE_SCREENING_PARAMS: ScreeningParams = {
  mode: 'oracle',
  markets: 'prime',
  recentDays: 10,
  sortBy: 'matchedDate',
  order: 'desc',
  limit: 50,
};

export const DEFAULT_RANKING_PARAMS: RankingParams = {
  markets: 'prime',
  limit: 20,
  lookbackDays: 1,
  periodDays: 250,
};

export const DEFAULT_FUNDAMENTAL_RANKING_PARAMS: FundamentalRankingParams = {
  markets: 'prime',
  limit: 20,
  forecastAboveRecentFyActuals: false,
  forecastLookbackFyCount: 3,
};

interface AnalysisState {
  activeScreeningJobId: string | null;
  activeOracleScreeningJobId: string | null;
  screeningResult: MarketScreeningResponse | null;
  oracleScreeningResult: MarketScreeningResponse | null;
  screeningJobHistory: ScreeningJobResponse[];
  oracleScreeningJobHistory: ScreeningJobResponse[];
  setActiveScreeningJobId: (jobId: string | null) => void;
  setActiveOracleScreeningJobId: (jobId: string | null) => void;
  setScreeningResult: (result: MarketScreeningResponse | null) => void;
  setOracleScreeningResult: (result: MarketScreeningResponse | null) => void;
  upsertScreeningJobHistory: (job: ScreeningJobResponse) => void;
  upsertOracleScreeningJobHistory: (job: ScreeningJobResponse) => void;
}

export type AnalysisPersistedState = Pick<
  AnalysisState,
  | 'activeScreeningJobId'
  | 'activeOracleScreeningJobId'
  | 'screeningResult'
  | 'oracleScreeningResult'
  | 'screeningJobHistory'
  | 'oracleScreeningJobHistory'
>;

export const createInitialAnalysisState = (): AnalysisPersistedState => ({
  activeScreeningJobId: null,
  activeOracleScreeningJobId: null,
  screeningResult: null,
  oracleScreeningResult: null,
  screeningJobHistory: [],
  oracleScreeningJobHistory: [],
});

const MAX_SCREENING_JOB_HISTORY = 30;

function sortByCreatedAtDesc(a: ScreeningJobResponse, b: ScreeningJobResponse): number {
  return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
}

export const useAnalysisStore = create<AnalysisState>()(
  persist(
    (set) => ({
      ...createInitialAnalysisState(),
      setActiveScreeningJobId: (jobId) => set({ activeScreeningJobId: jobId }),
      setActiveOracleScreeningJobId: (jobId) => set({ activeOracleScreeningJobId: jobId }),
      setScreeningResult: (result) => set({ screeningResult: result }),
      setOracleScreeningResult: (result) => set({ oracleScreeningResult: result }),
      upsertScreeningJobHistory: (job) =>
        set((state) => {
          const withoutCurrent = state.screeningJobHistory.filter((item) => item.job_id !== job.job_id);
          const merged = [job, ...withoutCurrent].sort(sortByCreatedAtDesc);
          return {
            screeningJobHistory: merged.slice(0, MAX_SCREENING_JOB_HISTORY),
          };
        }),
      upsertOracleScreeningJobHistory: (job) =>
        set((state) => {
          const withoutCurrent = state.oracleScreeningJobHistory.filter((item) => item.job_id !== job.job_id);
          const merged = [job, ...withoutCurrent].sort(sortByCreatedAtDesc);
          return {
            oracleScreeningJobHistory: merged.slice(0, MAX_SCREENING_JOB_HISTORY),
          };
        }),
    }),
    {
      name: 'trading25-analysis-store',
      storage: createJSONStorage(() => sessionStorage),
      partialize: (state) => ({
        activeScreeningJobId: state.activeScreeningJobId,
        activeOracleScreeningJobId: state.activeOracleScreeningJobId,
        screeningResult: state.screeningResult,
        oracleScreeningResult: state.oracleScreeningResult,
        screeningJobHistory: state.screeningJobHistory,
        oracleScreeningJobHistory: state.oracleScreeningJobHistory,
      }),
    }
  )
);
