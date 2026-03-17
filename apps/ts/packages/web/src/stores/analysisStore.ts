import { create } from 'zustand';
import { createJSONStorage, persist } from 'zustand/middleware';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import type { RankingParams } from '@/types/ranking';
import type { MarketScreeningResponse, ScreeningJobResponse, ScreeningParams } from '@/types/screening';

export type AnalysisSubTab = 'screening' | 'sameDayScreening' | 'ranking' | 'fundamentalRanking';

export const DEFAULT_SCREENING_PARAMS: ScreeningParams = {
  mode: 'standard',
  markets: 'prime',
  recentDays: 10,
  sortBy: 'matchedDate',
  order: 'desc',
  limit: 50,
};

export const DEFAULT_SAME_DAY_SCREENING_PARAMS: ScreeningParams = {
  mode: 'same_day',
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
  activeSameDayScreeningJobId: string | null;
  screeningResult: MarketScreeningResponse | null;
  sameDayScreeningResult: MarketScreeningResponse | null;
  screeningJobHistory: ScreeningJobResponse[];
  sameDayScreeningJobHistory: ScreeningJobResponse[];
  setActiveScreeningJobId: (jobId: string | null) => void;
  setActiveSameDayScreeningJobId: (jobId: string | null) => void;
  setScreeningResult: (result: MarketScreeningResponse | null) => void;
  setSameDayScreeningResult: (result: MarketScreeningResponse | null) => void;
  upsertScreeningJobHistory: (job: ScreeningJobResponse) => void;
  upsertSameDayScreeningJobHistory: (job: ScreeningJobResponse) => void;
}

export type AnalysisPersistedState = Pick<
  AnalysisState,
  | 'activeScreeningJobId'
  | 'activeSameDayScreeningJobId'
  | 'screeningResult'
  | 'sameDayScreeningResult'
  | 'screeningJobHistory'
  | 'sameDayScreeningJobHistory'
>;

export const createInitialAnalysisState = (): AnalysisPersistedState => ({
  activeScreeningJobId: null,
  activeSameDayScreeningJobId: null,
  screeningResult: null,
  sameDayScreeningResult: null,
  screeningJobHistory: [],
  sameDayScreeningJobHistory: [],
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
      setActiveSameDayScreeningJobId: (jobId) => set({ activeSameDayScreeningJobId: jobId }),
      setScreeningResult: (result) => set({ screeningResult: result }),
      setSameDayScreeningResult: (result) => set({ sameDayScreeningResult: result }),
      upsertScreeningJobHistory: (job) =>
        set((state) => {
          const withoutCurrent = state.screeningJobHistory.filter((item) => item.job_id !== job.job_id);
          const merged = [job, ...withoutCurrent].sort(sortByCreatedAtDesc);
          return {
            screeningJobHistory: merged.slice(0, MAX_SCREENING_JOB_HISTORY),
          };
        }),
      upsertSameDayScreeningJobHistory: (job) =>
        set((state) => {
          const withoutCurrent = state.sameDayScreeningJobHistory.filter((item) => item.job_id !== job.job_id);
          const merged = [job, ...withoutCurrent].sort(sortByCreatedAtDesc);
          return {
            sameDayScreeningJobHistory: merged.slice(0, MAX_SCREENING_JOB_HISTORY),
          };
        }),
    }),
    {
      name: 'trading25-analysis-store',
      storage: createJSONStorage(() => sessionStorage),
      partialize: (state) => ({
        activeScreeningJobId: state.activeScreeningJobId,
        activeSameDayScreeningJobId: state.activeSameDayScreeningJobId,
        screeningResult: state.screeningResult,
        sameDayScreeningResult: state.sameDayScreeningResult,
        screeningJobHistory: state.screeningJobHistory,
        sameDayScreeningJobHistory: state.sameDayScreeningJobHistory,
      }),
    }
  )
);
