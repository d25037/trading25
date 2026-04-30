import { create } from 'zustand';
import { createJSONStorage, persist } from 'zustand/middleware';
import { SCREENING_STORE_STORAGE_KEY } from '@/lib/persistedState';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import type { RankingParams } from '@/types/ranking';
import type { ScreeningJobResponse, ScreeningParams } from '@/types/screening';
import type { ValueCompositeRankingParams } from '@/types/valueCompositeRanking';

export type ScreeningSubTab = 'preOpenScreening' | 'inSessionScreening' | 'ranking' | 'fundamentalRanking';

export const DEFAULT_PRE_OPEN_SCREENING_PARAMS: ScreeningParams = {
  entry_decidability: 'pre_open_decidable',
  recentDays: 10,
  sortBy: 'matchedDate',
  order: 'desc',
  limit: 50,
};

export const DEFAULT_IN_SESSION_SCREENING_PARAMS: ScreeningParams = {
  entry_decidability: 'requires_same_session_observation',
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
  topix100StudyMode: 'swing_5d',
  topix100Metric: 'price_vs_sma_gap',
  topix100SmaWindow: 50,
  topix100PriceBucket: 'all',
  topix100SortBy: 'longScore5d',
  topix100SortOrder: 'desc',
};

export const DEFAULT_FUNDAMENTAL_RANKING_PARAMS: FundamentalRankingParams = {
  markets: 'prime',
  limit: 20,
  forecastAboveRecentFyActuals: false,
  forecastLookbackFyCount: 3,
};

export const DEFAULT_VALUE_COMPOSITE_RANKING_PARAMS: ValueCompositeRankingParams = {
  markets: 'standard',
  limit: 50,
  scoreMethod: 'standard_pbr_tilt',
  forwardEpsMode: 'latest',
};

interface ScreeningState {
  activePreOpenScreeningJobId: string | null;
  activeInSessionScreeningJobId: string | null;
  preOpenScreeningJobHistory: ScreeningJobResponse[];
  inSessionScreeningJobHistory: ScreeningJobResponse[];
  setActivePreOpenScreeningJobId: (jobId: string | null) => void;
  setActiveInSessionScreeningJobId: (jobId: string | null) => void;
  upsertPreOpenScreeningJobHistory: (job: ScreeningJobResponse) => void;
  upsertInSessionScreeningJobHistory: (job: ScreeningJobResponse) => void;
}

export type ScreeningStoreState = Pick<
  ScreeningState,
  | 'activePreOpenScreeningJobId'
  | 'activeInSessionScreeningJobId'
  | 'preOpenScreeningJobHistory'
  | 'inSessionScreeningJobHistory'
>;

export type ScreeningPersistedState = Pick<
  ScreeningStoreState,
  | 'activePreOpenScreeningJobId'
  | 'activeInSessionScreeningJobId'
  | 'preOpenScreeningJobHistory'
  | 'inSessionScreeningJobHistory'
>;

export const createInitialScreeningState = (): ScreeningStoreState => ({
  activePreOpenScreeningJobId: null,
  activeInSessionScreeningJobId: null,
  preOpenScreeningJobHistory: [],
  inSessionScreeningJobHistory: [],
});

const MAX_SCREENING_JOB_HISTORY = 30;

function sortByCreatedAtDesc(a: ScreeningJobResponse, b: ScreeningJobResponse): number {
  return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
}

export const useScreeningStore = create<ScreeningState>()(
  persist(
    (set) => ({
      ...createInitialScreeningState(),
      setActivePreOpenScreeningJobId: (jobId) => set({ activePreOpenScreeningJobId: jobId }),
      setActiveInSessionScreeningJobId: (jobId) => set({ activeInSessionScreeningJobId: jobId }),
      upsertPreOpenScreeningJobHistory: (job) =>
        set((state) => {
          const withoutCurrent = state.preOpenScreeningJobHistory.filter((item) => item.job_id !== job.job_id);
          const merged = [job, ...withoutCurrent].sort(sortByCreatedAtDesc);
          return {
            preOpenScreeningJobHistory: merged.slice(0, MAX_SCREENING_JOB_HISTORY),
          };
        }),
      upsertInSessionScreeningJobHistory: (job) =>
        set((state) => {
          const withoutCurrent = state.inSessionScreeningJobHistory.filter((item) => item.job_id !== job.job_id);
          const merged = [job, ...withoutCurrent].sort(sortByCreatedAtDesc);
          return {
            inSessionScreeningJobHistory: merged.slice(0, MAX_SCREENING_JOB_HISTORY),
          };
        }),
    }),
    {
      name: SCREENING_STORE_STORAGE_KEY,
      storage: createJSONStorage(() => sessionStorage),
      partialize: (state) => ({
        activePreOpenScreeningJobId: state.activePreOpenScreeningJobId,
        activeInSessionScreeningJobId: state.activeInSessionScreeningJobId,
        preOpenScreeningJobHistory: state.preOpenScreeningJobHistory,
        inSessionScreeningJobHistory: state.inSessionScreeningJobHistory,
      }),
    }
  )
);
