import { create } from 'zustand';
import { createJSONStorage, persist } from 'zustand/middleware';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import type { RankingParams } from '@/types/ranking';
import type { MarketScreeningResponse, ScreeningParams } from '@/types/screening';

export type AnalysisSubTab = 'screening' | 'ranking' | 'fundamentalRanking';

export const DEFAULT_SCREENING_PARAMS: ScreeningParams = {
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
  forecastAboveAllActuals: false,
};

interface AnalysisState {
  activeSubTab: AnalysisSubTab;
  screeningParams: ScreeningParams;
  rankingParams: RankingParams;
  fundamentalRankingParams: FundamentalRankingParams;
  activeScreeningJobId: string | null;
  screeningResult: MarketScreeningResponse | null;
  setActiveSubTab: (tab: AnalysisSubTab) => void;
  setScreeningParams: (params: ScreeningParams) => void;
  setRankingParams: (params: RankingParams) => void;
  setFundamentalRankingParams: (params: FundamentalRankingParams) => void;
  setActiveScreeningJobId: (jobId: string | null) => void;
  setScreeningResult: (result: MarketScreeningResponse | null) => void;
}

export type AnalysisPersistedState = Pick<
  AnalysisState,
  | 'activeSubTab'
  | 'screeningParams'
  | 'rankingParams'
  | 'fundamentalRankingParams'
  | 'activeScreeningJobId'
  | 'screeningResult'
>;

export const createInitialAnalysisState = (): AnalysisPersistedState => ({
  activeSubTab: 'screening',
  screeningParams: DEFAULT_SCREENING_PARAMS,
  rankingParams: DEFAULT_RANKING_PARAMS,
  fundamentalRankingParams: DEFAULT_FUNDAMENTAL_RANKING_PARAMS,
  activeScreeningJobId: null,
  screeningResult: null,
});

export const useAnalysisStore = create<AnalysisState>()(
  persist(
    (set) => ({
      ...createInitialAnalysisState(),
      setActiveSubTab: (tab) => set({ activeSubTab: tab }),
      setScreeningParams: (params) => set({ screeningParams: params }),
      setRankingParams: (params) => set({ rankingParams: params }),
      setFundamentalRankingParams: (params) => set({ fundamentalRankingParams: params }),
      setActiveScreeningJobId: (jobId) => set({ activeScreeningJobId: jobId }),
      setScreeningResult: (result) => set({ screeningResult: result }),
    }),
    {
      name: 'trading25-analysis-store',
      storage: createJSONStorage(() => sessionStorage),
      partialize: (state) => ({
        activeSubTab: state.activeSubTab,
        screeningParams: state.screeningParams,
        rankingParams: state.rankingParams,
        fundamentalRankingParams: state.fundamentalRankingParams,
        activeScreeningJobId: state.activeScreeningJobId,
        screeningResult: state.screeningResult,
      }),
    }
  )
);
