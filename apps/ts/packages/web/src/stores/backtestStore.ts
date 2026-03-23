import { create } from 'zustand';
import { createJSONStorage, persist } from 'zustand/middleware';
import { BACKTEST_STORE_STORAGE_KEY } from '@/lib/persistedState';

interface BacktestState {
  activeJobId: string | null;
  activeAttributionJobId: string | null;
  activeOptimizationJobId: string | null;
  activeDatasetJobId: string | null;
  activeLabJobId: string | null;

  // Actions
  setActiveJobId: (jobId: string | null) => void;
  setActiveAttributionJobId: (jobId: string | null) => void;
  setActiveOptimizationJobId: (jobId: string | null) => void;
  setActiveDatasetJobId: (jobId: string | null) => void;
  setActiveLabJobId: (jobId: string | null) => void;
}

export const useBacktestStore = create<BacktestState>()(
  persist(
    (set) => ({
      activeJobId: null,
      activeAttributionJobId: null,
      activeOptimizationJobId: null,
      activeDatasetJobId: null,
      activeLabJobId: null,

      setActiveJobId: (jobId) => set({ activeJobId: jobId }),
      setActiveAttributionJobId: (jobId) => set({ activeAttributionJobId: jobId }),
      setActiveOptimizationJobId: (jobId) => set({ activeOptimizationJobId: jobId }),
      setActiveDatasetJobId: (jobId) => set({ activeDatasetJobId: jobId }),
      setActiveLabJobId: (jobId) => set({ activeLabJobId: jobId }),
    }),
    {
      name: BACKTEST_STORE_STORAGE_KEY,
      storage: createJSONStorage(() => localStorage),
      partialize: (state) => ({
        activeJobId: state.activeJobId,
        activeAttributionJobId: state.activeAttributionJobId,
        activeOptimizationJobId: state.activeOptimizationJobId,
        activeDatasetJobId: state.activeDatasetJobId,
        activeLabJobId: state.activeLabJobId,
      }),
    }
  )
);
