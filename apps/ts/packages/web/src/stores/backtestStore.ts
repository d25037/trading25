import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { BacktestSubTab, LabType } from '@/types/backtest';

interface BacktestState {
  activeSubTab: BacktestSubTab;
  selectedStrategy: string | null;
  activeJobId: string | null;
  activeAttributionJobId: string | null;
  selectedResultJobId: string | null;
  activeOptimizationJobId: string | null;
  activeDatasetJobId: string | null;
  selectedDatasetName: string | null;
  activeLabJobId: string | null;
  activeLabType: LabType | null;

  // Actions
  setActiveSubTab: (tab: BacktestSubTab) => void;
  setSelectedStrategy: (name: string | null) => void;
  setActiveJobId: (jobId: string | null) => void;
  setActiveAttributionJobId: (jobId: string | null) => void;
  setSelectedResultJobId: (jobId: string | null) => void;
  setActiveOptimizationJobId: (jobId: string | null) => void;
  setActiveDatasetJobId: (jobId: string | null) => void;
  setSelectedDatasetName: (name: string | null) => void;
  setActiveLabJobId: (jobId: string | null) => void;
  setActiveLabType: (type: LabType | null) => void;
}

export const useBacktestStore = create<BacktestState>()(
  persist(
    (set) => ({
      activeSubTab: 'runner',
      selectedStrategy: null,
      activeJobId: null,
      activeAttributionJobId: null,
      selectedResultJobId: null,
      activeOptimizationJobId: null,
      activeDatasetJobId: null,
      selectedDatasetName: null,
      activeLabJobId: null,
      activeLabType: null,

      setActiveSubTab: (tab) => set({ activeSubTab: tab }),
      setSelectedStrategy: (name) => set({ selectedStrategy: name }),
      setActiveJobId: (jobId) => set({ activeJobId: jobId }),
      setActiveAttributionJobId: (jobId) => set({ activeAttributionJobId: jobId }),
      setSelectedResultJobId: (jobId) => set({ selectedResultJobId: jobId }),
      setActiveOptimizationJobId: (jobId) => set({ activeOptimizationJobId: jobId }),
      setActiveDatasetJobId: (jobId) => set({ activeDatasetJobId: jobId }),
      setSelectedDatasetName: (name) => set({ selectedDatasetName: name }),
      setActiveLabJobId: (jobId) => set({ activeLabJobId: jobId }),
      setActiveLabType: (type) => set({ activeLabType: type }),
    }),
    {
      name: 'trading25-backtest-store',
      partialize: (state) => ({
        activeSubTab: state.activeSubTab,
        selectedStrategy: state.selectedStrategy,
        selectedResultJobId: state.selectedResultJobId,
      }),
    }
  )
);
