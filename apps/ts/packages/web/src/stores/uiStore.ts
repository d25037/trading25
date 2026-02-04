import { create } from 'zustand';
import { persist } from 'zustand/middleware';

type PortfolioSubTab = 'portfolios' | 'watchlists';

interface UiState {
  activeTab: string;
  selectedPortfolioId: number | null;
  selectedWatchlistId: number | null;
  selectedIndexCode: string | null;
  portfolioSubTab: PortfolioSubTab;
  setActiveTab: (tab: string) => void;
  setSelectedPortfolioId: (id: number | null) => void;
  setSelectedWatchlistId: (id: number | null) => void;
  setSelectedIndexCode: (code: string | null) => void;
  setPortfolioSubTab: (tab: PortfolioSubTab) => void;
}

export const useUiStore = create<UiState>()(
  persist(
    (set) => ({
      activeTab: 'charts',
      selectedPortfolioId: null,
      selectedWatchlistId: null,
      selectedIndexCode: null,
      portfolioSubTab: 'portfolios' as PortfolioSubTab,
      setActiveTab: (tab) => set({ activeTab: tab }),
      setSelectedPortfolioId: (id) => set({ selectedPortfolioId: id }),
      setSelectedWatchlistId: (id) => set({ selectedWatchlistId: id }),
      setSelectedIndexCode: (code) => set({ selectedIndexCode: code }),
      setPortfolioSubTab: (tab) => set({ portfolioSubTab: tab }),
    }),
    {
      name: 'trading25-ui-store',
      partialize: (state) => ({
        activeTab: state.activeTab,
        selectedPortfolioId: state.selectedPortfolioId,
        selectedWatchlistId: state.selectedWatchlistId,
        selectedIndexCode: state.selectedIndexCode,
        portfolioSubTab: state.portfolioSubTab,
      }),
    }
  )
);
