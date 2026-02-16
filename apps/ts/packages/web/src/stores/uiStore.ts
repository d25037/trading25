import { create } from 'zustand';
import { persist } from 'zustand/middleware';

type PortfolioSubTab = 'portfolios' | 'watchlists';

interface UiState {
  selectedPortfolioId: number | null;
  selectedWatchlistId: number | null;
  selectedIndexCode: string | null;
  portfolioSubTab: PortfolioSubTab;
  setSelectedPortfolioId: (id: number | null) => void;
  setSelectedWatchlistId: (id: number | null) => void;
  setSelectedIndexCode: (code: string | null) => void;
  setPortfolioSubTab: (tab: PortfolioSubTab) => void;
}

export const useUiStore = create<UiState>()(
  persist(
    (set) => ({
      selectedPortfolioId: null,
      selectedWatchlistId: null,
      selectedIndexCode: null,
      portfolioSubTab: 'portfolios' as PortfolioSubTab,
      setSelectedPortfolioId: (id) => set({ selectedPortfolioId: id }),
      setSelectedWatchlistId: (id) => set({ selectedWatchlistId: id }),
      setSelectedIndexCode: (code) => set({ selectedIndexCode: code }),
      setPortfolioSubTab: (tab) => set({ portfolioSubTab: tab }),
    }),
    {
      name: 'trading25-ui-store',
      partialize: (state) => ({
        selectedPortfolioId: state.selectedPortfolioId,
        selectedWatchlistId: state.selectedWatchlistId,
        selectedIndexCode: state.selectedIndexCode,
        portfolioSubTab: state.portfolioSubTab,
      }),
    }
  )
);
