import { afterEach, describe, expect, it } from 'vitest';
import { useUiStore } from './uiStore';

describe('uiStore', () => {
  afterEach(() => {
    // Reset store to initial state
    const { setSelectedPortfolioId, setSelectedWatchlistId, setSelectedIndexCode, setPortfolioSubTab } =
      useUiStore.getState();
    setSelectedPortfolioId(null);
    setSelectedWatchlistId(null);
    setSelectedIndexCode(null);
    setPortfolioSubTab('portfolios');
  });

  it('has correct initial state', () => {
    const state = useUiStore.getState();
    expect(state.selectedPortfolioId).toBeNull();
    expect(state.selectedWatchlistId).toBeNull();
    expect(state.selectedIndexCode).toBeNull();
    expect(state.portfolioSubTab).toBe('portfolios');
  });

  it('setSelectedPortfolioId updates portfolio id', () => {
    useUiStore.getState().setSelectedPortfolioId(5);
    expect(useUiStore.getState().selectedPortfolioId).toBe(5);
  });

  it('setSelectedWatchlistId updates watchlist id', () => {
    useUiStore.getState().setSelectedWatchlistId(3);
    expect(useUiStore.getState().selectedWatchlistId).toBe(3);
  });

  it('setSelectedIndexCode updates index code', () => {
    useUiStore.getState().setSelectedIndexCode('topix');
    expect(useUiStore.getState().selectedIndexCode).toBe('topix');
  });

  it('setPortfolioSubTab updates sub tab', () => {
    useUiStore.getState().setPortfolioSubTab('watchlists');
    expect(useUiStore.getState().portfolioSubTab).toBe('watchlists');
  });
});
