import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useChartStore } from '@/stores/chartStore';
import { useUiStore } from '@/stores/uiStore';
import { useHistorySync } from './useHistorySync';

// Track history calls
const pushStateSpy = vi.fn();
const replaceStateSpy = vi.fn();

beforeEach(() => {
  // Reset stores
  useUiStore.setState({
    activeTab: 'charts',
    selectedPortfolioId: null,
    selectedIndexCode: null,
  });
  useChartStore.setState({
    selectedSymbol: null,
  });

  // Mock window.location and history
  Object.defineProperty(window, 'location', {
    writable: true,
    value: {
      pathname: '/',
      search: '',
    },
  });

  pushStateSpy.mockClear();
  replaceStateSpy.mockClear();

  window.history.pushState = pushStateSpy;
  window.history.replaceState = replaceStateSpy;
  Object.defineProperty(window.history, 'state', {
    writable: true,
    value: null,
  });
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('useHistorySync', () => {
  it('initializes from URL params on mount', () => {
    Object.defineProperty(window, 'location', {
      writable: true,
      value: {
        pathname: '/',
        search: '?tab=portfolio&symbol=7203&portfolio=1&index=TOPIX',
      },
    });

    renderHook(() => useHistorySync());

    expect(useUiStore.getState().activeTab).toBe('portfolio');
    expect(useChartStore.getState().selectedSymbol).toBe('7203');
    expect(useUiStore.getState().selectedPortfolioId).toBe(1);
    expect(useUiStore.getState().selectedIndexCode).toBe('TOPIX');
    // replaceState called during init
    expect(replaceStateSpy).toHaveBeenCalled();
  });

  it('defaults to charts tab for invalid tab in URL', () => {
    Object.defineProperty(window, 'location', {
      writable: true,
      value: {
        pathname: '/',
        search: '?tab=invalid',
      },
    });

    renderHook(() => useHistorySync());

    expect(useUiStore.getState().activeTab).toBe('charts');
  });

  it('defaults to charts tab when no tab is specified', () => {
    Object.defineProperty(window, 'location', {
      writable: true,
      value: {
        pathname: '/',
        search: '',
      },
    });

    renderHook(() => useHistorySync());

    expect(useUiStore.getState().activeTab).toBe('charts');
  });

  it('handles valid tabs from URL', () => {
    for (const tab of ['charts', 'portfolio', 'indices', 'analysis', 'backtest', 'history', 'settings'] as const) {
      Object.defineProperty(window, 'location', {
        writable: true,
        value: {
          pathname: '/',
          search: `?tab=${tab}`,
        },
      });

      // Reset init state for each iteration
      act(() => {
        useUiStore.setState({ activeTab: 'charts' });
      });

      const { unmount } = renderHook(() => useHistorySync());

      expect(useUiStore.getState().activeTab).toBe(tab);
      unmount();
    }
  });

  it('restores state on popstate event', () => {
    renderHook(() => useHistorySync());

    const popStateEvent = new PopStateEvent('popstate', {
      state: {
        tab: 'analysis',
        symbol: '9984',
        portfolioId: 2,
        indexCode: 'N225',
      },
    });
    act(() => {
      window.dispatchEvent(popStateEvent);
    });

    expect(useUiStore.getState().activeTab).toBe('analysis');
    expect(useChartStore.getState().selectedSymbol).toBe('9984');
    expect(useUiStore.getState().selectedPortfolioId).toBe(2);
    expect(useUiStore.getState().selectedIndexCode).toBe('N225');
  });

  it('falls back to URL for popstate without state', () => {
    Object.defineProperty(window, 'location', {
      writable: true,
      value: {
        pathname: '/',
        search: '?tab=backtest',
      },
    });

    renderHook(() => useHistorySync());

    const popStateEvent = new PopStateEvent('popstate', {
      state: null,
    });
    act(() => {
      window.dispatchEvent(popStateEvent);
    });

    expect(useUiStore.getState().activeTab).toBe('backtest');
  });

  it('handles popstate with invalid tab in state', () => {
    renderHook(() => useHistorySync());

    const popStateEvent = new PopStateEvent('popstate', {
      state: {
        tab: 'nonexistent',
        symbol: null,
        portfolioId: null,
        indexCode: null,
      },
    });
    act(() => {
      window.dispatchEvent(popStateEvent);
    });

    expect(useUiStore.getState().activeTab).toBe('charts');
  });

  it('normalizes empty symbol to null', () => {
    Object.defineProperty(window, 'location', {
      writable: true,
      value: {
        pathname: '/',
        search: '?tab=charts&symbol=',
      },
    });

    renderHook(() => useHistorySync());

    // Empty symbol should not set selectedSymbol
    expect(useChartStore.getState().selectedSymbol).toBeNull();
  });
});
