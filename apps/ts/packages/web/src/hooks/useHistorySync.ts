import { useEffect, useRef } from 'react';
import { useChartStore } from '@/stores/chartStore';
import { useUiStore } from '@/stores/uiStore';

const VALID_TABS = ['charts', 'portfolio', 'indices', 'analysis', 'backtest', 'history', 'settings'] as const;
type ValidTab = (typeof VALID_TABS)[number];

function isValidTab(tab: string): tab is ValidTab {
  return VALID_TABS.includes(tab as ValidTab);
}

function getStateFromUrl(): {
  tab: ValidTab;
  symbol: string | null;
  portfolioId: number | null;
  indexCode: string | null;
} {
  const params = new URLSearchParams(window.location.search);
  const tab = params.get('tab');
  const symbol = params.get('symbol');
  const portfolioId = params.get('portfolio');
  const indexCode = params.get('index');

  return {
    tab: tab && isValidTab(tab) ? tab : 'charts',
    symbol: symbol || null,
    portfolioId: portfolioId ? Number.parseInt(portfolioId, 10) : null,
    indexCode: indexCode || null,
  };
}

function getStateFromPopStateEvent(event: PopStateEvent): {
  tab: ValidTab;
  symbol: string | null;
  portfolioId: number | null;
  indexCode: string | null;
} {
  if (event.state) {
    const { tab, symbol, portfolioId, indexCode } = event.state as {
      tab: string;
      symbol: string | null;
      portfolioId: number | null;
      indexCode: string | null;
    };
    return {
      tab: isValidTab(tab) ? tab : 'charts',
      symbol: symbol || null,
      portfolioId: portfolioId ?? null,
      indexCode: indexCode ?? null,
    };
  }
  return getStateFromUrl();
}

function updateUrl(
  tab: string,
  symbol: string | null,
  portfolioId: number | null,
  indexCode: string | null,
  replace = false
) {
  const params = new URLSearchParams();
  params.set('tab', tab);
  if (symbol) {
    params.set('symbol', symbol);
  }
  if (portfolioId !== null) {
    params.set('portfolio', String(portfolioId));
  }
  if (indexCode) {
    params.set('index', indexCode);
  }

  const newUrl = `${window.location.pathname}?${params.toString()}`;
  const state = { tab, symbol, portfolioId, indexCode };

  if (replace) {
    window.history.replaceState(state, '', newUrl);
  } else {
    window.history.pushState(state, '', newUrl);
  }
}

/**
 * Syncs browser history with Zustand stores (activeTab, selectedSymbol, selectedPortfolioId, selectedIndexCode)
 * - Pushes new history entries on navigation
 * - Restores state on browser back/forward
 * - Initializes state from URL on mount
 */
export function useHistorySync() {
  const {
    activeTab,
    setActiveTab,
    selectedPortfolioId,
    setSelectedPortfolioId,
    selectedIndexCode,
    setSelectedIndexCode,
  } = useUiStore();
  const { selectedSymbol, setSelectedSymbol } = useChartStore();
  const isInitialized = useRef(false);
  const isPopState = useRef(false);

  // Initialize from URL on mount
  useEffect(() => {
    if (isInitialized.current) return;
    isInitialized.current = true;

    const { tab, symbol, portfolioId, indexCode } = getStateFromUrl();

    // Set initial state without pushing history
    setActiveTab(tab);
    if (symbol) {
      setSelectedSymbol(symbol);
    }
    if (portfolioId !== null) {
      setSelectedPortfolioId(portfolioId);
    }
    if (indexCode) {
      setSelectedIndexCode(indexCode);
    }

    // Replace current history entry with proper state
    updateUrl(tab, symbol, portfolioId, indexCode, true);
  }, [setActiveTab, setSelectedSymbol, setSelectedPortfolioId, setSelectedIndexCode]);

  // Handle browser back/forward buttons
  useEffect(() => {
    const handlePopState = (event: PopStateEvent) => {
      isPopState.current = true;

      const { tab, symbol, portfolioId, indexCode } = getStateFromPopStateEvent(event);
      setActiveTab(tab);
      setSelectedSymbol(symbol || '');
      setSelectedPortfolioId(portfolioId);
      setSelectedIndexCode(indexCode);

      // Reset flag after state updates and useEffect execution
      // Use setTimeout to ensure it runs after React's commit phase
      setTimeout(() => {
        isPopState.current = false;
      }, 0);
    };

    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, [setActiveTab, setSelectedSymbol, setSelectedPortfolioId, setSelectedIndexCode]);

  // Sync state changes to URL (only for user-initiated changes, not popstate)
  useEffect(() => {
    if (!isInitialized.current || isPopState.current) return;

    const currentState = window.history.state as {
      tab?: string;
      symbol?: string | null;
      portfolioId?: number | null;
      indexCode?: string | null;
    } | null;
    const currentTab = currentState?.tab;
    const currentSymbol = currentState?.symbol ?? null;
    const currentPortfolioId = currentState?.portfolioId ?? null;
    const currentIndexCode = currentState?.indexCode ?? null;
    const normalizedSymbol = selectedSymbol || null;

    // Only push if state actually changed
    if (
      currentTab !== activeTab ||
      currentSymbol !== normalizedSymbol ||
      currentPortfolioId !== selectedPortfolioId ||
      currentIndexCode !== selectedIndexCode
    ) {
      updateUrl(activeTab, normalizedSymbol, selectedPortfolioId, selectedIndexCode);
    }
  }, [activeTab, selectedSymbol, selectedPortfolioId, selectedIndexCode]);
}
