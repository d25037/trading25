import { RouterProvider } from '@tanstack/react-router';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const pageImportCounts = vi.hoisted(() => ({
  backtest: 0,
  history: 0,
  indices: 0,
  marketDb: 0,
  options225: 0,
  portfolio: 0,
  ranking: 0,
  research: 0,
  researchDetail: 0,
  screening: 0,
  symbolWorkbench: 0,
}));

vi.mock('@/components/Layout/MainLayout', () => ({
  MainLayout: ({ children }: { children: ReactNode }) => <div data-testid="main-layout">{children}</div>,
}));

vi.mock('@/pages/SymbolWorkbenchPage', () => {
  pageImportCounts.symbolWorkbench += 1;
  return {
    SymbolWorkbenchPage: () => <h1>Symbol Workbench Page</h1>,
  };
});

vi.mock('@/pages/PortfolioPage', () => {
  pageImportCounts.portfolio += 1;
  return {
    PortfolioPage: () => <h1>Portfolio Page</h1>,
  };
});

vi.mock('@/pages/IndicesPage', () => {
  pageImportCounts.indices += 1;
  return {
    IndicesPage: () => <h1>Indices Page</h1>,
  };
});

vi.mock('@/pages/ResearchPage', () => {
  pageImportCounts.research += 1;
  return {
    ResearchPage: () => <h1>Research Page</h1>,
  };
});

vi.mock('@/pages/ResearchDetailPage', () => {
  pageImportCounts.researchDetail += 1;
  return {
    ResearchDetailPage: () => <h1>Research Detail Page</h1>,
  };
});

vi.mock('@/pages/N225OptionsPage', () => {
  pageImportCounts.options225 += 1;
  return {
    N225OptionsPage: () => <h1>N225 Options Page</h1>,
  };
});

vi.mock('@/pages/ScreeningPage', () => {
  pageImportCounts.screening += 1;
  return {
    ScreeningPage: () => <h1>Screening Page</h1>,
  };
});

vi.mock('@/pages/RankingPage', () => {
  pageImportCounts.ranking += 1;
  return {
    RankingPage: () => <h1>Ranking Page</h1>,
  };
});

vi.mock('@/pages/BacktestPage', () => {
  pageImportCounts.backtest += 1;
  return {
    BacktestPage: () => <h1>Backtest Page</h1>,
  };
});

vi.mock('@/pages/HistoryPage', () => {
  pageImportCounts.history += 1;
  return {
    HistoryPage: () => <h1>History Page</h1>,
  };
});

vi.mock('@/pages/SettingsPage', () => {
  pageImportCounts.marketDb += 1;
  return {
    SettingsPage: () => <h1>Market DB Page</h1>,
  };
});

function resetPageImportCounts() {
  for (const key of Object.keys(pageImportCounts) as Array<keyof typeof pageImportCounts>) {
    pageImportCounts[key] = 0;
  }
}

async function renderRouterAt(path: string) {
  window.history.replaceState({}, '', path);
  const { createAppRouter } = await import('./router');
  const appRouter = createAppRouter();
  render(<RouterProvider router={appRouter} />);
}

describe('router route-level lazy loading', () => {
  beforeEach(() => {
    vi.resetModules();
    resetPageImportCounts();
  });

  afterEach(() => {
    cleanup();
  });

  it('does not import unrelated page modules when rendering the symbol workbench route', async () => {
    await renderRouterAt('/symbol-workbench');

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Symbol Workbench Page' })).toBeInTheDocument();
    });

    expect(pageImportCounts.symbolWorkbench).toBe(1);
    expect(pageImportCounts.backtest).toBe(0);
    expect(pageImportCounts.history).toBe(0);
    expect(pageImportCounts.indices).toBe(0);
    expect(pageImportCounts.marketDb).toBe(0);
    expect(pageImportCounts.options225).toBe(0);
    expect(pageImportCounts.portfolio).toBe(0);
    expect(pageImportCounts.ranking).toBe(0);
    expect(pageImportCounts.research).toBe(0);
    expect(pageImportCounts.researchDetail).toBe(0);
    expect(pageImportCounts.screening).toBe(0);
  });
});
