import { RouterProvider } from '@tanstack/react-router';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createAppRouter } from './router';

vi.mock('@/components/Layout/MainLayout', () => ({
  MainLayout: ({ children }: { children: ReactNode }) => <div data-testid="main-layout">{children}</div>,
}));

vi.mock('@/pages/SymbolWorkbenchPage', () => ({
  SymbolWorkbenchPage: () => <h1>Symbol Workbench Page</h1>,
}));

vi.mock('@/pages/WatchlistPage', () => ({
  WatchlistPage: () => <h1>Watchlist Page</h1>,
}));

vi.mock('@/pages/IndicesPage', () => ({
  IndicesPage: () => <h1>Indices Page</h1>,
}));

vi.mock('@/pages/N225OptionsPage', () => ({
  N225OptionsPage: () => <h1>N225 Options Page</h1>,
}));

vi.mock('@/pages/ScreeningPage', () => ({
  ScreeningPage: () => <h1>Screening Page</h1>,
}));

vi.mock('@/pages/RankingPage', () => ({
  RankingPage: () => <h1>Ranking Page</h1>,
}));

vi.mock('@/pages/BacktestPage', () => ({
  BacktestPage: () => <h1>Backtest Page</h1>,
}));

vi.mock('@/pages/HistoryPage', () => ({
  HistoryPage: () => <h1>History Page</h1>,
}));

vi.mock('@/pages/SettingsPage', () => ({
  SettingsPage: () => <h1>Market DB Page</h1>,
}));

function renderRouterAt(path: string) {
  window.history.replaceState({}, '', path);
  const appRouter = createAppRouter();
  render(<RouterProvider router={appRouter} />);
}

describe('router', () => {
  afterEach(() => {
    cleanup();
  });

  it('redirects root path to /symbol-workbench', async () => {
    renderRouterAt('/');

    await waitFor(() => {
      expect(window.location.pathname).toBe('/symbol-workbench');
    });
    expect(await screen.findByRole('heading', { name: 'Symbol Workbench Page' })).toBeInTheDocument();
  });

  it('redirects root path with query params to /symbol-workbench', async () => {
    renderRouterAt('/?tab=history');

    await waitFor(() => {
      expect(window.location.pathname).toBe('/symbol-workbench');
    });
    expect(await screen.findByRole('heading', { name: 'Symbol Workbench Page' })).toBeInTheDocument();
  });

  it('renders market db page when path is /market-db', async () => {
    renderRouterAt('/market-db');

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Market DB Page' })).toBeInTheDocument();
    });
    expect(window.location.pathname).toBe('/market-db');
  });

  it('renders n225 options page when path is /options-225', async () => {
    renderRouterAt('/options-225');

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'N225 Options Page' })).toBeInTheDocument();
    });
    expect(window.location.pathname).toBe('/options-225');
  });

  it('renders screening page when path is /screening', async () => {
    renderRouterAt('/screening');

    await waitFor(() => {
      expect(window.location.pathname).toBe('/screening');
    });
    expect(await screen.findByRole('heading', { name: 'Screening Page' })).toBeInTheDocument();
  });

});
