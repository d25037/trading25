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

vi.mock('@/pages/PortfolioPage', () => ({
  PortfolioPage: () => <h1>Portfolio Page</h1>,
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
    expect(screen.getByRole('heading', { name: 'Symbol Workbench Page' })).toBeInTheDocument();
  });

  it('shows migration guidance for legacy ?tab= query links', async () => {
    renderRouterAt('/?tab=history');

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: '404: Legacy URL is no longer supported' })).toBeInTheDocument();
    });
    expect(window.location.pathname).toBe('/');
    expect(window.location.search).toBe('?tab=history');
    expect(screen.getByRole('link', { name: 'Open suggested route (/history)' })).toHaveAttribute('href', '/history');
  });

  it('shows screening route guidance for renamed legacy query links', async () => {
    renderRouterAt('/?tab=screening');

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: '404: Legacy URL is no longer supported' })).toBeInTheDocument();
    });
    expect(window.location.pathname).toBe('/');
    expect(window.location.search).toBe('?tab=screening');
    expect(screen.getByRole('link', { name: 'Open suggested route (/screening)' })).toHaveAttribute(
      'href',
      '/screening'
    );
  });

  it('omits the suggested route link for unknown legacy tabs', async () => {
    renderRouterAt('/?tab=unknown');

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: '404: Legacy URL is no longer supported' })).toBeInTheDocument();
    });

    expect(window.location.pathname).toBe('/');
    expect(window.location.search).toBe('?tab=unknown');
    expect(screen.getByText('Requested URL:')).toBeInTheDocument();
    expect(screen.queryByRole('link', { name: /Open suggested route/i })).not.toBeInTheDocument();
  });

  it('does not offer a suggested route for removed charts legacy tabs', async () => {
    renderRouterAt('/?tab=charts');

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: '404: Legacy URL is no longer supported' })).toBeInTheDocument();
    });

    expect(window.location.pathname).toBe('/');
    expect(window.location.search).toBe('?tab=charts');
    expect(screen.queryByRole('link', { name: /Open suggested route/i })).not.toBeInTheDocument();
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
    expect(screen.getByRole('heading', { name: 'Screening Page' })).toBeInTheDocument();
  });

  it('redirects screening ranking tabs to /ranking', async () => {
    renderRouterAt('/screening?tab=ranking&rankingMarkets=growth');

    await waitFor(() => {
      expect(window.location.pathname).toBe('/ranking');
    });
    expect(window.location.search).toBe('?rankingMarkets=growth');
    expect(screen.getByRole('heading', { name: 'Ranking Page' })).toBeInTheDocument();
  });

  it('redirects legacy /settings path to /market-db', async () => {
    renderRouterAt('/settings');

    await waitFor(() => {
      expect(window.location.pathname).toBe('/market-db');
    });
    expect(screen.getByRole('heading', { name: 'Market DB Page' })).toBeInTheDocument();
  });
});
