import { RouterProvider } from '@tanstack/react-router';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createAppRouter } from './router';

vi.mock('@/components/Layout/MainLayout', () => ({
  MainLayout: ({ children }: { children: ReactNode }) => <div data-testid="main-layout">{children}</div>,
}));

vi.mock('@/pages/ChartsPage', () => ({
  ChartsPage: () => <h1>Charts Page</h1>,
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

vi.mock('@/pages/AnalysisPage', () => ({
  AnalysisPage: () => <h1>Analysis Page</h1>,
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

  it('redirects root path to /charts', async () => {
    renderRouterAt('/');

    await waitFor(() => {
      expect(window.location.pathname).toBe('/charts');
    });
    expect(screen.getByRole('heading', { name: 'Charts Page' })).toBeInTheDocument();
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

  it('omits suggested route for unknown legacy tabs', async () => {
    renderRouterAt('/?tab=unknown');

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: '404: Legacy URL is no longer supported' })).toBeInTheDocument();
    });
    expect(window.location.pathname).toBe('/');
    expect(window.location.search).toBe('?tab=unknown');
    expect(screen.getByText('Requested URL:')).toBeInTheDocument();
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

  it('redirects legacy /settings path to /market-db', async () => {
    renderRouterAt('/settings');

    await waitFor(() => {
      expect(window.location.pathname).toBe('/market-db');
    });
    expect(screen.getByRole('heading', { name: 'Market DB Page' })).toBeInTheDocument();
  });
});
