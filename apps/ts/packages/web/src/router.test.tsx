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
  SettingsPage: () => <h1>Settings Page</h1>,
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

  it('supports legacy ?tab= query links', async () => {
    renderRouterAt('/?tab=history');

    await waitFor(() => {
      expect(window.location.pathname).toBe('/history');
    });
    expect(screen.getByRole('heading', { name: 'History Page' })).toBeInTheDocument();
  });

  it('renders settings page when path is /settings', async () => {
    renderRouterAt('/settings');

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Settings Page' })).toBeInTheDocument();
    });
    expect(window.location.pathname).toBe('/settings');
  });
});
