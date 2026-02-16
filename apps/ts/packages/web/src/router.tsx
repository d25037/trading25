import { Outlet, createRootRoute, createRoute, createRouter, redirect } from '@tanstack/react-router';
import { MainLayout } from '@/components/Layout/MainLayout';
import { AnalysisPage } from '@/pages/AnalysisPage';
import { BacktestPage } from '@/pages/BacktestPage';
import { ChartsPage } from '@/pages/ChartsPage';
import { HistoryPage } from '@/pages/HistoryPage';
import { IndicesPage } from '@/pages/IndicesPage';
import { PortfolioPage } from '@/pages/PortfolioPage';
import { SettingsPage } from '@/pages/SettingsPage';

const LEGACY_TAB_ROUTE_MAP = {
  charts: '/charts',
  portfolio: '/portfolio',
  indices: '/indices',
  analysis: '/analysis',
  backtest: '/backtest',
  history: '/history',
  settings: '/settings',
} as const;

type LegacyTab = keyof typeof LEGACY_TAB_ROUTE_MAP;

function isLegacyTab(value: string): value is LegacyTab {
  return value in LEGACY_TAB_ROUTE_MAP;
}

function RootLayout() {
  return (
    <MainLayout>
      <Outlet />
    </MainLayout>
  );
}

const rootRoute = createRootRoute({
  component: RootLayout,
});

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  beforeLoad: ({ search }) => {
    const legacyTab = (search as Record<string, unknown>).tab;
    if (typeof legacyTab === 'string' && isLegacyTab(legacyTab)) {
      throw redirect({ to: LEGACY_TAB_ROUTE_MAP[legacyTab] });
    }
    throw redirect({ to: '/charts' });
  },
});

const chartsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/charts',
  component: ChartsPage,
});

const portfolioRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/portfolio',
  component: PortfolioPage,
});

const indicesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/indices',
  component: IndicesPage,
});

const analysisRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/analysis',
  component: AnalysisPage,
});

const backtestRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/backtest',
  component: BacktestPage,
});

const historyRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/history',
  component: HistoryPage,
});

const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/settings',
  component: SettingsPage,
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  chartsRoute,
  portfolioRoute,
  indicesRoute,
  analysisRoute,
  backtestRoute,
  historyRoute,
  settingsRoute,
]);

export function createAppRouter() {
  return createRouter({ routeTree });
}

export const router = createAppRouter();

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
