import { Link, Outlet, createRootRoute, createRoute, createRouter, redirect } from '@tanstack/react-router';
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

type IndexRouteSearch = {
  tab?: string;
};

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
  validateSearch: (search): IndexRouteSearch => {
    const tab = typeof search.tab === 'string' ? search.tab : undefined;
    return { tab };
  },
  beforeLoad: ({ search }) => {
    if (!search.tab) {
      throw redirect({ to: '/charts' });
    }
  },
  component: LegacyTabMigrationPage,
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

function LegacyTabMigrationPage() {
  const { tab } = indexRoute.useSearch();
  const suggestedPath = tab && isLegacyTab(tab) ? LEGACY_TAB_ROUTE_MAP[tab] : null;

  return (
    <div className="mx-auto max-w-2xl py-10 text-center">
      <h1 className="text-2xl font-semibold tracking-tight">404: Legacy URL is no longer supported</h1>
      <p className="mt-3 text-sm text-muted-foreground">
        The <code>?tab=</code> query format has been removed. Use path-based routes instead.
      </p>
      {tab ? (
        <p className="mt-2 text-sm text-muted-foreground">
          Requested URL: <code>?tab={tab}</code>
        </p>
      ) : null}
      <div className="mt-6 flex flex-wrap items-center justify-center gap-3">
        {suggestedPath ? (
          <Link to={suggestedPath} className="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground">
            Open suggested route ({suggestedPath})
          </Link>
        ) : null}
        <Link to="/charts" className="rounded-md border px-4 py-2 text-sm">
          Go to /charts
        </Link>
      </div>
    </div>
  );
}

export function createAppRouter() {
  return createRouter({ routeTree });
}

export const router = createAppRouter();

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
