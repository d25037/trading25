import { createRootRoute, createRoute, createRouter, Link, Outlet, redirect } from '@tanstack/react-router';
import {
  getRankingStateFromScreeningSearch,
  validateBacktestSearch,
  validateChartsSearch,
  validateIndicesSearch,
  validateOptions225Search,
  validatePortfolioSearch,
  validateRankingSearch,
  validateScreeningSearch,
  serializeRankingSearch,
} from '@/lib/routeSearch';
import { MainLayout } from '@/components/Layout/MainLayout';
import { BacktestPage } from '@/pages/BacktestPage';
import { ChartsPage } from '@/pages/ChartsPage';
import { HistoryPage } from '@/pages/HistoryPage';
import { IndicesPage } from '@/pages/IndicesPage';
import { N225OptionsPage } from '@/pages/N225OptionsPage';
import { PortfolioPage } from '@/pages/PortfolioPage';
import { RankingPage } from '@/pages/RankingPage';
import { SettingsPage } from '@/pages/SettingsPage';
import { ScreeningPage } from '@/pages/ScreeningPage';

const LEGACY_TAB_ROUTE_MAP = {
  charts: '/charts',
  portfolio: '/portfolio',
  indices: '/indices',
  screening: '/screening',
  backtest: '/backtest',
  history: '/history',
  settings: '/market-db',
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

export const chartsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/charts',
  validateSearch: validateChartsSearch,
  component: ChartsPage,
});

export const portfolioRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/portfolio',
  validateSearch: validatePortfolioSearch,
  component: PortfolioPage,
});

export const indicesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/indices',
  validateSearch: validateIndicesSearch,
  component: IndicesPage,
});

export const options225Route = createRoute({
  getParentRoute: () => rootRoute,
  path: '/options-225',
  validateSearch: validateOptions225Search,
  component: N225OptionsPage,
});

export const screeningRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/screening',
  validateSearch: validateScreeningSearch,
  beforeLoad: ({ search }) => {
    if (search.tab === 'ranking' || search.tab === 'fundamentalRanking') {
      throw redirect({
        to: '/ranking',
        search: serializeRankingSearch(getRankingStateFromScreeningSearch(search)),
      });
    }
  },
  component: ScreeningPage,
});

export const rankingRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/ranking',
  validateSearch: validateRankingSearch,
  component: RankingPage,
});

export const backtestRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/backtest',
  validateSearch: validateBacktestSearch,
  component: BacktestPage,
});

const historyRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/history',
  component: HistoryPage,
});

const marketDbRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/market-db',
  component: SettingsPage,
});

const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/settings',
  beforeLoad: () => {
    throw redirect({ to: '/market-db' });
  },
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  chartsRoute,
  portfolioRoute,
  indicesRoute,
  options225Route,
  screeningRoute,
  rankingRoute,
  backtestRoute,
  historyRoute,
  marketDbRoute,
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
