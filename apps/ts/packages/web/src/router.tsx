import { createRootRoute, createRoute, createRouter, Link, Outlet, redirect } from '@tanstack/react-router';
import { MainLayout } from '@/components/Layout/MainLayout';
import {
  getRankingStateFromScreeningSearch,
  serializeRankingSearch,
  validateBacktestSearch,
  validateSymbolWorkbenchSearch,
  validateIndicesSearch,
  validateOptions225Search,
  validatePortfolioSearch,
  validateResearchSearch,
  validateRankingSearch,
  validateScreeningSearch,
} from '@/lib/routeSearch';
import { BacktestPage } from '@/pages/BacktestPage';
import { SymbolWorkbenchPage } from '@/pages/SymbolWorkbenchPage';
import { HistoryPage } from '@/pages/HistoryPage';
import { IndicesPage } from '@/pages/IndicesPage';
import { N225OptionsPage } from '@/pages/N225OptionsPage';
import { PortfolioPage } from '@/pages/PortfolioPage';
import { ResearchDetailPage } from '@/pages/ResearchDetailPage';
import { RankingPage } from '@/pages/RankingPage';
import { ResearchPage } from '@/pages/ResearchPage';
import { ScreeningPage } from '@/pages/ScreeningPage';
import { SettingsPage } from '@/pages/SettingsPage';

const CANONICAL_SYMBOL_WORKBENCH_PATH = '/symbol-workbench';

const LEGACY_TAB_ROUTE_MAP = {
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
      throw redirect({ to: CANONICAL_SYMBOL_WORKBENCH_PATH });
    }
  },
  component: LegacyTabMigrationPage,
});

export const symbolWorkbenchRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: CANONICAL_SYMBOL_WORKBENCH_PATH,
  validateSearch: validateSymbolWorkbenchSearch,
  component: SymbolWorkbenchPage,
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

export const researchRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/research',
  component: ResearchPage,
});

export const researchDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/research/detail',
  validateSearch: validateResearchSearch,
  beforeLoad: ({ search }) => {
    if (!search.experimentId) {
      throw redirect({ to: '/research' });
    }
  },
  component: ResearchDetailPage,
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
  symbolWorkbenchRoute,
  portfolioRoute,
  indicesRoute,
  researchRoute,
  researchDetailRoute,
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
        <Link to={CANONICAL_SYMBOL_WORKBENCH_PATH} className="rounded-md border px-4 py-2 text-sm">
          Go to {CANONICAL_SYMBOL_WORKBENCH_PATH}
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
