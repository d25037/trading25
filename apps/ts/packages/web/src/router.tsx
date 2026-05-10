import { createRootRoute, createRoute, createRouter, Link, Outlet, redirect } from '@tanstack/react-router';
import { type ComponentType, lazy, Suspense } from 'react';
import { MainLayout } from '@/components/Layout/MainLayout';
import {
  getRankingStateFromScreeningSearch,
  serializeRankingSearch,
  validateBacktestSearch,
  validateIndicesSearch,
  validateOptions225Search,
  validatePortfolioSearch,
  validateRankingSearch,
  validateResearchSearch,
  validateScreeningSearch,
  validateSymbolWorkbenchSearch,
} from '@/lib/routeSearch';
import { DEFAULT_VALUE_COMPOSITE_RANKING_PARAMS } from '@/stores/screeningStore';

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

function PageLoadingFallback() {
  return (
    <div className="flex min-h-[12rem] items-center justify-center p-6 text-sm text-muted-foreground">
      Loading page...
    </div>
  );
}

function createLazyRouteComponent(load: () => Promise<{ default: ComponentType }>) {
  const LazyRoutePage = lazy(load);

  return function LazyRouteComponent() {
    return (
      <Suspense fallback={<PageLoadingFallback />}>
        <LazyRoutePage />
      </Suspense>
    );
  };
}

const SymbolWorkbenchPage = createLazyRouteComponent(() =>
  import('@/pages/SymbolWorkbenchPage').then((module) => ({ default: module.SymbolWorkbenchPage }))
);
const PortfolioPage = createLazyRouteComponent(() =>
  import('@/pages/PortfolioPage').then((module) => ({ default: module.PortfolioPage }))
);
const IndicesPage = createLazyRouteComponent(() =>
  import('@/pages/IndicesPage').then((module) => ({ default: module.IndicesPage }))
);
const ResearchPage = createLazyRouteComponent(() =>
  import('@/pages/ResearchPage').then((module) => ({ default: module.ResearchPage }))
);
const ResearchDetailPage = createLazyRouteComponent(() =>
  import('@/pages/ResearchDetailPage').then((module) => ({ default: module.ResearchDetailPage }))
);
const N225OptionsPage = createLazyRouteComponent(() =>
  import('@/pages/N225OptionsPage').then((module) => ({ default: module.N225OptionsPage }))
);
const ScreeningPage = createLazyRouteComponent(() =>
  import('@/pages/ScreeningPage').then((module) => ({ default: module.ScreeningPage }))
);
const RankingPage = createLazyRouteComponent(() =>
  import('@/pages/RankingPage').then((module) => ({ default: module.RankingPage }))
);
const BacktestPage = createLazyRouteComponent(() =>
  import('@/pages/BacktestPage').then((module) => ({ default: module.BacktestPage }))
);
const HistoryPage = createLazyRouteComponent(() =>
  import('@/pages/HistoryPage').then((module) => ({ default: module.HistoryPage }))
);
const SettingsPage = createLazyRouteComponent(() =>
  import('@/pages/SettingsPage').then((module) => ({ default: module.SettingsPage }))
);

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
        search: serializeRankingSearch({
          ...getRankingStateFromScreeningSearch(search),
          valueCompositeRankingParams: DEFAULT_VALUE_COMPOSITE_RANKING_PARAMS,
        }),
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
