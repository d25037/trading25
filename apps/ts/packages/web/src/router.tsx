import { createRootRoute, createRoute, createRouter, Outlet, redirect } from '@tanstack/react-router';
import { type ComponentType, lazy, Suspense } from 'react';
import { MainLayout } from '@/components/Layout/MainLayout';
import {
  validateBacktestSearch,
  validateIndicesSearch,
  validateOptions225Search,
  validateRankingSearch,
  validateResearchSearch,
  validateScreeningSearch,
  validateSymbolWorkbenchSearch,
  validateWatchlistSearch,
} from '@/lib/routeSearch';

const CANONICAL_SYMBOL_WORKBENCH_PATH = '/symbol-workbench';

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
const WatchlistPage = createLazyRouteComponent(() =>
  import('@/pages/WatchlistPage').then((module) => ({ default: module.WatchlistPage }))
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
  beforeLoad: () => {
    throw redirect({ to: CANONICAL_SYMBOL_WORKBENCH_PATH });
  },
});

export const symbolWorkbenchRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: CANONICAL_SYMBOL_WORKBENCH_PATH,
  validateSearch: validateSymbolWorkbenchSearch,
  component: SymbolWorkbenchPage,
});

export const watchlistRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/watchlist',
  validateSearch: validateWatchlistSearch,
  component: WatchlistPage,
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

const routeTree = rootRoute.addChildren([
  indexRoute,
  symbolWorkbenchRoute,
  watchlistRoute,
  indicesRoute,
  researchRoute,
  researchDetailRoute,
  options225Route,
  screeningRoute,
  rankingRoute,
  backtestRoute,
  historyRoute,
  marketDbRoute,
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
