import { useQueryClient } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { HttpRequestError } from '@trading25/api-clients/base/http-client';
import type { MarketRefreshResponse } from '@trading25/contracts/types/api-response-types';
import { AlertCircle, Loader2, TrendingUp } from 'lucide-react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { ChartControls } from '@/components/Chart/ChartControls';
import type { WorkbenchLatestMetricsOverride } from '@/components/Chart/FundamentalsPanel';
import { applyShikihoChartOverlay, useMultiTimeframeChart } from '@/components/Chart/hooks/useMultiTimeframeChart';
import { ErrorBoundary } from '@/components/ErrorBoundary';
import { SplitLayout, SplitMain, SplitSidebar, Surface } from '@/components/Layout/Workspace';
import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { countVisibleFundamentalMetrics, resolveFundamentalsPanelHeightPx } from '@/constants/fundamentalMetrics';
import { useBtMarginIndicators } from '@/hooks/useBtMarginIndicators';
import { useRefreshStocks } from '@/hooks/useDbSync';
import { useFundamentals } from '@/hooks/useFundamentals';
import { useSymbolWorkbenchRouteState } from '@/hooks/usePageRouteState';
import { rankingSymbolSnapshotKeys, useRankingSymbolSnapshot } from '@/hooks/useRankingSymbolSnapshot';
import { useShikihoSnapshot } from '@/hooks/useShikihoSnapshot';
import { stockInfoKeys, useStockInfo } from '@/hooks/useStockInfo';
import { ApiError } from '@/lib/api-client';
import { composeShikihoDailyOverlay } from '@/lib/shikihoDailyOverlay';
import { useChartStore } from '@/stores/chartStore';
import { logger } from '@/utils/logger';
import {
  ChartHeader,
  type ChartHeaderMarketCaps,
  type ChartRefreshFeedback,
  resolveLatestMarketCaps,
} from './SymbolWorkbenchHeader';
import { resolveFundamentalPanelVisibility, SymbolWorkbenchPanelsContent } from './SymbolWorkbenchPanels';

type ChartRecoveryReason = 'local_stock_data_missing' | 'stock_not_found' | 'topix_data_missing' | null;

type ChartRecoveryType = 'stock_refresh' | 'market_db_sync' | null;

interface ChartErrorContext {
  message: string;
  reason: ChartRecoveryReason;
  recovery: ChartRecoveryType;
}

function getIsMobileWorkbenchLayout(): boolean {
  return (
    typeof window !== 'undefined' &&
    typeof window.matchMedia === 'function' &&
    window.matchMedia('(max-width: 1023px)').matches
  );
}

function useIsMobileWorkbenchLayout(): boolean {
  const [isMobileWorkbenchLayout, setIsMobileWorkbenchLayout] = useState(getIsMobileWorkbenchLayout);

  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return;
    const mediaQuery = window.matchMedia('(max-width: 1023px)');
    const updateLayout = () => setIsMobileWorkbenchLayout(mediaQuery.matches);
    updateLayout();
    mediaQuery.addEventListener('change', updateLayout);
    return () => mediaQuery.removeEventListener('change', updateLayout);
  }, []);

  return isMobileWorkbenchLayout;
}

function useLazySectionVisibility(rootMargin = '160px 0px') {
  const [sectionElement, setSectionElement] = useState<HTMLDivElement | null>(null);
  const [isVisible, setIsVisible] = useState(false);
  const sectionRef = useCallback((node: HTMLDivElement | null) => {
    setSectionElement(node);
  }, []);

  useEffect(() => {
    if (isVisible) return;

    if (!sectionElement) return;

    if (typeof IntersectionObserver === 'undefined') {
      setIsVisible(true);
      return;
    }

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          setIsVisible(true);
          observer.disconnect();
        }
      },
      { rootMargin }
    );

    observer.observe(sectionElement);
    return () => observer.disconnect();
  }, [isVisible, rootMargin, sectionElement]);

  return {
    sectionRef,
    isVisible,
  };
}

function shouldRenderEmptyState(isLoading: boolean, error: unknown, selectedSymbol: string | null): boolean {
  return !isLoading && !error && !selectedSymbol;
}

function shouldRenderChartPanels(
  isLoading: boolean,
  error: unknown,
  selectedSymbol: string | null,
  chartData: unknown
): boolean {
  return !isLoading && !error && !!selectedSymbol && !!chartData;
}

function getErrorDetailMessage(details: unknown, fieldName: 'reason' | 'recovery'): string | null {
  if (!Array.isArray(details)) {
    return null;
  }

  for (const detail of details) {
    if (
      detail &&
      typeof detail === 'object' &&
      'field' in detail &&
      'message' in detail &&
      detail.field === fieldName &&
      typeof detail.message === 'string'
    ) {
      return detail.message;
    }
  }

  return null;
}

function getChartErrorContext(error: unknown): ChartErrorContext {
  const fallbackMessage =
    error instanceof Error ? error.message : 'An unexpected error occurred while fetching market data';

  if (!(error instanceof ApiError) || !error.details || typeof error.details !== 'object') {
    return { message: fallbackMessage, reason: null, recovery: null };
  }

  const body = error.details as { message?: unknown; details?: unknown };
  const reason = getErrorDetailMessage(body.details, 'reason') as ChartRecoveryReason;
  const recovery = getErrorDetailMessage(body.details, 'recovery') as ChartRecoveryType;

  return {
    message: typeof body.message === 'string' ? body.message : fallbackMessage,
    reason,
    recovery,
  };
}

function buildRefreshFeedback(result: MarketRefreshResponse, selectedSymbol: string): ChartRefreshFeedback {
  const stockResult = result.results.find((item) => item.code === selectedSymbol) ?? result.results[0];

  if (!stockResult) {
    return {
      tone: result.failedCount > 0 ? 'error' : 'success',
      message: result.failedCount > 0 ? 'Stock refresh failed.' : `Refreshed ${selectedSymbol}.`,
    };
  }

  if (!stockResult.success) {
    return {
      tone: 'error',
      message: stockResult.error ?? `Failed to refresh ${stockResult.code}.`,
    };
  }

  return {
    tone: 'success',
    message: `${stockResult.code} refreshed: fetched ${stockResult.recordsFetched}, stored ${stockResult.recordsStored}.`,
  };
}

function invalidateSelectedSymbolQueries(queryClient: ReturnType<typeof useQueryClient>, selectedSymbol: string): void {
  void Promise.all([
    queryClient.invalidateQueries({ queryKey: ['bt-ohlcv', 'resample', selectedSymbol] }),
    queryClient.invalidateQueries({ queryKey: ['bt-indicators', 'compute', selectedSymbol] }),
    queryClient.invalidateQueries({ queryKey: ['bt-signals', 'compute', selectedSymbol] }),
    queryClient.invalidateQueries({ queryKey: ['fundamentals', 'v2', selectedSymbol] }),
    queryClient.invalidateQueries({ queryKey: ['bt-margin', selectedSymbol] }),
    queryClient.invalidateQueries({ queryKey: stockInfoKeys.detail(selectedSymbol) }),
    queryClient.invalidateQueries({ queryKey: rankingSymbolSnapshotKeys.detail(selectedSymbol) }),
    queryClient.invalidateQueries({ queryKey: ['db-stats'] }),
    queryClient.invalidateQueries({ queryKey: ['db-validation'] }),
  ]);
}

function LoadingState({ selectedSymbol }: { selectedSymbol: string | null }) {
  return (
    <Surface className="flex min-h-[20rem] items-center justify-center px-6 py-12">
      <div className="space-y-4 text-center">
        <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-2xl bg-[var(--app-surface-muted)] text-primary">
          <Loader2 className="h-6 w-6 animate-spin" />
        </div>
        <div className="space-y-1">
          <p className="text-lg font-semibold text-foreground">Loading chart data...</p>
          <p className="text-sm text-muted-foreground">Fetching latest market data for {selectedSymbol}</p>
        </div>
      </div>
    </Surface>
  );
}

function ErrorState({ error }: { error: unknown }) {
  const context = getChartErrorContext(error);
  const showStockRefreshGuidance =
    context.reason === 'local_stock_data_missing' || context.recovery === 'stock_refresh';
  const showMarketDbGuidance = context.reason === 'topix_data_missing';

  return (
    <Surface className="flex min-h-[20rem] items-center justify-center px-6 py-12">
      <div className="max-w-xl space-y-5 text-center">
        <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-2xl bg-destructive/10 text-destructive">
          <AlertCircle className="h-6 w-6" />
        </div>

        <div className="space-y-2">
          <h3 className="text-xl font-semibold text-foreground">Unable to load chart data</h3>
          <p className="text-sm text-muted-foreground">{context.message}</p>
          {showStockRefreshGuidance && (
            <p className="text-sm text-emerald-700">
              Local stock history is missing for this symbol. Use Stock Refresh above to restore the DuckDB snapshot.
            </p>
          )}
          {showMarketDbGuidance && (
            <p className="text-sm text-amber-700">
              Relative mode requires local TOPIX data. Run incremental Market DB sync to restore the benchmark snapshot.
            </p>
          )}
        </div>

        <div className="flex justify-center gap-3">
          <Button variant="outline" onClick={() => window.location.reload()}>
            Try Again
          </Button>
          {showMarketDbGuidance && (
            <Button variant="default" asChild>
              <a href="/market-db">Open Market DB</a>
            </Button>
          )}
        </div>
      </div>
    </Surface>
  );
}

function FundamentalsErrorNotice({ error }: { error: unknown }) {
  const message = error instanceof Error ? error.message : 'Unable to load fundamentals data.';
  const correlationId = error instanceof HttpRequestError ? error.correlationId : undefined;
  const showMarketDbSyncRecovery =
    error instanceof HttpRequestError && error.status === 409 && error.recovery === 'market_db_sync';

  return (
    <Surface className="border-amber-500/30 bg-amber-500/10 px-4 py-3" role="alert">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="min-w-0 space-y-1">
          <p className="text-sm font-semibold text-amber-800 dark:text-amber-200">Unable to load fundamentals</p>
          <p className="text-sm text-foreground">{message}</p>
          {correlationId ? (
            <p className="text-xs text-muted-foreground">Correlation ID: {correlationId}</p>
          ) : null}
        </div>
        {showMarketDbSyncRecovery ? (
          <Button variant="outline" asChild className="shrink-0">
            <Link to="/market-db">Open Market DB sync</Link>
          </Button>
        ) : null}
      </div>
    </Surface>
  );
}

function EmptyState({ onSelectSymbol }: { onSelectSymbol: (symbol: string) => void }) {
  return (
    <Surface className="flex min-h-[24rem] items-center justify-center px-6 py-12">
      <div className="max-w-lg space-y-6 text-center">
        <div className="mx-auto flex h-16 w-16 items-center justify-center rounded-[1.25rem] bg-[var(--app-surface-muted)] text-primary">
          <TrendingUp className="h-8 w-8" />
        </div>

        <div className="space-y-2">
          <h3 className="text-2xl font-semibold text-foreground">Start Trading Analysis</h3>
          <p className="text-sm text-muted-foreground">
            Enter a stock symbol in the search rail to open price history, overlays, and supporting analytics.
          </p>
        </div>

        <div className="space-y-3">
          <p className="text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">Popular symbols</p>
          <div className="flex flex-wrap justify-center gap-2">
            {['7203', '6758', '8306', '9984', '4502'].map((symbol) => (
              <Button
                key={symbol}
                variant="outline"
                size="sm"
                onClick={() => {
                  logger.debug('Symbol selected from popular list', { symbol });
                  onSelectSymbol(symbol);
                }}
              >
                {symbol}
              </Button>
            ))}
          </div>
        </div>
      </div>
    </Surface>
  );
}

export function SymbolWorkbenchPage() {
  const queryClient = useQueryClient();
  const marginSection = useLazySectionVisibility();
  const fundamentalsPanelSection = useLazySectionVisibility();
  const fundamentalsHistorySection = useLazySectionVisibility();
  const factorSection = useLazySectionVisibility();
  const { selectedSymbol, strategyName, matchedDate, setSelectedSymbol } = useSymbolWorkbenchRouteState();

  const {
    chartData: officialChartData,
    signalMarkers,
    signalResponse,
    isLoading,
    error,
  } = useMultiTimeframeChart(selectedSymbol, strategyName);
  const { settings } = useChartStore();
  const isMobileWorkbenchLayout = useIsMobileWorkbenchLayout();
  const refreshStocks = useRefreshStocks();
  const [refreshFeedback, setRefreshFeedback] = useState<ChartRefreshFeedback | null>(null);
  const [isMobileSettingsOpen, setIsMobileSettingsOpen] = useState(false);
  const shouldFetchMarginPressure = settings.showMarginPressurePanel && marginSection.isVisible;
  const shouldFetchFundamentals = selectedSymbol != null;
  const {
    data: marginPressureData,
    isLoading: marginPressureLoading,
    error: marginPressureError,
  } = useBtMarginIndicators(selectedSymbol, { enabled: shouldFetchMarginPressure });
  const { data: stockInfo } = useStockInfo(selectedSymbol);
  const rankingSnapshotQuery = useRankingSymbolSnapshot(selectedSymbol);
  const shikihoSnapshot = useShikihoSnapshot(selectedSymbol);
  const tradingValuePeriod = Math.max(1, Math.trunc(settings.tradingValueMA.period ?? 15));
  const { data: fundamentalsData, error: fundamentalsError } = useFundamentals(selectedSymbol, {
    enabled: shouldFetchFundamentals,
    tradingValuePeriod,
  });
  useEffect(() => {
    if (selectedSymbol == null) {
      setRefreshFeedback(null);
      return;
    }
    setRefreshFeedback(null);
  }, [selectedSymbol]);

  const officialMarketCaps = useMemo<ChartHeaderMarketCaps>(() => {
    return resolveLatestMarketCaps(fundamentalsData?.dailyValuation);
  }, [fundamentalsData?.dailyValuation]);
  const dailyOverlay = useMemo(
    () =>
      composeShikihoDailyOverlay({
        selectedSymbol,
        quoteCode: shikihoSnapshot.snapshot?.code ?? null,
        quote: shikihoSnapshot.snapshot?.quote,
        snapshotCapturedAt: shikihoSnapshot.snapshot?.capturedAt,
        dailyBars: officialChartData?.daily?.candlestickData ?? [],
        rankingResponse: rankingSnapshotQuery.data,
        latestValuation: fundamentalsData?.dailyValuation?.at(-1),
        marketCaps: officialMarketCaps,
        relativeMode: settings.relativeMode,
        chartSmaPeriod: settings.indicators.sma.enabled ? settings.indicators.sma.period : undefined,
      }),
    [
      selectedSymbol,
      shikihoSnapshot.snapshot,
      officialChartData?.daily?.candlestickData,
      rankingSnapshotQuery.data,
      fundamentalsData?.dailyValuation,
      officialMarketCaps,
      settings.relativeMode,
      settings.indicators.sma.enabled,
      settings.indicators.sma.period,
    ]
  );
  const provisionalLabel = dailyOverlay.provenance ? '四季報 15分遅延・当日暫定' : null;
  const latestMetricsOverride = useMemo<WorkbenchLatestMetricsOverride | undefined>(() => {
    const latestMetrics = fundamentalsData?.latestMetrics;
    const valuation = fundamentalsData?.dailyValuation?.at(-1);
    const quote = shikihoSnapshot.snapshot?.quote;
    if (!dailyOverlay.provenance || latestMetrics == null || quote == null) return undefined;
    const officialPrice = valuation?.close ?? latestMetrics.stockPrice;
    const priceRatio = officialPrice != null && officialPrice > 0 ? quote.currentPrice / officialPrice : null;
    const scaleFallback = (value: number | null | undefined): number | null | undefined => {
      if (value == null) return value;
      return priceRatio === null ? null : value * priceRatio;
    };
    const priceRatioMetric = (
      denominator: number | null | undefined,
      fallback: number | null | undefined
    ): number | null | undefined =>
      denominator != null && denominator > 0 ? quote.currentPrice / denominator : scaleFallback(fallback);
    const issuedMarketCap = dailyOverlay.marketCaps.issuedShares;
    const marketCapRatioMetric = (
      denominator: number | null | undefined,
      fallback: number | null | undefined
    ): number | null | undefined =>
      issuedMarketCap != null && denominator != null && denominator > 0
        ? issuedMarketCap / denominator
        : scaleFallback(fallback);
    return {
      stockPrice: quote.currentPrice,
      per: priceRatioMetric(valuation?.eps, latestMetrics.per) ?? null,
      forwardPer: priceRatioMetric(valuation?.forwardEps, latestMetrics.forwardPer),
      pbr: priceRatioMetric(valuation?.bps, latestMetrics.pbr) ?? null,
      psr: marketCapRatioMetric(valuation?.sales, latestMetrics.psr),
      forwardPsr: marketCapRatioMetric(valuation?.forwardSales, latestMetrics.forwardPsr),
    };
  }, [
    dailyOverlay.marketCaps.issuedShares,
    dailyOverlay.provenance,
    fundamentalsData?.dailyValuation,
    fundamentalsData?.latestMetrics,
    shikihoSnapshot.snapshot?.quote,
  ]);
  const chartData = useMemo(
    () =>
      officialChartData === null
        ? null
        : applyShikihoChartOverlay(
            officialChartData,
            {
              dailyBars: dailyOverlay.dailyBars,
              chartSmaPoint: dailyOverlay.chartSmaPoint,
              provenance: dailyOverlay.provenance,
            },
            settings.relativeMode
          ),
    [officialChartData, dailyOverlay, settings.relativeMode]
  );
  const showEmptyState = shouldRenderEmptyState(isLoading, error, selectedSymbol);
  const showChartPanels = shouldRenderChartPanels(isLoading, error, selectedSymbol, chartData);

  logger.debug('SymbolWorkbenchPage render', {
    selectedSymbol,
    isLoading,
    error: error?.message,
    hasChartData: !!chartData,
  });
  const visibleFundamentalMetricCount = useMemo(
    () => countVisibleFundamentalMetrics(settings.fundamentalsMetricOrder, settings.fundamentalsMetricVisibility),
    [settings.fundamentalsMetricOrder, settings.fundamentalsMetricVisibility]
  );
  const fundamentalsPanelHeight = useMemo(
    () => resolveFundamentalsPanelHeightPx(visibleFundamentalMetricCount),
    [visibleFundamentalMetricCount]
  );

  const panelVisibilityById = resolveFundamentalPanelVisibility(settings);
  const handleSelectSymbol = useCallback(
    (symbol: string) => {
      setSelectedSymbol(symbol);
      if (isMobileWorkbenchLayout) {
        setIsMobileSettingsOpen(false);
      }
    },
    [isMobileWorkbenchLayout, setSelectedSymbol]
  );
  const handleRefresh = useCallback(() => {
    if (!selectedSymbol) {
      return;
    }

    setRefreshFeedback(null);
    refreshStocks.mutate(
      { codes: [selectedSymbol] },
      {
        onSuccess: (result) => {
          setRefreshFeedback(buildRefreshFeedback(result, selectedSymbol));
          invalidateSelectedSymbolQueries(queryClient, selectedSymbol);
        },
        onError: (mutationError) => {
          setRefreshFeedback({
            tone: 'error',
            message: mutationError instanceof Error ? mutationError.message : 'Stock refresh failed.',
          });
        },
      }
    );
  }, [queryClient, refreshStocks, selectedSymbol]);

  return (
    <SplitLayout className="min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-3 sm:p-4 lg:flex-row lg:items-stretch lg:overflow-hidden">
      <h1 className="sr-only">Symbol Workbench</h1>
      {!isMobileWorkbenchLayout || !selectedSymbol ? (
        <SplitSidebar className="w-full lg:w-[18rem]">
          <Surface className="h-full min-h-0 overflow-hidden">
            <ErrorBoundary>
              <ChartControls selectedSymbol={selectedSymbol} onSelectSymbol={handleSelectSymbol} />
            </ErrorBoundary>
          </Surface>
        </SplitSidebar>
      ) : null}

      <Dialog open={isMobileSettingsOpen} onOpenChange={setIsMobileSettingsOpen}>
        <DialogContent className="flex h-[calc(100dvh-1.5rem)] max-h-none max-w-none translate-y-[-50%] flex-col overflow-hidden p-0 sm:max-w-lg sm:rounded-lg lg:hidden">
          <DialogHeader className="border-b border-border/60 px-4 py-3 text-left">
            <DialogTitle>Symbol Workbench Settings</DialogTitle>
            <DialogDescription>Search, chart settings, panel order, and signal controls.</DialogDescription>
          </DialogHeader>
          <div className="min-h-0 flex-1 overflow-y-auto">
            <ChartControls selectedSymbol={selectedSymbol} onSelectSymbol={handleSelectSymbol} />
          </div>
        </DialogContent>
      </Dialog>

      <SplitMain className="min-h-0 gap-3 lg:overflow-y-auto lg:pr-1">
        {selectedSymbol && (
          <ChartHeader
            settings={settings}
            selectedSymbol={selectedSymbol}
            stockInfo={stockInfo}
            latestMarketCaps={dailyOverlay.marketCaps}
            rankingSnapshot={dailyOverlay.rankingResponse}
            rankingSnapshotLoading={rankingSnapshotQuery.isLoading}
            rankingSnapshotError={rankingSnapshotQuery.error}
            onRetryRankingSnapshot={() => void rankingSnapshotQuery.refetch()}
            shikihoSnapshot={shikihoSnapshot.displaySnapshot}
            shikihoCanonicalSnapshot={shikihoSnapshot.snapshot}
            shikihoCandidate={shikihoSnapshot.candidate}
            shikihoTrace={shikihoSnapshot.trace}
            shikihoDiagnostic={shikihoSnapshot.diagnostic}
            shikihoCaptureState={shikihoSnapshot.captureState}
            shikihoProvenance={dailyOverlay.provenance}
            isShikihoRefreshing={shikihoSnapshot.isRefreshing}
            onRefreshShikiho={shikihoSnapshot.refresh}
            onSelectSymbol={handleSelectSymbol}
            strategyName={strategyName}
            matchedDate={matchedDate}
            signalProvenance={signalResponse?.provenance}
            signalDiagnostics={signalResponse?.diagnostics}
            fundamentalsProvenance={fundamentalsData?.provenance}
            refreshFeedback={refreshFeedback}
            isRefreshing={refreshStocks.isPending}
            onRefresh={handleRefresh}
            onOpenMobileSettings={() => setIsMobileSettingsOpen(true)}
          />
        )}
        {fundamentalsError ? <FundamentalsErrorNotice error={fundamentalsError} /> : null}
        {error && <ErrorState error={error} />}
        {isLoading && <LoadingState selectedSymbol={selectedSymbol} />}
        {showEmptyState && <EmptyState onSelectSymbol={setSelectedSymbol} />}
        {showChartPanels && chartData && (
          <SymbolWorkbenchPanelsContent
            settings={settings}
            selectedSymbol={selectedSymbol}
            chartData={chartData}
            signalMarkers={signalMarkers}
            panelVisibilityById={panelVisibilityById}
            fundamentalsPanelHeight={fundamentalsPanelHeight}
            tradingValuePeriod={tradingValuePeriod}
            fundamentalsPanelSection={fundamentalsPanelSection}
            fundamentalsHistorySection={fundamentalsHistorySection}
            marginSection={marginSection}
            factorSection={factorSection}
            marginPressureData={marginPressureData}
            marginPressureLoading={marginPressureLoading}
            marginPressureError={marginPressureError}
            isMobileWorkbenchLayout={isMobileWorkbenchLayout}
            latestMetricsOverride={latestMetricsOverride}
            provisionalLabel={provisionalLabel}
            provisionalDate={dailyOverlay.provenance?.tradingDate ?? null}
            suppressFundamentalsErrors={fundamentalsError != null}
          />
        )}
      </SplitMain>
    </SplitLayout>
  );
}
