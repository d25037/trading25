import { useQueryClient } from '@tanstack/react-query';
import type { DataProvenance, ResponseDiagnostics } from '@trading25/contracts/types/api-types';
import { AlertCircle, BookOpen, Loader2, RotateCcw, TrendingUp, Wallet } from 'lucide-react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { ChartControls } from '@/components/Chart/ChartControls';
import { FactorRegressionPanel } from '@/components/Chart/FactorRegressionPanel';
import { FundamentalsHistoryPanel } from '@/components/Chart/FundamentalsHistoryPanel';
import { FundamentalsPanel } from '@/components/Chart/FundamentalsPanel';
import { useMultiTimeframeChart } from '@/components/Chart/hooks/useMultiTimeframeChart';
import { MarginPressureChart } from '@/components/Chart/MarginPressureChart';
import { PPOChart } from '@/components/Chart/PPOChart';
import { RiskAdjustedReturnChart } from '@/components/Chart/RiskAdjustedReturnChart';
import { StockChart } from '@/components/Chart/StockChart';
import { TimeframeSelector } from '@/components/Chart/TimeframeSelector';
import { TradingValueMAChart } from '@/components/Chart/TradingValueMAChart';
import { VolumeComparisonChart } from '@/components/Chart/VolumeComparisonChart';
import { ErrorBoundary } from '@/components/ErrorBoundary';
import { SplitLayout, SplitMain, SplitSidebar } from '@/components/Layout/Workspace';
import { Button } from '@/components/ui/button';
import { countVisibleFundamentalMetrics, resolveFundamentalsPanelHeightPx } from '@/constants/fundamentalMetrics';
import { useBtMarginIndicators } from '@/hooks/useBtMarginIndicators';
import { useRefreshStocks } from '@/hooks/useDbSync';
import { useFundamentals } from '@/hooks/useFundamentals';
import { useChartsRouteState, useMigrateChartsRouteState } from '@/hooks/usePageRouteState';
import { type StockInfoResponse, stockInfoKeys, useStockInfo } from '@/hooks/useStockInfo';
import { ApiError } from '@/lib/api-client';
import { cn } from '@/lib/utils';
import { type FundamentalsPanelId, useChartStore } from '@/stores/chartStore';
import type {
  BollingerBandsData,
  IndicatorValue,
  MarginPressureIndicatorsResponse,
  PPOIndicatorData,
  RiskAdjustedReturnData,
  TradingValueMAData,
  VolumeComparisonData,
} from '@/types/chart';
import type { MarketRefreshResponse } from '@/types/sync';
import { formatMarketCap } from '@/utils/formatters';
import { logger } from '@/utils/logger';

type ChartSettings = ReturnType<typeof useChartStore.getState>['settings'];

interface LazySectionState {
  sectionRef: (node: HTMLDivElement | null) => void;
  isVisible: boolean;
}

type ChartRecoveryReason = 'local_stock_data_missing' | 'stock_not_found' | 'topix_data_missing' | null;

type ChartRecoveryType = 'stock_refresh' | 'market_db_sync' | null;

interface ChartErrorContext {
  message: string;
  reason: ChartRecoveryReason;
  recovery: ChartRecoveryType;
}

interface ChartRefreshFeedback {
  tone: 'success' | 'error';
  message: string;
}

interface ChartHeaderMarketCaps {
  freeFloat: number | null;
  issuedShares: number | null;
}

function resolveLatestMarketCaps(
  dailyValuation:
    | Array<{
        freeFloatMarketCap?: number | null;
        marketCap?: number | null;
      }>
    | null
    | undefined
): ChartHeaderMarketCaps {
  if (!dailyValuation || dailyValuation.length === 0) {
    return {
      freeFloat: null,
      issuedShares: null,
    };
  }

  const latest = dailyValuation[dailyValuation.length - 1];
  return {
    freeFloat: latest?.freeFloatMarketCap ?? null,
    issuedShares: latest?.marketCap ?? null,
  };
}

function ChartHeaderInfoField({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 rounded-lg border border-border/60 bg-card/72 px-3 py-2">
      <div className="text-[11px] font-medium uppercase tracking-[0.08em] text-muted-foreground">{label}</div>
      <div className="truncate text-sm font-semibold text-foreground">{value}</div>
    </div>
  );
}

function ChartHeaderMetaChip({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-full border border-border/60 bg-background/78 px-3 py-1 text-xs shadow-sm shadow-black/5">
      <span className="text-muted-foreground">{label}: </span>
      <span className="font-medium text-foreground">{value}</span>
    </div>
  );
}

function formatOptionalDate(value: string | null | undefined): string {
  if (!value) return '-';
  return value;
}

function formatList(values: string[] | null | undefined): string {
  if (!values || values.length === 0) return '-';
  return values.join(', ');
}

function mergeUniqueStrings(...groups: Array<string[] | null | undefined>): string[] {
  const seen = new Set<string>();
  for (const group of groups) {
    for (const value of group ?? []) {
      if (value) {
        seen.add(value);
      }
    }
  }
  return [...seen];
}

function mergeWarnings(...groups: Array<ResponseDiagnostics | DataProvenance | null | undefined>): string[] {
  return mergeUniqueStrings(...groups.map((group) => group?.warnings));
}

// Helper component for margin pressure indicators section
function MarginPressureIndicatorsSection({
  data,
  isLoading,
  error,
}: {
  data: MarginPressureIndicatorsResponse | undefined;
  isLoading: boolean;
  error: Error | null;
}) {
  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="h-6 w-6 animate-spin" />
        <span className="ml-2 text-muted-foreground">Loading...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-sm text-muted-foreground">Failed to load margin pressure data</p>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-sm text-muted-foreground">No data available</p>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-3 gap-4 h-full">
      <ErrorBoundary>
        <MarginPressureChart type="longPressure" longPressureData={data.longPressure} />
      </ErrorBoundary>
      <ErrorBoundary>
        <MarginPressureChart type="flowPressure" flowPressureData={data.flowPressure} />
      </ErrorBoundary>
      <ErrorBoundary>
        <MarginPressureChart type="turnoverDays" turnoverDaysData={data.turnoverDays} />
      </ErrorBoundary>
    </div>
  );
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
    queryClient.invalidateQueries({ queryKey: ['db-stats'] }),
    queryClient.invalidateQueries({ queryKey: ['db-validation'] }),
  ]);
}

function resolveFundamentalPanelVisibility(settings: ChartSettings): Record<FundamentalsPanelId, boolean> {
  return {
    fundamentals: settings.showFundamentalsPanel,
    fundamentalsHistory: settings.showFundamentalsHistoryPanel,
    marginPressure: settings.showMarginPressurePanel,
    factorRegression: settings.showFactorRegressionPanel,
  };
}

function renderOrderedPanelSection({
  panelId,
  selectedSymbol,
  settings,
  fundamentalsPanelHeight,
  tradingValuePeriod,
  fundamentalsPanelSection,
  fundamentalsHistorySection,
  marginSection,
  factorSection,
  marginPressureData,
  marginPressureLoading,
  marginPressureError,
}: {
  panelId: FundamentalsPanelId;
  selectedSymbol: string | null;
  settings: ChartSettings;
  fundamentalsPanelHeight: number;
  tradingValuePeriod: number;
  fundamentalsPanelSection: LazySectionState;
  fundamentalsHistorySection: LazySectionState;
  marginSection: LazySectionState;
  factorSection: LazySectionState;
  marginPressureData: MarginPressureIndicatorsResponse | undefined;
  marginPressureLoading: boolean;
  marginPressureError: Error | null;
}) {
  switch (panelId) {
    case 'fundamentals':
      return (
        <div
          key={panelId}
          ref={fundamentalsPanelSection.sectionRef}
          data-testid="fundamentals-panel-section"
          style={{ height: `${fundamentalsPanelHeight}px` }}
        >
          <div className={cn('h-full rounded-xl glass-panel', 'relative overflow-hidden')}>
            <div className="absolute inset-0 gradient-glass opacity-50" />
            <div className="relative z-10 h-full">
              <div className="p-4 border-b border-border/30">
                <h3 className="text-lg font-semibold text-foreground">Fundamental Analysis</h3>
              </div>
              <div className="h-[calc(100%-4rem)] p-4">
                <ErrorBoundary>
                  <FundamentalsPanel
                    symbol={selectedSymbol}
                    enabled={fundamentalsPanelSection.isVisible}
                    tradingValuePeriod={tradingValuePeriod}
                    metricOrder={settings.fundamentalsMetricOrder}
                    metricVisibility={settings.fundamentalsMetricVisibility}
                  />
                </ErrorBoundary>
              </div>
            </div>
          </div>
        </div>
      );
    case 'fundamentalsHistory':
      return (
        <div key={panelId} ref={fundamentalsHistorySection.sectionRef} className="h-[340px]">
          <div className={cn('h-full rounded-xl glass-panel', 'relative overflow-hidden')}>
            <div className="absolute inset-0 gradient-glass opacity-50" />
            <div className="relative z-10 h-full">
              <div className="p-4 border-b border-border/30">
                <h3 className="text-lg font-semibold text-foreground">FY推移</h3>
              </div>
              <div className="h-[calc(100%-4rem)] p-4">
                <ErrorBoundary>
                  <FundamentalsHistoryPanel
                    symbol={selectedSymbol}
                    enabled={fundamentalsHistorySection.isVisible}
                    metricOrder={settings.fundamentalsHistoryMetricOrder}
                    metricVisibility={settings.fundamentalsHistoryMetricVisibility}
                  />
                </ErrorBoundary>
              </div>
            </div>
          </div>
        </div>
      );
    case 'marginPressure':
      return (
        <div key={panelId} ref={marginSection.sectionRef} className="h-72">
          <div className={cn('h-full rounded-xl glass-panel', 'relative overflow-hidden')}>
            <div className="absolute inset-0 gradient-glass opacity-50" />
            <div className="relative z-10 h-full">
              <div className="p-4 border-b border-border/30">
                <h3 className="text-lg font-semibold text-foreground">
                  信用圧力指標
                  {marginPressureData && (
                    <span className="text-sm font-normal text-muted-foreground ml-2">
                      ({marginPressureData.averagePeriod}日平均)
                    </span>
                  )}
                </h3>
              </div>
              <div className="h-[calc(100%-4rem)] p-4">
                <MarginPressureIndicatorsSection
                  data={marginPressureData}
                  isLoading={marginPressureLoading}
                  error={marginPressureError}
                />
              </div>
            </div>
          </div>
        </div>
      );
    case 'factorRegression':
      return (
        <div key={panelId} ref={factorSection.sectionRef} className="h-64">
          <div className={cn('h-full rounded-xl glass-panel', 'relative overflow-hidden')}>
            <div className="absolute inset-0 gradient-glass opacity-50" />
            <div className="relative z-10 h-full">
              <div className="p-4 border-b border-border/30">
                <h3 className="text-lg font-semibold text-foreground">Factor Regression Analysis</h3>
              </div>
              <div className="h-[calc(100%-4rem)] p-4">
                <ErrorBoundary>
                  <FactorRegressionPanel
                    symbol={selectedSymbol}
                    enabled={settings.showFactorRegressionPanel && factorSection.isVisible}
                  />
                </ErrorBoundary>
              </div>
            </div>
          </div>
        </div>
      );
    default:
      return null;
  }
}

function LoadingState({ selectedSymbol }: { selectedSymbol: string | null }) {
  return (
    <div className="flex h-full items-center justify-center">
      <div className="text-center space-y-4">
        <div className="relative">
          <div className="gradient-primary rounded-full p-4 mx-auto w-fit">
            <Loader2 className="h-8 w-8 text-white animate-spin" />
          </div>
          <div className="absolute inset-0 gradient-primary rounded-full animate-ping opacity-20" />
        </div>

        <div className="space-y-2">
          <p className="text-lg font-semibold text-foreground">Loading chart data...</p>
          <p className="text-sm text-muted-foreground">Fetching latest market data for {selectedSymbol}</p>

          <div className="flex justify-center gap-1 mt-4">
            <div className="w-2 h-4 bg-primary/30 rounded-full animate-pulse" style={{ animationDelay: '0ms' }} />
            <div className="w-2 h-6 bg-primary/30 rounded-full animate-pulse" style={{ animationDelay: '100ms' }} />
            <div className="w-2 h-3 bg-primary/30 rounded-full animate-pulse" style={{ animationDelay: '200ms' }} />
            <div className="w-2 h-5 bg-primary/30 rounded-full animate-pulse" style={{ animationDelay: '300ms' }} />
            <div className="w-2 h-4 bg-primary/30 rounded-full animate-pulse" style={{ animationDelay: '400ms' }} />
          </div>
        </div>
      </div>
    </div>
  );
}

function ErrorState({ error }: { error: unknown }) {
  const context = getChartErrorContext(error);
  const showStockRefreshGuidance =
    context.reason === 'local_stock_data_missing' || context.recovery === 'stock_refresh';
  const showMarketDbGuidance = context.reason === 'topix_data_missing';

  return (
    <div className="flex h-full items-center justify-center">
      <div className="text-center space-y-6 max-w-md">
        <div className="relative">
          <div className="bg-destructive/10 rounded-full p-4 mx-auto w-fit">
            <AlertCircle className="h-8 w-8 text-destructive" />
          </div>
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
              Relative mode requires local TOPIX data. Run Market DB sync or repair to restore the benchmark snapshot.
            </p>
          )}
        </div>

        <div className="flex gap-3 justify-center">
          <Button variant="outline" onClick={() => window.location.reload()} className="glass-panel hover:bg-accent/50">
            Try Again
          </Button>
          {showMarketDbGuidance && (
            <Button variant="default" className="gradient-primary hover:opacity-90" asChild>
              <a href="/market-db">Open Market DB</a>
            </Button>
          )}
        </div>
      </div>
    </div>
  );
}

function EmptyState() {
  return (
    <div className="flex h-full items-center justify-center">
      <div className="text-center space-y-6 max-w-md">
        <div className="relative">
          <div className="gradient-secondary rounded-full p-8 mx-auto w-fit">
            <TrendingUp className="h-12 w-12 text-primary" />
          </div>
          <div className="absolute inset-0 gradient-glass rounded-full" />
        </div>

        <div className="space-y-3">
          <h3 className="text-2xl font-bold text-foreground">Start Trading Analysis</h3>
          <p className="text-muted-foreground">
            Enter a stock symbol in the search box to view real-time charts and technical analysis
          </p>

          <div className="pt-4">
            <p className="text-sm text-muted-foreground mb-3">Popular symbols:</p>
            <div className="flex flex-wrap gap-2 justify-center">
              {['7203', '6758', '8306', '9984', '4502'].map((symbol) => (
                <Button
                  key={symbol}
                  variant="outline"
                  size="sm"
                  className="glass-panel hover:bg-primary/10 hover:border-primary/50 transition-all duration-200"
                  onClick={() => {
                    logger.debug('Symbol selected from popular list', { symbol });
                  }}
                >
                  {symbol}
                </Button>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function openCompanyPage(baseUrl: string, selectedSymbol: string | null, suffix = '') {
  if (!selectedSymbol) return;
  window.open(`${baseUrl}${selectedSymbol}${suffix}`, '_blank', 'noopener,noreferrer');
}

function ChartRefreshFeedbackBanner({ feedback }: { feedback: ChartRefreshFeedback }) {
  const toneClassName =
    feedback.tone === 'success'
      ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-700'
      : 'border-red-500/20 bg-red-500/10 text-red-700';

  return <div className={cn('rounded-xl border px-4 py-3 text-sm', toneClassName)}>{feedback.message}</div>;
}

function ChartHeader({
  settings,
  selectedSymbol,
  stockInfo,
  latestMarketCaps,
  strategyName,
  matchedDate,
  signalProvenance,
  signalDiagnostics,
  fundamentalsProvenance,
  refreshFeedback,
  isRefreshing,
  onRefresh,
}: {
  settings: ChartSettings;
  selectedSymbol: string;
  stockInfo: StockInfoResponse | undefined;
  latestMarketCaps: ChartHeaderMarketCaps;
  strategyName: string | null;
  matchedDate: string | null;
  signalProvenance: DataProvenance | null | undefined;
  signalDiagnostics: ResponseDiagnostics | null | undefined;
  fundamentalsProvenance: DataProvenance | null | undefined;
  refreshFeedback: ChartRefreshFeedback | null;
  isRefreshing: boolean;
  onRefresh: () => void;
}) {
  const mergedLoadedDomains = mergeUniqueStrings(
    signalProvenance?.loaded_domains,
    fundamentalsProvenance?.loaded_domains
  );
  const warnings = mergeWarnings(signalProvenance, fundamentalsProvenance, signalDiagnostics);
  const marketSnapshotId = signalProvenance?.market_snapshot_id ?? fundamentalsProvenance?.market_snapshot_id ?? '-';
  let overlayLabel = '-';
  if (strategyName) {
    overlayLabel = `${strategyName} (strategy)`;
  } else if (settings.signalOverlay?.enabled) {
    overlayLabel = 'ad hoc signal overlay';
  }

  return (
    <div className="space-y-3">
      <div className="rounded-xl border border-border/70 bg-card/90 px-5 py-4 shadow-sm shadow-black/5">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-center gap-3">
            <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-primary/10 text-primary">
              <TrendingUp className="h-5 w-5" />
            </div>
            <div className="flex flex-col">
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                Selected Symbol
              </p>
              <h2 className="text-2xl font-semibold tracking-tight text-foreground">
                {selectedSymbol}
                {stockInfo?.companyName && (
                  <span className="ml-2 font-medium text-foreground">{stockInfo.companyName}</span>
                )}
                {settings.relativeMode && <span className="font-medium text-muted-foreground"> / TOPIX</span>}
              </h2>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2 lg:justify-end">
            <Button variant="outline" size="sm" onClick={onRefresh} disabled={isRefreshing}>
              {isRefreshing ? (
                <>
                  <Loader2 className="mr-1 h-4 w-4 animate-spin" />
                  Refreshing...
                </>
              ) : (
                <>
                  <RotateCcw className="mr-1 h-4 w-4" />
                  Stock Refresh
                </>
              )}
            </Button>
            <TimeframeSelector />
          </div>
        </div>
        <div className="mt-4 flex flex-wrap gap-2">
          <ChartHeaderMetaChip label="Overlay" value={overlayLabel} />
          <ChartHeaderMetaChip label="Matched Date" value={formatOptionalDate(matchedDate)} />
          <ChartHeaderMetaChip label="Market Snapshot" value={marketSnapshotId} />
          <ChartHeaderMetaChip label="Signal Domains" value={formatList(mergedLoadedDomains)} />
        </div>
      </div>

      <div className="rounded-xl border border-border/70 bg-card/82 px-5 py-4 shadow-sm shadow-black/5">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div className="grid flex-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <ChartHeaderInfoField label="セクター17" value={stockInfo?.sector17Name || '-'} />
            <ChartHeaderInfoField label="セクター33" value={stockInfo?.sector33Name || '-'} />
            <ChartHeaderInfoField label="時価総額 (Free Float)" value={formatMarketCap(latestMarketCaps.freeFloat)} />
            <ChartHeaderInfoField
              label="時価総額 (発行済み株式数)"
              value={formatMarketCap(latestMarketCaps.issuedShares)}
            />
          </div>
          <div className="flex flex-wrap items-center gap-2 lg:justify-end">
            <Button
              variant="outline"
              size="sm"
              onClick={() => openCompanyPage('https://shikiho.toyokeizai.net/stocks/', selectedSymbol)}
              title="四季報を開く"
            >
              <BookOpen className="mr-1 h-4 w-4" />
              四季報
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => openCompanyPage('https://www.buffett-code.com/company/', selectedSymbol, '/')}
              title="Buffett Codeを開く"
            >
              <Wallet className="mr-1 h-4 w-4" />
              B.C.
            </Button>
          </div>
        </div>
        {(signalProvenance?.reference_date || fundamentalsProvenance?.reference_date || warnings.length > 0) && (
          <div className="mt-4 space-y-1 border-t border-border/30 pt-4 text-xs text-muted-foreground">
            <div>
              Reference Date:{' '}
              <span className="font-medium text-foreground">
                {signalProvenance?.reference_date ?? fundamentalsProvenance?.reference_date ?? '-'}
              </span>
            </div>
            {warnings.length > 0 && (
              <div>
                Warnings: <span className="font-medium text-foreground">{warnings.join(' | ')}</span>
              </div>
            )}
          </div>
        )}
      </div>

      {refreshFeedback && <ChartRefreshFeedbackBanner feedback={refreshFeedback} />}
    </div>
  );
}

function ChartsPanelsContent({
  settings,
  selectedSymbol,
  chartData,
  signalMarkers,
  panelVisibilityById,
  fundamentalsPanelHeight,
  tradingValuePeriod,
  fundamentalsPanelSection,
  fundamentalsHistorySection,
  marginSection,
  factorSection,
  marginPressureData,
  marginPressureLoading,
  marginPressureError,
}: {
  settings: ChartSettings;
  selectedSymbol: string | null;
  chartData: ReturnType<typeof useMultiTimeframeChart>['chartData'];
  signalMarkers: ReturnType<typeof useMultiTimeframeChart>['signalMarkers'];
  panelVisibilityById: Record<FundamentalsPanelId, boolean>;
  fundamentalsPanelHeight: number;
  tradingValuePeriod: number;
  fundamentalsPanelSection: LazySectionState;
  fundamentalsHistorySection: LazySectionState;
  marginSection: LazySectionState;
  factorSection: LazySectionState;
  marginPressureData: MarginPressureIndicatorsResponse | undefined;
  marginPressureLoading: boolean;
  marginPressureError: Error | null;
}) {
  return (
    <div className="h-full flex flex-col gap-4">
      <div className="h-[512px] shrink-0">
        <div className="h-full overflow-hidden rounded-xl border border-border/70 bg-card/92 shadow-sm shadow-black/5">
          <div className="h-full">
            <div className="p-4 border-b border-border/30">
              <h3 className="text-lg font-semibold text-foreground capitalize">{settings.displayTimeframe} Chart</h3>
            </div>
            <div className="h-[448px]">
              <ErrorBoundary>
                <StockChart
                  data={chartData[settings.displayTimeframe]?.candlestickData || []}
                  atrSupport={
                    chartData[settings.displayTimeframe]?.indicators.atrSupport as IndicatorValue[] | undefined
                  }
                  nBarSupport={
                    chartData[settings.displayTimeframe]?.indicators.nBarSupport as IndicatorValue[] | undefined
                  }
                  bollingerBands={
                    chartData[settings.displayTimeframe]?.bollingerBands as BollingerBandsData[] | undefined
                  }
                  vwema={chartData[settings.displayTimeframe]?.indicators.vwema as IndicatorValue[] | undefined}
                  signalMarkers={signalMarkers?.[settings.displayTimeframe]}
                />
              </ErrorBoundary>
            </div>
          </div>
        </div>
      </div>

      {settings.showPPOChart && (
        <div className="h-96 shrink-0">
          <div className="h-full overflow-hidden rounded-xl border border-border/70 bg-card/92 shadow-sm shadow-black/5">
            <div className="h-full">
              <div className="p-4 border-b border-border/30">
                <h3 className="text-lg font-semibold text-foreground capitalize">{settings.displayTimeframe} PPO</h3>
              </div>
              <div className="h-[calc(100%-4rem)]">
                <ErrorBoundary>
                  <PPOChart
                    data={(chartData[settings.displayTimeframe]?.indicators.ppo as PPOIndicatorData[]) || []}
                    title={`${settings.displayTimeframe.charAt(0).toUpperCase() + settings.displayTimeframe.slice(1)} PPO`}
                  />
                </ErrorBoundary>
              </div>
            </div>
          </div>
        </div>
      )}

      {settings.showRiskAdjustedReturnChart && (
        <div className="h-[240px] shrink-0">
          <div className="h-full overflow-hidden rounded-xl border border-border/70 bg-card/92 shadow-sm shadow-black/5">
            <div className="h-full">
              <div className="p-4 border-b border-border/30">
                <h3 className="text-lg font-semibold text-foreground capitalize">
                  {settings.displayTimeframe} Risk Adjusted Return
                </h3>
              </div>
              <div className="h-[calc(100%-4rem)]">
                <ErrorBoundary>
                  <RiskAdjustedReturnChart
                    data={
                      (chartData[settings.displayTimeframe]?.indicators
                        .riskAdjustedReturn as RiskAdjustedReturnData[]) || []
                    }
                    lookbackPeriod={settings.riskAdjustedReturn.lookbackPeriod}
                    ratioType={settings.riskAdjustedReturn.ratioType}
                    threshold={settings.riskAdjustedReturn.threshold}
                    condition={settings.riskAdjustedReturn.condition}
                    title={`${settings.displayTimeframe.charAt(0).toUpperCase() + settings.displayTimeframe.slice(1)} Risk Adjusted Return`}
                  />
                </ErrorBoundary>
              </div>
            </div>
          </div>
        </div>
      )}

      {settings.showVolumeComparison && (
        <div className="h-[240px] shrink-0">
          <div className="h-full overflow-hidden rounded-xl border border-border/70 bg-card/92 shadow-sm shadow-black/5">
            <div className="h-full">
              <div className="p-4 border-b border-border/30">
                <h3 className="text-lg font-semibold text-foreground capitalize">
                  {settings.displayTimeframe} Volume Comparison
                </h3>
              </div>
              <div className="h-[calc(100%-4rem)]">
                <ErrorBoundary>
                  <VolumeComparisonChart
                    data={(chartData[settings.displayTimeframe]?.volumeComparison as VolumeComparisonData[]) || []}
                    shortPeriod={settings.volumeComparison.shortPeriod}
                    longPeriod={settings.volumeComparison.longPeriod}
                    lowerMultiplier={settings.volumeComparison.lowerMultiplier}
                    higherMultiplier={settings.volumeComparison.higherMultiplier}
                  />
                </ErrorBoundary>
              </div>
            </div>
          </div>
        </div>
      )}

      {settings.showTradingValueMA && (
        <div className="h-[200px] shrink-0">
          <div className="h-full overflow-hidden rounded-xl border border-border/70 bg-card/92 shadow-sm shadow-black/5">
            <div className="h-full">
              <div className="p-4 border-b border-border/30">
                <h3 className="text-lg font-semibold text-foreground capitalize">
                  {settings.displayTimeframe} Trading Value MA
                </h3>
              </div>
              <div className="h-[calc(100%-4rem)]">
                <ErrorBoundary>
                  <TradingValueMAChart
                    data={(chartData[settings.displayTimeframe]?.tradingValueMA as TradingValueMAData[]) || []}
                    period={settings.tradingValueMA.period}
                  />
                </ErrorBoundary>
              </div>
            </div>
          </div>
        </div>
      )}

      {settings.fundamentalsPanelOrder
        .filter((panelId) => panelVisibilityById[panelId])
        .map((panelId) =>
          renderOrderedPanelSection({
            panelId,
            selectedSymbol,
            settings,
            fundamentalsPanelHeight,
            tradingValuePeriod,
            fundamentalsPanelSection,
            fundamentalsHistorySection,
            marginSection,
            factorSection,
            marginPressureData,
            marginPressureLoading,
            marginPressureError,
          })
        )}
    </div>
  );
}

export function ChartsPage() {
  useMigrateChartsRouteState();
  const queryClient = useQueryClient();
  const marginSection = useLazySectionVisibility();
  const fundamentalsPanelSection = useLazySectionVisibility();
  const fundamentalsHistorySection = useLazySectionVisibility();
  const factorSection = useLazySectionVisibility();
  const { selectedSymbol, strategyName, matchedDate, setSelectedSymbol } = useChartsRouteState();

  const { chartData, signalMarkers, signalResponse, isLoading, error } = useMultiTimeframeChart(
    selectedSymbol,
    strategyName
  );
  const { settings } = useChartStore();
  const refreshStocks = useRefreshStocks();
  const [refreshFeedback, setRefreshFeedback] = useState<ChartRefreshFeedback | null>(null);
  const shouldFetchMarginPressure = settings.showMarginPressurePanel && marginSection.isVisible;
  const shouldFetchFundamentals = selectedSymbol != null;
  const {
    data: marginPressureData,
    isLoading: marginPressureLoading,
    error: marginPressureError,
  } = useBtMarginIndicators(selectedSymbol, { enabled: shouldFetchMarginPressure });
  const { data: stockInfo } = useStockInfo(selectedSymbol);
  const tradingValuePeriod = Math.max(1, Math.trunc(settings.tradingValueMA.period ?? 15));
  const { data: fundamentalsData } = useFundamentals(selectedSymbol, {
    enabled: shouldFetchFundamentals,
    tradingValuePeriod,
  });
  const showEmptyState = shouldRenderEmptyState(isLoading, error, selectedSymbol);
  const showChartPanels = shouldRenderChartPanels(isLoading, error, selectedSymbol, chartData);

  logger.debug('ChartsPage render', {
    selectedSymbol,
    isLoading,
    error: error?.message,
    hasChartData: !!chartData,
  });

  useEffect(() => {
    if (selectedSymbol == null) {
      setRefreshFeedback(null);
      return;
    }
    setRefreshFeedback(null);
  }, [selectedSymbol]);

  const latestMarketCaps = useMemo<ChartHeaderMarketCaps>(() => {
    return resolveLatestMarketCaps(fundamentalsData?.dailyValuation);
  }, [fundamentalsData?.dailyValuation]);
  const visibleFundamentalMetricCount = useMemo(
    () => countVisibleFundamentalMetrics(settings.fundamentalsMetricOrder, settings.fundamentalsMetricVisibility),
    [settings.fundamentalsMetricOrder, settings.fundamentalsMetricVisibility]
  );
  const fundamentalsPanelHeight = useMemo(
    () => resolveFundamentalsPanelHeightPx(visibleFundamentalMetricCount),
    [visibleFundamentalMetricCount]
  );

  const panelVisibilityById = resolveFundamentalPanelVisibility(settings);
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
    <SplitLayout className="gap-0">
      <SplitSidebar className={cn('w-72 border-r border-border/30', 'glass-panel')}>
        <ErrorBoundary>
          <ChartControls selectedSymbol={selectedSymbol} onSelectSymbol={(symbol) => setSelectedSymbol(symbol)} />
        </ErrorBoundary>
      </SplitSidebar>

      <SplitMain className="space-y-4 overflow-auto p-6">
        {selectedSymbol && (
          <ChartHeader
            settings={settings}
            selectedSymbol={selectedSymbol}
            stockInfo={stockInfo}
            latestMarketCaps={latestMarketCaps}
            strategyName={strategyName}
            matchedDate={matchedDate}
            signalProvenance={signalResponse?.provenance}
            signalDiagnostics={signalResponse?.diagnostics}
            fundamentalsProvenance={fundamentalsData?.provenance}
            refreshFeedback={refreshFeedback}
            isRefreshing={refreshStocks.isPending}
            onRefresh={handleRefresh}
          />
        )}
        {error && <ErrorState error={error} />}
        {isLoading && <LoadingState selectedSymbol={selectedSymbol} />}
        {showEmptyState && <EmptyState />}
        {showChartPanels && (
          <ChartsPanelsContent
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
          />
        )}
      </SplitMain>
    </SplitLayout>
  );
}
