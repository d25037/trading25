import { useQueryClient } from '@tanstack/react-query';
import type { DataProvenance, ResponseDiagnostics } from '@trading25/contracts/types/api-types';
import { AlertCircle, BookOpen, Loader2, RotateCcw, TrendingUp, Wallet } from 'lucide-react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { ChartControls } from '@/components/Chart/ChartControls';
import { CostStructurePanel } from '@/components/Chart/CostStructurePanel';
import { FactorRegressionPanel } from '@/components/Chart/FactorRegressionPanel';
import { FundamentalsHistoryPanel } from '@/components/Chart/FundamentalsHistoryPanel';
import { FundamentalsPanel } from '@/components/Chart/FundamentalsPanel';
import { useMultiTimeframeChart } from '@/components/Chart/hooks/useMultiTimeframeChart';
import { MarginPressureChart } from '@/components/Chart/MarginPressureChart';
import { PPOChart } from '@/components/Chart/PPOChart';
import { RecentReturnChart } from '@/components/Chart/RecentReturnChart';
import { RiskAdjustedReturnChart } from '@/components/Chart/RiskAdjustedReturnChart';
import { SingleValueIndicatorChart } from '@/components/Chart/SingleValueIndicatorChart';
import { StockChart } from '@/components/Chart/StockChart';
import { TimeframeSelector } from '@/components/Chart/TimeframeSelector';
import { TradingValueMAChart } from '@/components/Chart/TradingValueMAChart';
import { VolumeComparisonChart } from '@/components/Chart/VolumeComparisonChart';
import { ErrorBoundary } from '@/components/ErrorBoundary';
import { SectionEyebrow, SplitLayout, SplitMain, SplitSidebar, Surface } from '@/components/Layout/Workspace';
import { Button } from '@/components/ui/button';
import { countVisibleFundamentalMetrics, resolveFundamentalsPanelHeightPx } from '@/constants/fundamentalMetrics';
import { useBtMarginIndicators } from '@/hooks/useBtMarginIndicators';
import { useRefreshStocks } from '@/hooks/useDbSync';
import { useFundamentals } from '@/hooks/useFundamentals';
import { useMigrateSymbolWorkbenchRouteState, useSymbolWorkbenchRouteState } from '@/hooks/usePageRouteState';
import { type StockInfoResponse, stockInfoKeys, useStockInfo } from '@/hooks/useStockInfo';
import { ApiError } from '@/lib/api-client';
import { cn } from '@/lib/utils';
import { type FundamentalsPanelId, useChartStore } from '@/stores/chartStore';
import type {
  BollingerBandsData,
  IndicatorValue,
  MarginPressureIndicatorsResponse,
  PPOIndicatorData,
  RecentReturnData,
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

function formatDisplayTimeframeLabel(timeframe: ChartSettings['displayTimeframe']): string {
  return timeframe.charAt(0).toUpperCase() + timeframe.slice(1);
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
    <div className="min-w-0 border-l border-border/70 pl-3">
      <div className="text-[11px] font-medium uppercase tracking-[0.12em] text-muted-foreground">{label}</div>
      <div className="truncate text-sm font-semibold text-foreground">{value}</div>
    </div>
  );
}

function ChartHeaderMetaChip({ label, value }: { label: string; value: string }) {
  return (
    <div className="inline-flex items-center gap-2 text-xs">
      <span className="uppercase tracking-[0.14em] text-muted-foreground">{label}</span>
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

const MARKET_CODE_LABELS: Record<string, string> = {
  prime: 'Prime',
  standard: 'Standard',
  growth: 'Growth',
  '0111': 'Prime',
  '0112': 'Standard',
  '0113': 'Growth',
};

function formatMarketLabel(stockInfo: StockInfoResponse | undefined): string {
  if (!stockInfo) {
    return '-';
  }

  const rawMarketCode = stockInfo.marketCode?.trim() ?? '';
  const canonicalLabel = rawMarketCode ? (MARKET_CODE_LABELS[rawMarketCode.toLowerCase()] ?? '') : '';
  return canonicalLabel || stockInfo.marketName?.trim() || rawMarketCode || '-';
}

function formatScaleCategoryLabel(scaleCategory: string | null | undefined): string {
  const normalized = scaleCategory?.trim();
  if (!normalized) {
    return '-';
  }

  const shortLabel = normalized.replace(/^TOPIX\s+/u, '');
  return shortLabel || normalized;
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
    <div className="grid h-full grid-cols-1 gap-4 xl:grid-cols-3">
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
    queryClient.invalidateQueries({ queryKey: ['cost-structure', selectedSymbol] }),
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
    costStructure: settings.showCostStructurePanel,
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
  costStructureSection,
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
  costStructureSection: LazySectionState;
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
          <Surface className="h-full overflow-hidden">
            <div className="border-b border-border/60 px-4 py-3">
              <h3 className="text-base font-semibold text-foreground">Fundamental Analysis</h3>
            </div>
            <div className="h-[calc(100%-3.75rem)] p-4">
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
          </Surface>
        </div>
      );
    case 'fundamentalsHistory':
      return (
        <div key={panelId} ref={fundamentalsHistorySection.sectionRef} className="h-[340px]">
          <Surface className="h-full overflow-hidden">
            <div className="border-b border-border/60 px-4 py-3">
              <h3 className="text-base font-semibold text-foreground">FY推移</h3>
            </div>
            <div className="h-[calc(100%-3.75rem)] p-4">
              <ErrorBoundary>
                <FundamentalsHistoryPanel
                  symbol={selectedSymbol}
                  enabled={fundamentalsHistorySection.isVisible}
                  metricOrder={settings.fundamentalsHistoryMetricOrder}
                  metricVisibility={settings.fundamentalsHistoryMetricVisibility}
                />
              </ErrorBoundary>
            </div>
          </Surface>
        </div>
      );
    case 'costStructure':
      return (
        <div key={panelId} ref={costStructureSection.sectionRef} className="h-[46rem] lg:h-[32rem]">
          <Surface className="h-full overflow-hidden">
            <div className="border-b border-border/60 px-4 py-3">
              <h3 className="text-base font-semibold text-foreground">Cost Structure Analysis</h3>
            </div>
            <div className="h-[calc(100%-3.75rem)] p-4">
              <ErrorBoundary>
                <CostStructurePanel
                  symbol={selectedSymbol}
                  enabled={settings.showCostStructurePanel && costStructureSection.isVisible}
                />
              </ErrorBoundary>
            </div>
          </Surface>
        </div>
      );
    case 'marginPressure':
      return (
        <div key={panelId} ref={marginSection.sectionRef} className="h-72">
          <Surface className="h-full overflow-hidden">
            <div className="border-b border-border/60 px-4 py-3">
              <h3 className="text-base font-semibold text-foreground">
                信用圧力指標
                {marginPressureData && (
                  <span className="ml-2 text-sm font-normal text-muted-foreground">
                    ({marginPressureData.averagePeriod}日平均)
                  </span>
                )}
              </h3>
            </div>
            <div className="h-[calc(100%-3.75rem)] p-4">
              <MarginPressureIndicatorsSection
                data={marginPressureData}
                isLoading={marginPressureLoading}
                error={marginPressureError}
              />
            </div>
          </Surface>
        </div>
      );
    case 'factorRegression':
      return (
        <div key={panelId} ref={factorSection.sectionRef} className="h-64">
          <Surface className="h-full overflow-hidden">
            <div className="border-b border-border/60 px-4 py-3">
              <h3 className="text-base font-semibold text-foreground">Factor Regression Analysis</h3>
            </div>
            <div className="h-[calc(100%-3.75rem)] p-4">
              <ErrorBoundary>
                <FactorRegressionPanel
                  symbol={selectedSymbol}
                  enabled={settings.showFactorRegressionPanel && factorSection.isVisible}
                />
              </ErrorBoundary>
            </div>
          </Surface>
        </div>
      );
    default:
      return null;
  }
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
              Relative mode requires local TOPIX data. Run Market DB sync or repair to restore the benchmark snapshot.
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

function EmptyState() {
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
      <Surface className="px-5 py-4">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0 space-y-3">
            <div className="flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-[var(--app-surface-muted)] text-primary">
                <TrendingUp className="h-5 w-5" />
              </div>
              <div className="min-w-0">
                <SectionEyebrow>Symbol Workbench</SectionEyebrow>
                <h2 className="truncate text-2xl font-semibold tracking-tight text-foreground">
                  {selectedSymbol}
                  {stockInfo?.companyName && (
                    <span className="ml-2 font-medium text-foreground">{stockInfo.companyName}</span>
                  )}
                  {settings.relativeMode && <span className="font-medium text-muted-foreground"> / TOPIX</span>}
                </h2>
              </div>
            </div>

            <div className="flex flex-wrap gap-x-5 gap-y-2">
              <ChartHeaderMetaChip label="Overlay" value={overlayLabel} />
              <ChartHeaderMetaChip label="Matched Date" value={formatOptionalDate(matchedDate)} />
              <ChartHeaderMetaChip label="Market Snapshot" value={marketSnapshotId} />
              <ChartHeaderMetaChip label="Signal Domains" value={formatList(mergedLoadedDomains)} />
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2 xl:justify-end">
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

        <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
          <ChartHeaderInfoField label="市場" value={formatMarketLabel(stockInfo)} />
          <ChartHeaderInfoField label="指数採用" value={formatScaleCategoryLabel(stockInfo?.scaleCategory)} />
          <ChartHeaderInfoField label="セクター17" value={stockInfo?.sector17Name || '-'} />
          <ChartHeaderInfoField label="セクター33" value={stockInfo?.sector33Name || '-'} />
          <ChartHeaderInfoField label="時価総額 (Free Float)" value={formatMarketCap(latestMarketCaps.freeFloat)} />
          <ChartHeaderInfoField
            label="時価総額 (発行済み株式数)"
            value={formatMarketCap(latestMarketCaps.issuedShares)}
          />
        </div>

        {(signalProvenance?.reference_date || fundamentalsProvenance?.reference_date || warnings.length > 0) && (
          <div className="mt-4 border-t border-border/60 pt-3 text-xs text-muted-foreground">
            <div>
              Reference Date:{' '}
              <span className="font-medium text-foreground">
                {signalProvenance?.reference_date ?? fundamentalsProvenance?.reference_date ?? '-'}
              </span>
            </div>
            {warnings.length > 0 && (
              <div className="mt-1">
                Warnings: <span className="font-medium text-foreground">{warnings.join(' | ')}</span>
              </div>
            )}
          </div>
        )}
      </Surface>

      {refreshFeedback && <ChartRefreshFeedbackBanner feedback={refreshFeedback} />}
    </div>
  );
}

type TimeframeChartData = ReturnType<typeof useMultiTimeframeChart>['chartData'][ChartSettings['displayTimeframe']];

interface WorkbenchSubChartGroupProps {
  settings: ChartSettings;
  currentChartData: TimeframeChartData;
  timeframeLabel: string;
}

function ReturnSubCharts({ settings, currentChartData, timeframeLabel }: WorkbenchSubChartGroupProps) {
  return (
    <>
      {settings.showPPOChart && (
        <Surface className="h-96 shrink-0 overflow-hidden">
          <ErrorBoundary>
            <PPOChart
              data={(currentChartData?.indicators.ppo as PPOIndicatorData[]) || []}
              title={`${timeframeLabel} PPO`}
            />
          </ErrorBoundary>
        </Surface>
      )}

      {settings.showRiskAdjustedReturnChart && (
        <Surface className="h-[240px] shrink-0 overflow-hidden">
          <ErrorBoundary>
            <RiskAdjustedReturnChart
              data={(currentChartData?.indicators.riskAdjustedReturn as RiskAdjustedReturnData[]) || []}
              lookbackPeriod={settings.riskAdjustedReturn.lookbackPeriod}
              ratioType={settings.riskAdjustedReturn.ratioType}
              threshold={settings.riskAdjustedReturn.threshold}
              condition={settings.riskAdjustedReturn.condition}
              title={`${timeframeLabel} Risk Adjusted Return`}
            />
          </ErrorBoundary>
        </Surface>
      )}

      {settings.showRecentReturnChart && (
        <Surface className="h-[240px] shrink-0 overflow-hidden">
          <ErrorBoundary>
            <RecentReturnChart
              shortData={
                (currentChartData?.indicators[
                  `recentReturn${settings.recentReturn.shortPeriod}`
                ] as RecentReturnData[]) || []
              }
              longData={
                (currentChartData?.indicators[
                  `recentReturn${settings.recentReturn.longPeriod}`
                ] as RecentReturnData[]) || []
              }
              shortPeriod={settings.recentReturn.shortPeriod}
              longPeriod={settings.recentReturn.longPeriod}
              title={`${timeframeLabel} Recent Return`}
            />
          </ErrorBoundary>
        </Surface>
      )}
    </>
  );
}

function VolumeFlowSubCharts({ settings, currentChartData, timeframeLabel }: WorkbenchSubChartGroupProps) {
  return (
    <>
      {settings.showVolumeComparison && (
        <Surface className="h-[240px] shrink-0 overflow-hidden">
          <ErrorBoundary>
            <VolumeComparisonChart
              data={(currentChartData?.volumeComparison as VolumeComparisonData[]) || []}
              shortPeriod={settings.volumeComparison.shortPeriod}
              longPeriod={settings.volumeComparison.longPeriod}
              lowerMultiplier={settings.volumeComparison.lowerMultiplier}
              higherMultiplier={settings.volumeComparison.higherMultiplier}
            />
          </ErrorBoundary>
        </Surface>
      )}

      {settings.showCMF && (
        <Surface className="h-[220px] shrink-0 overflow-hidden">
          <ErrorBoundary>
            <SingleValueIndicatorChart
              data={(currentChartData?.indicators.cmf as IndicatorValue[]) || []}
              title={`${timeframeLabel} CMF`}
              periodLabel={`${settings.accumulationFlow.cmfPeriod}`}
              accentColor="#0EA5E9"
            />
          </ErrorBoundary>
        </Surface>
      )}

      {settings.showChaikinOscillator && (
        <Surface className="h-[220px] shrink-0 overflow-hidden">
          <ErrorBoundary>
            <SingleValueIndicatorChart
              data={(currentChartData?.indicators.chaikinOscillator as IndicatorValue[]) || []}
              title={`${timeframeLabel} Chaikin Oscillator`}
              periodLabel={`${settings.accumulationFlow.chaikinFastPeriod}/${settings.accumulationFlow.chaikinSlowPeriod}`}
              accentColor="#14B8A6"
            />
          </ErrorBoundary>
        </Surface>
      )}

      {settings.showOBVFlowScore && (
        <Surface className="h-[220px] shrink-0 overflow-hidden">
          <ErrorBoundary>
            <SingleValueIndicatorChart
              data={(currentChartData?.indicators.obvFlowScore as IndicatorValue[]) || []}
              title={`${timeframeLabel} OBV Flow Score`}
              periodLabel={`${settings.accumulationFlow.obvLookbackPeriod}`}
              accentColor="#A855F7"
            />
          </ErrorBoundary>
        </Surface>
      )}

      {settings.showTradingValueMA && (
        <Surface className="h-[200px] shrink-0 overflow-hidden">
          <ErrorBoundary>
            <TradingValueMAChart
              data={(currentChartData?.tradingValueMA as TradingValueMAData[]) || []}
              period={settings.tradingValueMA.period}
            />
          </ErrorBoundary>
        </Surface>
      )}
    </>
  );
}

function WorkbenchSubCharts({
  settings,
  chartData,
}: {
  settings: ChartSettings;
  chartData: ReturnType<typeof useMultiTimeframeChart>['chartData'];
}) {
  const timeframe = settings.displayTimeframe;
  const timeframeLabel = formatDisplayTimeframeLabel(timeframe);
  const currentChartData = chartData[timeframe];

  return (
    <>
      <ReturnSubCharts settings={settings} currentChartData={currentChartData} timeframeLabel={timeframeLabel} />
      <VolumeFlowSubCharts settings={settings} currentChartData={currentChartData} timeframeLabel={timeframeLabel} />
    </>
  );
}

function SymbolWorkbenchPanelsContent({
  settings,
  selectedSymbol,
  chartData,
  signalMarkers,
  panelVisibilityById,
  fundamentalsPanelHeight,
  tradingValuePeriod,
  fundamentalsPanelSection,
  fundamentalsHistorySection,
  costStructureSection,
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
  costStructureSection: LazySectionState;
  marginSection: LazySectionState;
  factorSection: LazySectionState;
  marginPressureData: MarginPressureIndicatorsResponse | undefined;
  marginPressureLoading: boolean;
  marginPressureError: Error | null;
}) {
  return (
    <div className="flex h-full flex-col gap-3">
      <Surface className="min-h-[34rem] overflow-hidden lg:min-h-[40rem]">
        <div className="flex items-center justify-between border-b border-border/60 px-4 py-3">
          <div>
            <SectionEyebrow>Primary</SectionEyebrow>
            <h3 className="text-base font-semibold capitalize text-foreground">{settings.displayTimeframe} Chart</h3>
          </div>
        </div>
        <div className="h-[calc(100%-4.25rem)]">
          <ErrorBoundary>
            <StockChart
              data={chartData[settings.displayTimeframe]?.candlestickData || []}
              atrSupport={chartData[settings.displayTimeframe]?.indicators.atrSupport as IndicatorValue[] | undefined}
              nBarSupport={chartData[settings.displayTimeframe]?.indicators.nBarSupport as IndicatorValue[] | undefined}
              bollingerBands={chartData[settings.displayTimeframe]?.bollingerBands as BollingerBandsData[] | undefined}
              vwema={chartData[settings.displayTimeframe]?.indicators.vwema as IndicatorValue[] | undefined}
              signalMarkers={signalMarkers?.[settings.displayTimeframe]}
            />
          </ErrorBoundary>
        </div>
      </Surface>

      <WorkbenchSubCharts settings={settings} chartData={chartData} />

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
            costStructureSection,
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

export function SymbolWorkbenchPage() {
  useMigrateSymbolWorkbenchRouteState();
  const queryClient = useQueryClient();
  const marginSection = useLazySectionVisibility();
  const fundamentalsPanelSection = useLazySectionVisibility();
  const fundamentalsHistorySection = useLazySectionVisibility();
  const costStructureSection = useLazySectionVisibility();
  const factorSection = useLazySectionVisibility();
  const { selectedSymbol, strategyName, matchedDate, setSelectedSymbol } = useSymbolWorkbenchRouteState();

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

  logger.debug('SymbolWorkbenchPage render', {
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
    <SplitLayout className="min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-4 lg:flex-row lg:items-stretch lg:overflow-hidden">
      <h1 className="sr-only">Symbol Workbench</h1>
      <SplitSidebar className="w-full lg:w-[18rem]">
        <Surface className="h-full min-h-0 overflow-hidden">
          <ErrorBoundary>
            <ChartControls selectedSymbol={selectedSymbol} onSelectSymbol={(symbol) => setSelectedSymbol(symbol)} />
          </ErrorBoundary>
        </Surface>
      </SplitSidebar>

      <SplitMain className="min-h-0 gap-3 lg:overflow-y-auto lg:pr-1">
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
            costStructureSection={costStructureSection}
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
