import { useEffect, useMemo, useRef, useState } from 'react';
import { AlertCircle, BookOpen, Loader2, TrendingUp, Wallet } from 'lucide-react';
import { ChartControls } from '@/components/Chart/ChartControls';
import { FactorRegressionPanel } from '@/components/Chart/FactorRegressionPanel';
import { FundamentalsHistoryPanel } from '@/components/Chart/FundamentalsHistoryPanel';
import { FundamentalsPanel } from '@/components/Chart/FundamentalsPanel';
import { useMultiTimeframeChart } from '@/components/Chart/hooks/useMultiTimeframeChart';
import { MarginPressureChart } from '@/components/Chart/MarginPressureChart';
import { PPOChart } from '@/components/Chart/PPOChart';
import { StockChart } from '@/components/Chart/StockChart';
import { TimeframeSelector } from '@/components/Chart/TimeframeSelector';
import { TradingValueMAChart } from '@/components/Chart/TradingValueMAChart';
import { VolumeComparisonChart } from '@/components/Chart/VolumeComparisonChart';
import { ErrorBoundary } from '@/components/ErrorBoundary';
import { Button } from '@/components/ui/button';
import { useBtMarginIndicators } from '@/hooks/useBtMarginIndicators';
import { useFundamentals } from '@/hooks/useFundamentals';
import { useStockData } from '@/hooks/useStockData';
import { cn } from '@/lib/utils';
import { useChartStore } from '@/stores/chartStore';
import type {
  BollingerBandsData,
  IndicatorValue,
  MarginPressureIndicatorsResponse,
  PPOIndicatorData,
  TradingValueMAData,
  VolumeComparisonData,
} from '@/types/chart';
import { logger } from '@/utils/logger';
import { formatMarketCap } from '@/utils/formatters';

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
  const sectionRef = useRef<HTMLDivElement>(null);
  const [isVisible, setIsVisible] = useState(false);

  useEffect(() => {
    if (isVisible) return;

    const element = sectionRef.current;
    if (!element) return;

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

    observer.observe(element);
    return () => observer.disconnect();
  }, [isVisible, rootMargin]);

  return {
    sectionRef,
    isVisible,
  };
}

export function ChartsPage() {
  const marginSection = useLazySectionVisibility();
  const fundamentalsSection = useLazySectionVisibility();
  const factorSection = useLazySectionVisibility();

  const { chartData, signalMarkers, isLoading, error, selectedSymbol } = useMultiTimeframeChart();
  const {
    data: marginPressureData,
    isLoading: marginPressureLoading,
    error: marginPressureError,
  } = useBtMarginIndicators(selectedSymbol, { enabled: marginSection.isVisible });
  const { data: stockData } = useStockData(selectedSymbol, 'daily'); // Get stock data for company name
  const { data: fundamentalsData } = useFundamentals(selectedSymbol, { enabled: fundamentalsSection.isVisible });
  const { settings } = useChartStore();

  logger.debug('ChartsPage render', {
    selectedSymbol,
    isLoading,
    error: error?.message,
    hasChartData: !!chartData,
  });

  const renderLoadingState = () => (
    <div className="flex h-full items-center justify-center">
      <div className="text-center space-y-4">
        {/* Animated loading spinner */}
        <div className="relative">
          <div className="gradient-primary rounded-full p-4 mx-auto w-fit">
            <Loader2 className="h-8 w-8 text-white animate-spin" />
          </div>
          <div className="absolute inset-0 gradient-primary rounded-full animate-ping opacity-20" />
        </div>

        {/* Loading text with skeleton */}
        <div className="space-y-2">
          <p className="text-lg font-semibold text-foreground">Loading chart data...</p>
          <p className="text-sm text-muted-foreground">Fetching latest market data for {selectedSymbol}</p>

          {/* Skeleton bars */}
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

  const latestMarketCap = useMemo(() => {
    const daily = fundamentalsData?.dailyValuation;
    if (!daily || daily.length === 0) return null;
    return daily[daily.length - 1]?.marketCap ?? null;
  }, [fundamentalsData]);

  const renderErrorState = () => (
    <div className="flex h-full items-center justify-center">
      <div className="text-center space-y-6 max-w-md">
        {/* Error icon */}
        <div className="relative">
          <div className="bg-destructive/10 rounded-full p-4 mx-auto w-fit">
            <AlertCircle className="h-8 w-8 text-destructive" />
          </div>
        </div>

        {/* Error message */}
        <div className="space-y-2">
          <h3 className="text-xl font-semibold text-foreground">Unable to load chart data</h3>
          <p className="text-sm text-muted-foreground">
            {error instanceof Error ? error.message : 'An unexpected error occurred while fetching market data'}
          </p>
        </div>

        {/* Action buttons */}
        <div className="flex gap-3 justify-center">
          <Button variant="outline" onClick={() => window.location.reload()} className="glass-panel hover:bg-accent/50">
            Try Again
          </Button>
          <Button variant="default" className="gradient-primary hover:opacity-90">
            Contact Support
          </Button>
        </div>
      </div>
    </div>
  );

  const renderEmptyState = () => (
    <div className="flex h-full items-center justify-center">
      <div className="text-center space-y-6 max-w-md">
        {/* Empty state illustration */}
        <div className="relative">
          <div className="gradient-secondary rounded-full p-8 mx-auto w-fit">
            <TrendingUp className="h-12 w-12 text-primary" />
          </div>
          <div className="absolute inset-0 gradient-glass rounded-full" />
        </div>

        {/* Empty state message */}
        <div className="space-y-3">
          <h3 className="text-2xl font-bold text-foreground">Start Trading Analysis</h3>
          <p className="text-muted-foreground">
            Enter a stock symbol in the search box to view real-time charts and technical analysis
          </p>

          {/* Popular symbols */}
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
                    // TODO: Connect to chart store
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

  return (
    <div className="flex">
      {/* Chart Controls Sidebar */}
      <div className={cn('w-72 shrink-0 border-r border-border/30', 'glass-panel')}>
        <ErrorBoundary>
          <ChartControls />
        </ErrorBoundary>
      </div>

      {/* Main Chart Area */}
      <div className="flex-1 p-6">
        {error && renderErrorState()}
        {isLoading && renderLoadingState()}
        {!isLoading && !error && !selectedSymbol && renderEmptyState()}

        {!isLoading && !error && selectedSymbol && chartData && (
          <div className="h-full flex flex-col gap-4">
            {/* 共通タイトルヘッダー */}
            <div className="px-6 py-4 gradient-primary rounded-xl">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="gradient-secondary rounded-lg p-2">
                    <TrendingUp className="h-6 w-6 text-white" />
                  </div>
                  <div className="flex flex-col">
                    <h2 className="text-2xl font-bold text-white">
                      {selectedSymbol}
                      {stockData?.companyName && (
                        <span className="text-white/90 font-medium ml-2">{stockData.companyName}</span>
                      )}
                      {latestMarketCap != null && (
                        <span className="text-white/80 text-sm font-medium ml-3">
                          時価総額 {formatMarketCap(latestMarketCap)}
                        </span>
                      )}
                      {settings.relativeMode && <span className="text-white/70 font-medium"> / TOPIX</span>}
                    </h2>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  {/* 外部リンクボタン */}
                  <div className="flex items-center gap-2">
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => {
                        window.open(
                          `https://shikiho.toyokeizai.net/stocks/${selectedSymbol}`,
                          '_blank',
                          'noopener,noreferrer'
                        );
                      }}
                      className="text-white/80 hover:text-white hover:bg-white/10"
                      title="四季報を開く"
                    >
                      <BookOpen className="h-4 w-4 mr-1" />
                      四季報
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => {
                        window.open(
                          `https://www.buffett-code.com/company/${selectedSymbol}/`,
                          '_blank',
                          'noopener,noreferrer'
                        );
                      }}
                      className="text-white/80 hover:text-white hover:bg-white/10"
                      title="Buffett Codeを開く"
                    >
                      <Wallet className="h-4 w-4 mr-1" />
                      B.C.
                    </Button>
                  </div>
                  <TimeframeSelector />
                </div>
              </div>
            </div>

            {/* Main OHLC Chart - Single timeframe based on selection */}
            <div className="h-[512px]">
              <div className={cn('h-full rounded-xl glass-panel', 'relative overflow-hidden')}>
                <div className="absolute inset-0 gradient-glass opacity-50" />
                <div className="relative z-10 h-full">
                  <div className="p-4 border-b border-border/30">
                    <h3 className="text-lg font-semibold text-foreground capitalize">
                      {settings.displayTimeframe} Chart
                    </h3>
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
                        signalMarkers={signalMarkers?.[settings.displayTimeframe]}
                      />
                    </ErrorBoundary>
                  </div>
                </div>
              </div>
            </div>

            {/* PPO Chart - Single timeframe based on selection (conditionally displayed) */}
            {settings.showPPOChart && (
              <div className="h-96">
                <div className={cn('h-full rounded-xl glass-panel', 'relative overflow-hidden')}>
                  <div className="absolute inset-0 gradient-glass opacity-50" />
                  <div className="relative z-10 h-full">
                    <div className="p-4 border-b border-border/30">
                      <h3 className="text-lg font-semibold text-foreground capitalize">
                        {settings.displayTimeframe} PPO
                      </h3>
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

            {/* Volume Comparison Chart (conditionally displayed) */}
            {settings.showVolumeComparison && (
              <div className="h-[200px]">
                <div className={cn('h-full rounded-xl glass-panel', 'relative overflow-hidden')}>
                  <div className="absolute inset-0 gradient-glass opacity-50" />
                  <div className="relative z-10 h-full">
                    <div className="p-4 border-b border-border/30">
                      <h3 className="text-lg font-semibold text-foreground capitalize">
                        {settings.displayTimeframe} Volume Comparison
                      </h3>
                    </div>
                    <div className="h-[calc(100%-4rem)]">
                      <ErrorBoundary>
                        <VolumeComparisonChart
                          data={
                            (chartData[settings.displayTimeframe]?.volumeComparison as VolumeComparisonData[]) || []
                          }
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

            {/* Trading Value MA Chart (conditionally displayed) */}
            {settings.showTradingValueMA && (
              <div className="h-[200px]">
                <div className={cn('h-full rounded-xl glass-panel', 'relative overflow-hidden')}>
                  <div className="absolute inset-0 gradient-glass opacity-50" />
                  <div className="relative z-10 h-full">
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

            {/* Margin Pressure Indicators Row - 3 charts */}
            <div ref={marginSection.sectionRef} className="h-72">
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

            {/* Fundamentals Panel Section */}
            <div ref={fundamentalsSection.sectionRef} className="h-[540px]">
              <div className={cn('h-full rounded-xl glass-panel', 'relative overflow-hidden')}>
                <div className="absolute inset-0 gradient-glass opacity-50" />
                <div className="relative z-10 h-full">
                  <div className="p-4 border-b border-border/30">
                    <h3 className="text-lg font-semibold text-foreground">Fundamental Analysis</h3>
                  </div>
                  <div className="h-[calc(100%-4rem)] p-4">
                    <ErrorBoundary>
                      <FundamentalsPanel symbol={selectedSymbol} enabled={fundamentalsSection.isVisible} />
                    </ErrorBoundary>
                  </div>
                </div>
              </div>
            </div>

            {/* FY History Panel Section */}
            <div className="h-[340px]">
              <div className={cn('h-full rounded-xl glass-panel', 'relative overflow-hidden')}>
                <div className="absolute inset-0 gradient-glass opacity-50" />
                <div className="relative z-10 h-full">
                  <div className="p-4 border-b border-border/30">
                    <h3 className="text-lg font-semibold text-foreground">FY推移（過去5期）</h3>
                  </div>
                  <div className="h-[calc(100%-4rem)] p-4">
                    <ErrorBoundary>
                      <FundamentalsHistoryPanel symbol={selectedSymbol} enabled={fundamentalsSection.isVisible} />
                    </ErrorBoundary>
                  </div>
                </div>
              </div>
            </div>

            {/* Factor Regression Panel Section */}
            <div ref={factorSection.sectionRef} className="h-64">
              <div className={cn('h-full rounded-xl glass-panel', 'relative overflow-hidden')}>
                <div className="absolute inset-0 gradient-glass opacity-50" />
                <div className="relative z-10 h-full">
                  <div className="p-4 border-b border-border/30">
                    <h3 className="text-lg font-semibold text-foreground">Factor Regression Analysis</h3>
                  </div>
                  <div className="h-[calc(100%-4rem)] p-4">
                    <ErrorBoundary>
                      <FactorRegressionPanel symbol={selectedSymbol} enabled={factorSection.isVisible} />
                    </ErrorBoundary>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
