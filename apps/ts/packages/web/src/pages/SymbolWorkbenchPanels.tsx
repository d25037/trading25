import { Loader2 } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { FactorRegressionPanel } from '@/components/Chart/FactorRegressionPanel';
import { FundamentalsHistoryPanel } from '@/components/Chart/FundamentalsHistoryPanel';
import type { WorkbenchLatestMetricsOverride } from '@/components/Chart/FundamentalsPanel';
import { FundamentalsPanel } from '@/components/Chart/FundamentalsPanel';
import type { useMultiTimeframeChart } from '@/components/Chart/hooks/useMultiTimeframeChart';
import { MarginPressureChart } from '@/components/Chart/MarginPressureChart';
import { PPOChart } from '@/components/Chart/PPOChart';
import { RecentReturnChart } from '@/components/Chart/RecentReturnChart';
import { RiskAdjustedReturnChart } from '@/components/Chart/RiskAdjustedReturnChart';
import { SingleValueIndicatorChart } from '@/components/Chart/SingleValueIndicatorChart';
import { StockChart } from '@/components/Chart/StockChart';
import { TradingValueMAChart } from '@/components/Chart/TradingValueMAChart';
import { ValueCompositeScoreStrip } from '@/components/Chart/ValueCompositeScoreStrip';
import { VolumeComparisonChart } from '@/components/Chart/VolumeComparisonChart';
import { ErrorBoundary } from '@/components/ErrorBoundary';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { cn } from '@/lib/utils';
import {
  DEFAULT_WORKBENCH_PANEL_ORDER,
  type FundamentalsPanelId,
  type useChartStore,
  type WorkbenchPanelId,
} from '@/stores/chartStore';
import type {
  BollingerBandsData,
  IndicatorValue,
  MarginPressureIndicatorsResponse,
  PPOIndicatorData,
  RecentReturnData,
  RiskAdjustedReturnData,
  SMAATRBandsData,
  TradingValueMAData,
  VolumeComparisonData,
} from '@/types/chart';

type ChartSettings = ReturnType<typeof useChartStore.getState>['settings'];
type WorkbenchDisplayPanelId = 'primary' | WorkbenchPanelId;

export interface LazySectionState {
  sectionRef: (node: HTMLDivElement | null) => void;
  isVisible: boolean;
}

interface WorkbenchPanelOption {
  id: WorkbenchDisplayPanelId;
  label: string;
  kind: 'Primary' | 'Sub-chart' | 'Panel';
}

const WORKBENCH_PANEL_LABELS: Record<WorkbenchPanelId, string> = {
  ppo: 'PPO',
  riskAdjustedReturn: 'Risk',
  recentReturn: 'Recent',
  volumeComparison: 'Volume',
  cmf: 'CMF',
  chaikinOscillator: 'Chaikin',
  obvFlowScore: 'OBV',
  tradingValueMA: 'Trading Value',
  fundamentals: 'Fundamentals',
  fundamentalsHistory: 'Earnings',
  marginPressure: 'Margin',
  factorRegression: 'Factor',
};

function isSubChartPanel(panelId: WorkbenchPanelId): boolean {
  return (
    panelId === 'ppo' ||
    panelId === 'riskAdjustedReturn' ||
    panelId === 'recentReturn' ||
    panelId === 'volumeComparison' ||
    panelId === 'cmf' ||
    panelId === 'chaikinOscillator' ||
    panelId === 'obvFlowScore' ||
    panelId === 'tradingValueMA'
  );
}

function formatDisplayTimeframeLabel(timeframe: ChartSettings['displayTimeframe']): string {
  return timeframe.charAt(0).toUpperCase() + timeframe.slice(1);
}

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

export function resolveFundamentalPanelVisibility(settings: ChartSettings): Record<FundamentalsPanelId, boolean> {
  return {
    fundamentals: settings.showFundamentalsPanel,
    fundamentalsHistory: settings.showFundamentalsHistoryPanel,
    marginPressure: settings.showMarginPressurePanel,
    factorRegression: settings.showFactorRegressionPanel,
  };
}

function isWorkbenchPanelVisible(
  panelId: WorkbenchPanelId,
  settings: ChartSettings,
  panelVisibilityById: Record<FundamentalsPanelId, boolean>
): boolean {
  switch (panelId) {
    case 'ppo':
      return settings.showPPOChart;
    case 'riskAdjustedReturn':
      return settings.showRiskAdjustedReturnChart;
    case 'recentReturn':
      return settings.showRecentReturnChart;
    case 'volumeComparison':
      return settings.showVolumeComparison;
    case 'cmf':
      return settings.showCMF;
    case 'chaikinOscillator':
      return settings.showChaikinOscillator;
    case 'obvFlowScore':
      return settings.showOBVFlowScore;
    case 'tradingValueMA':
      return settings.showTradingValueMA;
    default:
      return panelVisibilityById[panelId];
  }
}

function buildWorkbenchPanelOptions(
  settings: ChartSettings,
  panelVisibilityById: Record<FundamentalsPanelId, boolean>
): WorkbenchPanelOption[] {
  const workbenchPanelOrder = settings.workbenchPanelOrder ?? DEFAULT_WORKBENCH_PANEL_ORDER;
  return [
    { id: 'primary', label: 'Primary', kind: 'Primary' },
    ...workbenchPanelOrder
      .filter((panelId) => isWorkbenchPanelVisible(panelId, settings, panelVisibilityById))
      .map(
        (panelId): WorkbenchPanelOption => ({
          id: panelId,
          label: WORKBENCH_PANEL_LABELS[panelId],
          kind: isSubChartPanel(panelId) ? 'Sub-chart' : 'Panel',
        })
      ),
  ];
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
  latestMetricsOverride,
  provisionalLabel,
  suppressFundamentalsErrors,
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
  latestMetricsOverride?: WorkbenchLatestMetricsOverride;
  provisionalLabel?: string | null;
  suppressFundamentalsErrors: boolean;
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
                <div className="flex h-full min-h-0 flex-col gap-3">
                  <ValueCompositeScoreStrip symbol={selectedSymbol} enabled={fundamentalsPanelSection.isVisible} />
                  <div className="min-h-0 flex-1">
                    <FundamentalsPanel
                      symbol={selectedSymbol}
                      enabled={fundamentalsPanelSection.isVisible}
                      tradingValuePeriod={tradingValuePeriod}
                      metricOrder={settings.fundamentalsMetricOrder}
                      metricVisibility={settings.fundamentalsMetricVisibility}
                      latestMetricsOverride={latestMetricsOverride}
                      provisionalLabel={provisionalLabel}
                      suppressError={suppressFundamentalsErrors}
                    />
                  </div>
                </div>
              </ErrorBoundary>
            </div>
          </Surface>
        </div>
      );
    case 'fundamentalsHistory':
      return (
        <div key={panelId} ref={fundamentalsHistorySection.sectionRef} className="h-[420px]">
          <Surface className="h-full overflow-hidden">
            <div className="border-b border-border/60 px-4 py-2.5">
              <h3 className="text-base font-semibold text-foreground">業績履歴</h3>
            </div>
            <div className="h-[calc(100%-3.25rem)] p-3">
              <ErrorBoundary>
                <FundamentalsHistoryPanel
                  symbol={selectedSymbol}
                  enabled={fundamentalsHistorySection.isVisible}
                  metricOrder={settings.fundamentalsHistoryMetricOrder}
                  metricVisibility={settings.fundamentalsHistoryMetricVisibility}
                  suppressError={suppressFundamentalsErrors}
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

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: keeps each persisted workbench panel id mapped in one renderer.
function renderOrderedWorkbenchSection({
  panelId,
  selectedSymbol,
  settings,
  chartData,
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
  latestMetricsOverride,
  provisionalLabel,
  suppressFundamentalsErrors,
}: {
  panelId: WorkbenchPanelId;
  selectedSymbol: string | null;
  settings: ChartSettings;
  chartData: ReturnType<typeof useMultiTimeframeChart>['chartData'];
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
  latestMetricsOverride?: WorkbenchLatestMetricsOverride;
  provisionalLabel?: string | null;
  suppressFundamentalsErrors: boolean;
}) {
  const timeframe = settings.displayTimeframe;
  const timeframeLabel = formatDisplayTimeframeLabel(timeframe);
  const currentChartData = chartData[timeframe];

  switch (panelId) {
    case 'ppo':
      if (!settings.showPPOChart) return null;
      return (
        <Surface key={panelId} className="h-96 shrink-0 overflow-hidden">
          <ErrorBoundary>
            <PPOChart
              data={(currentChartData?.indicators.ppo as PPOIndicatorData[]) || []}
              title={`${timeframeLabel} PPO`}
            />
          </ErrorBoundary>
        </Surface>
      );
    case 'riskAdjustedReturn':
      if (!settings.showRiskAdjustedReturnChart) return null;
      return (
        <Surface key={panelId} className="h-[240px] shrink-0 overflow-hidden">
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
      );
    case 'recentReturn':
      if (!settings.showRecentReturnChart) return null;
      return (
        <Surface key={panelId} className="h-[240px] shrink-0 overflow-hidden">
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
      );
    case 'volumeComparison':
      if (!settings.showVolumeComparison) return null;
      return (
        <Surface key={panelId} className="h-[240px] shrink-0 overflow-hidden">
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
      );
    case 'cmf':
      if (!settings.showCMF) return null;
      return (
        <Surface key={panelId} className="h-[220px] shrink-0 overflow-hidden">
          <ErrorBoundary>
            <SingleValueIndicatorChart
              data={(currentChartData?.indicators.cmf as IndicatorValue[]) || []}
              title={`${timeframeLabel} CMF`}
              periodLabel={`${settings.accumulationFlow.cmfPeriod}`}
              accentColor="#0EA5E9"
            />
          </ErrorBoundary>
        </Surface>
      );
    case 'chaikinOscillator':
      if (!settings.showChaikinOscillator) return null;
      return (
        <Surface key={panelId} className="h-[220px] shrink-0 overflow-hidden">
          <ErrorBoundary>
            <SingleValueIndicatorChart
              data={(currentChartData?.indicators.chaikinOscillator as IndicatorValue[]) || []}
              title={`${timeframeLabel} Chaikin Oscillator`}
              periodLabel={`${settings.accumulationFlow.chaikinFastPeriod}/${settings.accumulationFlow.chaikinSlowPeriod}`}
              accentColor="#14B8A6"
            />
          </ErrorBoundary>
        </Surface>
      );
    case 'obvFlowScore':
      if (!settings.showOBVFlowScore) return null;
      return (
        <Surface key={panelId} className="h-[220px] shrink-0 overflow-hidden">
          <ErrorBoundary>
            <SingleValueIndicatorChart
              data={(currentChartData?.indicators.obvFlowScore as IndicatorValue[]) || []}
              title={`${timeframeLabel} OBV Flow Score`}
              periodLabel={`${settings.accumulationFlow.obvLookbackPeriod}`}
              accentColor="#A855F7"
            />
          </ErrorBoundary>
        </Surface>
      );
    case 'tradingValueMA':
      if (!settings.showTradingValueMA) return null;
      return (
        <Surface key={panelId} className="h-[200px] shrink-0 overflow-hidden">
          <ErrorBoundary>
            <TradingValueMAChart
              data={(currentChartData?.tradingValueMA as TradingValueMAData[]) || []}
              period={settings.tradingValueMA.period}
            />
          </ErrorBoundary>
        </Surface>
      );
    default:
      if (!panelVisibilityById[panelId]) return null;
      return renderOrderedPanelSection({
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
        latestMetricsOverride,
        provisionalLabel,
        suppressFundamentalsErrors,
      });
  }
}

function renderPrimaryChartSection({
  settings,
  chartData,
  signalMarkers,
  mobile = false,
  provisionalDate,
}: {
  settings: ChartSettings;
  chartData: ReturnType<typeof useMultiTimeframeChart>['chartData'];
  signalMarkers: ReturnType<typeof useMultiTimeframeChart>['signalMarkers'];
  mobile?: boolean;
  provisionalDate?: string | null;
}) {
  return (
    <Surface
      className={cn(
        'overflow-hidden',
        mobile ? 'h-[min(58dvh,34rem)] min-h-[26rem]' : 'min-h-[34rem] lg:min-h-[40rem]'
      )}
    >
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
            smaAtrBands={chartData[settings.displayTimeframe]?.smaAtrBands as SMAATRBandsData[] | undefined}
            sma={chartData[settings.displayTimeframe]?.indicators.sma as IndicatorValue[] | undefined}
            ema={chartData[settings.displayTimeframe]?.indicators.ema as IndicatorValue[] | undefined}
            vwema={chartData[settings.displayTimeframe]?.indicators.vwema as IndicatorValue[] | undefined}
            signalMarkers={signalMarkers?.[settings.displayTimeframe]}
            provisionalDate={settings.displayTimeframe === 'daily' ? provisionalDate : null}
          />
        </ErrorBoundary>
      </div>
    </Surface>
  );
}

export function SymbolWorkbenchPanelsContent({
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
  isMobileWorkbenchLayout,
  latestMetricsOverride,
  provisionalLabel,
  provisionalDate,
  suppressFundamentalsErrors,
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
  isMobileWorkbenchLayout: boolean;
  latestMetricsOverride?: WorkbenchLatestMetricsOverride;
  provisionalLabel?: string | null;
  provisionalDate?: string | null;
  suppressFundamentalsErrors: boolean;
}) {
  const workbenchPanelOrder = settings.workbenchPanelOrder ?? DEFAULT_WORKBENCH_PANEL_ORDER;
  const panelOptions = useMemo(
    () => buildWorkbenchPanelOptions(settings, panelVisibilityById),
    [settings, panelVisibilityById]
  );
  const [activeMobilePanelId, setActiveMobilePanelId] = useState<WorkbenchDisplayPanelId>('primary');

  useEffect(() => {
    if (!panelOptions.some((option) => option.id === activeMobilePanelId)) {
      setActiveMobilePanelId('primary');
    }
  }, [activeMobilePanelId, panelOptions]);

  const activeMobilePanel = panelOptions.find((option) => option.id === activeMobilePanelId) ?? panelOptions[0];

  const renderWorkbenchPanel = (panelId: WorkbenchPanelId) =>
    renderOrderedWorkbenchSection({
      panelId,
      selectedSymbol,
      settings,
      chartData,
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
      latestMetricsOverride,
      provisionalLabel,
      suppressFundamentalsErrors,
    });

  return (
    <div className="flex h-full flex-col gap-3">
      {isMobileWorkbenchLayout ? (
        <div className="flex flex-col gap-3">
          <Surface className="px-3 py-3">
            <div className="mb-2 flex items-center justify-between gap-2">
              <div>
                <SectionEyebrow>Panel</SectionEyebrow>
                <p className="text-sm font-semibold text-foreground">{activeMobilePanel?.label ?? 'Primary'}</p>
              </div>
              <p className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">
                {activeMobilePanel?.kind ?? 'Primary'}
              </p>
            </div>
            <Select
              value={activeMobilePanel?.id ?? 'primary'}
              onValueChange={(value) => setActiveMobilePanelId(value as WorkbenchDisplayPanelId)}
            >
              <SelectTrigger aria-label="Workbench panel">
                <SelectValue placeholder="Select panel" />
              </SelectTrigger>
              <SelectContent>
                {panelOptions.map((option) => (
                  <SelectItem key={option.id} value={option.id}>
                    {option.label} · {option.kind}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </Surface>

          {!activeMobilePanel || activeMobilePanel.id === 'primary'
            ? renderPrimaryChartSection({ settings, chartData, signalMarkers, mobile: true, provisionalDate })
            : renderWorkbenchPanel(activeMobilePanel.id)}
        </div>
      ) : (
        <div className="flex flex-col gap-3">
          {renderPrimaryChartSection({ settings, chartData, signalMarkers, provisionalDate })}
          {workbenchPanelOrder.map((panelId) => renderWorkbenchPanel(panelId))}
        </div>
      )}
    </div>
  );
}
