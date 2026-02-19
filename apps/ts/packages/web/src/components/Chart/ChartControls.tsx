import {
  Activity,
  ArrowDown,
  ArrowUp,
  BarChart3,
  BookOpen,
  Eye,
  Search,
  Settings as SettingsIcon,
  TrendingUp,
} from 'lucide-react';
import { useCallback, useId, useMemo, useState } from 'react';
import { StockSearchInput } from '@/components/Stock/StockSearchInput';
import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import {
  countVisibleFundamentalMetrics,
  FUNDAMENTAL_METRIC_DEFINITIONS,
  type FundamentalMetricId,
} from '@/constants/fundamentalMetrics';
import { useSignalReference } from '@/hooks/useBacktest';
import type { StockSearchResultItem } from '@/hooks/useStockSearch';
import type { ChartSettings, FundamentalsPanelId } from '@/stores/chartStore';
import { useChartStore } from '@/stores/chartStore';
import { logger } from '@/utils/logger';
import { ChartPresetSelector } from './ChartPresetSelector';
import { IndicatorToggle, NumberInput } from './IndicatorToggle';
import { SignalOverlayControls } from './SignalMarkers';
import { buildSignalPanelLinks, type SignalLinkedPanel } from './signalPanelLinks';

const VISIBLE_BAR_OPTIONS = [
  { value: 30, label: '30 bars' },
  { value: 60, label: '60 bars' },
  { value: 90, label: '90 bars' },
  { value: 120, label: '120 bars (default)' },
  { value: 180, label: '180 bars' },
  { value: 250, label: '250 bars' },
] as const;

type PanelVisibilitySettingKey =
  | 'showFundamentalsPanel'
  | 'showFundamentalsHistoryPanel'
  | 'showMarginPressurePanel'
  | 'showFactorRegressionPanel';

interface PanelVisibilityToggle {
  id: string;
  label: string;
  settingKey: PanelVisibilitySettingKey;
  panelId: FundamentalsPanelId;
  linkPanel: SignalLinkedPanel;
}

const PANEL_VISIBILITY_TOGGLES: PanelVisibilityToggle[] = [
  {
    id: 'show-fundamentals-panel',
    label: 'Fundamentals',
    settingKey: 'showFundamentalsPanel',
    panelId: 'fundamentals',
    linkPanel: 'fundamentals',
  },
  {
    id: 'show-fundamentals-history-panel',
    label: 'FY History',
    settingKey: 'showFundamentalsHistoryPanel',
    panelId: 'fundamentalsHistory',
    linkPanel: 'fundamentalsHistory',
  },
  {
    id: 'show-margin-pressure-panel',
    label: 'Margin Pressure',
    settingKey: 'showMarginPressurePanel',
    panelId: 'marginPressure',
    linkPanel: 'marginPressure',
  },
  {
    id: 'show-factor-regression-panel',
    label: 'Factor Regression',
    settingKey: 'showFactorRegressionPanel',
    panelId: 'factorRegression',
    linkPanel: 'factorRegression',
  },
];

const PANEL_TOGGLE_BY_ID = Object.fromEntries(
  PANEL_VISIBILITY_TOGGLES.map((toggle) => [toggle.panelId, toggle])
) as Record<FundamentalsPanelId, PanelVisibilityToggle>;
const FUNDAMENTAL_METRIC_LABEL_BY_ID = Object.fromEntries(
  FUNDAMENTAL_METRIC_DEFINITIONS.map((definition) => [definition.id, definition.label])
) as Record<FundamentalMetricId, string>;

type SettingDialogId =
  | 'chartSettings'
  | 'panelLayout'
  | 'fundamentalMetrics'
  | 'overlayIndicators'
  | 'subChartIndicators'
  | 'signalOverlay';

interface SettingDialogDefinition {
  id: SettingDialogId;
  title: string;
  description: string;
  icon: React.ComponentType<{ className?: string }>;
}

const SETTING_DIALOGS: SettingDialogDefinition[] = [
  {
    id: 'chartSettings',
    title: 'Chart Settings',
    description: 'Main chart display and base toggles.',
    icon: SettingsIcon,
  },
  {
    id: 'panelLayout',
    title: 'Panel Layout',
    description: 'Choose visible panels and their display order.',
    icon: Eye,
  },
  {
    id: 'fundamentalMetrics',
    title: 'Fundamental Metrics',
    description: 'Toggle and reorder metrics shown inside the fundamentals panel.',
    icon: BookOpen,
  },
  {
    id: 'overlayIndicators',
    title: 'Overlay Indicators',
    description: 'Configure overlays rendered on the price chart.',
    icon: TrendingUp,
  },
  {
    id: 'subChartIndicators',
    title: 'Sub-Chart Indicators',
    description: 'Configure additional panels rendered below the chart.',
    icon: BarChart3,
  },
  {
    id: 'signalOverlay',
    title: 'Signal Overlay',
    description: 'Select signal markers and overlay behavior.',
    icon: Activity,
  },
];

function formatPanelSignalMeta(requirements: string[], signalTypes: string[]): string {
  return `Signal req: ${requirements.join(', ')} | Signals: ${signalTypes.join(', ')}`;
}

export function ChartControls() {
  const {
    selectedSymbol,
    settings,
    setSelectedSymbol,
    updateSettings,
    toggleRelativeMode,
    updateIndicatorSettings,
    updateVolumeComparison,
    updateTradingValueMA,
  } = useChartStore();

  const [symbolInput, setSymbolInput] = useState('');
  const [openDialogId, setOpenDialogId] = useState<SettingDialogId | null>(null);

  const symbolSearchId = useId();
  const showVolumeId = useId();
  const relativeModeId = useId();
  const visibleBarsId = useId();
  const { data: signalReferenceData, error: signalReferenceError } = useSignalReference();

  const signalPanelLinks = useMemo(
    () =>
      buildSignalPanelLinks({
        signals: settings.signalOverlay?.signals ?? [],
        definitions: signalReferenceData?.signals ?? [],
      }),
    [settings.signalOverlay?.signals, signalReferenceData?.signals]
  );
  const showSignalMeta = !!signalReferenceData && !signalReferenceError;

  const getPanelSignalMeta = useCallback(
    (panel: SignalLinkedPanel): string | undefined => {
      if (!showSignalMeta) return undefined;
      const link = signalPanelLinks[panel];
      if (link.signalTypes.length === 0) return undefined;
      return formatPanelSignalMeta(link.requirements, link.signalTypes);
    },
    [showSignalMeta, signalPanelLinks]
  );

  const handleSelectStock = useCallback(
    (stock: StockSearchResultItem) => {
      logger.debug('Stock selected from search', { code: stock.code, companyName: stock.companyName });
      setSelectedSymbol(stock.code);
      setSymbolInput('');
    },
    [setSelectedSymbol]
  );

  const handleSymbolSubmit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (symbolInput.trim()) {
      const symbol = symbolInput.trim().toUpperCase();
      logger.debug('Setting selected symbol', { symbol });
      setSelectedSymbol(symbol);
      setSymbolInput('');
    }
  };

  const updatePanelVisibility = useCallback(
    (settingKey: PanelVisibilitySettingKey, checked: boolean) => {
      updateSettings({ [settingKey]: checked } as Partial<ChartSettings>);
    },
    [updateSettings]
  );

  const movePanelOrder = useCallback(
    (panelId: FundamentalsPanelId, direction: 'up' | 'down') => {
      const currentOrder = settings.fundamentalsPanelOrder;
      const currentIndex = currentOrder.indexOf(panelId);
      if (currentIndex < 0) return;

      const targetIndex = direction === 'up' ? currentIndex - 1 : currentIndex + 1;
      if (targetIndex < 0 || targetIndex >= currentOrder.length) return;

      const nextOrder = [...currentOrder];
      const currentPanel = nextOrder[currentIndex];
      const targetPanel = nextOrder[targetIndex];
      if (!currentPanel || !targetPanel) return;
      nextOrder[currentIndex] = targetPanel;
      nextOrder[targetIndex] = currentPanel;
      updateSettings({ fundamentalsPanelOrder: nextOrder });
    },
    [settings.fundamentalsPanelOrder, updateSettings]
  );

  const updateFundamentalMetricVisibility = useCallback(
    (metricId: FundamentalMetricId, checked: boolean) => {
      updateSettings({
        fundamentalsMetricVisibility: {
          ...settings.fundamentalsMetricVisibility,
          [metricId]: checked,
        },
      });
    },
    [settings.fundamentalsMetricVisibility, updateSettings]
  );

  const moveFundamentalMetricOrder = useCallback(
    (metricId: FundamentalMetricId, direction: 'up' | 'down') => {
      const currentOrder = settings.fundamentalsMetricOrder;
      const currentIndex = currentOrder.indexOf(metricId);
      if (currentIndex < 0) return;

      const targetIndex = direction === 'up' ? currentIndex - 1 : currentIndex + 1;
      if (targetIndex < 0 || targetIndex >= currentOrder.length) return;

      const nextOrder = [...currentOrder];
      const currentMetric = nextOrder[currentIndex];
      const targetMetric = nextOrder[targetIndex];
      if (!currentMetric || !targetMetric) return;

      nextOrder[currentIndex] = targetMetric;
      nextOrder[targetIndex] = currentMetric;
      updateSettings({ fundamentalsMetricOrder: nextOrder });
    },
    [settings.fundamentalsMetricOrder, updateSettings]
  );

  const visibleFundamentalMetricCount = useMemo(
    () => countVisibleFundamentalMetrics(settings.fundamentalsMetricOrder, settings.fundamentalsMetricVisibility),
    [settings.fundamentalsMetricOrder, settings.fundamentalsMetricVisibility]
  );

  const updateRiskAdjustedReturn = useCallback(
    (newSettings: Partial<ChartSettings['riskAdjustedReturn']>) => {
      updateSettings({
        riskAdjustedReturn: {
          ...settings.riskAdjustedReturn,
          ...newSettings,
        },
      });
    },
    [settings.riskAdjustedReturn, updateSettings]
  );

  const renderDialogBody = (dialogId: SettingDialogId) => {
    switch (dialogId) {
      case 'chartSettings':
        return (
          <div className="space-y-2">
            <ToggleRow
              id={showVolumeId}
              icon={BarChart3}
              label="Show Volume"
              checked={settings.showVolume}
              onCheckedChange={(checked) => updateSettings({ showVolume: checked })}
            />

            <ToggleRow
              id="showPPOChart"
              icon={Activity}
              label="Show PPO Chart"
              checked={settings.showPPOChart}
              onCheckedChange={(checked) => updateSettings({ showPPOChart: checked })}
              meta={getPanelSignalMeta('ppo')}
            />

            <ToggleRow
              id={relativeModeId}
              icon={Activity}
              label="Relative to TOPIX"
              checked={settings.relativeMode}
              onCheckedChange={toggleRelativeMode}
            />

            <div className="space-y-1.5 p-2 rounded glass-panel">
              <div className="flex items-center gap-1.5">
                <Eye className="h-3.5 w-3.5 text-muted-foreground" />
                <Label htmlFor={visibleBarsId} className="text-xs font-medium">
                  Visible Bars
                </Label>
              </div>
              <Select
                value={settings.visibleBars.toString()}
                onValueChange={(value) => updateSettings({ visibleBars: Number.parseInt(value, 10) })}
              >
                <SelectTrigger
                  id={visibleBarsId}
                  className="h-8 text-xs glass-panel border-border/30 focus:border-primary/50"
                >
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="bg-background/95 backdrop-blur-md border-border shadow-xl">
                  {VISIBLE_BAR_OPTIONS.map((option) => (
                    <SelectItem key={option.value} value={option.value.toString()} className="text-xs">
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        );

      case 'panelLayout':
        return (
          <div className="space-y-2">
            {settings.fundamentalsPanelOrder.map((panelId, index) => {
              const toggle = PANEL_TOGGLE_BY_ID[panelId];
              const panelMeta = getPanelSignalMeta(toggle.linkPanel);
              const isVisible = settings[toggle.settingKey];
              return (
                <div key={panelId} className="rounded glass-panel p-2 space-y-2">
                  <div className="flex items-start justify-between gap-2">
                    <div className="min-w-0">
                      <p className="text-xs text-muted-foreground">Order {index + 1}</p>
                      <Label htmlFor={toggle.id} className="text-sm font-medium cursor-pointer">
                        {toggle.label}
                      </Label>
                      {panelMeta && (
                        <p className="text-[10px] text-muted-foreground leading-tight mt-0.5">{panelMeta}</p>
                      )}
                    </div>
                    <Switch
                      id={toggle.id}
                      checked={isVisible}
                      onCheckedChange={(checked) => updatePanelVisibility(toggle.settingKey, checked)}
                    />
                  </div>
                  <div className="flex items-center gap-1">
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      className="h-7 px-2"
                      onClick={() => movePanelOrder(panelId, 'up')}
                      disabled={index === 0}
                    >
                      <ArrowUp className="h-3.5 w-3.5 mr-1" />
                      Up
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      className="h-7 px-2"
                      onClick={() => movePanelOrder(panelId, 'down')}
                      disabled={index === settings.fundamentalsPanelOrder.length - 1}
                    >
                      <ArrowDown className="h-3.5 w-3.5 mr-1" />
                      Down
                    </Button>
                  </div>
                </div>
              );
            })}
          </div>
        );

      case 'fundamentalMetrics':
        return (
          <div className="space-y-2">
            <div className="rounded glass-panel p-2">
              <p className="text-xs text-muted-foreground">
                Visible: {visibleFundamentalMetricCount} / {settings.fundamentalsMetricOrder.length}
              </p>
            </div>
            {settings.fundamentalsMetricOrder.map((metricId, index) => {
              const metricLabel = FUNDAMENTAL_METRIC_LABEL_BY_ID[metricId];
              const isVisible = settings.fundamentalsMetricVisibility[metricId];
              const switchId = `fundamental-metric-${metricId}`;
              return (
                <div key={metricId} className="rounded glass-panel p-2 space-y-2">
                  <div className="flex items-start justify-between gap-2">
                    <div className="min-w-0">
                      <p className="text-xs text-muted-foreground">Order {index + 1}</p>
                      <Label htmlFor={switchId} className="text-sm font-medium cursor-pointer">
                        {metricLabel}
                      </Label>
                    </div>
                    <Switch
                      id={switchId}
                      checked={isVisible}
                      onCheckedChange={(checked) => updateFundamentalMetricVisibility(metricId, checked)}
                    />
                  </div>
                  <div className="flex items-center gap-1">
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      className="h-7 px-2"
                      onClick={() => moveFundamentalMetricOrder(metricId, 'up')}
                      disabled={index === 0}
                    >
                      <ArrowUp className="h-3.5 w-3.5 mr-1" />
                      Up
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      className="h-7 px-2"
                      onClick={() => moveFundamentalMetricOrder(metricId, 'down')}
                      disabled={index === settings.fundamentalsMetricOrder.length - 1}
                    >
                      <ArrowDown className="h-3.5 w-3.5 mr-1" />
                      Down
                    </Button>
                  </div>
                </div>
              );
            })}
          </div>
        );

      case 'overlayIndicators':
        return (
          <div className="space-y-2">
            <IndicatorToggle
              label="ATR Support Line"
              enabled={settings.indicators.atrSupport.enabled}
              onToggle={(enabled) => updateIndicatorSettings('atrSupport', { enabled })}
            >
              <div className="grid grid-cols-2 gap-1.5">
                <NumberInput
                  label="Period"
                  value={settings.indicators.atrSupport.period}
                  onChange={(period) => updateIndicatorSettings('atrSupport', { period })}
                  defaultValue={20}
                />
                <NumberInput
                  label="Multiplier"
                  value={settings.indicators.atrSupport.multiplier}
                  onChange={(multiplier) => updateIndicatorSettings('atrSupport', { multiplier })}
                  step="0.1"
                  defaultValue={3.0}
                />
              </div>
            </IndicatorToggle>

            <IndicatorToggle
              label="N-Bar Support Line"
              enabled={settings.indicators.nBarSupport.enabled}
              onToggle={(enabled) => updateIndicatorSettings('nBarSupport', { enabled })}
            >
              <NumberInput
                label="Period"
                value={settings.indicators.nBarSupport.period}
                onChange={(period) => updateIndicatorSettings('nBarSupport', { period })}
                defaultValue={60}
              />
            </IndicatorToggle>

            <IndicatorToggle
              label="Bollinger Bands"
              enabled={settings.indicators.bollinger.enabled}
              onToggle={(enabled) => updateIndicatorSettings('bollinger', { enabled })}
            >
              <div className="grid grid-cols-2 gap-1.5">
                <NumberInput
                  label="Period"
                  value={settings.indicators.bollinger.period}
                  onChange={(period) => updateIndicatorSettings('bollinger', { period })}
                  defaultValue={20}
                />
                <NumberInput
                  label="Deviation"
                  value={settings.indicators.bollinger.deviation}
                  onChange={(deviation) => updateIndicatorSettings('bollinger', { deviation })}
                  step="0.1"
                  defaultValue={2.0}
                />
              </div>
            </IndicatorToggle>
          </div>
        );

      case 'subChartIndicators':
        return (
          <div className="space-y-2">
            <IndicatorToggle
              label="Risk Adjusted Return"
              enabled={settings.showRiskAdjustedReturnChart}
              onToggle={(checked) => updateSettings({ showRiskAdjustedReturnChart: checked })}
              meta={getPanelSignalMeta('riskAdjustedReturn')}
            >
              <div className="grid grid-cols-2 gap-1.5">
                <NumberInput
                  label="Lookback"
                  value={settings.riskAdjustedReturn.lookbackPeriod}
                  onChange={(lookbackPeriod) => updateRiskAdjustedReturn({ lookbackPeriod })}
                  defaultValue={60}
                />
                <NumberInput
                  label="Threshold"
                  value={settings.riskAdjustedReturn.threshold}
                  onChange={(threshold) => updateRiskAdjustedReturn({ threshold })}
                  step="0.1"
                  defaultValue={1.0}
                />
                <div className="space-y-1">
                  <Label className="text-xs text-muted-foreground">Ratio Type</Label>
                  <Select
                    value={settings.riskAdjustedReturn.ratioType}
                    onValueChange={(ratioType) =>
                      updateRiskAdjustedReturn({
                        ratioType: ratioType as ChartSettings['riskAdjustedReturn']['ratioType'],
                      })
                    }
                  >
                    <SelectTrigger className="h-7 text-xs glass-panel border-border/30 focus:border-primary/50">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent className="bg-background/95 backdrop-blur-md border-border shadow-xl">
                      <SelectItem value="sortino" className="text-xs">
                        sortino
                      </SelectItem>
                      <SelectItem value="sharpe" className="text-xs">
                        sharpe
                      </SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-1">
                  <Label className="text-xs text-muted-foreground">Condition</Label>
                  <Select
                    value={settings.riskAdjustedReturn.condition}
                    onValueChange={(condition) =>
                      updateRiskAdjustedReturn({
                        condition: condition as ChartSettings['riskAdjustedReturn']['condition'],
                      })
                    }
                  >
                    <SelectTrigger className="h-7 text-xs glass-panel border-border/30 focus:border-primary/50">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent className="bg-background/95 backdrop-blur-md border-border shadow-xl">
                      <SelectItem value="above" className="text-xs">
                        above
                      </SelectItem>
                      <SelectItem value="below" className="text-xs">
                        below
                      </SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </IndicatorToggle>

            <IndicatorToggle
              label="Volume Comparison"
              enabled={settings.showVolumeComparison}
              onToggle={(checked) => updateSettings({ showVolumeComparison: checked })}
              meta={getPanelSignalMeta('volumeComparison')}
            >
              <div className="grid grid-cols-2 gap-1.5">
                <NumberInput
                  label="Short Period"
                  value={settings.volumeComparison.shortPeriod}
                  onChange={(shortPeriod) => updateVolumeComparison({ shortPeriod })}
                  defaultValue={20}
                />
                <NumberInput
                  label="Long Period"
                  value={settings.volumeComparison.longPeriod}
                  onChange={(longPeriod) => updateVolumeComparison({ longPeriod })}
                  defaultValue={100}
                />
                <NumberInput
                  label="Lower Mult."
                  value={settings.volumeComparison.lowerMultiplier}
                  onChange={(lowerMultiplier) => updateVolumeComparison({ lowerMultiplier })}
                  step="0.1"
                  defaultValue={1.0}
                />
                <NumberInput
                  label="Higher Mult."
                  value={settings.volumeComparison.higherMultiplier}
                  onChange={(higherMultiplier) => updateVolumeComparison({ higherMultiplier })}
                  step="0.1"
                  defaultValue={1.5}
                />
              </div>
            </IndicatorToggle>

            <IndicatorToggle
              label="Trading Value MA"
              enabled={settings.showTradingValueMA}
              onToggle={(checked) => updateSettings({ showTradingValueMA: checked })}
              meta={getPanelSignalMeta('tradingValueMA')}
            >
              <NumberInput
                label="Period"
                value={settings.tradingValueMA.period}
                onChange={(period) => updateTradingValueMA({ period })}
                defaultValue={15}
              />
            </IndicatorToggle>
          </div>
        );

      case 'signalOverlay':
        return <SignalOverlayControls />;
    }
  };

  return (
    <div className="space-y-3 p-3">
      <ChartPresetSelector />

      <div className="glass-panel rounded-lg p-3 space-y-2">
        <SectionHeader icon={Search} title="Symbol Search" />
        <div className="space-y-2">
          <form onSubmit={handleSymbolSubmit} className="space-y-2" autoComplete="off">
            <StockSearchInput
              id={symbolSearchId}
              name="symbol-search"
              value={symbolInput}
              onValueChange={setSymbolInput}
              onSelect={handleSelectStock}
              className="glass-panel border-border/30 focus:border-primary/50 transition-all duration-200"
              searchLimit={50}
            />
            <Button
              type="submit"
              size="sm"
              className="w-full gradient-primary hover:opacity-90 transition-all duration-200"
            >
              <Search className="h-3.5 w-3.5 mr-1.5" />
              検索
            </Button>
          </form>
          {selectedSymbol && (
            <div className="flex items-center gap-1.5 px-2 py-1 rounded bg-primary/5 border border-primary/20">
              <TrendingUp className="h-3 w-3 text-primary" />
              <span className="text-xs font-medium text-primary">選択中: {selectedSymbol}</span>
            </div>
          )}
        </div>
      </div>

      <div className="glass-panel rounded-lg p-3 space-y-2">
        <SectionHeader icon={SettingsIcon} title="Settings" />
        <div className="space-y-1.5">
          {SETTING_DIALOGS.map((dialog) => (
            <Button
              key={dialog.id}
              type="button"
              variant="outline"
              className="w-full justify-start h-auto py-2 px-3 glass-panel border-border/30 hover:bg-accent/40"
              onClick={() => setOpenDialogId(dialog.id)}
            >
              <dialog.icon className="h-4 w-4 mr-2 text-muted-foreground" />
              <span className="text-sm">{dialog.title}</span>
            </Button>
          ))}
        </div>
      </div>

      {SETTING_DIALOGS.map((dialog) => (
        <Dialog
          key={dialog.id}
          open={openDialogId === dialog.id}
          onOpenChange={(open) => setOpenDialogId(open ? dialog.id : null)}
        >
          <DialogContent className="max-w-2xl">
            <DialogHeader>
              <DialogTitle>{dialog.title}</DialogTitle>
              <DialogDescription>{dialog.description}</DialogDescription>
            </DialogHeader>
            <div className="max-h-[70vh] overflow-y-auto pr-1">{renderDialogBody(dialog.id)}</div>
          </DialogContent>
        </Dialog>
      ))}
    </div>
  );
}

interface SectionHeaderProps {
  icon: React.ComponentType<{ className?: string }>;
  title: string;
}

function SectionHeader({ icon: Icon, title }: SectionHeaderProps) {
  return (
    <div className="flex items-center gap-2">
      <div className="gradient-primary rounded p-1.5">
        <Icon className="h-3.5 w-3.5 text-white" />
      </div>
      <h3 className="text-sm font-semibold text-foreground">{title}</h3>
    </div>
  );
}

interface ToggleRowProps {
  id: string;
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
  meta?: string;
}

function ToggleRow({ id, icon: Icon, label, checked, onCheckedChange, meta }: ToggleRowProps) {
  return (
    <div className="flex items-start justify-between p-2 rounded glass-panel gap-2">
      <div className="flex items-start gap-1.5 min-w-0">
        <Icon className="h-3.5 w-3.5 text-muted-foreground" />
        <div className="min-w-0">
          <Label htmlFor={id} className="text-xs font-medium cursor-pointer">
            {label}
          </Label>
          {meta && <p className="text-[10px] text-muted-foreground leading-tight">{meta}</p>}
        </div>
      </div>
      <Switch id={id} checked={checked} onCheckedChange={onCheckedChange} />
    </div>
  );
}
