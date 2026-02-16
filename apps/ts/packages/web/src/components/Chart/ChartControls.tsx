import { Activity, BarChart3, Eye, Loader2, Search, Settings as SettingsIcon, TrendingUp } from 'lucide-react';
import { useCallback, useEffect, useId, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { useSignalReference } from '@/hooks/useBacktest';
import { type StockSearchResultItem, useStockSearch } from '@/hooks/useStockSearch';
import { cn } from '@/lib/utils';
import type { ChartSettings } from '@/stores/chartStore';
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
  linkPanel: SignalLinkedPanel;
}

const PANEL_VISIBILITY_TOGGLES: PanelVisibilityToggle[] = [
  {
    id: 'show-fundamentals-panel',
    label: 'Fundamentals',
    settingKey: 'showFundamentalsPanel',
    linkPanel: 'fundamentals',
  },
  {
    id: 'show-fundamentals-history-panel',
    label: 'FY History',
    settingKey: 'showFundamentalsHistoryPanel',
    linkPanel: 'fundamentalsHistory',
  },
  {
    id: 'show-margin-pressure-panel',
    label: 'Margin Pressure',
    settingKey: 'showMarginPressurePanel',
    linkPanel: 'marginPressure',
  },
  {
    id: 'show-factor-regression-panel',
    label: 'Factor Regression',
    settingKey: 'showFactorRegressionPanel',
    linkPanel: 'factorRegression',
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
  const [debouncedQuery, setDebouncedQuery] = useState('');
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(-1);
  const [dropdownPosition, setDropdownPosition] = useState({ top: 0, left: 0, width: 0 });
  const inputRef = useRef<HTMLInputElement>(null);
  const suggestionsRef = useRef<HTMLDivElement>(null);

  // Generate unique IDs for form elements
  const symbolSearchId = useId();
  const showVolumeId = useId();
  const relativeModeId = useId();
  const visibleBarsId = useId();

  // Update dropdown position when input is focused or suggestions shown
  useEffect(() => {
    if (showSuggestions && inputRef.current) {
      const rect = inputRef.current.getBoundingClientRect();
      setDropdownPosition({ top: rect.bottom, left: rect.left, width: rect.width });
    }
  }, [showSuggestions]);

  // Debounce search query
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedQuery(symbolInput), 300);
    return () => clearTimeout(timer);
  }, [symbolInput]);

  // Search stocks with debounced query
  const { data: searchResults, isLoading: isSearching } = useStockSearch(debouncedQuery, {
    limit: 50,
    enabled: debouncedQuery.length >= 1,
  });
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

  // Auto-scroll selected item into view
  useEffect(() => {
    if (selectedIndex >= 0 && suggestionsRef.current) {
      const selectedElement = suggestionsRef.current.children[selectedIndex] as HTMLElement;
      selectedElement?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
  }, [selectedIndex]);

  // Handle click outside to close suggestions
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (
        suggestionsRef.current &&
        !suggestionsRef.current.contains(event.target as Node) &&
        inputRef.current &&
        !inputRef.current.contains(event.target as Node)
      ) {
        setShowSuggestions(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const handleSelectStock = useCallback(
    (stock: StockSearchResultItem) => {
      logger.debug('Stock selected from search', { code: stock.code, companyName: stock.companyName });
      setSelectedSymbol(stock.code);
      setSymbolInput('');
      setShowSuggestions(false);
      setSelectedIndex(-1);
    },
    [setSelectedSymbol]
  );

  const handleSymbolSubmit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (selectedIndex >= 0 && searchResults?.results?.[selectedIndex]) {
      handleSelectStock(searchResults.results[selectedIndex]);
      return;
    }
    if (symbolInput.trim()) {
      const symbol = symbolInput.trim().toUpperCase();
      logger.debug('Setting selected symbol', { symbol });
      setSelectedSymbol(symbol);
      setSymbolInput('');
      setShowSuggestions(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (!showSuggestions || !searchResults?.results?.length) return;

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setSelectedIndex((prev) => Math.min(prev + 1, searchResults.results.length - 1));
        break;
      case 'ArrowUp':
        e.preventDefault();
        setSelectedIndex((prev) => Math.max(prev - 1, -1));
        break;
      case 'Enter':
        if (selectedIndex >= 0 && searchResults.results[selectedIndex]) {
          e.preventDefault();
          handleSelectStock(searchResults.results[selectedIndex]);
        }
        break;
      case 'Escape':
        setShowSuggestions(false);
        setSelectedIndex(-1);
        break;
    }
  };

  const updatePanelVisibility = useCallback(
    (settingKey: PanelVisibilitySettingKey, checked: boolean) => {
      updateSettings({ [settingKey]: checked } as Partial<ChartSettings>);
    },
    [updateSettings]
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

  return (
    <div className="space-y-3 p-3">
      <ChartPresetSelector />

      {/* Symbol Search Section */}
      <div className="glass-panel rounded-lg p-3 space-y-2">
        <SectionHeader icon={Search} title="Symbol Search" />
        <div className="space-y-2">
          <form onSubmit={handleSymbolSubmit} className="space-y-2" autoComplete="off">
            <div className="relative">
              <Input
                ref={inputRef}
                id={symbolSearchId}
                type="search"
                name="symbol-search"
                placeholder="銘柄コードまたは会社名で検索..."
                value={symbolInput}
                onChange={(e) => {
                  setSymbolInput(e.target.value);
                  setShowSuggestions(true);
                  setSelectedIndex(-1);
                }}
                onFocus={() => setShowSuggestions(true)}
                onKeyDown={handleKeyDown}
                className="w-full glass-panel border-border/30 focus:border-primary/50 transition-all duration-200 pr-10"
                autoComplete="off"
                autoCapitalize="off"
                autoCorrect="off"
                spellCheck={false}
                inputMode="search"
                enterKeyHint="search"
                data-form-type="other"
                data-lpignore="true"
                data-1p-ignore="true"
              />
              {isSearching && (
                <div className="absolute right-3 top-1/2 -translate-y-1/2">
                  <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                </div>
              )}
              {showSuggestions && searchResults && searchResults.results.length > 0 && (
                <SearchSuggestions
                  containerRef={suggestionsRef}
                  results={searchResults.results}
                  selectedIndex={selectedIndex}
                  position={dropdownPosition}
                  onSelect={handleSelectStock}
                />
              )}
            </div>
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

      {/* Chart Settings */}
      <div className="glass-panel rounded-lg p-3 space-y-2">
        <SectionHeader icon={SettingsIcon} title="Chart Settings" />

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

      {/* Panel Visibility */}
      <div className="glass-panel rounded-lg p-3 space-y-2">
        <SectionHeader icon={Eye} title="Panel Visibility" />
        <div className="space-y-1.5">
          {PANEL_VISIBILITY_TOGGLES.map((toggle) => (
            <ToggleRow
              key={toggle.id}
              id={toggle.id}
              icon={BarChart3}
              label={toggle.label}
              checked={settings[toggle.settingKey]}
              onCheckedChange={(checked) => updatePanelVisibility(toggle.settingKey, checked)}
              meta={getPanelSignalMeta(toggle.linkPanel)}
            />
          ))}
        </div>
      </div>

      {/* Overlay Indicators */}
      <div className="glass-panel rounded-lg p-3 space-y-2">
        <SectionHeader icon={TrendingUp} title="Overlay Indicators" />

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

      {/* Sub-Chart Indicators */}
      <div className="glass-panel rounded-lg p-3 space-y-2">
        <SectionHeader icon={BarChart3} title="Sub-Chart Indicators" />

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

      {/* Signal Overlay */}
      <div className="glass-panel rounded-lg p-3 space-y-2">
        <SectionHeader icon={Activity} title="Signal Overlay" />
        <SignalOverlayControls />
      </div>
    </div>
  );
}

// Helper Components

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

interface SearchSuggestionsProps {
  containerRef: React.RefObject<HTMLDivElement | null>;
  results: StockSearchResultItem[];
  selectedIndex: number;
  position: { top: number; left: number; width: number };
  onSelect: (stock: StockSearchResultItem) => void;
}

function SearchSuggestions({ containerRef, results, selectedIndex, position, onSelect }: SearchSuggestionsProps) {
  return createPortal(
    <div
      ref={containerRef}
      style={{ position: 'fixed', top: position.top, left: position.left, width: position.width }}
      className="z-[9999] max-h-96 overflow-auto rounded-lg border border-border/50 bg-background/95 backdrop-blur-md shadow-xl"
    >
      {results.map((stock, index) => (
        <button
          key={stock.code}
          type="button"
          onClick={() => onSelect(stock)}
          className={cn(
            'w-full px-4 py-3 text-left hover:bg-accent/50 transition-colors',
            'border-b border-border/30 last:border-b-0',
            index === selectedIndex && 'bg-accent/50'
          )}
        >
          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-3 min-w-0">
              <span className="font-mono font-bold text-primary text-base">{stock.code}</span>
              <span className="text-sm text-foreground truncate">{stock.companyName}</span>
            </div>
            <span className="text-xs text-muted-foreground whitespace-nowrap">{stock.marketName}</span>
          </div>
          <div className="text-xs text-muted-foreground mt-1">{stock.sector33Name}</div>
        </button>
      ))}
    </div>,
    document.body
  );
}
