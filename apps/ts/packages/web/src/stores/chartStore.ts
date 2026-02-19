import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export type DisplayTimeframe = 'daily' | 'weekly' | 'monthly';
export type RiskAdjustedReturnRatioType = 'sharpe' | 'sortino';
export type RiskAdjustedReturnCondition = 'above' | 'below';
export type FundamentalsPanelId = 'fundamentals' | 'fundamentalsHistory' | 'marginPressure' | 'factorRegression';

export const DEFAULT_FUNDAMENTALS_PANEL_ORDER: FundamentalsPanelId[] = [
  'fundamentals',
  'fundamentalsHistory',
  'marginPressure',
  'factorRegression',
];

// Signal Overlay Types
export interface SignalConfig {
  type: string;
  params: Record<string, number | string | boolean>;
  mode: 'entry' | 'exit';
  enabled: boolean;
}

export interface SignalOverlaySettings {
  enabled: boolean;
  signals: SignalConfig[];
}

export interface ChartSettings {
  timeframe: '1D' | '1W' | '1M' | '3M' | '1Y';
  displayTimeframe: DisplayTimeframe;
  indicators: {
    sma: { enabled: boolean; period: number };
    ema: { enabled: boolean; period: number };
    macd: { enabled: boolean; fast: number; slow: number; signal: number };
    ppo: { enabled: boolean; fast: number; slow: number; signal: number };
    atrSupport: { enabled: boolean; period: number; multiplier: number };
    nBarSupport: { enabled: boolean; period: number };
    bollinger: { enabled: boolean; period: number; deviation: number };
  };
  volumeComparison: {
    shortPeriod: number;
    longPeriod: number;
    lowerMultiplier: number;
    higherMultiplier: number;
  };
  tradingValueMA: {
    period: number;
  };
  riskAdjustedReturn: {
    lookbackPeriod: number;
    ratioType: RiskAdjustedReturnRatioType;
    threshold: number;
    condition: RiskAdjustedReturnCondition;
  };
  chartType: 'candlestick' | 'line' | 'area';
  showVolume: boolean;
  showPPOChart: boolean;
  showVolumeComparison: boolean;
  showTradingValueMA: boolean;
  showRiskAdjustedReturnChart: boolean;
  showFundamentalsPanel: boolean;
  showFundamentalsHistoryPanel: boolean;
  showMarginPressurePanel: boolean;
  showFactorRegressionPanel: boolean;
  fundamentalsPanelOrder: FundamentalsPanelId[];
  visibleBars: number;
  relativeMode: boolean;
  signalOverlay: SignalOverlaySettings;
}

export interface ChartPreset {
  id: string;
  name: string;
  settings: ChartSettings;
  createdAt: number;
  updatedAt: number;
}

/**
 * Helper type for indicator settings update
 */
type IndicatorName = keyof ChartSettings['indicators'];
type IndicatorSettings<T extends IndicatorName> = Partial<ChartSettings['indicators'][T]>;

interface ChartState {
  // Current state
  selectedSymbol: string | null;
  settings: ChartSettings;

  // Preset management
  presets: ChartPreset[];
  activePresetId: string | null;

  // Symbol actions
  setSelectedSymbol: (symbol: string) => void;

  // Settings actions
  updateSettings: (settings: Partial<ChartSettings>) => void;
  toggleIndicator: (indicator: keyof ChartSettings['indicators']) => void;
  toggleRelativeMode: () => void;
  setDisplayTimeframe: (timeframe: DisplayTimeframe) => void;

  // Fine-grained indicator actions
  updateIndicatorSettings: <T extends IndicatorName>(indicator: T, settings: IndicatorSettings<T>) => void;
  updateVolumeComparison: (settings: Partial<ChartSettings['volumeComparison']>) => void;
  updateTradingValueMA: (settings: Partial<ChartSettings['tradingValueMA']>) => void;

  // Signal overlay actions
  toggleSignalOverlay: () => void;
  addSignal: (signal: Omit<SignalConfig, 'enabled'> & { enabled?: boolean }) => void;
  removeSignal: (type: string) => void;
  updateSignal: (type: string, updates: Partial<SignalConfig>) => void;
  toggleSignal: (type: string) => void;

  // Preset actions
  createPreset: (name: string) => string;
  updatePreset: (id: string, name?: string) => void;
  deletePreset: (id: string) => void;
  loadPreset: (id: string) => void;
  renamePreset: (id: string, name: string) => void;
  duplicatePreset: (id: string, newName: string) => string;
}

export const defaultSettings: ChartSettings = {
  timeframe: '1D',
  displayTimeframe: 'daily',
  indicators: {
    sma: { enabled: false, period: 20 },
    ema: { enabled: false, period: 12 },
    macd: { enabled: false, fast: 12, slow: 26, signal: 9 },
    ppo: { enabled: true, fast: 12, slow: 26, signal: 9 },
    atrSupport: { enabled: false, period: 20, multiplier: 3.0 },
    nBarSupport: { enabled: false, period: 60 },
    bollinger: { enabled: false, period: 20, deviation: 2.0 },
  },
  volumeComparison: {
    shortPeriod: 20,
    longPeriod: 100,
    lowerMultiplier: 1.0,
    higherMultiplier: 1.5,
  },
  tradingValueMA: {
    period: 15,
  },
  riskAdjustedReturn: {
    lookbackPeriod: 60,
    ratioType: 'sortino',
    threshold: 1.0,
    condition: 'above',
  },
  chartType: 'candlestick',
  showVolume: true,
  showPPOChart: false,
  showVolumeComparison: false,
  showTradingValueMA: false,
  showRiskAdjustedReturnChart: false,
  showFundamentalsPanel: true,
  showFundamentalsHistoryPanel: true,
  showMarginPressurePanel: true,
  showFactorRegressionPanel: true,
  fundamentalsPanelOrder: [...DEFAULT_FUNDAMENTALS_PANEL_ORDER],
  visibleBars: 120,
  relativeMode: false,
  signalOverlay: {
    enabled: false,
    signals: [],
  },
};

function generateId(): string {
  return crypto.randomUUID();
}

type PersistedChartStoreState = Partial<{
  selectedSymbol: string | null;
  settings: Partial<ChartSettings>;
  presets: ChartPreset[];
  activePresetId: string | null;
}>;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function toPartialRecord<T extends object>(value: unknown): Partial<T> {
  return isRecord(value) ? (value as Partial<T>) : {};
}

function normalizeSignal(signal: unknown): SignalConfig | null {
  if (!isRecord(signal)) return null;
  if (typeof signal.type !== 'string' || signal.type.length === 0) return null;
  if (signal.mode !== 'entry' && signal.mode !== 'exit') return null;

  const params = isRecord(signal.params)
    ? (Object.fromEntries(
        Object.entries(signal.params).filter(
          ([, value]) => typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean'
        )
      ) as Record<string, number | string | boolean>)
    : {};

  return {
    type: signal.type,
    params,
    mode: signal.mode,
    enabled: signal.enabled !== false,
  };
}

function normalizeBoolean(value: unknown, fallback: boolean): boolean {
  return typeof value === 'boolean' ? value : fallback;
}

function normalizeFiniteNumber(value: unknown, fallback: number): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
}

function normalizePositiveInt(value: unknown, fallback: number): number {
  const normalized = normalizeFiniteNumber(value, fallback);
  return Math.max(1, Math.trunc(normalized));
}

function isValidDisplayTimeframe(value: unknown): value is DisplayTimeframe {
  return value === 'daily' || value === 'weekly' || value === 'monthly';
}

function isValidChartTimeframe(value: unknown): value is ChartSettings['timeframe'] {
  return value === '1D' || value === '1W' || value === '1M' || value === '3M' || value === '1Y';
}

function isValidChartType(value: unknown): value is ChartSettings['chartType'] {
  return value === 'candlestick' || value === 'line' || value === 'area';
}

function isValidRiskAdjustedReturnRatioType(value: unknown): value is RiskAdjustedReturnRatioType {
  return value === 'sharpe' || value === 'sortino';
}

function isValidRiskAdjustedReturnCondition(value: unknown): value is RiskAdjustedReturnCondition {
  return value === 'above' || value === 'below';
}

function isValidFundamentalsPanelId(value: unknown): value is FundamentalsPanelId {
  return (
    value === 'fundamentals' ||
    value === 'fundamentalsHistory' ||
    value === 'marginPressure' ||
    value === 'factorRegression'
  );
}

function normalizeFundamentalsPanelOrder(value: unknown): FundamentalsPanelId[] {
  const normalizedOrder: FundamentalsPanelId[] = [];
  const seen = new Set<FundamentalsPanelId>();

  if (Array.isArray(value)) {
    for (const panelId of value) {
      if (!isValidFundamentalsPanelId(panelId) || seen.has(panelId)) continue;
      seen.add(panelId);
      normalizedOrder.push(panelId);
    }
  }

  for (const panelId of DEFAULT_FUNDAMENTALS_PANEL_ORDER) {
    if (seen.has(panelId)) continue;
    normalizedOrder.push(panelId);
  }

  return normalizedOrder;
}

function normalizeSettings(settings: unknown): ChartSettings {
  if (!isRecord(settings)) {
    return structuredClone(defaultSettings);
  }

  const partial = settings as Partial<ChartSettings>;
  const partialIndicators = toPartialRecord<ChartSettings['indicators']>(partial.indicators);
  const partialVolumeComparison = toPartialRecord<ChartSettings['volumeComparison']>(partial.volumeComparison);
  const partialTradingValueMA = toPartialRecord<ChartSettings['tradingValueMA']>(partial.tradingValueMA);
  const partialRiskAdjustedReturn = toPartialRecord<ChartSettings['riskAdjustedReturn']>(partial.riskAdjustedReturn);
  const partialSignalOverlay = toPartialRecord<ChartSettings['signalOverlay']>(partial.signalOverlay);
  const partialSma = toPartialRecord<ChartSettings['indicators']['sma']>(partialIndicators.sma);
  const partialEma = toPartialRecord<ChartSettings['indicators']['ema']>(partialIndicators.ema);
  const partialMacd = toPartialRecord<ChartSettings['indicators']['macd']>(partialIndicators.macd);
  const partialPpo = toPartialRecord<ChartSettings['indicators']['ppo']>(partialIndicators.ppo);
  const partialAtrSupport = toPartialRecord<ChartSettings['indicators']['atrSupport']>(partialIndicators.atrSupport);
  const partialNBarSupport = toPartialRecord<ChartSettings['indicators']['nBarSupport']>(partialIndicators.nBarSupport);
  const partialBollinger = toPartialRecord<ChartSettings['indicators']['bollinger']>(partialIndicators.bollinger);
  const defaultSignalOverlay = defaultSettings.signalOverlay;

  return {
    timeframe: isValidChartTimeframe(partial.timeframe) ? partial.timeframe : defaultSettings.timeframe,
    displayTimeframe: isValidDisplayTimeframe(partial.displayTimeframe)
      ? partial.displayTimeframe
      : defaultSettings.displayTimeframe,
    indicators: {
      sma: {
        enabled: normalizeBoolean(partialSma.enabled, defaultSettings.indicators.sma.enabled),
        period: normalizePositiveInt(partialSma.period, defaultSettings.indicators.sma.period),
      },
      ema: {
        enabled: normalizeBoolean(partialEma.enabled, defaultSettings.indicators.ema.enabled),
        period: normalizePositiveInt(partialEma.period, defaultSettings.indicators.ema.period),
      },
      macd: {
        enabled: normalizeBoolean(partialMacd.enabled, defaultSettings.indicators.macd.enabled),
        fast: normalizePositiveInt(partialMacd.fast, defaultSettings.indicators.macd.fast),
        slow: normalizePositiveInt(partialMacd.slow, defaultSettings.indicators.macd.slow),
        signal: normalizePositiveInt(partialMacd.signal, defaultSettings.indicators.macd.signal),
      },
      ppo: {
        enabled: normalizeBoolean(partialPpo.enabled, defaultSettings.indicators.ppo.enabled),
        fast: normalizePositiveInt(partialPpo.fast, defaultSettings.indicators.ppo.fast),
        slow: normalizePositiveInt(partialPpo.slow, defaultSettings.indicators.ppo.slow),
        signal: normalizePositiveInt(partialPpo.signal, defaultSettings.indicators.ppo.signal),
      },
      atrSupport: {
        enabled: normalizeBoolean(partialAtrSupport.enabled, defaultSettings.indicators.atrSupport.enabled),
        period: normalizePositiveInt(partialAtrSupport.period, defaultSettings.indicators.atrSupport.period),
        multiplier: normalizeFiniteNumber(
          partialAtrSupport.multiplier,
          defaultSettings.indicators.atrSupport.multiplier
        ),
      },
      nBarSupport: {
        enabled: normalizeBoolean(partialNBarSupport.enabled, defaultSettings.indicators.nBarSupport.enabled),
        period: normalizePositiveInt(partialNBarSupport.period, defaultSettings.indicators.nBarSupport.period),
      },
      bollinger: {
        enabled: normalizeBoolean(partialBollinger.enabled, defaultSettings.indicators.bollinger.enabled),
        period: normalizePositiveInt(partialBollinger.period, defaultSettings.indicators.bollinger.period),
        deviation: normalizeFiniteNumber(partialBollinger.deviation, defaultSettings.indicators.bollinger.deviation),
      },
    },
    volumeComparison: {
      shortPeriod: normalizePositiveInt(
        partialVolumeComparison.shortPeriod,
        defaultSettings.volumeComparison.shortPeriod
      ),
      longPeriod: normalizePositiveInt(partialVolumeComparison.longPeriod, defaultSettings.volumeComparison.longPeriod),
      lowerMultiplier: normalizeFiniteNumber(
        partialVolumeComparison.lowerMultiplier,
        defaultSettings.volumeComparison.lowerMultiplier
      ),
      higherMultiplier: normalizeFiniteNumber(
        partialVolumeComparison.higherMultiplier,
        defaultSettings.volumeComparison.higherMultiplier
      ),
    },
    tradingValueMA: {
      period: normalizePositiveInt(partialTradingValueMA.period, defaultSettings.tradingValueMA.period),
    },
    riskAdjustedReturn: {
      lookbackPeriod: normalizePositiveInt(
        partialRiskAdjustedReturn.lookbackPeriod,
        defaultSettings.riskAdjustedReturn.lookbackPeriod
      ),
      ratioType: isValidRiskAdjustedReturnRatioType(partialRiskAdjustedReturn.ratioType)
        ? partialRiskAdjustedReturn.ratioType
        : defaultSettings.riskAdjustedReturn.ratioType,
      threshold: normalizeFiniteNumber(
        partialRiskAdjustedReturn.threshold,
        defaultSettings.riskAdjustedReturn.threshold
      ),
      condition: isValidRiskAdjustedReturnCondition(partialRiskAdjustedReturn.condition)
        ? partialRiskAdjustedReturn.condition
        : defaultSettings.riskAdjustedReturn.condition,
    },
    chartType: isValidChartType(partial.chartType) ? partial.chartType : defaultSettings.chartType,
    showVolume: normalizeBoolean(partial.showVolume, defaultSettings.showVolume),
    showPPOChart: normalizeBoolean(partial.showPPOChart, defaultSettings.showPPOChart),
    showVolumeComparison: normalizeBoolean(partial.showVolumeComparison, defaultSettings.showVolumeComparison),
    showTradingValueMA: normalizeBoolean(partial.showTradingValueMA, defaultSettings.showTradingValueMA),
    showRiskAdjustedReturnChart: normalizeBoolean(
      partial.showRiskAdjustedReturnChart,
      defaultSettings.showRiskAdjustedReturnChart
    ),
    showFundamentalsPanel: normalizeBoolean(partial.showFundamentalsPanel, defaultSettings.showFundamentalsPanel),
    showFundamentalsHistoryPanel: normalizeBoolean(
      partial.showFundamentalsHistoryPanel,
      defaultSettings.showFundamentalsHistoryPanel
    ),
    showMarginPressurePanel: normalizeBoolean(partial.showMarginPressurePanel, defaultSettings.showMarginPressurePanel),
    showFactorRegressionPanel: normalizeBoolean(
      partial.showFactorRegressionPanel,
      defaultSettings.showFactorRegressionPanel
    ),
    fundamentalsPanelOrder: normalizeFundamentalsPanelOrder(partial.fundamentalsPanelOrder),
    visibleBars: normalizePositiveInt(partial.visibleBars, defaultSettings.visibleBars),
    relativeMode: normalizeBoolean(partial.relativeMode, defaultSettings.relativeMode),
    signalOverlay: {
      enabled: normalizeBoolean(partialSignalOverlay.enabled, defaultSignalOverlay.enabled),
      signals: Array.isArray(partialSignalOverlay.signals)
        ? partialSignalOverlay.signals.map(normalizeSignal).filter((signal): signal is SignalConfig => signal !== null)
        : defaultSignalOverlay.signals,
    },
  };
}

function normalizePresets(presets: unknown): ChartPreset[] {
  if (!Array.isArray(presets)) return [];

  return presets
    .map((preset): ChartPreset | null => {
      if (!isRecord(preset)) return null;
      if (typeof preset.id !== 'string' || typeof preset.name !== 'string') return null;

      return {
        id: preset.id,
        name: preset.name,
        settings: normalizeSettings(preset.settings),
        createdAt: typeof preset.createdAt === 'number' ? preset.createdAt : Date.now(),
        updatedAt: typeof preset.updatedAt === 'number' ? preset.updatedAt : Date.now(),
      };
    })
    .filter((preset): preset is ChartPreset => preset !== null);
}

function mergePersistedChartStoreState(persistedState: unknown, currentState: ChartState): ChartState {
  const persisted = (isRecord(persistedState) ? persistedState : {}) as PersistedChartStoreState;
  const presets = normalizePresets(persisted.presets);
  const activePresetId =
    typeof persisted.activePresetId === 'string' && presets.some((preset) => preset.id === persisted.activePresetId)
      ? persisted.activePresetId
      : null;

  return {
    ...currentState,
    selectedSymbol: typeof persisted.selectedSymbol === 'string' ? persisted.selectedSymbol : null,
    settings: normalizeSettings(persisted.settings),
    presets,
    activePresetId,
  };
}

export const useChartStore = create<ChartState>()(
  persist(
    (set, get) => ({
      selectedSymbol: null,
      settings: defaultSettings,
      presets: [],
      activePresetId: null,

      setSelectedSymbol: (symbol) => set({ selectedSymbol: symbol }),

      updateSettings: (newSettings) =>
        set((state) => ({
          settings: { ...state.settings, ...newSettings },
        })),

      toggleIndicator: (indicator) =>
        set((state) => ({
          settings: {
            ...state.settings,
            indicators: {
              ...state.settings.indicators,
              [indicator]: {
                ...state.settings.indicators[indicator],
                enabled: !state.settings.indicators[indicator].enabled,
              },
            },
          },
        })),

      toggleRelativeMode: () =>
        set((state) => ({
          settings: {
            ...state.settings,
            relativeMode: !state.settings.relativeMode,
          },
        })),

      setDisplayTimeframe: (timeframe) =>
        set((state) => ({
          settings: {
            ...state.settings,
            displayTimeframe: timeframe,
          },
        })),

      updateIndicatorSettings: (indicator, newSettings) =>
        set((state) => ({
          settings: {
            ...state.settings,
            indicators: {
              ...state.settings.indicators,
              [indicator]: {
                ...state.settings.indicators[indicator],
                ...newSettings,
              },
            },
          },
        })),

      updateVolumeComparison: (newSettings) =>
        set((state) => ({
          settings: {
            ...state.settings,
            volumeComparison: {
              ...state.settings.volumeComparison,
              ...newSettings,
            },
          },
        })),

      updateTradingValueMA: (newSettings) =>
        set((state) => ({
          settings: {
            ...state.settings,
            tradingValueMA: {
              ...state.settings.tradingValueMA,
              ...newSettings,
            },
          },
        })),

      toggleSignalOverlay: () =>
        set((state) => ({
          settings: {
            ...state.settings,
            signalOverlay: {
              ...state.settings.signalOverlay,
              enabled: !state.settings.signalOverlay.enabled,
            },
          },
        })),

      addSignal: (signal) =>
        set((state) => {
          // 同じtypeのシグナルが既にあれば追加しない
          if (state.settings.signalOverlay.signals.some((s) => s.type === signal.type)) {
            return state;
          }
          return {
            settings: {
              ...state.settings,
              signalOverlay: {
                ...state.settings.signalOverlay,
                signals: [...state.settings.signalOverlay.signals, { ...signal, enabled: signal.enabled ?? true }],
              },
            },
          };
        }),

      removeSignal: (type) =>
        set((state) => ({
          settings: {
            ...state.settings,
            signalOverlay: {
              ...state.settings.signalOverlay,
              signals: state.settings.signalOverlay.signals.filter((s) => s.type !== type),
            },
          },
        })),

      updateSignal: (type, updates) =>
        set((state) => ({
          settings: {
            ...state.settings,
            signalOverlay: {
              ...state.settings.signalOverlay,
              signals: state.settings.signalOverlay.signals.map((s) => (s.type === type ? { ...s, ...updates } : s)),
            },
          },
        })),

      toggleSignal: (type) =>
        set((state) => ({
          settings: {
            ...state.settings,
            signalOverlay: {
              ...state.settings.signalOverlay,
              signals: state.settings.signalOverlay.signals.map((s) =>
                s.type === type ? { ...s, enabled: !s.enabled } : s
              ),
            },
          },
        })),

      createPreset: (name) => {
        const id = generateId();
        const now = Date.now();
        const newPreset: ChartPreset = {
          id,
          name,
          settings: structuredClone(get().settings),
          createdAt: now,
          updatedAt: now,
        };
        set((state) => ({
          presets: [...state.presets, newPreset],
          activePresetId: id,
        }));
        return id;
      },

      updatePreset: (id) => {
        set((state) => ({
          presets: state.presets.map((preset) =>
            preset.id === id
              ? {
                  ...preset,
                  settings: structuredClone(state.settings),
                  updatedAt: Date.now(),
                }
              : preset
          ),
        }));
      },

      deletePreset: (id) => {
        set((state) => ({
          presets: state.presets.filter((preset) => preset.id !== id),
          activePresetId: state.activePresetId === id ? null : state.activePresetId,
        }));
      },

      loadPreset: (id) => {
        const preset = get().presets.find((p) => p.id === id);
        if (preset) {
          set({
            settings: structuredClone(preset.settings),
            activePresetId: id,
          });
        }
      },

      renamePreset: (id, name) => {
        set((state) => ({
          presets: state.presets.map((preset) =>
            preset.id === id ? { ...preset, name, updatedAt: Date.now() } : preset
          ),
        }));
      },

      duplicatePreset: (id, newName) => {
        const preset = get().presets.find((p) => p.id === id);
        if (!preset) return '';

        const newId = generateId();
        const now = Date.now();
        const newPreset: ChartPreset = {
          id: newId,
          name: newName,
          settings: structuredClone(preset.settings),
          createdAt: now,
          updatedAt: now,
        };
        set((state) => ({
          presets: [...state.presets, newPreset],
          activePresetId: newId,
        }));
        return newId;
      },
    }),
    {
      name: 'trading25-chart-store',
      merge: mergePersistedChartStoreState,
      partialize: (state) => ({
        selectedSymbol: state.selectedSymbol,
        settings: state.settings,
        presets: state.presets,
        activePresetId: state.activePresetId,
      }),
    }
  )
);
