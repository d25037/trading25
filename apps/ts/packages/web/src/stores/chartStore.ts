import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export type DisplayTimeframe = 'daily' | 'weekly' | 'monthly';

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
  chartType: 'candlestick' | 'line' | 'area';
  showVolume: boolean;
  showPPOChart: boolean;
  showVolumeComparison: boolean;
  showTradingValueMA: boolean;
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
  chartType: 'candlestick',
  showVolume: true,
  showPPOChart: false,
  showVolumeComparison: false,
  showTradingValueMA: false,
  visibleBars: 120,
  relativeMode: false,
  signalOverlay: {
    enabled: false,
    signals: [],
  },
};

const generateId = () => crypto.randomUUID();

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
                signals: [
                  ...state.settings.signalOverlay.signals,
                  { ...signal, enabled: signal.enabled ?? true },
                ],
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
              signals: state.settings.signalOverlay.signals.map((s) =>
                s.type === type ? { ...s, ...updates } : s
              ),
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
      partialize: (state) => ({
        selectedSymbol: state.selectedSymbol,
        settings: state.settings,
        presets: state.presets,
        activePresetId: state.activePresetId,
      }),
    }
  )
);
