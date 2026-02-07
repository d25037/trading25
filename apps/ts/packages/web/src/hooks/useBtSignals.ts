import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';
import { apiPost } from '@/lib/api-client';
import type { SignalOverlaySettings } from '@/stores/chartStore';

// ===== Types =====

export interface BtSignalSpec {
  type: string;
  params: Record<string, number | string | boolean>;
  mode: 'entry' | 'exit';
}

interface BtSignalComputeRequest {
  stock_code: string;
  source: 'market';
  timeframe: 'daily' | 'weekly' | 'monthly';
  signals: BtSignalSpec[];
  start_date?: string;
  end_date?: string;
}

interface BtSignalResult {
  trigger_dates: string[];
  count: number;
  error?: string;
}

interface BtSignalComputeResponse {
  stock_code: string;
  timeframe: string;
  signals: Record<string, BtSignalResult>;
}

// Phase 1対応シグナルリスト
export const PHASE1_SIGNAL_TYPES = [
  // oscillator
  'rsi_threshold',
  'rsi_spread',
  // breakout
  'period_breakout',
  'ma_breakout',
  'atr_support_break',
  'retracement',
  'mean_reversion',
  'crossover',
  'buy_and_hold',
  // volatility
  'bollinger_bands',
  // volume
  'volume',
  'trading_value',
  'trading_value_range',
] as const;

export type Phase1SignalType = (typeof PHASE1_SIGNAL_TYPES)[number];

// シグナルカテゴリ定義
export const SIGNAL_CATEGORIES = {
  oscillator: {
    label: 'Oscillator',
    signals: ['rsi_threshold', 'rsi_spread'],
  },
  breakout: {
    label: 'Breakout',
    signals: ['period_breakout', 'ma_breakout', 'atr_support_break', 'retracement', 'mean_reversion', 'crossover'],
  },
  volatility: {
    label: 'Volatility',
    signals: ['bollinger_bands'],
  },
  volume: {
    label: 'Volume',
    signals: ['volume', 'trading_value', 'trading_value_range'],
  },
} as const;

// シグナルデフォルトパラメータ
export const SIGNAL_DEFAULTS: Record<Phase1SignalType, Record<string, number | string | boolean>> = {
  rsi_threshold: { period: 14, threshold: 30, condition: 'below' },
  rsi_spread: { fast_period: 5, slow_period: 14, threshold: 10, condition: 'above' },
  period_breakout: { period: 20, direction: 'high', condition: 'above' },
  ma_breakout: { period: 20, ma_type: 'sma', direction: 'golden' },
  atr_support_break: { lookback_period: 20, atr_multiplier: 2.0, direction: 'below' },
  retracement: { lookback_period: 20, retracement_level: 0.382, direction: 'below' },
  mean_reversion: { baseline_type: 'sma', baseline_period: 20, deviation_threshold: 5.0, deviation_direction: 'below' },
  crossover: { type: 'sma', fast_period: 5, slow_period: 20, direction: 'golden' },
  buy_and_hold: {},
  bollinger_bands: { window: 20, alpha: 2.0, position: 'below_lower' },
  volume: { direction: 'above', threshold: 2.0, short_period: 5, long_period: 20 },
  trading_value: { period: 15, threshold_value: 100000000, direction: 'above' },
  trading_value_range: { period: 15, min_threshold: 50000000, max_threshold: 500000000 },
};

// シグナルラベル定義
export const SIGNAL_LABELS: Record<Phase1SignalType, string> = {
  rsi_threshold: 'RSI Threshold',
  rsi_spread: 'RSI Spread',
  period_breakout: 'Period Breakout',
  ma_breakout: 'MA Breakout',
  atr_support_break: 'ATR Support Break',
  retracement: 'Retracement',
  mean_reversion: 'Mean Reversion',
  crossover: 'Crossover',
  buy_and_hold: 'Buy & Hold',
  bollinger_bands: 'Bollinger Bands',
  volume: 'Volume',
  trading_value: 'Trading Value',
  trading_value_range: 'Trading Value Range',
};

// ===== Query Key Factory =====

export const btSignalKeys = {
  all: ['bt-signals'] as const,
  compute: (stockCode: string, timeframe: string, specsKey: string) =>
    ['bt-signals', 'compute', stockCode, timeframe, specsKey] as const,
};

// ===== Signal Marker Types =====

export interface SignalMarker {
  time: string;
  position: 'belowBar' | 'aboveBar';
  color: string;
  shape: 'arrowUp' | 'arrowDown';
  text: string;
  size: number;
}

// ===== Spec Builder =====

export function buildSignalSpecs(settings: SignalOverlaySettings | undefined): BtSignalSpec[] {
  // Guard: settingsまたはsignalsがundefinedの場合は空配列を返す
  if (!settings?.signals) {
    return [];
  }

  const specs: BtSignalSpec[] = [];

  for (const signal of settings.signals) {
    if (signal.enabled) {
      specs.push({
        type: signal.type,
        params: signal.params,
        mode: signal.mode,
      });
    }
  }

  return specs;
}

// ===== Response Mapper =====

interface SignalConfig {
  type: string;
  mode: 'entry' | 'exit';
  enabled: boolean;
}

function createMarkersForSignal(
  signal: SignalConfig,
  triggerDates: string[]
): SignalMarker[] {
  const isEntry = signal.mode === 'entry';
  const color = isEntry ? '#26a69a' : '#ef5350'; // green for entry, red for exit
  const shape = isEntry ? 'arrowUp' : 'arrowDown';
  const position = isEntry ? 'belowBar' : 'aboveBar';
  const text = SIGNAL_LABELS[signal.type as Phase1SignalType] || signal.type;

  return triggerDates.map((date) => ({
    time: date,
    position,
    color,
    shape,
    text,
    size: 1,
  }));
}

export function mapBtResponseToMarkers(
  response: BtSignalComputeResponse,
  settings: SignalOverlaySettings | undefined
): SignalMarker[] {
  if (!settings?.signals) {
    return [];
  }

  const markers: SignalMarker[] = [];

  for (const signal of settings.signals) {
    if (!signal.enabled) continue;

    const result = response.signals[signal.type];
    if (!result || result.error) continue;

    markers.push(...createMarkersForSignal(signal, result.trigger_dates));
  }

  markers.sort((a, b) => a.time.localeCompare(b.time));

  return markers;
}

// ===== Empty Result =====

const EMPTY_MARKERS: SignalMarker[] = [];

// ===== Main Hook =====

export function useBtSignals(
  stockCode: string | null,
  timeframe: 'daily' | 'weekly' | 'monthly',
  settings: SignalOverlaySettings | undefined,
  relativeMode: boolean
) {
  const specs = useMemo(() => buildSignalSpecs(settings), [settings]);
  const isEnabled = settings?.enabled ?? false;

  const specsKey = useMemo(() => JSON.stringify({ specs, relativeMode }), [specs, relativeMode]);

  // relativeModeでは売買代金シグナルを除外
  const filteredSpecs = useMemo(() => {
    if (!relativeMode) return specs;
    // relativeMode時は実価格が必要なシグナルを除外
    const realPriceSignals = new Set(['trading_value', 'trading_value_range', 'volume']);
    return specs.filter((s) => !realPriceSignals.has(s.type));
  }, [specs, relativeMode]);

  const query = useQuery({
    queryKey: btSignalKeys.compute(stockCode ?? '', timeframe, specsKey),
    queryFn: async () => {
      if (!stockCode) throw new Error('stockCode is required');
      const request: BtSignalComputeRequest = {
        stock_code: stockCode,
        source: 'market',
        timeframe,
        signals: filteredSpecs,
      };
      return apiPost<BtSignalComputeResponse>('/api/signals/compute', request);
    },
    enabled: !!stockCode && filteredSpecs.length > 0 && isEnabled,
    staleTime: 60_000,
    gcTime: 5 * 60_000,
    retry: 2,
  });

  const markers = useMemo(() => {
    if (!query.data) return EMPTY_MARKERS;
    return mapBtResponseToMarkers(query.data, settings);
  }, [query.data, settings]);

  return {
    markers,
    isLoading: query.isLoading,
    error: query.error,
    isError: query.isError,
  };
}
