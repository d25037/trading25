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
  'baseline_deviation',
  'period_extrema_break',
  'period_extrema_position',
  'atr_support_position',
  'atr_support_cross',
  'buy_and_hold',
  // trend
  'baseline_cross',
  'baseline_position',
  'retracement_position',
  'retracement_cross',
  'crossover',
  // volatility
  'volatility_percentile',
  'bollinger_position',
  'bollinger_cross',
  // volume
  'volume_ratio_above',
  'volume_ratio_below',
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
    signals: [
      'baseline_deviation',
      'period_extrema_break',
      'period_extrema_position',
      'atr_support_position',
      'atr_support_cross',
      'buy_and_hold',
    ],
  },
  trend: {
    label: 'Trend',
    signals: ['baseline_cross', 'baseline_position', 'retracement_position', 'retracement_cross', 'crossover'],
  },
  volatility: {
    label: 'Volatility',
    signals: ['volatility_percentile', 'bollinger_position', 'bollinger_cross'],
  },
  volume: {
    label: 'Volume',
    signals: ['volume_ratio_above', 'volume_ratio_below', 'trading_value', 'trading_value_range'],
  },
} as const;

// シグナルデフォルトパラメータ
export const SIGNAL_DEFAULTS: Record<Phase1SignalType, Record<string, number | string | boolean>> = {
  rsi_threshold: { period: 14, threshold: 30, condition: 'below' },
  rsi_spread: { fast_period: 5, slow_period: 14, threshold: 10, condition: 'above' },
  baseline_deviation: { baseline_type: 'sma', baseline_period: 20, deviation_threshold: 0.05, direction: 'below' },
  period_extrema_break: { period: 20, direction: 'high', lookback_days: 1 },
  period_extrema_position: { period: 20, direction: 'high', state: 'at_extrema', lookback_days: 1 },
  atr_support_position: { lookback_period: 20, atr_multiplier: 2.0, direction: 'below', price_column: 'close' },
  atr_support_cross: { lookback_period: 20, atr_multiplier: 2.0, direction: 'below', lookback_days: 1, price_column: 'close' },
  buy_and_hold: {},
  baseline_cross: { baseline_type: 'sma', baseline_period: 200, direction: 'above', lookback_days: 1, price_column: 'close' },
  baseline_position: { baseline_type: 'sma', baseline_period: 20, price_column: 'close', direction: 'above' },
  retracement_position: { lookback_period: 20, retracement_level: 0.382, direction: 'below', price_column: 'close' },
  retracement_cross: {
    lookback_period: 20,
    retracement_level: 0.382,
    direction: 'below',
    lookback_days: 1,
    price_column: 'close',
  },
  crossover: { type: 'sma', fast_period: 5, slow_period: 20, direction: 'golden' },
  volatility_percentile: { window: 20, lookback: 252, percentile: 50.0 },
  bollinger_position: { window: 20, alpha: 2.0, level: 'lower', direction: 'below' },
  bollinger_cross: { window: 20, alpha: 2.0, level: 'lower', direction: 'below', lookback_days: 1 },
  volume_ratio_above: { ratio_threshold: 1.5, short_period: 20, long_period: 100, ma_type: 'sma' },
  volume_ratio_below: { ratio_threshold: 0.7, short_period: 20, long_period: 100, ma_type: 'sma' },
  trading_value: { period: 15, threshold_value: 100000000, direction: 'above' },
  trading_value_range: { period: 15, min_threshold: 50000000, max_threshold: 500000000 },
};

// シグナルラベル定義
export const SIGNAL_LABELS: Record<Phase1SignalType, string> = {
  rsi_threshold: 'RSI Threshold',
  rsi_spread: 'RSI Spread',
  baseline_deviation: 'Baseline Deviation',
  period_extrema_break: 'Period Extrema Break',
  period_extrema_position: 'Period Extrema Position',
  atr_support_position: 'ATR Support Position',
  atr_support_cross: 'ATR Support Cross',
  buy_and_hold: 'Buy & Hold',
  baseline_cross: 'Baseline Cross',
  baseline_position: 'Baseline Position',
  retracement_position: 'Retracement Position',
  retracement_cross: 'Retracement Cross',
  crossover: 'Crossover',
  volatility_percentile: 'Volatility Percentile',
  bollinger_position: 'Bollinger Position',
  bollinger_cross: 'Bollinger Cross',
  volume_ratio_above: 'Volume Ratio Above',
  volume_ratio_below: 'Volume Ratio Below',
  trading_value: 'Trading Value',
  trading_value_range: 'Trading Value Range',
};

const RELATIVE_MODE_DISABLED_SIGNAL_TYPES = new Set<Phase1SignalType>([
  'volume_ratio_above',
  'volume_ratio_below',
  'trading_value',
  'trading_value_range',
]);

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
    return specs.filter(
      (s): s is BtSignalSpec =>
        !RELATIVE_MODE_DISABLED_SIGNAL_TYPES.has(s.type as Phase1SignalType)
    );
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
