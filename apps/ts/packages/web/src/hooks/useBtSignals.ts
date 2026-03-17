import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';
import { useSignalReference } from '@/hooks/useBacktest';
import { apiPost } from '@/lib/api-client';
import type { SignalOverlaySettings } from '@/stores/chartStore';
import type { SignalDefinition } from '@/types/backtest';

export interface BtSignalSpec {
  type: string;
  params: Record<string, number | string | boolean>;
  mode: 'entry' | 'exit';
}

interface BtSignalComputeRequest {
  stock_code: string;
  source: 'market';
  timeframe: 'daily' | 'weekly' | 'monthly';
  signals?: BtSignalSpec[];
  strategy_name?: string;
  start_date?: string;
  end_date?: string;
}

interface SignalDiagnostics {
  missing_required_data?: string[];
  used_fields?: string[];
  effective_period_type?: string | null;
  warnings?: string[];
}

interface BtSignalResult {
  label?: string | null;
  mode?: 'entry' | 'exit' | null;
  trigger_dates: string[];
  count: number;
  error?: string;
  diagnostics?: SignalDiagnostics;
}

interface DataProvenance {
  source_kind: 'market' | 'dataset';
  market_snapshot_id?: string | null;
  dataset_snapshot_id?: string | null;
  reference_date?: string | null;
  loaded_domains?: string[];
  strategy_name?: string | null;
  strategy_fingerprint?: string | null;
  warnings?: string[];
}

interface BtSignalComputeResponse {
  stock_code: string;
  timeframe: string;
  strategy_name?: string | null;
  signals: Record<string, BtSignalResult>;
  combined_entry?: BtSignalResult | null;
  combined_exit?: BtSignalResult | null;
  provenance: DataProvenance;
  diagnostics?: SignalDiagnostics;
}

export const btSignalKeys = {
  all: ['bt-signals'] as const,
  compute: (stockCode: string, timeframe: string, specsKey: string, strategyName: string | null) =>
    ['bt-signals', 'compute', stockCode, timeframe, specsKey, strategyName ?? 'manual'] as const,
};

const FALLBACK_RELATIVE_MODE_UNSUPPORTED_SIGNAL_TYPES = new Set([
  'volume_ratio_above',
  'volume_ratio_below',
  'trading_value',
  'trading_value_range',
]);

export interface SignalMarker {
  time: string;
  position: 'belowBar' | 'aboveBar';
  color: string;
  shape: 'arrowUp' | 'arrowDown';
  text: string;
  size: number;
}

export function buildSignalSpecs(settings: SignalOverlaySettings | undefined): BtSignalSpec[] {
  if (!settings?.signals) {
    return [];
  }

  return settings.signals
    .filter((signal) => signal.enabled)
    .map((signal) => ({
      type: signal.type,
      params: signal.params,
      mode: signal.mode,
    }));
}

function createMarkersForResult(
  result: BtSignalResult,
  fallbackLabel: string
): SignalMarker[] {
  const isEntry = result.mode !== 'exit';
  const color = isEntry ? '#26a69a' : '#ef5350';
  const shape = isEntry ? 'arrowUp' : 'arrowDown';
  const position = isEntry ? 'belowBar' : 'aboveBar';
  const text = result.label ?? fallbackLabel;

  return result.trigger_dates.map((date) => ({
    time: date,
    position,
    color,
    shape,
    text,
    size: 1,
  }));
}

function mapBtResponseToMarkers(
  response: BtSignalComputeResponse,
  settings: SignalOverlaySettings | undefined,
  strategyName: string | null
): SignalMarker[] {
  const markers: SignalMarker[] = [];

  if (strategyName) {
    if (response.combined_entry) {
      markers.push(...createMarkersForResult(response.combined_entry, `${strategyName} entry`));
    }
    if (response.combined_exit) {
      markers.push(...createMarkersForResult(response.combined_exit, `${strategyName} exit`));
    }
    markers.sort((a, b) => a.time.localeCompare(b.time));
    return markers;
  }

  if (!settings?.signals) {
    return [];
  }

  for (const signal of settings.signals) {
    if (!signal.enabled) continue;
    const result = response.signals[signal.type];
    if (!result || result.error) continue;
    markers.push(...createMarkersForResult(result, signal.type));
  }

  markers.sort((a, b) => a.time.localeCompare(b.time));
  return markers;
}

const EMPTY_MARKERS: SignalMarker[] = [];

function supportsRelativeMode(
  signalType: string,
  definitionsByType: Map<string, SignalDefinition>
): boolean {
  const definition = definitionsByType.get(signalType);
  if (definition) {
    return definition.chart?.supports_relative_mode !== false;
  }
  return !FALLBACK_RELATIVE_MODE_UNSUPPORTED_SIGNAL_TYPES.has(signalType);
}

export function useBtSignals(
  stockCode: string | null,
  timeframe: 'daily' | 'weekly' | 'monthly',
  settings: SignalOverlaySettings | undefined,
  strategyName: string | null,
  relativeMode: boolean
) {
  const { data: signalReference } = useSignalReference();
  const specs = useMemo(() => buildSignalSpecs(settings), [settings]);
  const definitionsByType = useMemo(
    () => new Map((signalReference?.signals ?? []).map((definition) => [definition.signal_type, definition])),
    [signalReference?.signals]
  );
  const filteredManualSpecs = useMemo(() => {
    if (!relativeMode) {
      return specs;
    }
    return specs.filter((spec) => supportsRelativeMode(spec.type, definitionsByType));
  }, [definitionsByType, relativeMode, specs]);
  const specsKey = useMemo(
    () => JSON.stringify({ specs: filteredManualSpecs, relativeMode }),
    [filteredManualSpecs, relativeMode]
  );
  const isManualOverlayEnabled = settings?.enabled ?? false;
  const shouldRun =
    !!stockCode && (Boolean(strategyName) || (isManualOverlayEnabled && filteredManualSpecs.length > 0));

  const query = useQuery({
    queryKey: btSignalKeys.compute(stockCode ?? '', timeframe, specsKey, strategyName),
    queryFn: async () => {
      if (!stockCode) throw new Error('stockCode is required');
      const request: BtSignalComputeRequest = {
        stock_code: stockCode,
        source: 'market',
        timeframe,
        ...(strategyName ? { strategy_name: strategyName } : { signals: filteredManualSpecs }),
      };
      return apiPost<BtSignalComputeResponse>('/api/signals/compute', request);
    },
    enabled: shouldRun,
    staleTime: 60_000,
    gcTime: 5 * 60_000,
    retry: 2,
  });

  const markers = useMemo(() => {
    if (!query.data) return EMPTY_MARKERS;
    return mapBtResponseToMarkers(query.data, settings, strategyName);
  }, [query.data, settings, strategyName]);

  return {
    markers,
    response: query.data ?? null,
    isLoading: query.isLoading,
    error: query.error,
    isError: query.isError,
  };
}
