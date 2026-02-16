import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';
import { apiPost } from '@/lib/api-client';
import type { ChartSettings } from '@/stores/chartStore';
import type {
  BollingerBandsData,
  ChartData,
  IndicatorData,
  IndicatorValue,
  MACDIndicatorData,
  PPOIndicatorData,
  TradingValueMAData,
  VolumeComparisonData,
} from '@/types/chart';

// ===== Types =====

export interface BtIndicatorSpec {
  type: string;
  params: Record<string, number | string>;
}

interface BtIndicatorComputeRequest {
  stock_code: string;
  source: 'market';
  timeframe: 'daily' | 'weekly' | 'monthly';
  indicators: BtIndicatorSpec[];
  benchmark_code?: string;
  relative_options?: {
    align_dates?: boolean;
    handle_zero_division?: 'skip' | 'zero';
  };
}

interface BtIndicatorComputeResponse {
  stock_code: string;
  timeframe: string;
  meta: { bars: number };
  indicators: Record<string, BtIndicatorRecord[]>;
}

interface BtIndicatorRecord {
  date: string;
  [key: string]: string | number | null;
}

// ===== Query Key Factory =====

export const btIndicatorKeys = {
  all: ['bt-indicators'] as const,
  compute: (stockCode: string, timeframe: string, specsKey: string) =>
    ['bt-indicators', 'compute', stockCode, timeframe, specsKey] as const,
};

// ===== Spec Builder =====

export function buildIndicatorSpecs(settings: ChartSettings): BtIndicatorSpec[] {
  const specs: BtIndicatorSpec[] = [];
  const { indicators } = settings;

  if (indicators.sma.enabled) {
    specs.push({ type: 'sma', params: { period: indicators.sma.period } });
  }
  if (indicators.ema.enabled) {
    specs.push({ type: 'ema', params: { period: indicators.ema.period } });
  }
  if (indicators.macd.enabled) {
    specs.push({
      type: 'macd',
      params: {
        fast_period: indicators.macd.fast,
        slow_period: indicators.macd.slow,
        signal_period: indicators.macd.signal,
      },
    });
  }
  if (indicators.ppo.enabled) {
    specs.push({
      type: 'ppo',
      params: {
        fast_period: indicators.ppo.fast,
        slow_period: indicators.ppo.slow,
        signal_period: indicators.ppo.signal,
      },
    });
  }
  if (indicators.bollinger.enabled) {
    specs.push({
      type: 'bollinger',
      params: {
        period: indicators.bollinger.period,
        std_dev: indicators.bollinger.deviation,
      },
    });
  }
  if (indicators.atrSupport.enabled) {
    specs.push({
      type: 'atr_support',
      params: {
        lookback_period: indicators.atrSupport.period,
        atr_multiplier: indicators.atrSupport.multiplier,
      },
    });
  }
  if (indicators.nBarSupport.enabled) {
    specs.push({ type: 'nbar_support', params: { period: indicators.nBarSupport.period } });
  }
  if (settings.showVolumeComparison) {
    specs.push({
      type: 'volume_comparison',
      params: {
        short_period: settings.volumeComparison.shortPeriod,
        long_period: settings.volumeComparison.longPeriod,
        lower_multiplier: settings.volumeComparison.lowerMultiplier,
        higher_multiplier: settings.volumeComparison.higherMultiplier,
      },
    });
  }
  if (settings.showTradingValueMA) {
    specs.push({ type: 'trading_value_ma', params: { period: settings.tradingValueMA.period } });
  }
  if (settings.showRiskAdjustedReturnChart) {
    specs.push({
      type: 'risk_adjusted_return',
      params: {
        lookback_period: settings.riskAdjustedReturn.lookbackPeriod,
        ratio_type: settings.riskAdjustedReturn.ratioType,
      },
    });
  }

  return specs;
}

// ===== Response Transformers =====
// Note: Type assertions (as number) are used here because apps/bt/ API is an internal service
// with OpenAPI-defined contracts. Null checks are performed via .filter() before mapping.

function transformSingleValueRecords(records: BtIndicatorRecord[]): IndicatorValue[] {
  return records.filter((r) => r.value != null).map((r) => ({ time: r.date, value: r.value as number }));
}

function transformMACDRecords(records: BtIndicatorRecord[]): MACDIndicatorData[] {
  return records
    .filter((r) => r.macd != null && r.signal != null && r.histogram != null)
    .map((r) => ({
      time: r.date,
      macd: r.macd as number,
      signal: r.signal as number,
      histogram: r.histogram as number,
    }));
}

function transformPPORecords(records: BtIndicatorRecord[]): PPOIndicatorData[] {
  return records
    .filter((r) => r.ppo != null && r.signal != null && r.histogram != null)
    .map((r) => ({
      time: r.date,
      ppo: r.ppo as number,
      signal: r.signal as number,
      histogram: r.histogram as number,
    }));
}

function transformBollingerRecords(records: BtIndicatorRecord[]): BollingerBandsData[] {
  return records
    .filter((r) => r.upper != null && r.middle != null && r.lower != null)
    .map((r) => ({
      time: r.date,
      upper: r.upper as number,
      middle: r.middle as number,
      lower: r.lower as number,
    }));
}

function transformVolumeComparisonRecords(records: BtIndicatorRecord[]): VolumeComparisonData[] {
  return records
    .filter((r) => r.shortMA != null && r.longThresholdLower != null && r.longThresholdHigher != null)
    .map((r) => ({
      time: r.date,
      shortMA: r.shortMA as number,
      longThresholdLower: r.longThresholdLower as number,
      longThresholdHigher: r.longThresholdHigher as number,
    }));
}

// transformTradingValueMARecords is intentionally separate from transformSingleValueRecords
// for type clarity, even though they have the same implementation
function transformTradingValueMARecords(records: BtIndicatorRecord[]): TradingValueMAData[] {
  return transformSingleValueRecords(records);
}

// ===== Response Mapper =====

type IndicatorTransformResult =
  | {
      target: 'indicator';
      name: string;
      data: IndicatorData[];
    }
  | {
      target: 'bollinger';
      data: BollingerBandsData[];
    }
  | {
      target: 'volumeComparison';
      data: VolumeComparisonData[];
    }
  | {
      target: 'tradingValueMA';
      data: TradingValueMAData[];
    };

const INDICATOR_KEY_TRANSFORMS: Array<{
  prefix: string;
  transform: (records: BtIndicatorRecord[]) => IndicatorTransformResult;
}> = [
  { prefix: 'sma_', transform: (r) => ({ target: 'indicator', name: 'sma', data: transformSingleValueRecords(r) }) },
  { prefix: 'ema_', transform: (r) => ({ target: 'indicator', name: 'ema', data: transformSingleValueRecords(r) }) },
  { prefix: 'macd_', transform: (r) => ({ target: 'indicator', name: 'macd', data: transformMACDRecords(r) }) },
  { prefix: 'ppo_', transform: (r) => ({ target: 'indicator', name: 'ppo', data: transformPPORecords(r) }) },
  { prefix: 'bollinger_', transform: (r) => ({ target: 'bollinger', data: transformBollingerRecords(r) }) },
  {
    prefix: 'atr_support_',
    transform: (r) => ({ target: 'indicator', name: 'atrSupport', data: transformSingleValueRecords(r) }),
  },
  {
    prefix: 'nbar_support_',
    transform: (r) => ({ target: 'indicator', name: 'nBarSupport', data: transformSingleValueRecords(r) }),
  },
  {
    prefix: 'volume_comparison_',
    transform: (r) => ({ target: 'volumeComparison', data: transformVolumeComparisonRecords(r) }),
  },
  {
    prefix: 'trading_value_ma_',
    transform: (r) => ({ target: 'tradingValueMA', data: transformTradingValueMARecords(r) }),
  },
  {
    prefix: 'risk_adjusted_return_',
    transform: (r) => ({ target: 'indicator', name: 'riskAdjustedReturn', data: transformSingleValueRecords(r) }),
  },
];

function transformIndicatorKey(key: string, records: BtIndicatorRecord[]): IndicatorTransformResult | null {
  const entry = INDICATOR_KEY_TRANSFORMS.find((e) => key.startsWith(e.prefix));
  return entry ? entry.transform(records) : null;
}

export function mapBtResponseToChartData(response: BtIndicatorComputeResponse): Omit<ChartData, 'candlestickData'> {
  const indicators: Record<string, IndicatorData[]> = {};
  let bollingerBands: BollingerBandsData[] | undefined;
  let volumeComparison: VolumeComparisonData[] | undefined;
  let tradingValueMA: TradingValueMAData[] | undefined;

  for (const [key, records] of Object.entries(response.indicators)) {
    const result = transformIndicatorKey(key, records);
    if (!result) continue;

    switch (result.target) {
      case 'indicator':
        indicators[result.name] = result.data;
        break;
      case 'bollinger':
        bollingerBands = result.data;
        break;
      case 'volumeComparison':
        volumeComparison = result.data;
        break;
      case 'tradingValueMA':
        tradingValueMA = result.data;
        break;
    }
  }

  return { indicators, bollingerBands, volumeComparison, tradingValueMA };
}

// ===== Empty Chart Data =====

const EMPTY_CHART_PARTIAL: Omit<ChartData, 'candlestickData'> = {
  indicators: {},
};

// ===== Main Hook =====

export function useBtIndicators(
  stockCode: string | null,
  timeframe: 'daily' | 'weekly' | 'monthly',
  settings: ChartSettings
) {
  const specs = useMemo(() => buildIndicatorSpecs(settings), [settings]);

  const specsKey = useMemo(() => JSON.stringify({ specs, relativeMode: settings.relativeMode }), [
    specs,
    settings.relativeMode,
  ]);

  const query = useQuery({
    queryKey: btIndicatorKeys.compute(stockCode ?? '', timeframe, specsKey),
    queryFn: () => {
      if (!stockCode) throw new Error('stockCode is required');
      const request: BtIndicatorComputeRequest = {
        stock_code: stockCode,
        source: 'market',
        timeframe,
        indicators: specs,
        ...(settings.relativeMode && {
          benchmark_code: 'topix',
          relative_options: {
            align_dates: true,
            handle_zero_division: 'skip',
          },
        }),
      };
      return apiPost<BtIndicatorComputeResponse>('/api/indicators/compute', request);
    },
    enabled: !!stockCode && specs.length > 0,
    staleTime: 60_000,
    gcTime: 5 * 60_000,
    retry: 2,
  });

  const data = useMemo(() => {
    if (!query.data) return EMPTY_CHART_PARTIAL;
    return mapBtResponseToChartData(query.data);
  }, [query.data]);

  return {
    data,
    isLoading: query.isLoading,
    error: query.error,
    isError: query.isError,
  };
}
