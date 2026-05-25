import { useQuery } from '@tanstack/react-query';
import type { components } from '@trading25/contracts/clients/backtest/generated/bt-api-types';
import { useMemo } from 'react';
import { apiPost } from '@/lib/api-client';
import type { MarginPressureIndicatorsResponse } from '@/types/chart';

// ===== Types =====

type BtMarginRequest = components['schemas']['MarginIndicatorRequest'];
type BtMarginResponse = components['schemas']['MarginIndicatorResponse'];
type BtMarginRecord = BtMarginResponse['indicators'][string][number];

// ===== Query Keys =====

export const btMarginKeys = {
  all: ['bt-margin'] as const,
  compute: (stockCode: string, period: number) => ['bt-margin', stockCode, period] as const,
};

// ===== Response Transformer =====

function getRecordDate(record: BtMarginRecord): string {
  return typeof record.date === 'string' ? record.date : String(record.date ?? '');
}

function transformBtMarginResponse(response: BtMarginResponse, period: number): MarginPressureIndicatorsResponse {
  const longPressure = (response.indicators.margin_long_pressure ?? []).map((r) => ({
    date: getRecordDate(r),
    pressure: r.pressure as number,
    longVol: r.longVol as number,
    shortVol: r.shortVol as number,
    avgVolume: r.avgVolume as number,
  }));

  const flowPressure = (response.indicators.margin_flow_pressure ?? []).map((r) => ({
    date: getRecordDate(r),
    flowPressure: r.flowPressure as number,
    currentNetMargin: r.currentNetMargin as number,
    previousNetMargin: (r.previousNetMargin as number) ?? 0,
    avgVolume: r.avgVolume as number,
  }));

  const turnoverDays = (response.indicators.margin_turnover_days ?? []).map((r) => ({
    date: getRecordDate(r),
    turnoverDays: r.turnoverDays as number,
    longVol: r.longVol as number,
    avgVolume: r.avgVolume as number,
  }));

  return {
    symbol: response.stock_code,
    averagePeriod: period,
    longPressure,
    flowPressure,
    turnoverDays,
    lastUpdated: new Date().toISOString(),
    provenance: response.provenance,
    diagnostics: response.diagnostics ?? {},
  };
}

// ===== Main Hook =====

interface UseBtMarginIndicatorsOptions {
  period?: number;
  enabled?: boolean;
}

export function useBtMarginIndicators(symbol: string | null, options: number | UseBtMarginIndicatorsOptions = 15) {
  const { period, enabled } =
    typeof options === 'number'
      ? { period: options, enabled: true }
      : { period: options.period ?? 15, enabled: options.enabled ?? true };

  const query = useQuery({
    queryKey: btMarginKeys.compute(symbol ?? '', period),
    queryFn: () => {
      if (!symbol) throw new Error('symbol is required');
      const request: BtMarginRequest = {
        stock_code: symbol,
        source: 'market',
        indicators: ['margin_long_pressure', 'margin_flow_pressure', 'margin_turnover_days'],
        average_period: period,
      };
      return apiPost<BtMarginResponse>('/api/indicators/margin', request);
    },
    enabled: !!symbol && enabled,
    staleTime: 5 * 60_000,
    gcTime: 10 * 60_000,
    retry: 2,
  });

  const data = useMemo(() => {
    if (!query.data) return undefined;
    return transformBtMarginResponse(query.data, period);
  }, [query.data, period]);

  return {
    data,
    isLoading: query.isLoading,
    error: query.error,
  };
}
