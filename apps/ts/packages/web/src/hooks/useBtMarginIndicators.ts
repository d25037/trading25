import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';
import { apiPost } from '@/lib/api-client';
import type { MarginPressureIndicatorsResponse } from '@/types/chart';

// ===== Types =====

interface BtMarginRequest {
  stock_code: string;
  indicators: ('margin_long_pressure' | 'margin_flow_pressure' | 'margin_turnover_days')[];
  average_period: number;
}

interface BtMarginResponse {
  stock_code: string;
  indicators: {
    margin_long_pressure?: BtMarginRecord[];
    margin_flow_pressure?: BtMarginRecord[];
    margin_turnover_days?: BtMarginRecord[];
  };
}

interface BtMarginRecord {
  date: string;
  [key: string]: string | number | null;
}

// ===== Query Keys =====

export const btMarginKeys = {
  all: ['bt-margin'] as const,
  compute: (stockCode: string, period: number) => ['bt-margin', stockCode, period] as const,
};

// ===== Response Transformer =====

function transformBtMarginResponse(response: BtMarginResponse, period: number): MarginPressureIndicatorsResponse {
  const longPressure = (response.indicators.margin_long_pressure ?? []).map((r) => ({
    date: r.date,
    pressure: r.pressure as number,
    longVol: r.longVol as number,
    shortVol: r.shortVol as number,
    avgVolume: r.avgVolume as number,
  }));

  const flowPressure = (response.indicators.margin_flow_pressure ?? []).map((r) => ({
    date: r.date,
    flowPressure: r.flowPressure as number,
    currentNetMargin: r.currentNetMargin as number,
    previousNetMargin: (r.previousNetMargin as number) ?? 0,
    avgVolume: r.avgVolume as number,
  }));

  const turnoverDays = (response.indicators.margin_turnover_days ?? []).map((r) => ({
    date: r.date,
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
  };
}

// ===== Main Hook =====

export function useBtMarginIndicators(symbol: string | null, period = 15) {
  const query = useQuery({
    queryKey: btMarginKeys.compute(symbol ?? '', period),
    queryFn: () => {
      if (!symbol) throw new Error('symbol is required');
      const request: BtMarginRequest = {
        stock_code: symbol,
        indicators: ['margin_long_pressure', 'margin_flow_pressure', 'margin_turnover_days'],
        average_period: period,
      };
      return apiPost<BtMarginResponse>('/api/indicators/margin', request);
    },
    enabled: !!symbol,
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
