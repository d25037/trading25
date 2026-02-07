/**
 * useBtOHLCV - apps/bt/ APIを使用したOHLCVデータ取得フック
 *
 * apps/bt/ APIの /api/ohlcv/resample エンドポイントを使用して、
 * Timeframe変換およびRelative OHLC変換されたOHLCVデータを取得する。
 *
 * 仕様: apps/bt/docs/spec-timeframe-resample.md
 */
import type {
  BtTimeframe,
  OHLCVRecord,
  OHLCVResampleRequest,
  OHLCVResampleResponse,
} from '@trading25/shared/clients/backtest';
import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { apiPost } from '@/lib/api-client';
import type { StockDataPoint } from '@/types/chart';

// ===== Query Key Factory =====

export const btOHLCVKeys = {
  all: ['bt-ohlcv'] as const,
  resample: (stockCode: string, timeframe: string, relativeMode: boolean) =>
    ['bt-ohlcv', 'resample', stockCode, timeframe, relativeMode] as const,
};

// ===== Response Transformer =====

function transformOHLCVToStockDataPoints(records: OHLCVRecord[]): StockDataPoint[] {
  return records.map((r) => ({
    time: r.date,
    open: r.open,
    high: r.high,
    low: r.low,
    close: r.close,
    volume: r.volume,
  }));
}

// ===== Main Hook =====

export interface UseBtOHLCVOptions {
  /** 銘柄コード */
  stockCode: string | null;
  /** 出力タイムフレーム */
  timeframe: BtTimeframe;
  /** 相対モード（TOPIX比較） */
  relativeMode?: boolean;
  /** フック有効化フラグ */
  enabled?: boolean;
}

export interface UseBtOHLCVResult {
  data: StockDataPoint[] | null;
  isLoading: boolean;
  error: Error | null;
  isError: boolean;
}

/**
 * apps/bt/ APIを使用してOHLCVデータを取得
 *
 * - daily: そのまま返却
 * - weekly/monthly: apps/bt/側でリサンプル
 * - relativeMode=true: apps/bt/側でTOPIX相対化
 */
export function useBtOHLCV({
  stockCode,
  timeframe,
  relativeMode = false,
  enabled = true,
}: UseBtOHLCVOptions): UseBtOHLCVResult {
  const query = useQuery({
    queryKey: btOHLCVKeys.resample(stockCode ?? '', timeframe, relativeMode),
    queryFn: async () => {
      if (!stockCode) throw new Error('stockCode is required');

      const request: OHLCVResampleRequest = {
        stock_code: stockCode,
        source: 'market',
        timeframe,
        ...(relativeMode && {
          benchmark_code: 'topix',
          relative_options: {
            handle_zero_division: 'skip',
          },
        }),
      };

      return apiPost<OHLCVResampleResponse>('/api/ohlcv/resample', request);
    },
    enabled: enabled && !!stockCode,
    staleTime: 60_000,
    gcTime: 5 * 60_000,
    retry: 2,
  });

  const data = useMemo(() => {
    if (!query.data) return null;
    return transformOHLCVToStockDataPoints(query.data.data);
  }, [query.data]);

  return {
    data,
    isLoading: query.isLoading,
    error: query.error,
    isError: query.isError,
  };
}

/**
 * 複数タイムフレームのOHLCVを一括取得
 */
export function useMultiTimeframeBtOHLCV(stockCode: string | null, relativeMode: boolean) {
  const enabled = !!stockCode;

  const daily = useBtOHLCV({ stockCode, timeframe: 'daily', relativeMode, enabled });
  const weekly = useBtOHLCV({ stockCode, timeframe: 'weekly', relativeMode, enabled });
  const monthly = useBtOHLCV({ stockCode, timeframe: 'monthly', relativeMode, enabled });

  return {
    daily,
    weekly,
    monthly,
    isLoading: daily.isLoading || weekly.isLoading || monthly.isLoading,
    error: daily.error || weekly.error || monthly.error,
  };
}
