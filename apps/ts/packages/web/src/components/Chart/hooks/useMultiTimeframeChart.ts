/**
 * useMultiTimeframeChart
 *
 * 複数タイムフレームのチャートデータを提供するフック。
 * Timeframe変換およびRelative OHLC変換はbt/ APIに委譲。
 *
 * 仕様: apps/bt/docs/spec-timeframe-resample.md
 */
import { useMemo } from 'react';
import { useBtIndicators } from '@/hooks/useBtIndicators';
import { useMultiTimeframeBtOHLCV } from '@/hooks/useBtOHLCV';
import { type SignalMarker, useBtSignals } from '@/hooks/useBtSignals';
import { useChartStore } from '@/stores/chartStore';
import type { ChartData } from '@/types/chart';
import { logger } from '@/utils/logger';

export interface MultiTimeframeChartData {
  daily: ChartData | null;
  weekly: ChartData | null;
  monthly: ChartData | null;
}

export interface MultiTimeframeSignalMarkers {
  daily: SignalMarker[];
  weekly: SignalMarker[];
  monthly: SignalMarker[];
}

export function useMultiTimeframeChart() {
  const { selectedSymbol, settings } = useChartStore();

  logger.debug('useMultiTimeframeChart called', { selectedSymbol, relativeMode: settings.relativeMode });

  // OHLCV data via apps/bt/ API (handles timeframe conversion + relative OHLC)
  const ohlcv = useMultiTimeframeBtOHLCV(selectedSymbol, settings.relativeMode);

  // apps/bt/ API indicators (both normal and relativeMode use apps/bt/ API)
  const dailyInd = useBtIndicators(selectedSymbol, 'daily', settings);
  const weeklyInd = useBtIndicators(selectedSymbol, 'weekly', settings);
  const monthlyInd = useBtIndicators(selectedSymbol, 'monthly', settings);

  // apps/bt/ API signals
  const dailySig = useBtSignals(selectedSymbol, 'daily', settings.signalOverlay, settings.relativeMode);
  const weeklySig = useBtSignals(selectedSymbol, 'weekly', settings.signalOverlay, settings.relativeMode);
  const monthlySig = useBtSignals(selectedSymbol, 'monthly', settings.signalOverlay, settings.relativeMode);

  const isLoading =
    ohlcv.isLoading ||
    dailyInd.isLoading ||
    weeklyInd.isLoading ||
    monthlyInd.isLoading ||
    dailySig.isLoading ||
    weeklySig.isLoading ||
    monthlySig.isLoading;

  const error =
    ohlcv.error ??
    dailyInd.error ??
    weeklyInd.error ??
    monthlyInd.error ??
    dailySig.error ??
    weeklySig.error ??
    monthlySig.error;

  // Process chart data for all timeframes
  const chartData = useMemo((): MultiTimeframeChartData => {
    // Build ChartData by merging candlestick data with indicators
    const buildChartData = (
      candlestickData: typeof ohlcv.daily.data,
      indicators: typeof dailyInd.data
    ): ChartData | null => {
      if (!candlestickData) return null;
      return { candlestickData, ...indicators };
    };

    // Require daily data as minimum
    if (!ohlcv.daily.data || ohlcv.daily.data.length === 0) {
      return { daily: null, weekly: null, monthly: null };
    }

    return {
      daily: buildChartData(ohlcv.daily.data, dailyInd.data),
      weekly: buildChartData(ohlcv.weekly.data, weeklyInd.data),
      monthly: buildChartData(ohlcv.monthly.data, monthlyInd.data),
    };
  }, [ohlcv.daily.data, ohlcv.weekly.data, ohlcv.monthly.data, dailyInd.data, weeklyInd.data, monthlyInd.data]);

  // Signal markers for all timeframes
  const signalMarkers = useMemo(
    (): MultiTimeframeSignalMarkers => ({
      daily: dailySig.markers,
      weekly: weeklySig.markers,
      monthly: monthlySig.markers,
    }),
    [dailySig.markers, weeklySig.markers, monthlySig.markers]
  );

  return {
    chartData,
    signalMarkers,
    isLoading,
    error,
    selectedSymbol,
  };
}
