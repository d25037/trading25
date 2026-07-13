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
import type { ShikihoDailyOverlayProvenance } from '@/lib/shikihoDailyOverlay';

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

export interface WorkbenchDailyChartOverlay {
  dailyBars: ChartData['candlestickData'];
  chartSmaPoint: ChartData['indicators']['sma'][number] | null;
  provenance: ShikihoDailyOverlayProvenance | null;
}

export interface MultiTimeframeChartResult extends MultiTimeframeChartData {
  provenance: ShikihoDailyOverlayProvenance | null;
}

export function applyShikihoChartOverlay(
  chartData: MultiTimeframeChartData,
  overlay: WorkbenchDailyChartOverlay | null | undefined,
  relativeMode: boolean
): MultiTimeframeChartResult {
  if (relativeMode || overlay?.provenance == null || chartData.daily == null) {
    return { ...chartData, provenance: null };
  }

  const previousSma = chartData.daily.indicators.sma ?? [];
  return {
    ...chartData,
    daily: {
      ...chartData.daily,
      candlestickData: overlay.dailyBars,
      indicators: {
        ...chartData.daily.indicators,
        sma: overlay.chartSmaPoint === null ? previousSma : [...previousSma, overlay.chartSmaPoint],
      },
    },
    provenance: overlay.provenance,
  };
}

export function useMultiTimeframeChart(
  selectedSymbol: string | null,
  strategyName: string | null,
  overlay?: WorkbenchDailyChartOverlay | null
) {
  const { settings } = useChartStore();

  logger.debug('useMultiTimeframeChart called', { selectedSymbol, relativeMode: settings.relativeMode });

  // OHLCV data via apps/bt/ API (handles timeframe conversion + relative OHLC)
  const ohlcv = useMultiTimeframeBtOHLCV(selectedSymbol, settings.relativeMode);

  // apps/bt/ API indicators (both normal and relativeMode use apps/bt/ API)
  const dailyInd = useBtIndicators(selectedSymbol, 'daily', settings);
  const weeklyInd = useBtIndicators(selectedSymbol, 'weekly', settings);
  const monthlyInd = useBtIndicators(selectedSymbol, 'monthly', settings);

  // apps/bt/ API signals
  const dailySig = useBtSignals(selectedSymbol, 'daily', settings.signalOverlay, strategyName, settings.relativeMode);
  const weeklySig = useBtSignals(selectedSymbol, 'weekly', settings.signalOverlay, strategyName, settings.relativeMode);
  const monthlySig = useBtSignals(
    selectedSymbol,
    'monthly',
    settings.signalOverlay,
    strategyName,
    settings.relativeMode
  );

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
  const officialChartData = useMemo((): MultiTimeframeChartData => {
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

  const chartData = useMemo(
    () => applyShikihoChartOverlay(officialChartData, overlay, settings.relativeMode),
    [officialChartData, overlay, settings.relativeMode]
  );

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
    signalResponse: dailySig.response,
    isLoading,
    error,
    selectedSymbol,
  };
}
