import {
  CandlestickSeries,
  createChart,
  createSeriesMarkers,
  HistogramSeries,
  type IChartApi,
  type ISeriesApi,
  type ISeriesMarkersPluginApi,
  LineSeries,
  type SeriesMarker,
  type Time,
} from 'lightweight-charts';
import { useEffect, useRef, useState } from 'react';
import { CHART_COLORS, CHART_DIMENSIONS, CHART_LINE_WIDTHS, VOLUME_SCALE_MARGINS } from '@/lib/constants';
import { useChartStore } from '@/stores/chartStore';
import type { BollingerBandsData, IndicatorValue, StockDataPoint, VolumeData } from '@/types/chart';
import { formatPrice } from '@/utils/formatters';
import { logger } from '@/utils/logger';
import { hasVolumeData } from '@/utils/typeGuards';

// Re-export formatPrice for backward compatibility with tests
export { formatPrice };

// Helper function to set visible bars range
function setChartVisibleBars(chart: IChartApi, dataLength: number, barsToShow: number) {
  if (dataLength === 0) return;

  const from = Math.max(0, dataLength - barsToShow - 0.5);
  const to = dataLength - 0.5;

  chart.timeScale().setVisibleLogicalRange({
    from: from,
    to: to,
  });
}

interface SignalMarkerData {
  time: string;
  position: 'belowBar' | 'aboveBar';
  color: string;
  shape: 'arrowUp' | 'arrowDown';
  text: string;
  size: number;
}

interface StockChartProps {
  data?: StockDataPoint[];
  atrSupport?: IndicatorValue[];
  nBarSupport?: IndicatorValue[];
  bollingerBands?: BollingerBandsData[];
  signalMarkers?: SignalMarkerData[];
}

// Helper function to create volume series
function createVolumeSeries(chart: IChartApi) {
  const volumeSeries = chart.addSeries(HistogramSeries, {
    color: CHART_COLORS.UP,
    priceFormat: {
      type: 'volume',
    },
    priceScaleId: '',
  });

  volumeSeries.priceScale().applyOptions({
    scaleMargins: {
      top: VOLUME_SCALE_MARGINS.TOP,
      bottom: VOLUME_SCALE_MARGINS.BOTTOM,
    },
  });

  return volumeSeries;
}

// Helper function to update volume data
function updateVolumeData(volumeSeries: ISeriesApi<'Histogram'>, data: StockDataPoint[]) {
  if (!data.some(hasVolumeData)) return;

  const volumeData: VolumeData[] = data.filter(hasVolumeData).map((item) => ({
    time: item.time,
    value: item.volume,
    color: item.close >= item.open ? CHART_COLORS.UP : CHART_COLORS.DOWN,
  }));
  volumeSeries.setData(volumeData);
}

// Bollinger Bands series config - Blue theme like TradingView (Upper line only)
const BOLLINGER_SERIES_CONFIG = {
  upper: { color: CHART_COLORS.BOLLINGER, lineWidth: 1 as const },
} as const;

// Helper to create a single Bollinger Bands series
function createBollingerSeries(
  chart: IChartApi,
  config: { color: string; lineWidth: 1; lineStyle?: 2 }
): ISeriesApi<'Line'> {
  return chart.addSeries(LineSeries, config);
}

// Helper to remove a series if it exists
function removeSeries(chart: IChartApi, series: ISeriesApi<'Line'> | null): null {
  if (series) {
    chart.removeSeries(series);
  }
  return null;
}

interface CrosshairOHLC {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  changePct?: number;
}

interface BusinessDay {
  year: number;
  month: number;
  day: number;
}

function isBusinessDay(time: unknown): time is BusinessDay {
  return typeof time === 'object' && time !== null && 'year' in time && 'month' in time && 'day' in time;
}

export function calculateChangePct(
  timeStr: string,
  currentClose: number,
  dataSource: StockDataPoint[]
): number | undefined {
  const idx = dataSource.findIndex((d) => d.time === timeStr);
  if (idx <= 0) return undefined;

  const prevClose = dataSource[idx - 1]?.close;
  if (!prevClose) return undefined;

  return ((currentClose - prevClose) / prevClose) * 100;
}

export function timeToDateString(time: unknown): string {
  if (typeof time === 'string') return time;
  if (typeof time === 'number') return new Date(time * 1000).toISOString().split('T')[0] ?? '';
  if (isBusinessDay(time)) {
    return `${time.year}-${String(time.month).padStart(2, '0')}-${String(time.day).padStart(2, '0')}`;
  }
  return '';
}

function buildCrosshairOHLC(
  ohlcData: { open: number; high: number; low: number; close: number },
  timeStr: string,
  dataSource: StockDataPoint[]
): CrosshairOHLC {
  return {
    time: timeStr,
    ...ohlcData,
    changePct: calculateChangePct(timeStr, ohlcData.close, dataSource),
  };
}

export function StockChart({ data = [], atrSupport, nBarSupport, bollingerBands, signalMarkers = [] }: StockChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candlestickSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  // Overlay indicator series refs
  const atrSupportSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const nBarSupportSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const bollingerUpperSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  // Signal markers ref (v5 API)
  const signalMarkersRef = useRef<ISeriesMarkersPluginApi<Time> | null>(null);

  // Keep data ref for crosshair calculations
  const dataRef = useRef<StockDataPoint[]>(data);
  dataRef.current = data;

  // Crosshair OHLC state
  const [crosshairData, setCrosshairData] = useState<CrosshairOHLC | null>(null);

  const { settings } = useChartStore();

  // Initialize chart once on mount
  useEffect(() => {
    logger.debug('StockChart mounting');
    if (!chartContainerRef.current) {
      logger.error('Chart container ref is null');
      return;
    }

    // Create chart - use container dimensions or fallback to fixed height
    const containerHeight = chartContainerRef.current.clientHeight || CHART_DIMENSIONS.DEFAULT_HEIGHT;
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { color: 'transparent' },
        textColor: CHART_COLORS.TEXT,
      },
      grid: {
        vertLines: { color: CHART_COLORS.GRID },
        horzLines: { color: CHART_COLORS.GRID },
      },
      width: chartContainerRef.current.clientWidth,
      height: containerHeight,
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
      },
    });

    chartRef.current = chart;

    // Add candlestick series using v5 API (global standard: up=green, down=red)
    const candlestickSeries = chart.addSeries(CandlestickSeries, {
      upColor: CHART_COLORS.UP,
      downColor: CHART_COLORS.DOWN,
      borderVisible: false,
      wickUpColor: CHART_COLORS.UP,
      wickDownColor: CHART_COLORS.DOWN,
    });

    candlestickSeriesRef.current = candlestickSeries;

    chart.subscribeCrosshairMove((param) => {
      const ohlcData = param.time && param.seriesData?.get(candlestickSeries);
      if (ohlcData && 'open' in ohlcData) {
        setCrosshairData(buildCrosshairOHLC(ohlcData, timeToDateString(param.time), dataRef.current));
      } else {
        setCrosshairData(null);
      }
    });

    // Cleanup function
    return () => {
      chart.remove();
      chartRef.current = null;
      candlestickSeriesRef.current = null;
      volumeSeriesRef.current = null;
      atrSupportSeriesRef.current = null;
      nBarSupportSeriesRef.current = null;
      bollingerUpperSeriesRef.current = null;
      signalMarkersRef.current = null;
    };
  }, []); // Remove showVolume dependency

  // Handle volume series toggle separately
  useEffect(() => {
    logger.debug('Volume toggle changed', { showVolume: settings.showVolume });
    if (!chartRef.current) {
      return;
    }

    if (settings.showVolume) {
      // Add volume series if not already added
      if (!volumeSeriesRef.current) {
        const volumeSeries = createVolumeSeries(chartRef.current);
        volumeSeriesRef.current = volumeSeries;

        // Update volume data if we have data
        if (data.length) {
          updateVolumeData(volumeSeries, data);
        }
      }
    } else {
      // Remove volume series if it exists
      if (volumeSeriesRef.current) {
        chartRef.current.removeSeries(volumeSeriesRef.current);
        volumeSeriesRef.current = null;
      }
    }
  }, [settings.showVolume, data]);

  // Handle ATR Support Line indicator
  useEffect(() => {
    if (!chartRef.current) return;

    if (settings.indicators.atrSupport.enabled && atrSupport && atrSupport.length > 0) {
      if (!atrSupportSeriesRef.current) {
        atrSupportSeriesRef.current = chartRef.current.addSeries(LineSeries, {
          color: CHART_COLORS.ATR_SUPPORT,
          lineWidth: CHART_LINE_WIDTHS.EMPHASIZED,
        });
      }
      atrSupportSeriesRef.current.setData(atrSupport.map((item) => ({ time: item.time, value: item.value })));
    } else {
      if (atrSupportSeriesRef.current) {
        chartRef.current.removeSeries(atrSupportSeriesRef.current);
        atrSupportSeriesRef.current = null;
      }
    }
  }, [settings.indicators.atrSupport.enabled, atrSupport]);

  // Handle N-Bar Support Line indicator
  useEffect(() => {
    if (!chartRef.current) return;

    if (settings.indicators.nBarSupport.enabled && nBarSupport && nBarSupport.length > 0) {
      if (!nBarSupportSeriesRef.current) {
        nBarSupportSeriesRef.current = chartRef.current.addSeries(LineSeries, {
          color: CHART_COLORS.N_BAR_SUPPORT,
          lineWidth: CHART_LINE_WIDTHS.STANDARD,
        });
      }
      nBarSupportSeriesRef.current.setData(nBarSupport.map((item) => ({ time: item.time, value: item.value })));
    } else {
      if (nBarSupportSeriesRef.current) {
        chartRef.current.removeSeries(nBarSupportSeriesRef.current);
        nBarSupportSeriesRef.current = null;
      }
    }
  }, [settings.indicators.nBarSupport.enabled, nBarSupport]);

  // Handle Bollinger Bands indicator (Upper line only)
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;

    const shouldShow = settings.indicators.bollinger.enabled && bollingerBands && bollingerBands.length > 0;

    if (shouldShow) {
      // Create upper line series if it doesn't exist
      bollingerUpperSeriesRef.current ??= createBollingerSeries(chart, BOLLINGER_SERIES_CONFIG.upper);

      // Update data
      const upperData = bollingerBands.map((item) => ({ time: item.time, value: item.upper }));
      bollingerUpperSeriesRef.current.setData(upperData);
    } else {
      // Remove Bollinger upper series
      bollingerUpperSeriesRef.current = removeSeries(chart, bollingerUpperSeriesRef.current);
    }
  }, [settings.indicators.bollinger.enabled, bollingerBands]);

  // Handle Signal Markers (v5 API using createSeriesMarkers)
  useEffect(() => {
    if (!candlestickSeriesRef.current) return;

    const markers: SeriesMarker<Time>[] = signalMarkers.map((marker) => ({
      time: marker.time as Time,
      position: marker.position,
      color: marker.color,
      shape: marker.shape,
      text: marker.text,
      size: marker.size,
    }));

    if (!signalMarkersRef.current && candlestickSeriesRef.current) {
      // Create markers primitive on first use
      signalMarkersRef.current = createSeriesMarkers(candlestickSeriesRef.current, markers);
    } else if (signalMarkersRef.current) {
      // Update existing markers
      signalMarkersRef.current.setMarkers(markers);
    }
  }, [signalMarkers]);

  // Update data when data prop changes
  useEffect(() => {
    logger.debug('StockChart data update', { dataLength: data.length });
    if (!candlestickSeriesRef.current || !chartRef.current) {
      logger.warn('Candlestick series ref or chart ref is null');
      return;
    }
    if (!data.length) {
      logger.debug('No data provided');
      return;
    }

    const formattedData = data.map((item) => ({
      time: item.time,
      open: item.open,
      high: item.high,
      low: item.low,
      close: item.close,
    }));

    candlestickSeriesRef.current.setData(formattedData);

    // Update volume data only if volume series exists and is enabled
    if (volumeSeriesRef.current && settings.showVolume) {
      updateVolumeData(volumeSeriesRef.current, data);
    }

    // Set visible range based on settings
    if (chartRef.current) {
      setChartVisibleBars(chartRef.current, data.length, settings.visibleBars);
    }
  }, [data, settings.showVolume, settings.visibleBars]);

  // Handle container resize - only resize if dimensions are reasonable
  useEffect(() => {
    const container = chartContainerRef.current;
    if (!container) return;

    const resizeObserver = new ResizeObserver(() => {
      if (chartRef.current) {
        const width = container.clientWidth;
        const height = container.clientHeight;
        // Only resize if dimensions are above minimum threshold
        if (width > 0 && height >= CHART_DIMENSIONS.MIN_HEIGHT) {
          chartRef.current.applyOptions({ width, height });
        }
      }
    });

    resizeObserver.observe(container);
    return () => resizeObserver.disconnect();
  }, []);

  return (
    <div className="relative h-full w-full">
      <div ref={chartContainerRef} className="absolute inset-0" />
      {crosshairData && <OHLCOverlay data={crosshairData} />}
      {!data.length && (
        <div className="absolute inset-0 flex items-center justify-center">
          <p className="text-muted-foreground">No chart data available</p>
        </div>
      )}
    </div>
  );
}

function getPriceColor(isPositive: boolean): string {
  return isPositive ? CHART_COLORS.UP : CHART_COLORS.DOWN;
}

interface OHLCOverlayProps {
  data: CrosshairOHLC;
}

function OHLCOverlay({ data }: OHLCOverlayProps): React.ReactNode {
  const priceColor = getPriceColor(data.close >= data.open);
  const changeColor = data.changePct !== undefined ? getPriceColor(data.changePct >= 0) : undefined;

  const ohlcItems = [
    { label: 'O', value: data.open },
    { label: 'H', value: data.high },
    { label: 'L', value: data.low },
    { label: 'C', value: data.close },
  ];

  return (
    <div className="absolute top-2 left-2 z-10 flex gap-3 text-xs font-mono bg-background/80 px-2 py-1 rounded">
      <span className="text-muted-foreground">{data.time}</span>
      {ohlcItems.map(({ label, value }) => (
        <span key={label} style={{ color: priceColor }}>
          {label}
          <span className="ml-1">{formatPrice(value)}</span>
        </span>
      ))}
      {data.changePct !== undefined && (
        <span style={{ color: changeColor }}>
          {data.changePct >= 0 ? '+' : ''}
          {data.changePct.toFixed(2)}%
        </span>
      )}
    </div>
  );
}
