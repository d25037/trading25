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
import { type CSSProperties, useEffect, useRef, useState } from 'react';
import { PAGE_SCROLL_CHART_INTERACTION_OPTIONS } from '@/components/Chart/chartInteractionOptions';
import { CHART_COLORS, CHART_DIMENSIONS, CHART_LINE_WIDTHS, VOLUME_SCALE_MARGINS } from '@/lib/constants';
import { useChartStore } from '@/stores/chartStore';
import type { BollingerBandsData, IndicatorValue, SMAATRBandsData, StockDataPoint, VolumeData } from '@/types/chart';
import { formatPrice } from '@/utils/formatters';
import { logger } from '@/utils/logger';
import { hasVolumeData } from '@/utils/typeGuards';

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
  smaAtrBands?: SMAATRBandsData[];
  sma?: IndicatorValue[];
  ema?: IndicatorValue[];
  vwema?: IndicatorValue[];
  signalMarkers?: SignalMarkerData[];
  height?: number;
  provisionalDate?: string | null;
}

const PROVISIONAL_CANDLE_COLORS = {
  color: '#f59e0b',
  borderColor: '#d97706',
  wickColor: '#f59e0b',
} as const;

function formatCandlestickData(data: StockDataPoint[], provisionalDate: string | null | undefined) {
  return data.map((item) => ({
    time: item.time,
    open: item.open,
    high: item.high,
    low: item.low,
    close: item.close,
    ...(item.time === provisionalDate ? PROVISIONAL_CANDLE_COLORS : {}),
  }));
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

export function StockChart({
  data = [],
  atrSupport,
  nBarSupport,
  bollingerBands,
  smaAtrBands,
  sma,
  ema,
  vwema,
  signalMarkers = [],
  height,
  provisionalDate = null,
}: StockChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candlestickSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  // Overlay indicator series refs
  const atrSupportSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const nBarSupportSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const bollingerUpperSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const smaAtrUpperSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const smaAtrMiddleSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const smaAtrLowerSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const smaSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const emaSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const vwemaSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  // Signal markers ref (v5 API)
  const signalMarkersRef = useRef<ISeriesMarkersPluginApi<Time> | null>(null);

  // Keep data ref for crosshair calculations
  const dataRef = useRef<StockDataPoint[]>(data);
  dataRef.current = data;
  const provisionalDateRef = useRef<string | null>(provisionalDate);
  provisionalDateRef.current = provisionalDate;

  // Crosshair OHLC state
  const [crosshairData, setCrosshairData] = useState<CrosshairOHLC | null>(null);

  const { settings } = useChartStore();
  const visibleBarsRef = useRef(settings.visibleBars);
  visibleBarsRef.current = settings.visibleBars;

  // Initialize chart once on mount
  useEffect(() => {
    logger.debug('StockChart mounting');
    if (!chartContainerRef.current) {
      logger.error('Chart container ref is null');
      return;
    }

    // Create chart - use container dimensions or fallback to fixed height
    const containerHeight = chartContainerRef.current.clientHeight || height || CHART_DIMENSIONS.DEFAULT_HEIGHT;
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
      ...PAGE_SCROLL_CHART_INTERACTION_OPTIONS,
    });

    chartRef.current = chart;
    const cleanupChart = () => {
      chart.remove();
      chartRef.current = null;
      candlestickSeriesRef.current = null;
      volumeSeriesRef.current = null;
      atrSupportSeriesRef.current = null;
      nBarSupportSeriesRef.current = null;
      bollingerUpperSeriesRef.current = null;
      smaAtrUpperSeriesRef.current = null;
      smaAtrMiddleSeriesRef.current = null;
      smaAtrLowerSeriesRef.current = null;
      smaSeriesRef.current = null;
      emaSeriesRef.current = null;
      vwemaSeriesRef.current = null;
      signalMarkersRef.current = null;
    };

    // Add candlestick series using v5 API (global standard: up=green, down=red)
    const candlestickSeries = chart.addSeries(CandlestickSeries, {
      upColor: CHART_COLORS.UP,
      downColor: CHART_COLORS.DOWN,
      borderVisible: false,
      wickUpColor: CHART_COLORS.UP,
      wickDownColor: CHART_COLORS.DOWN,
    });

    if (!candlestickSeries) {
      logger.warn('Candlestick series creation failed; skipping stock chart data initialization');
      return cleanupChart;
    }

    candlestickSeriesRef.current = candlestickSeries;

    const initialData = dataRef.current;
    if (initialData.length) {
      candlestickSeries.setData(formatCandlestickData(initialData, provisionalDateRef.current));
      setChartVisibleBars(chart, initialData.length, visibleBarsRef.current);
    }

    chart.subscribeCrosshairMove((param) => {
      const ohlcData = param.time && param.seriesData?.get(candlestickSeries);
      if (ohlcData && 'open' in ohlcData) {
        setCrosshairData(buildCrosshairOHLC(ohlcData, timeToDateString(param.time), dataRef.current));
      } else {
        setCrosshairData(null);
      }
    });

    return cleanupChart;
  }, [height]); // Remove showVolume dependency

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

  // Handle SMA indicator
  useEffect(() => {
    if (!chartRef.current) return;

    if (settings.indicators.sma.enabled && sma && sma.length > 0) {
      if (!smaSeriesRef.current) {
        smaSeriesRef.current = chartRef.current.addSeries(LineSeries, {
          color: CHART_COLORS.SMA,
          lineWidth: CHART_LINE_WIDTHS.STANDARD,
        });
      }
      smaSeriesRef.current.setData(sma.map((item) => ({ time: item.time, value: item.value })));
    } else if (smaSeriesRef.current) {
      chartRef.current.removeSeries(smaSeriesRef.current);
      smaSeriesRef.current = null;
    }
  }, [settings.indicators.sma.enabled, sma]);

  // Handle EMA indicator
  useEffect(() => {
    if (!chartRef.current) return;

    if (settings.indicators.ema.enabled && ema && ema.length > 0) {
      if (!emaSeriesRef.current) {
        emaSeriesRef.current = chartRef.current.addSeries(LineSeries, {
          color: CHART_COLORS.EMA,
          lineWidth: CHART_LINE_WIDTHS.STANDARD,
        });
      }
      emaSeriesRef.current.setData(ema.map((item) => ({ time: item.time, value: item.value })));
    } else if (emaSeriesRef.current) {
      chartRef.current.removeSeries(emaSeriesRef.current);
      emaSeriesRef.current = null;
    }
  }, [settings.indicators.ema.enabled, ema]);

  // Handle VWEMA indicator
  useEffect(() => {
    if (!chartRef.current) return;

    if (settings.indicators.vwema.enabled && vwema && vwema.length > 0) {
      if (!vwemaSeriesRef.current) {
        vwemaSeriesRef.current = chartRef.current.addSeries(LineSeries, {
          color: CHART_COLORS.VWEMA,
          lineWidth: CHART_LINE_WIDTHS.STANDARD,
        });
      }
      vwemaSeriesRef.current.setData(vwema.map((item) => ({ time: item.time, value: item.value })));
    } else if (vwemaSeriesRef.current) {
      chartRef.current.removeSeries(vwemaSeriesRef.current);
      vwemaSeriesRef.current = null;
    }
  }, [settings.indicators.vwema.enabled, vwema]);

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

  // Handle research-compatible SMA +/- ATR position bands.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;

    const shouldShow = settings.indicators.smaAtrBands.enabled && smaAtrBands && smaAtrBands.length > 0;
    if (!shouldShow) {
      smaAtrUpperSeriesRef.current = removeSeries(chart, smaAtrUpperSeriesRef.current);
      smaAtrMiddleSeriesRef.current = removeSeries(chart, smaAtrMiddleSeriesRef.current);
      smaAtrLowerSeriesRef.current = removeSeries(chart, smaAtrLowerSeriesRef.current);
      return;
    }

    smaAtrUpperSeriesRef.current ??= chart.addSeries(LineSeries, {
      color: CHART_COLORS.SMA_ATR_UPPER,
      lineWidth: CHART_LINE_WIDTHS.EMPHASIZED,
    });
    smaAtrMiddleSeriesRef.current ??= chart.addSeries(LineSeries, {
      color: CHART_COLORS.SMA_ATR_MIDDLE,
      lineWidth: CHART_LINE_WIDTHS.STANDARD,
    });
    smaAtrLowerSeriesRef.current ??= chart.addSeries(LineSeries, {
      color: CHART_COLORS.SMA_ATR_LOWER,
      lineWidth: CHART_LINE_WIDTHS.EMPHASIZED,
    });

    smaAtrUpperSeriesRef.current.setData(smaAtrBands.map((item) => ({ time: item.time, value: item.upper })));
    smaAtrMiddleSeriesRef.current.setData(smaAtrBands.map((item) => ({ time: item.time, value: item.middle })));
    smaAtrLowerSeriesRef.current.setData(smaAtrBands.map((item) => ({ time: item.time, value: item.lower })));
  }, [settings.indicators.smaAtrBands.enabled, smaAtrBands]);

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

    const formattedData = formatCandlestickData(data, provisionalDate);

    candlestickSeriesRef.current.setData(formattedData);

    // Update volume data only if volume series exists and is enabled
    if (volumeSeriesRef.current && settings.showVolume) {
      updateVolumeData(volumeSeriesRef.current, data);
    }

    // Set visible range based on settings
    if (chartRef.current) {
      setChartVisibleBars(chartRef.current, data.length, settings.visibleBars);
    }
  }, [data, provisionalDate, settings.showVolume, settings.visibleBars]);

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

  const wrapperStyle = height !== undefined ? ({ height } satisfies CSSProperties) : undefined;

  return (
    <div className="relative h-full w-full" style={wrapperStyle}>
      <div ref={chartContainerRef} className="absolute inset-0" />
      {provisionalDate ? (
        <span
          role="note"
          title={`${provisionalDate} の日足は四季報の当日暫定値です`}
          className="absolute right-2 top-2 z-10 rounded bg-amber-500/15 px-2 py-1 text-[10px] font-medium text-amber-700 dark:text-amber-300"
        >
          四季報 15分遅延・当日暫定
        </span>
      ) : null}
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
