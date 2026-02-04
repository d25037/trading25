import { createChart, type IChartApi, type ISeriesApi, LineSeries } from 'lightweight-charts';
import { useEffect, useRef } from 'react';
import { useChartStore } from '@/stores/chartStore';
import type { VolumeComparisonData } from '@/types/chart';
import { formatVolume } from '@/utils/formatters';

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

interface VolumeComparisonChartProps {
  data: VolumeComparisonData[];
  title?: string;
  shortPeriod?: number;
  longPeriod?: number;
  lowerMultiplier?: number;
  higherMultiplier?: number;
}

export function VolumeComparisonChart({
  data,
  title,
  shortPeriod = 20,
  longPeriod = 100,
  lowerMultiplier = 1.0,
  higherMultiplier = 1.5,
}: VolumeComparisonChartProps) {
  // Get latest values for display
  const latest = data.length > 0 ? data[data.length - 1] : null;
  const { settings } = useChartStore();
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const shortMASeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const longLowerSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const longHigherSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);

  // Initialize chart once on mount
  useEffect(() => {
    if (!chartContainerRef.current) return;

    // Create chart
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { color: 'transparent' },
        textColor: '#666',
      },
      grid: {
        vertLines: { color: '#e1e1e1' },
        horzLines: { color: '#e1e1e1' },
      },
      width: chartContainerRef.current.clientWidth,
      height: chartContainerRef.current.clientHeight,
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
      },
      crosshair: {
        mode: 1,
      },
      rightPriceScale: {
        scaleMargins: {
          top: 0.1,
          bottom: 0.1,
        },
      },
    });

    chartRef.current = chart;

    // Add short MA series (amber/gold - visible on both light and dark backgrounds)
    const shortMASeries = chart.addSeries(LineSeries, {
      color: '#F59E0B',
      lineWidth: 2,
      priceFormat: {
        type: 'volume',
      },
    });
    shortMASeriesRef.current = shortMASeries;

    // Add long threshold lower series (red like TradingView)
    const longLowerSeries = chart.addSeries(LineSeries, {
      color: '#F23645',
      lineWidth: 2,
      priceFormat: {
        type: 'volume',
      },
    });
    longLowerSeriesRef.current = longLowerSeries;

    // Add long threshold higher series (blue like TradingView)
    const longHigherSeries = chart.addSeries(LineSeries, {
      color: '#2962FF',
      lineWidth: 1,
      priceFormat: {
        type: 'volume',
      },
    });
    longHigherSeriesRef.current = longHigherSeries;

    // Cleanup function
    return () => {
      chart.remove();
      chartRef.current = null;
      shortMASeriesRef.current = null;
      longLowerSeriesRef.current = null;
      longHigherSeriesRef.current = null;
    };
  }, []);

  // Update chart data when data changes
  useEffect(() => {
    if (!shortMASeriesRef.current || !longLowerSeriesRef.current || !longHigherSeriesRef.current || !data.length) {
      return;
    }

    const shortMAData = data.map((item) => ({ time: item.time, value: item.shortMA }));
    const longLowerData = data.map((item) => ({ time: item.time, value: item.longThresholdLower }));
    const longHigherData = data.map((item) => ({ time: item.time, value: item.longThresholdHigher }));

    shortMASeriesRef.current.setData(shortMAData);
    longLowerSeriesRef.current.setData(longLowerData);
    longHigherSeriesRef.current.setData(longHigherData);

    if (chartRef.current) {
      setChartVisibleBars(chartRef.current, data.length, settings.visibleBars);
    }
  }, [data, settings.visibleBars]);

  // Handle container resize using ResizeObserver
  useEffect(() => {
    const container = chartContainerRef.current;
    if (!container) return;

    const MIN_HEIGHT = 100;

    const resizeObserver = new ResizeObserver(() => {
      if (chartRef.current) {
        const width = container.clientWidth;
        const height = container.clientHeight;
        if (width > 0 && height >= MIN_HEIGHT) {
          chartRef.current.applyOptions({ width, height });
        }
      }
    });

    resizeObserver.observe(container);
    return () => resizeObserver.disconnect();
  }, []);

  return (
    <div className="h-full">
      <div className="p-2 text-sm font-medium text-muted-foreground border-b border-border/30 flex items-center gap-2 flex-wrap">
        <span>{title || 'Volume Comparison'}</span>
        <span className="text-[#F59E0B]">{shortPeriod} SMA</span>
        <span className="text-[#2962FF]">{longPeriod} SMA</span>
        <span className="text-[#F23645]">×{lowerMultiplier}</span>
        <span className="text-[#2962FF]">×{higherMultiplier}</span>
        {latest && (
          <>
            <span className="text-[#F59E0B]">{formatVolume(latest.shortMA)}</span>
            <span className="text-[#F23645]">{formatVolume(latest.longThresholdLower)}</span>
            <span className="text-[#2962FF]">{formatVolume(latest.longThresholdHigher)}</span>
          </>
        )}
      </div>
      <div ref={chartContainerRef} className="flex-1 h-full" />
    </div>
  );
}
