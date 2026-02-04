import { createChart, type IChartApi, type ISeriesApi, LineSeries } from 'lightweight-charts';
import { useEffect, useRef } from 'react';
import { useChartStore } from '@/stores/chartStore';
import type { TradingValueMAData } from '@/types/chart';
import { formatInteger } from '@/utils/formatters';

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

interface TradingValueMAChartProps {
  data: TradingValueMAData[];
  title?: string;
  period?: number;
}

export function TradingValueMAChart({ data, title, period = 15 }: TradingValueMAChartProps) {
  // Get latest value for display
  const latestValue = data.length > 0 ? (data[data.length - 1]?.value ?? 0) : 0;
  const { settings } = useChartStore();
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const maSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);

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

    // Add Trading Value MA series (blue like TradingView)
    const maSeries = chart.addSeries(LineSeries, {
      color: '#2962FF',
      lineWidth: 2,
      priceFormat: {
        type: 'price',
        precision: 0,
        minMove: 1,
      },
    });
    maSeriesRef.current = maSeries;

    // Cleanup function
    return () => {
      chart.remove();
      chartRef.current = null;
      maSeriesRef.current = null;
    };
  }, []);

  // Update chart data when data changes
  useEffect(() => {
    if (!maSeriesRef.current || !data.length) {
      return;
    }

    const maData = data.map((item) => ({ time: item.time, value: item.value }));
    maSeriesRef.current.setData(maData);

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
      <div className="p-2 text-sm font-medium text-muted-foreground border-b border-border/30 flex items-center gap-2">
        <span>{title || 'Trading Value MA'}</span>
        <span className="text-[#2962FF]">{period}</span>
        {latestValue > 0 && <span className="text-[#2962FF]">{formatInteger(latestValue)}</span>}
      </div>
      <div ref={chartContainerRef} className="flex-1 h-full" />
    </div>
  );
}
