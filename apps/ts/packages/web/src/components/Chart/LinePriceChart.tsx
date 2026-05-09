import { createChart, type IChartApi, type ISeriesApi, LineSeries } from 'lightweight-charts';
import { type CSSProperties, useEffect, useRef } from 'react';
import { PAGE_SCROLL_CHART_INTERACTION_OPTIONS } from '@/components/Chart/chartInteractionOptions';
import { CHART_COLORS, CHART_DIMENSIONS, CHART_LINE_WIDTHS } from '@/lib/constants';
import { useChartStore } from '@/stores/chartStore';

export interface LinePricePoint {
  time: string;
  value: number;
}

interface LinePriceChartProps {
  data?: LinePricePoint[];
  height?: number;
}

function setChartVisibleBars(chart: IChartApi, dataLength: number, barsToShow: number) {
  if (dataLength === 0) return;

  const from = Math.max(0, dataLength - barsToShow - 0.5);
  const to = dataLength - 0.5;

  chart.timeScale().setVisibleLogicalRange({
    from,
    to,
  });
}

export function LinePriceChart({ data = [], height }: LinePriceChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const lineSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const dataRef = useRef<LinePricePoint[]>(data);
  dataRef.current = data;
  const { settings } = useChartStore();
  const visibleBarsRef = useRef(settings.visibleBars);
  visibleBarsRef.current = settings.visibleBars;

  useEffect(() => {
    const container = chartContainerRef.current;
    if (!container) {
      return;
    }

    const chart = createChart(container, {
      layout: {
        background: { color: 'transparent' },
        textColor: CHART_COLORS.TEXT,
      },
      grid: {
        vertLines: { color: CHART_COLORS.GRID },
        horzLines: { color: CHART_COLORS.GRID },
      },
      width: container.clientWidth,
      height: container.clientHeight || height || CHART_DIMENSIONS.DEFAULT_HEIGHT,
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
      },
      ...PAGE_SCROLL_CHART_INTERACTION_OPTIONS,
    });
    const lineSeries = chart.addSeries(LineSeries, {
      color: CHART_COLORS.BOLLINGER,
      lineWidth: CHART_LINE_WIDTHS.EMPHASIZED,
      crosshairMarkerVisible: true,
      lastValueVisible: true,
      priceLineVisible: true,
    });

    chartRef.current = chart;
    lineSeriesRef.current = lineSeries;

    if (dataRef.current.length) {
      lineSeries.setData(dataRef.current);
      setChartVisibleBars(chart, dataRef.current.length, visibleBarsRef.current);
    }

    return () => {
      chart.remove();
      chartRef.current = null;
      lineSeriesRef.current = null;
    };
  }, [height]);

  useEffect(() => {
    const lineSeries = lineSeriesRef.current;
    const chart = chartRef.current;
    if (!lineSeries || !chart) {
      return;
    }
    if (data.length === 0) {
      lineSeries.setData([]);
      return;
    }
    lineSeries.setData(data);
    setChartVisibleBars(chart, data.length, settings.visibleBars);
  }, [data, settings.visibleBars]);

  useEffect(() => {
    const container = chartContainerRef.current;
    if (!container) {
      return;
    }

    const resizeObserver = new ResizeObserver(() => {
      const chart = chartRef.current;
      if (!chart) {
        return;
      }
      const width = container.clientWidth;
      const height = container.clientHeight;
      if (width > 0 && height >= CHART_DIMENSIONS.MIN_HEIGHT) {
        chart.applyOptions({ width, height });
      }
    });
    resizeObserver.observe(container);
    return () => resizeObserver.disconnect();
  }, []);

  const wrapperStyle = height !== undefined ? ({ height } satisfies CSSProperties) : undefined;

  return (
    <div className="relative h-full w-full" style={wrapperStyle}>
      <div ref={chartContainerRef} className="absolute inset-0" />
      {!data.length && (
        <div className="absolute inset-0 flex items-center justify-center">
          <p className="text-muted-foreground">No chart data available</p>
        </div>
      )}
    </div>
  );
}
