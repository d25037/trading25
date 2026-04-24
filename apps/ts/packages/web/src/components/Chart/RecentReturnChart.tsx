import { createChart, type IChartApi, type ISeriesApi, LineSeries } from 'lightweight-charts';
import { useEffect, useMemo, useRef } from 'react';
import { PAGE_SCROLL_CHART_INTERACTION_OPTIONS } from '@/components/Chart/chartInteractionOptions';
import { useChartStore } from '@/stores/chartStore';
import type { RecentReturnData } from '@/types/chart';

const SHORT_RETURN_COLOR = '#2563EB';
const LONG_RETURN_COLOR = '#DC2626';

function setChartVisibleBars(chart: IChartApi, dataLength: number, barsToShow: number) {
  if (dataLength === 0) return;

  const from = Math.max(0, dataLength - barsToShow - 0.5);
  const to = dataLength - 0.5;
  chart.timeScale().setVisibleLogicalRange({ from, to });
}

function formatPercent(value: number): string {
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}%`;
}

interface RecentReturnChartProps {
  shortData: RecentReturnData[];
  longData: RecentReturnData[];
  shortPeriod: number;
  longPeriod: number;
  title?: string;
}

export function RecentReturnChart({
  shortData,
  longData,
  shortPeriod,
  longPeriod,
  title,
}: RecentReturnChartProps) {
  const { settings } = useChartStore();
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const shortSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const longSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const latestShortValue = useMemo(() => {
    if (shortData.length === 0) return null;
    return shortData[shortData.length - 1]?.value ?? null;
  }, [shortData]);
  const latestLongValue = useMemo(() => {
    if (longData.length === 0) return null;
    return longData[longData.length - 1]?.value ?? null;
  }, [longData]);

  useEffect(() => {
    if (!chartContainerRef.current) return;

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
      ...PAGE_SCROLL_CHART_INTERACTION_OPTIONS,
    });
    chartRef.current = chart;

    shortSeriesRef.current = chart.addSeries(LineSeries, {
      color: SHORT_RETURN_COLOR,
      lineWidth: 2,
      priceFormat: {
        type: 'price',
        precision: 2,
        minMove: 0.01,
      },
    });

    longSeriesRef.current = chart.addSeries(LineSeries, {
      color: LONG_RETURN_COLOR,
      lineWidth: 2,
      priceFormat: {
        type: 'price',
        precision: 2,
        minMove: 0.01,
      },
    });

    return () => {
      chart.remove();
      chartRef.current = null;
      shortSeriesRef.current = null;
      longSeriesRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!shortSeriesRef.current || !longSeriesRef.current) return;

    const shortLineData = shortData.map((item) => ({ time: item.time, value: item.value }));
    const longLineData = longData.map((item) => ({ time: item.time, value: item.value }));
    shortSeriesRef.current.setData(shortLineData);
    longSeriesRef.current.setData(longLineData);

    if (chartRef.current) {
      setChartVisibleBars(chartRef.current, Math.max(shortLineData.length, longLineData.length), settings.visibleBars);
    }
  }, [shortData, longData, settings.visibleBars]);

  useEffect(() => {
    const container = chartContainerRef.current;
    if (!container) return;

    const MIN_HEIGHT = 100;
    const resizeObserver = new ResizeObserver(() => {
      if (!chartRef.current) return;
      const width = container.clientWidth;
      const height = container.clientHeight;
      if (width > 0 && height >= MIN_HEIGHT) {
        chartRef.current.applyOptions({ width, height });
      }
    });

    resizeObserver.observe(container);
    return () => resizeObserver.disconnect();
  }, []);

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex flex-wrap items-center gap-2 border-b border-border/30 p-2 text-sm font-medium text-muted-foreground">
        <span>{title || 'Recent Return'}</span>
        <span style={{ color: SHORT_RETURN_COLOR }}>{shortPeriod}</span>
        {latestShortValue !== null && (
          <span style={{ color: SHORT_RETURN_COLOR }}>{formatPercent(latestShortValue)}</span>
        )}
        <span style={{ color: LONG_RETURN_COLOR }}>{longPeriod}</span>
        {latestLongValue !== null && <span style={{ color: LONG_RETURN_COLOR }}>{formatPercent(latestLongValue)}</span>}
      </div>
      <div ref={chartContainerRef} className="min-h-0 flex-1" />
    </div>
  );
}
