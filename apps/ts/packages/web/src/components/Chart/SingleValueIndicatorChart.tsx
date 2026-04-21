import { createChart, type IChartApi, type ISeriesApi, LineSeries } from 'lightweight-charts';
import { useEffect, useMemo, useRef } from 'react';
import { PAGE_SCROLL_CHART_INTERACTION_OPTIONS } from '@/components/Chart/chartInteractionOptions';
import { useChartStore } from '@/stores/chartStore';
import type { IndicatorValue } from '@/types/chart';

function setChartVisibleBars(chart: IChartApi, dataLength: number, barsToShow: number) {
  if (dataLength === 0) return;

  const from = Math.max(0, dataLength - barsToShow - 0.5);
  const to = dataLength - 0.5;
  chart.timeScale().setVisibleLogicalRange({ from, to });
}

interface SingleValueIndicatorChartProps {
  data: IndicatorValue[];
  title: string;
  accentColor?: string;
  periodLabel?: string;
  precision?: number;
}

export function SingleValueIndicatorChart({
  data,
  title,
  accentColor = '#2962FF',
  periodLabel,
  precision = 2,
}: SingleValueIndicatorChartProps) {
  const { settings } = useChartStore();
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const lineSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const latestValue = useMemo(() => {
    if (data.length === 0) return null;
    return data[data.length - 1]?.value ?? null;
  }, [data]);

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

    lineSeriesRef.current = chart.addSeries(LineSeries, {
      color: accentColor,
      lineWidth: 2,
      priceFormat: {
        type: 'price',
        precision,
        minMove: 10 ** -precision,
      },
    });

    return () => {
      chart.remove();
      chartRef.current = null;
      lineSeriesRef.current = null;
    };
  }, [accentColor, precision]);

  useEffect(() => {
    if (!lineSeriesRef.current) return;

    if (data.length === 0) {
      lineSeriesRef.current.setData([]);
      return;
    }

    const lineData = data.map((item) => ({ time: item.time, value: item.value }));
    lineSeriesRef.current.setData(lineData);

    if (chartRef.current) {
      setChartVisibleBars(chartRef.current, lineData.length, settings.visibleBars);
    }
  }, [data, settings.visibleBars]);

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
      <div className="flex items-center gap-2 border-b border-border/30 p-2 text-sm font-medium text-muted-foreground">
        <span>{title}</span>
        {periodLabel && <span style={{ color: accentColor }}>{periodLabel}</span>}
        {latestValue !== null && <span style={{ color: accentColor }}>{latestValue.toFixed(precision)}</span>}
      </div>
      <div ref={chartContainerRef} className="min-h-0 flex-1" />
    </div>
  );
}
