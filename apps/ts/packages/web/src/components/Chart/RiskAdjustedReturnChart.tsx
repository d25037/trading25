import { createChart, type IChartApi, type ISeriesApi, LineSeries } from 'lightweight-charts';
import { useEffect, useMemo, useRef } from 'react';
import { CHART_COLORS } from '@/lib/constants';
import { useChartStore } from '@/stores/chartStore';
import type { RiskAdjustedReturnCondition, RiskAdjustedReturnRatioType } from '@/stores/chartStore';
import type { RiskAdjustedReturnData } from '@/types/chart';

function setChartVisibleBars(chart: IChartApi, dataLength: number, barsToShow: number) {
  if (dataLength === 0) return;

  const from = Math.max(0, dataLength - barsToShow - 0.5);
  const to = dataLength - 0.5;
  chart.timeScale().setVisibleLogicalRange({ from, to });
}

interface RiskAdjustedReturnChartProps {
  data: RiskAdjustedReturnData[];
  lookbackPeriod: number;
  ratioType: RiskAdjustedReturnRatioType;
  threshold: number;
  condition: RiskAdjustedReturnCondition;
  title?: string;
}

export function RiskAdjustedReturnChart({
  data,
  lookbackPeriod,
  ratioType,
  threshold,
  condition,
  title,
}: RiskAdjustedReturnChartProps) {
  const { settings } = useChartStore();
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const ratioSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const thresholdSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
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
    });
    chartRef.current = chart;

    ratioSeriesRef.current = chart.addSeries(LineSeries, {
      color: '#F59E0B',
      lineWidth: 2,
      priceFormat: {
        type: 'price',
        precision: 2,
        minMove: 0.01,
      },
    });

    thresholdSeriesRef.current = chart.addSeries(LineSeries, {
      color: CHART_COLORS.UP,
      lineWidth: 1,
      lineStyle: 2,
      priceFormat: {
        type: 'price',
        precision: 2,
        minMove: 0.01,
      },
    });

    return () => {
      chart.remove();
      chartRef.current = null;
      ratioSeriesRef.current = null;
      thresholdSeriesRef.current = null;
    };
  }, []);

  useEffect(() => {
    thresholdSeriesRef.current?.applyOptions({
      color: condition === 'above' ? CHART_COLORS.UP : CHART_COLORS.DOWN,
    });
  }, [condition]);

  useEffect(() => {
    if (!ratioSeriesRef.current || !thresholdSeriesRef.current) return;

    if (data.length === 0) {
      ratioSeriesRef.current.setData([]);
      thresholdSeriesRef.current.setData([]);
      return;
    }

    const ratioData = data.map((item) => ({ time: item.time, value: item.value }));
    ratioSeriesRef.current.setData(ratioData);

    const firstTime = ratioData[0]?.time;
    const lastTime = ratioData[ratioData.length - 1]?.time;
    if (firstTime && lastTime) {
      thresholdSeriesRef.current.setData([
        { time: firstTime, value: threshold },
        { time: lastTime, value: threshold },
      ]);
    } else {
      thresholdSeriesRef.current.setData([]);
    }

    if (chartRef.current) {
      setChartVisibleBars(chartRef.current, ratioData.length, settings.visibleBars);
    }
  }, [condition, data, settings.visibleBars, threshold]);

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
    <div className="h-full">
      <div className="p-2 text-sm font-medium text-muted-foreground border-b border-border/30 flex items-center gap-2 flex-wrap">
        <span>{title || 'Risk Adjusted Return'}</span>
        <span className="text-[#F59E0B]">{lookbackPeriod}</span>
        <span className="capitalize">{ratioType}</span>
        <span className={condition === 'above' ? 'text-[#26a69a]' : 'text-[#ef5350]'}>
          {condition} {threshold.toFixed(2)}
        </span>
        {latestValue !== null && <span className="text-[#F59E0B]">{latestValue.toFixed(2)}</span>}
      </div>
      <div ref={chartContainerRef} className="flex-1 h-full" />
    </div>
  );
}
