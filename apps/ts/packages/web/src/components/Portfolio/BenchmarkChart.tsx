import { createChart, type IChartApi, type ISeriesApi, LineSeries } from 'lightweight-charts';
import { useEffect, useRef } from 'react';
import type { BenchmarkDataPoint } from '@/hooks/usePortfolioPerformance';

interface BenchmarkChartProps {
  data: BenchmarkDataPoint[];
  benchmarkName?: string;
}

/**
 * Convert log return to percentage for display
 */
function logToPercent(logReturn: number): number {
  return (Math.exp(logReturn) - 1) * 100;
}

export function BenchmarkChart({ data, benchmarkName = 'TOPIX' }: BenchmarkChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const portfolioSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const benchmarkSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);

  // Initialize chart
  useEffect(() => {
    if (!chartContainerRef.current) return;

    const containerHeight = chartContainerRef.current.clientHeight || 300;
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { color: 'transparent' },
        textColor: '#666',
      },
      grid: {
        vertLines: { color: 'rgba(128, 128, 128, 0.2)' },
        horzLines: { color: 'rgba(128, 128, 128, 0.2)' },
      },
      width: chartContainerRef.current.clientWidth,
      height: containerHeight,
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
      },
      rightPriceScale: {
        borderVisible: false,
      },
      crosshair: {
        horzLine: {
          visible: true,
          labelVisible: true,
        },
        vertLine: {
          visible: true,
          labelVisible: true,
        },
      },
    });

    chartRef.current = chart;

    // Portfolio line (primary color - blue)
    const portfolioSeries = chart.addSeries(LineSeries, {
      color: '#3b82f6',
      lineWidth: 2,
      title: 'Portfolio',
      priceFormat: {
        type: 'custom',
        formatter: (price: number) => `${price >= 0 ? '+' : ''}${price.toFixed(2)}%`,
      },
    });
    portfolioSeriesRef.current = portfolioSeries;

    // Benchmark line (muted color - gray)
    const benchmarkSeries = chart.addSeries(LineSeries, {
      color: '#9ca3af',
      lineWidth: 2,
      lineStyle: 2, // Dashed
      title: benchmarkName,
      priceFormat: {
        type: 'custom',
        formatter: (price: number) => `${price >= 0 ? '+' : ''}${price.toFixed(2)}%`,
      },
    });
    benchmarkSeriesRef.current = benchmarkSeries;

    // Handle resize
    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
        });
      }
    };

    const resizeObserver = new ResizeObserver(handleResize);
    resizeObserver.observe(chartContainerRef.current);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
      chartRef.current = null;
      portfolioSeriesRef.current = null;
      benchmarkSeriesRef.current = null;
    };
  }, [benchmarkName]);

  // Update data
  useEffect(() => {
    if (!portfolioSeriesRef.current || !benchmarkSeriesRef.current || data.length === 0) {
      return;
    }

    // Convert to percentage and format for chart
    const portfolioData = data.map((d) => ({
      time: d.date,
      value: logToPercent(d.portfolioReturn),
    }));

    const benchmarkData = data.map((d) => ({
      time: d.date,
      value: logToPercent(d.benchmarkReturn),
    }));

    portfolioSeriesRef.current.setData(portfolioData);
    benchmarkSeriesRef.current.setData(benchmarkData);

    // Fit content
    if (chartRef.current) {
      chartRef.current.timeScale().fitContent();
    }
  }, [data]);

  return (
    <div className="w-full">
      <div className="flex items-center gap-4 mb-2 text-sm">
        <div className="flex items-center gap-2">
          <div className="w-4 h-0.5 bg-primary" />
          <span>Portfolio</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-0.5 bg-muted-foreground" style={{ borderStyle: 'dashed' }} />
          <span>{benchmarkName}</span>
        </div>
      </div>
      <div ref={chartContainerRef} className="h-[300px] w-full" />
    </div>
  );
}
