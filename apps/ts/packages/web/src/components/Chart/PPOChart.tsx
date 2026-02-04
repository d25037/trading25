import { createChart, HistogramSeries, type IChartApi, type ISeriesApi, LineSeries } from 'lightweight-charts';
import { useCallback, useEffect, useRef } from 'react';
import { useChartStore } from '@/stores/chartStore';
import type { PPOIndicatorData } from '@/types/chart';

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

interface PPOChartProps {
  data: PPOIndicatorData[];
  title?: string;
}

export function PPOChart({ data, title }: PPOChartProps) {
  const { settings } = useChartStore();
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const ppoLineSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const signalLineSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const histogramSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  const zeroLineSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);

  /**
   * Process individual data item for all series
   */
  const processDataItem = useCallback(
    (
      item: PPOIndicatorData,
      ppoLineData: Array<{ time: string; value: number }>,
      signalLineData: Array<{ time: string; value: number }>,
      histogramData: Array<{ time: string; value: number; color: string }>
    ): void => {
      if (typeof item.ppo === 'number' && !Number.isNaN(item.ppo)) {
        ppoLineData.push({ time: item.time, value: item.ppo });
      }

      if (typeof item.signal === 'number' && !Number.isNaN(item.signal)) {
        signalLineData.push({ time: item.time, value: item.signal });
      }

      if (typeof item.histogram === 'number' && !Number.isNaN(item.histogram)) {
        histogramData.push({
          time: item.time,
          value: item.histogram,
          color: item.histogram >= 0 ? '#f44336' : '#4caf50',
        });
      }
    },
    []
  );

  /**
   * Check if all chart series are ready for updates
   */
  const isChartReadyForUpdate = useCallback((): boolean => {
    return !!(
      ppoLineSeriesRef.current &&
      signalLineSeriesRef.current &&
      histogramSeriesRef.current &&
      zeroLineSeriesRef.current
    );
  }, []);

  /**
   * Process raw data into chart series data
   */
  const processChartData = useCallback(
    (rawData: PPOIndicatorData[]) => {
      const ppoLineData: Array<{ time: string; value: number }> = [];
      const signalLineData: Array<{ time: string; value: number }> = [];
      const histogramData: Array<{ time: string; value: number; color: string }> = [];

      // Single pass through data for all three series (more memory efficient)
      for (const item of rawData) {
        processDataItem(item, ppoLineData, signalLineData, histogramData);
      }

      return { ppoLineData, signalLineData, histogramData };
    },
    [processDataItem]
  );

  /**
   * Update all chart series with processed data
   */
  const updateSeriesData = useCallback(
    (processedData: {
      ppoLineData: Array<{ time: string; value: number }>;
      signalLineData: Array<{ time: string; value: number }>;
      histogramData: Array<{ time: string; value: number; color: string }>;
    }): void => {
      const { ppoLineData, signalLineData, histogramData } = processedData;

      ppoLineSeriesRef.current?.setData(ppoLineData);
      signalLineSeriesRef.current?.setData(signalLineData);
      histogramSeriesRef.current?.setData(histogramData);
    },
    []
  );

  /**
   * Update the zero reference line
   */
  const updateZeroLine = useCallback((rawData: PPOIndicatorData[]): void => {
    const firstTime = rawData[0]?.time;
    const lastTime = rawData[rawData.length - 1]?.time;

    if (firstTime && lastTime && zeroLineSeriesRef.current) {
      const zeroLineData = [
        { time: firstTime, value: 0 },
        { time: lastTime, value: 0 },
      ];
      zeroLineSeriesRef.current.setData(zeroLineData);
    }
  }, []);

  /**
   * Update the visible range of the chart
   */
  const updateVisibleRange = useCallback(
    (processedData: {
      ppoLineData: Array<{ time: string; value: number }>;
      signalLineData: Array<{ time: string; value: number }>;
      histogramData: Array<{ time: string; value: number; color: string }>;
    }): void => {
      if (!chartRef.current) return;

      const { ppoLineData, signalLineData, histogramData } = processedData;
      const displayedDataLength = Math.max(ppoLineData.length, signalLineData.length, histogramData.length);
      setChartVisibleBars(chartRef.current, displayedDataLength, settings.visibleBars);
    },
    [settings.visibleBars]
  );

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

    // Add histogram series first (background)
    const histogramSeries = chart.addSeries(HistogramSeries, {
      color: '#2196f3',
      priceFormat: {
        type: 'percent',
        precision: 2,
      },
      priceScaleId: '',
    });

    histogramSeries.priceScale().applyOptions({
      scaleMargins: {
        top: 0.8,
        bottom: 0,
      },
    });

    histogramSeriesRef.current = histogramSeries;

    // Add PPO line series
    const ppoLineSeries = chart.addSeries(LineSeries, {
      color: '#FFA500',
      lineWidth: 2,
      priceFormat: {
        type: 'percent',
        precision: 2,
      },
    });

    ppoLineSeriesRef.current = ppoLineSeries;

    // Add signal line series
    const signalLineSeries = chart.addSeries(LineSeries, {
      color: '#0000FF',
      lineWidth: 2,
      lineStyle: 2, // dashed
      priceFormat: {
        type: 'percent',
        precision: 2,
      },
    });

    signalLineSeriesRef.current = signalLineSeries;

    // Add zero line
    const zeroLineSeries = chart.addSeries(LineSeries, {
      color: '#9e9e9e',
      lineWidth: 1,
      lineStyle: 1, // dotted
    });

    zeroLineSeriesRef.current = zeroLineSeries;

    // Cleanup function
    return () => {
      chart.remove();
      chartRef.current = null;
      ppoLineSeriesRef.current = null;
      signalLineSeriesRef.current = null;
      histogramSeriesRef.current = null;
      zeroLineSeriesRef.current = null;
    };
  }, []);

  // Update chart data when data changes
  useEffect(() => {
    if (!isChartReadyForUpdate() || !data.length) {
      return;
    }

    const processedData = processChartData(data);
    updateSeriesData(processedData);
    updateZeroLine(data);
    updateVisibleRange(processedData);
  }, [data, isChartReadyForUpdate, processChartData, updateSeriesData, updateZeroLine, updateVisibleRange]);

  // Handle window resize
  useEffect(() => {
    const handleResize = () => {
      if (chartRef.current && chartContainerRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
          height: chartContainerRef.current.clientHeight,
        });
      }
    };

    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  return (
    <div className="h-full">
      {title && <div className="p-2 text-sm font-medium text-muted-foreground border-b border-border/30">{title}</div>}
      <div ref={chartContainerRef} className="flex-1 h-full" />
    </div>
  );
}
