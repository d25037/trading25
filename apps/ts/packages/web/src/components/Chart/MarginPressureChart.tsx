import { createChart, type IChartApi, type ISeriesApi, LineSeries, type Time } from 'lightweight-charts';
import { useEffect, useRef } from 'react';
import type { MarginFlowPressureData, MarginLongPressureData, MarginTurnoverDaysData } from '@/types/chart';
import { logger } from '@/utils/logger';

type ChartType = 'longPressure' | 'flowPressure' | 'turnoverDays';

interface BaseDataItem {
  date: string;
}

// Helper function to get value from data item based on chart type
function getValue(
  item: MarginLongPressureData | MarginFlowPressureData | MarginTurnoverDaysData,
  type: ChartType
): number {
  switch (type) {
    case 'longPressure':
      return (item as MarginLongPressureData).pressure;
    case 'flowPressure':
      return (item as MarginFlowPressureData).flowPressure;
    case 'turnoverDays':
      return (item as MarginTurnoverDaysData).turnoverDays;
  }
}

// Helper function to set chart visible range to last 6 months
function setDefaultVisibleRange<T extends BaseDataItem>(chart: IChartApi, data: T[]) {
  if (data.length === 0) return;

  const lastItem = data[data.length - 1];
  if (!lastItem?.date) return;

  const lastTime = lastItem.date;
  const lastDate = new Date(lastTime);
  const sixMonthsAgo = new Date(lastDate);
  sixMonthsAgo.setMonth(lastDate.getMonth() - 6);
  const fromTime = sixMonthsAgo.getTime() / 1000;

  chart.timeScale().setVisibleRange({
    from: fromTime as Time,
    to: lastTime as Time,
  });
}

interface MarginPressureChartProps {
  type: ChartType;
  longPressureData?: MarginLongPressureData[];
  flowPressureData?: MarginFlowPressureData[];
  turnoverDaysData?: MarginTurnoverDaysData[];
}

const CHART_CONFIG: Record<ChartType, { title: string; color: string; hasZeroLine: boolean }> = {
  longPressure: {
    title: '信用ロング圧力',
    color: '#2563eb', // Blue
    hasZeroLine: true,
  },
  flowPressure: {
    title: '信用フロー圧力',
    color: '#7c3aed', // Purple
    hasZeroLine: true,
  },
  turnoverDays: {
    title: '信用回転日数',
    color: '#059669', // Green
    hasZeroLine: false,
  },
};

export function MarginPressureChart({
  type,
  longPressureData,
  flowPressureData,
  turnoverDaysData,
}: MarginPressureChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const zeroLineRef = useRef<ISeriesApi<'Line'> | null>(null);

  const config = CHART_CONFIG[type];

  // Get data based on chart type
  type DataItem = MarginLongPressureData | MarginFlowPressureData | MarginTurnoverDaysData;

  const getData = (): DataItem[] => {
    switch (type) {
      case 'longPressure':
        return longPressureData || [];
      case 'flowPressure':
        return flowPressureData || [];
      case 'turnoverDays':
        return turnoverDaysData || [];
    }
  };

  const data = getData();

  useEffect(() => {
    logger.debug('MarginPressureChart mounting', { type });
    if (!chartContainerRef.current) {
      logger.error('Chart container ref is null');
      return;
    }

    // Create chart
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { color: 'transparent' },
        textColor: '#333',
      },
      grid: {
        vertLines: { color: '#e1e1e1' },
        horzLines: { color: '#e1e1e1' },
      },
      width: chartContainerRef.current.clientWidth,
      height: 200,
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
        borderColor: '#e1e1e1',
      },
      rightPriceScale: {
        borderColor: '#e1e1e1',
      },
    });

    chartRef.current = chart;

    // Add main series (no title label to keep chart clean, last value is still shown)
    const series = chart.addSeries(LineSeries, {
      color: config.color,
      lineWidth: 2,
    });
    seriesRef.current = series;

    // Add zero line for pressure charts
    if (config.hasZeroLine) {
      const zeroLine = chart.addSeries(LineSeries, {
        color: '#9ca3af',
        lineWidth: 1,
        lineStyle: 2, // Dashed
        title: '',
        priceLineVisible: false,
        lastValueVisible: false,
      });
      zeroLineRef.current = zeroLine;
    }

    // Cleanup function
    return () => {
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      zeroLineRef.current = null;
    };
  }, [type, config.color, config.hasZeroLine]);

  // Update data when props change
  useEffect(() => {
    logger.debug('MarginPressureChart data update', { type, dataLength: data.length });

    if (!seriesRef.current || data.length === 0) return;

    // Format data for chart
    const formattedData = data.map((item) => ({
      time: item.date,
      value: getValue(item, type),
    }));

    seriesRef.current.setData(formattedData);

    // Update zero line if present
    if (zeroLineRef.current && config.hasZeroLine) {
      const zeroLineData = data.map((item) => ({
        time: item.date,
        value: 0,
      }));
      zeroLineRef.current.setData(zeroLineData);
    }

    // Set default visible range
    if (chartRef.current) {
      setDefaultVisibleRange(chartRef.current, data);
    }
  }, [data, type, config.hasZeroLine]);

  // Handle chart resize
  useEffect(() => {
    const handleResize = () => {
      if (chartRef.current && chartContainerRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
        });
      }
    };

    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  return (
    <div className="relative h-full w-full">
      {/* Chart title (not the axis label) */}
      <div className="absolute left-2 top-1 z-10 text-xs font-medium text-muted-foreground">{config.title}</div>
      <div ref={chartContainerRef} className="h-full w-full" />
      {data.length === 0 && (
        <div className="absolute inset-0 flex items-center justify-center">
          <p className="text-sm text-muted-foreground">No data available</p>
        </div>
      )}
    </div>
  );
}
