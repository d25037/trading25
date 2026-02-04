import { createChart, type IChartApi, type ISeriesApi, LineSeries, type Time } from 'lightweight-charts';
import { useEffect, useRef } from 'react';
import type { MarginVolumeRatioData } from '@/types/chart';
import { logger } from '@/utils/logger';

// Helper function to set chart visible range to last 6 months
function setDefaultVisibleRange(chart: IChartApi, data: MarginVolumeRatioData[]) {
  if (data.length === 0) return;

  const lastItem = data[data.length - 1];
  if (!lastItem?.date) return;

  const lastTime = lastItem.date;

  // Calculate 6 months ago timestamp
  let fromTime: number;
  if (typeof lastTime === 'string') {
    const lastDate = new Date(lastTime);
    const sixMonthsAgo = new Date(lastDate);
    sixMonthsAgo.setMonth(lastDate.getMonth() - 6);
    fromTime = sixMonthsAgo.getTime() / 1000;
  } else {
    // If lastTime is already a timestamp, subtract 6 months (approximately 180 days)
    fromTime = lastTime - 180 * 24 * 60 * 60;
  }

  chart.timeScale().setVisibleRange({
    from: fromTime as Time,
    to: lastTime as Time,
  });
}

interface MarginRatioChartProps {
  data: MarginVolumeRatioData[];
  title: string;
  longData?: MarginVolumeRatioData[];
  shortData?: MarginVolumeRatioData[];
  type: 'ratio' | 'comparison';
}

export function MarginRatioChart({ data, title, longData, shortData, type }: MarginRatioChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const longSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const shortSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const ratioSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);

  useEffect(() => {
    logger.debug('MarginRatioChart mounting', { type });
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
      height: 280,
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

    if (type === 'comparison' && longData && shortData) {
      // 買い残高と売り残高の比率を同一チャートに表示
      const longSeries = chart.addSeries(LineSeries, {
        color: '#2563eb', // 青色
        lineWidth: 2,
        title: '買い残',
      });

      const shortSeries = chart.addSeries(LineSeries, {
        color: '#dc2626', // 赤色
        lineWidth: 2,
        title: '売り残',
      });

      longSeriesRef.current = longSeries;
      shortSeriesRef.current = shortSeries;
    } else {
      // 買い/売り比率を単独で表示
      const ratioSeries = chart.addSeries(LineSeries, {
        color: '#059669', // 緑色
        lineWidth: 2,
        title: title,
      });

      ratioSeriesRef.current = ratioSeries;
    }

    // Cleanup function
    return () => {
      chart.remove();
      chartRef.current = null;
      longSeriesRef.current = null;
      shortSeriesRef.current = null;
      ratioSeriesRef.current = null;
    };
  }, [type, title, longData, shortData]);

  // Update data when props change
  useEffect(() => {
    logger.debug('MarginRatioChart data update', { type, dataLength: data.length });

    if (type === 'comparison' && longSeriesRef.current && shortSeriesRef.current && longData && shortData) {
      // 買い残高データ
      const formattedLongData = longData.map((item) => ({
        time: item.date,
        value: item.ratio,
      }));

      // 売り残高データ
      const formattedShortData = shortData.map((item) => ({
        time: item.date,
        value: item.ratio,
      }));

      longSeriesRef.current.setData(formattedLongData);
      shortSeriesRef.current.setData(formattedShortData);

      // Set default visible range to last 6 months for comparison chart
      if (chartRef.current && longData.length > 0) {
        setDefaultVisibleRange(chartRef.current, longData);
      }
    } else if (type === 'ratio' && ratioSeriesRef.current && data.length > 0) {
      // 比率データ
      const formattedData = data.map((item) => ({
        time: item.date,
        value: item.ratio,
      }));

      ratioSeriesRef.current.setData(formattedData);

      // Set default visible range to last 6 months for ratio chart
      if (chartRef.current) {
        setDefaultVisibleRange(chartRef.current, data);
      }
    }
  }, [data, longData, shortData, type]);

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

  // Check if there's no data to display based on chart type
  const hasNoData =
    type === 'comparison'
      ? (!longData || longData.length === 0) && (!shortData || shortData.length === 0)
      : data.length === 0;

  return (
    <div className="h-full w-full">
      <div ref={chartContainerRef} className="h-full w-full" />
      {hasNoData && (
        <div className="absolute inset-0 flex items-center justify-center">
          <p className="text-muted-foreground">No margin ratio data available</p>
        </div>
      )}
    </div>
  );
}
