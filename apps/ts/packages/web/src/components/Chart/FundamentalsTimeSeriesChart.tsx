import { isFiscalYear } from '@trading25/shared/fundamental-analysis/utils';
import type { ApiDailyValuationDataPoint, ApiFundamentalDataPoint } from '@trading25/shared/types/api-types';
import { createChart, type IChartApi, type ISeriesApi, type LineData, LineSeries } from 'lightweight-charts';
import { useEffect, useMemo, useRef, useState } from 'react';
import { cn } from '@/lib/utils';

type MetricKey =
  | 'roe'
  | 'per'
  | 'pbr'
  | 'eps'
  | 'bps'
  | 'roa'
  | 'operatingMargin'
  | 'netMargin'
  | 'cashFlowOperating'
  | 'cashFlowInvesting'
  | 'cashFlowFinancing'
  | 'cashAndEquivalents'
  | 'fcf'
  | 'fcfYield'
  | 'fcfMargin';

interface MetricOption {
  key: MetricKey;
  label: string;
  unit: string;
  /** 'daily' uses dailyValuation data, 'disclosure' uses disclosure-based data */
  source: 'daily' | 'disclosure';
}

const METRIC_OPTIONS: MetricOption[] = [
  { key: 'per', label: 'PER', unit: '倍', source: 'daily' },
  { key: 'pbr', label: 'PBR', unit: '倍', source: 'daily' },
  { key: 'eps', label: 'EPS', unit: '円', source: 'disclosure' },
  { key: 'bps', label: 'BPS', unit: '円', source: 'disclosure' },
  { key: 'roe', label: 'ROE', unit: '%', source: 'disclosure' },
  { key: 'roa', label: 'ROA', unit: '%', source: 'disclosure' },
  { key: 'operatingMargin', label: '営業利益率', unit: '%', source: 'disclosure' },
  { key: 'netMargin', label: '純利益率', unit: '%', source: 'disclosure' },
  { key: 'cashFlowOperating', label: '営業CF', unit: '百万円', source: 'disclosure' },
  { key: 'cashFlowInvesting', label: '投資CF', unit: '百万円', source: 'disclosure' },
  { key: 'cashFlowFinancing', label: '財務CF', unit: '百万円', source: 'disclosure' },
  { key: 'cashAndEquivalents', label: '現金', unit: '百万円', source: 'disclosure' },
  { key: 'fcf', label: 'FCF', unit: '百万円', source: 'disclosure' },
  { key: 'fcfYield', label: 'FCF利回り', unit: '%', source: 'disclosure' },
  { key: 'fcfMargin', label: 'FCFマージン', unit: '%', source: 'disclosure' },
];

interface FundamentalsTimeSeriesChartProps {
  data: ApiFundamentalDataPoint[] | undefined;
  /** Daily PER/PBR time series (calculated with daily close prices) */
  dailyValuation?: ApiDailyValuationDataPoint[];
}

export function FundamentalsTimeSeriesChart({ data, dailyValuation }: FundamentalsTimeSeriesChartProps) {
  const [selectedMetric, setSelectedMetric] = useState<MetricKey>('per');
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<'Line'> | null>(null);

  // Prepare chart data based on selected metric's data source
  const chartData: LineData[] = useMemo(() => {
    const selectedOption = METRIC_OPTIONS.find((o) => o.key === selectedMetric);

    // For PER/PBR, use daily valuation data if available
    if (selectedOption?.source === 'daily' && dailyValuation && dailyValuation.length > 0) {
      return dailyValuation
        .filter((d) => d[selectedMetric as 'per' | 'pbr'] !== null)
        .map((d) => ({
          time: d.date,
          value: d[selectedMetric as 'per' | 'pbr'] as number,
        }))
        .sort((a, b) => (a.time < b.time ? -1 : 1));
    }

    // For other metrics or fallback, use disclosure-based data
    if (!data) return [];

    // Filter to FY (full year) data only
    const fyData = data.filter((d) => isFiscalYear(d.periodType));

    // Deduplicate by date, keeping most recent disclosure
    const deduped = new Map<string, ApiFundamentalDataPoint>();
    for (const d of fyData) {
      const existing = deduped.get(d.date);
      if (!existing || d.disclosedDate > existing.disclosedDate) {
        deduped.set(d.date, d);
      }
    }

    const resolveDisclosureValue = (item: ApiFundamentalDataPoint, key: MetricKey): number | null => {
      if (key === 'eps') return item.adjustedEps ?? item.eps ?? null;
      if (key === 'bps') return item.adjustedBps ?? item.bps ?? null;
      return (item[key] as number | null) ?? null;
    };

    return Array.from(deduped.values())
      .map((d) => ({
        time: d.date,
        value: resolveDisclosureValue(d, selectedMetric),
      }))
      .filter((d) => d.value !== null)
      .map((d) => ({
        time: d.time,
        value: d.value as number,
      }))
      .sort((a, b) => (a.time < b.time ? -1 : 1));
  }, [data, dailyValuation, selectedMetric]);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    // Create chart
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { color: 'transparent' },
        textColor: '#9ca3af',
      },
      grid: {
        vertLines: { color: 'rgba(255, 255, 255, 0.05)' },
        horzLines: { color: 'rgba(255, 255, 255, 0.05)' },
      },
      crosshair: {
        mode: 0,
      },
      rightPriceScale: {
        borderColor: 'rgba(255, 255, 255, 0.1)',
        scaleMargins: {
          top: 0.1,
          bottom: 0.1,
        },
      },
      timeScale: {
        borderColor: 'rgba(255, 255, 255, 0.1)',
        timeVisible: false,
      },
      handleScale: {
        axisPressedMouseMove: true,
      },
      handleScroll: {
        vertTouchDrag: false,
      },
    });

    chartRef.current = chart;

    // Add line series
    const lineSeries = chart.addSeries(LineSeries, {
      color: '#6366f1',
      lineWidth: 2,
      crosshairMarkerVisible: true,
      crosshairMarkerRadius: 4,
      crosshairMarkerBorderColor: '#ffffff',
      crosshairMarkerBackgroundColor: '#6366f1',
    });

    seriesRef.current = lineSeries;

    // Handle resize
    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
          height: chartContainerRef.current.clientHeight,
        });
      }
    };

    const resizeObserver = new ResizeObserver(handleResize);
    resizeObserver.observe(chartContainerRef.current);

    // Initial size
    handleResize();

    return () => {
      resizeObserver.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, []);

  // Update data when metric or data changes
  useEffect(() => {
    if (seriesRef.current && chartData.length > 0) {
      seriesRef.current.setData(chartData);
      chartRef.current?.timeScale().fitContent();
    }
  }, [chartData]);

  const selectedOption = METRIC_OPTIONS.find((o) => o.key === selectedMetric);

  if (!data || data.length === 0) {
    return (
      <div className="h-full flex items-center justify-center">
        <p className="text-sm text-muted-foreground">No historical data available</p>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      {/* Metric selector */}
      <div className="flex gap-1 p-2 overflow-x-auto">
        {METRIC_OPTIONS.map((option) => (
          <button
            key={option.key}
            type="button"
            onClick={() => setSelectedMetric(option.key)}
            className={cn(
              'px-2 py-1 text-xs rounded-md whitespace-nowrap transition-colors',
              selectedMetric === option.key
                ? 'bg-primary text-primary-foreground'
                : 'bg-background/50 text-muted-foreground hover:bg-background/80'
            )}
          >
            {option.label}
          </button>
        ))}
      </div>

      {/* Chart */}
      <div className="flex-1 relative min-h-0">
        <div ref={chartContainerRef} className="absolute inset-0" />
        {chartData.length === 0 && (
          <div className="absolute inset-0 flex items-center justify-center">
            <p className="text-sm text-muted-foreground">No {selectedOption?.label} data available</p>
          </div>
        )}
      </div>

      {/* Legend */}
      <div className="px-3 py-1 text-xs text-muted-foreground border-t border-border/30">
        {selectedOption?.label} ({selectedOption?.unit}) - {chartData.length} data points
      </div>
    </div>
  );
}
