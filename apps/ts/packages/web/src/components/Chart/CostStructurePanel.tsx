import { useState } from 'react';
import type { CostStructureAnalysisView } from '@trading25/api-clients/analytics';
import type { ApiCostStructurePoint } from '@trading25/contracts/types/api-types';
import { AlertTriangle } from 'lucide-react';
import { SegmentedTabs } from '@/components/Layout/Workspace';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { useCostStructureAnalysis } from '@/hooks/useCostStructureAnalysis';
import { formatFundamentalValue } from '@/utils/formatters';

interface CostStructurePanelProps {
  symbol: string | null;
  enabled?: boolean;
}

type CostStructurePanelMode = 'recent12' | 'recent20' | 'same_quarter' | 'fiscal_year_only';

const COST_STRUCTURE_MODE_ITEMS = [
  { label: '12Q', value: 'recent12' },
  { label: '20Q', value: 'recent20' },
  { label: 'Same Q', value: 'same_quarter' },
  { label: 'FY only', value: 'fiscal_year_only' },
] as const satisfies ReadonlyArray<{ label: string; value: CostStructurePanelMode }>;

interface ScatterChartProps {
  points: ApiCostStructurePoint[];
  intercept: number;
  slope: number;
}

interface ScatterLayoutPoint {
  point: ApiCostStructurePoint;
  cx: number;
  cy: number;
}

interface ScatterLayout {
  points: ScatterLayoutPoint[];
  line: {
    x1: number;
    y1: number;
    x2: number;
    y2: number;
  };
  xTicks: Array<{ value: number; x: number }>;
  yTicks: Array<{ value: number; y: number }>;
  width: number;
  height: number;
  padding: {
    top: number;
    right: number;
    bottom: number;
    left: number;
  };
}

function formatMillions(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return formatFundamentalValue(value, 'millions');
}

function formatPercent(value: number | null | undefined, decimals = 1): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toFixed(decimals)}%`;
}

function formatRatioPercent(value: number | null | undefined, decimals = 1): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${(value * 100).toFixed(decimals)}%`;
}

function getAnalysisOptions(mode: CostStructurePanelMode): {
  view: CostStructureAnalysisView;
  windowQuarters: number;
} {
  switch (mode) {
    case 'recent20':
      return { view: 'recent', windowQuarters: 20 };
    case 'same_quarter':
      return { view: 'same_quarter', windowQuarters: 12 };
    case 'fiscal_year_only':
      return { view: 'fiscal_year_only', windowQuarters: 12 };
    default:
      return { view: 'recent', windowQuarters: 12 };
  }
}

function expandRange(min: number, max: number): [number, number] {
  if (min === max) {
    const delta = Math.abs(min || 1) * 0.1;
    return [min - delta, max + delta];
  }
  const padding = (max - min) * 0.12;
  return [min - padding, max + padding];
}

function buildScatterLayout(points: ApiCostStructurePoint[], intercept: number, slope: number): ScatterLayout {
  const width = 680;
  const height = 300;
  const padding = { top: 16, right: 18, bottom: 36, left: 56 };

  const xValues = points.map((point) => point.sales);
  const regressionMinX = Math.min(...xValues);
  const regressionMaxX = Math.max(...xValues);
  const regressionYValues = [intercept + slope * regressionMinX, intercept + slope * regressionMaxX];
  const yValues = [...points.map((point) => point.operatingProfit), ...regressionYValues];

  const [xMin, xMax] = expandRange(Math.min(...xValues), Math.max(...xValues));
  const [yMin, yMax] = expandRange(Math.min(...yValues), Math.max(...yValues));

  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;

  const scaleX = (value: number) => padding.left + ((value - xMin) / (xMax - xMin)) * plotWidth;
  const scaleY = (value: number) => padding.top + (1 - (value - yMin) / (yMax - yMin)) * plotHeight;

  const tickCount = 4;
  const xTicks = Array.from({ length: tickCount }, (_, index) => {
    const ratio = index / (tickCount - 1);
    const value = xMin + (xMax - xMin) * ratio;
    return { value, x: scaleX(value) };
  });
  const yTicks = Array.from({ length: tickCount }, (_, index) => {
    const ratio = index / (tickCount - 1);
    const value = yMax - (yMax - yMin) * ratio;
    return { value, y: scaleY(value) };
  });

  return {
    width,
    height,
    padding,
    points: points.map((point) => ({
      point,
      cx: scaleX(point.sales),
      cy: scaleY(point.operatingProfit),
    })),
    line: {
      x1: scaleX(regressionMinX),
      y1: scaleY(intercept + slope * regressionMinX),
      x2: scaleX(regressionMaxX),
      y2: scaleY(intercept + slope * regressionMaxX),
    },
    xTicks,
    yTicks,
  };
}

function SummaryCard({ label, value, subValue }: { label: string; value: string; subValue?: string }) {
  return (
    <div className="rounded-xl border border-border/60 bg-background/30 p-3">
      <div className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">{label}</div>
      <div className="mt-2 text-xl font-semibold text-foreground">{value}</div>
      {subValue && <div className="mt-1 text-xs text-muted-foreground">{subValue}</div>}
    </div>
  );
}

function ScatterChart({ points, intercept, slope }: ScatterChartProps) {
  const layout = buildScatterLayout(points, intercept, slope);
  const latestPoint = points[points.length - 1];

  return (
    <svg
      viewBox={`0 0 ${layout.width} ${layout.height}`}
      className="h-full w-full"
      role="img"
      aria-label="Cost structure scatter plot"
    >
      <rect
        x={layout.padding.left}
        y={layout.padding.top}
        width={layout.width - layout.padding.left - layout.padding.right}
        height={layout.height - layout.padding.top - layout.padding.bottom}
        rx="12"
        className="fill-[color:var(--app-surface-muted)]"
        opacity="0.45"
      />

      {layout.yTicks.map((tick) => (
        <g key={`y-${tick.value.toFixed(3)}`}>
          <line
            x1={layout.padding.left}
            x2={layout.width - layout.padding.right}
            y1={tick.y}
            y2={tick.y}
            stroke="currentColor"
            strokeOpacity="0.12"
            strokeDasharray="4 6"
          />
          <text
            x={layout.padding.left - 10}
            y={tick.y + 4}
            textAnchor="end"
            className="fill-muted-foreground text-[11px]"
          >
            {formatMillions(tick.value)}
          </text>
        </g>
      ))}

      {layout.xTicks.map((tick) => (
        <g key={`x-${tick.value.toFixed(3)}`}>
          <line
            x1={tick.x}
            x2={tick.x}
            y1={layout.padding.top}
            y2={layout.height - layout.padding.bottom}
            stroke="currentColor"
            strokeOpacity="0.1"
            strokeDasharray="4 6"
          />
          <text
            x={tick.x}
            y={layout.height - layout.padding.bottom + 18}
            textAnchor="middle"
            className="fill-muted-foreground text-[11px]"
          >
            {formatMillions(tick.value)}
          </text>
        </g>
      ))}

      <line
        x1={layout.line.x1}
        y1={layout.line.y1}
        x2={layout.line.x2}
        y2={layout.line.y2}
        stroke="#f97316"
        strokeWidth="2.5"
        strokeDasharray="7 6"
      />

      {layout.points.map(({ point, cx, cy }) => {
        const isLatest =
          latestPoint?.disclosedDate === point.disclosedDate &&
          latestPoint?.analysisPeriodType === point.analysisPeriodType;

        return (
          <g key={`${point.disclosedDate}-${point.analysisPeriodType}`}>
            {isLatest && <circle cx={cx} cy={cy} r="9" fill="#f43f5e" fillOpacity="0.2" />}
            <circle
              cx={cx}
              cy={cy}
              r={isLatest ? 5.5 : 4.5}
              fill={isLatest ? '#f43f5e' : point.isDerived ? '#60a5fa' : '#22c55e'}
              stroke="#0f172a"
              strokeWidth="1.5"
            />
          </g>
        );
      })}

      <text
        x={(layout.padding.left + layout.width - layout.padding.right) / 2}
        y={layout.height - 4}
        textAnchor="middle"
        className="fill-muted-foreground text-xs"
      >
        売上
      </text>
      <text
        x="18"
        y={layout.height / 2}
        textAnchor="middle"
        transform={`rotate(-90 18 ${layout.height / 2})`}
        className="fill-muted-foreground text-xs"
      >
        営業利益
      </text>
    </svg>
  );
}

export function CostStructurePanel({ symbol, enabled = true }: CostStructurePanelProps) {
  const [mode, setMode] = useState<CostStructurePanelMode>('recent12');
  const analysisOptions = getAnalysisOptions(mode);
  const { data, isLoading, error } = useCostStructureAnalysis(symbol, {
    enabled,
    ...analysisOptions,
  });

  if (!symbol) {
    return (
      <div className="flex h-full items-center justify-center">
        <p className="text-sm text-muted-foreground">銘柄を選択してください</p>
      </div>
    );
  }

  const normalizedError =
    error instanceof Error ? error : error ? new Error('Failed to load cost structure data') : null;

  return (
    <DataStateWrapper
      isLoading={isLoading}
      error={normalizedError}
      isEmpty={!data}
      emptyMessage="No cost structure data available"
      loadingMessage="Analyzing cost structure..."
      height="h-full"
    >
      {data && <CostStructureContent data={data} mode={mode} onModeChange={setMode} />}
    </DataStateWrapper>
  );
}

interface CostStructureContentProps {
  data: NonNullable<ReturnType<typeof useCostStructureAnalysis>['data']>;
  mode: CostStructurePanelMode;
  onModeChange: (mode: CostStructurePanelMode) => void;
}

function CostStructureContent({ data, mode, onModeChange }: CostStructureContentProps) {
  const warnings = Array.from(new Set([...(data.diagnostics?.warnings ?? []), ...(data.provenance?.warnings ?? [])]));
  const analysisLabel = COST_STRUCTURE_MODE_ITEMS.find((item) => item.value === mode)?.label ?? '12Q';

  return (
    <div className="flex h-full min-h-0 flex-col gap-4">
      <div className="rounded-xl border border-border/60 bg-background/20 px-4 py-3">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <div className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Recommended Analysis</div>
            <div className="mt-1 text-sm text-foreground">Default is recent 12 quarters. Use Same Q or FY only for seasonality checks.</div>
          </div>
          <SegmentedTabs items={COST_STRUCTURE_MODE_ITEMS} value={mode} onChange={onModeChange} />
        </div>
      </div>

      <div className="grid min-h-0 flex-1 gap-4 lg:grid-cols-[minmax(0,1.6fr)_minmax(21rem,1fr)]">
        <div className="min-h-[18rem] rounded-xl border border-border/60 bg-background/20 p-3 lg:min-h-0">
          <ScatterChart points={data.points} intercept={data.regression.intercept} slope={data.regression.slope} />
        </div>

        <div className="flex min-h-0 flex-col gap-4 overflow-auto">
          <div className="grid gap-3 sm:grid-cols-2">
            <SummaryCard
              label="Latest Sales"
              value={formatMillions(data.latestPoint.sales)}
              subValue={data.latestPoint.disclosedDate}
            />
            <SummaryCard
              label="Operating Margin"
              value={formatPercent(data.latestPoint.operatingMargin)}
              subValue={`${data.latestPoint.fiscalYear} ${data.latestPoint.analysisPeriodType}`}
            />
            <SummaryCard label="Variable Cost Ratio" value={formatRatioPercent(data.regression.variableCostRatio)} />
            <SummaryCard label="Fixed Cost" value={formatMillions(data.regression.fixedCost)} />
            <SummaryCard label="Break-Even Sales" value={formatMillions(data.regression.breakEvenSales)} />
            <SummaryCard
              label="R²"
              value={formatRatioPercent(data.regression.rSquared)}
              subValue={`${data.regression.sampleCount} samples`}
            />
          </div>

          <div className="rounded-xl border border-border/60 bg-background/20 px-4 py-3 text-sm text-muted-foreground">
            <div>
              Analysis View: <span className="font-medium text-foreground">{analysisLabel}</span>
            </div>
            <div className="mt-1">
              Analysis Window:{' '}
              <span className="font-medium text-foreground">
                {data.dateRange.from} ~ {data.dateRange.to}
              </span>
            </div>
            <div className="mt-1">
              Latest Point:{' '}
              <span className="font-medium text-foreground">
                {data.latestPoint.analysisPeriodType} / {data.latestPoint.disclosedDate}
              </span>
            </div>
          </div>

          {warnings.length > 0 && (
            <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-800">
              <div className="flex items-start gap-2">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                <p>{warnings.join(' | ')}</p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
