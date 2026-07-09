import { ArrowDown, ArrowUp, ArrowUpDown, TrendingUp } from 'lucide-react';
import { type ReactNode, type UIEvent, useCallback, useEffect, useRef, useState } from 'react';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { useVirtualizedRows } from '@/hooks/useVirtualizedRows';
import { cn } from '@/lib/utils';
import {
  DAILY_RANKING_VALUE_METRICS,
  type DailyRankingMetric,
  type DailyRankingMetricKey,
  DailyRankingMetricValue,
  DailyRankingRegimeChip,
  DailyRankingSignalChips,
  formatDailyRankingTradingValue,
  SectorStrengthScoreChip,
} from './dailyRankingPresentation';
import type { EquityRiskFlag, EquityTechnicalFlag } from './rankingState';

export const EQUITY_SORT_FIELDS = [
  'tradingValue',
  'changePercentage',
  'code',
  'currentPrice',
  'sma5AboveCount5d',
  'sectorStrengthScore',
  'per',
  'forwardPer',
  'forecastOperatingProfitGrowthRatio',
  'psr',
  'forwardPsr',
  'pbr',
  'valueCompositeScore',
  'marketCap',
  'liquidityResidualZ',
  'adv60ToFreeFloatPct',
] as const;

export type EquitySortField = (typeof EQUITY_SORT_FIELDS)[number];
export type EquitySortOrder = 'asc' | 'desc';
export type EquityRankingLabels = Record<
  'code' | 'market' | 'company' | 'sector' | 'sectorScore' | 'price' | 'marketCap' | 'tradingValue' | 'change',
  string
>;

export interface EquityRankingItem {
  rank: number;
  code: string;
  companyName: string;
  marketCode: string;
  sector33Name: string;
  sectorStrengthScore?: number | null;
  sectorStrengthBucket?: 'sector_strong' | 'sector_neutral' | 'sector_weak' | null;
  currentPrice: number;
  volume: number;
  tradingValue?: number | null;
  tradingValueAverage?: number | null;
  changePercentage?: number | null;
  sma5AboveCount5d?: number | null;
  per?: number | null;
  perPercentile?: number | null;
  forwardPer?: number | null;
  forwardPerPercentile?: number | null;
  pOp?: number | null;
  forwardPOp?: number | null;
  forwardPOpPercentile?: number | null;
  forecastOperatingProfitGrowthRatio?: number | null;
  forecastOperatingProfitGrowthPct?: number | null;
  psr?: number | null;
  psrPercentile?: number | null;
  forwardPsr?: number | null;
  forwardPsrPercentile?: number | null;
  pbr?: number | null;
  pbrPercentile?: number | null;
  valueCompositeScore?: number | null;
  overvaluationCompositeScore?: number | null;
  marketCap?: number | null;
  liquidityResidualZ?: number | null;
  liquidityRegime?:
    | 'neutral_rerating'
    | 'crowded_rerating'
    | 'distribution_stress'
    | 'stale_liquidity'
    | 'neutral'
    | null;
  adv60ToFreeFloatPct?: number | null;
  riskFlags?: EquityRiskFlag[];
  technicalFlags?: EquityTechnicalFlag[];
}

interface EquityRankingTableProps<T extends EquityRankingItem> {
  items: T[];
  isLoading: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
  showChange?: boolean;
  showMarket?: boolean;
  showValuation?: boolean;
  showLiquidity?: boolean;
  emptyMessage?: string;
  emptySubMessage?: string;
  formatLargeValue?: (value: number | null | undefined) => string;
  labels?: Partial<EquityRankingLabels>;
  sortState?: {
    field: EquitySortField;
    order: EquitySortOrder;
    onSort: (field: EquitySortField) => void;
  };
  scrollRestorationKey?: string;
}

const DAILY_RANKING_METRICS_BY_KEY = Object.fromEntries(
  DAILY_RANKING_VALUE_METRICS.map((metric) => [metric.key, metric])
) as Record<DailyRankingMetricKey, DailyRankingMetric>;

const VIRTUALIZATION_THRESHOLD = 120;
const ROW_HEIGHT = 52;
const CARD_ROW_HEIGHT = 160;
const VIEWPORT_HEIGHT = 520;
const DEFAULT_EQUITY_RANKING_LABELS: EquityRankingLabels = {
  code: 'コード',
  market: '市場',
  company: '銘柄名',
  sector: '業種',
  sectorScore: DAILY_RANKING_METRICS_BY_KEY.sectorStrengthScore.label,
  price: DAILY_RANKING_METRICS_BY_KEY.currentPrice.label,
  marketCap: '時価総額',
  tradingValue: DAILY_RANKING_METRICS_BY_KEY.tradingValue.label,
  change: DAILY_RANKING_METRICS_BY_KEY.changePercentage.label,
};

function getIsMobileLayout(): boolean {
  return (
    typeof window !== 'undefined' &&
    typeof window.matchMedia === 'function' &&
    window.matchMedia('(max-width: 1023px)').matches
  );
}

function useIsMobileLayout(): boolean {
  const [isMobileLayout, setIsMobileLayout] = useState(getIsMobileLayout);

  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return;
    const mediaQuery = window.matchMedia('(max-width: 1023px)');
    const updateLayout = () => setIsMobileLayout(mediaQuery.matches);
    updateLayout();
    mediaQuery.addEventListener('change', updateLayout);
    return () => mediaQuery.removeEventListener('change', updateLayout);
  }, []);

  return isMobileLayout;
}

function readStoredScrollTop(key: string): number | null {
  if (typeof window === 'undefined') return null;
  const value = window.sessionStorage.getItem(key);
  if (value == null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function writeStoredScrollTop(key: string, scrollTop: number) {
  if (typeof window === 'undefined') return;
  window.sessionStorage.setItem(key, String(Math.max(0, Math.round(scrollTop))));
}

function SortHeader({
  field,
  sortState,
  align = 'left',
  children,
}: {
  field: EquitySortField;
  sortState?: EquityRankingTableProps<EquityRankingItem>['sortState'];
  align?: 'left' | 'right';
  children: ReactNode;
}) {
  if (!sortState) {
    return <span>{children}</span>;
  }
  const isActive = sortState.field === field;
  const Icon = isActive ? (sortState.order === 'asc' ? ArrowUp : ArrowDown) : ArrowUpDown;
  return (
    <button
      type="button"
      onClick={() => sortState.onSort(field)}
      className={cn(
        'inline-flex items-center gap-1 font-semibold transition-colors hover:text-foreground',
        align === 'right' && 'justify-end'
      )}
    >
      {children}
      <Icon className={cn('h-3 w-3', isActive ? 'text-primary' : 'text-muted-foreground')} />
    </button>
  );
}

function VirtualSpacer({ height }: { height: number }) {
  if (height <= 0) return null;
  return <div aria-hidden="true" className="shrink-0" style={{ height }} />;
}

function EvidenceColorLegend() {
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-border/40 px-3 py-2 text-[11px] text-muted-foreground">
      <span>Prime 20d excess evidence</span>
      <span className="font-medium text-green-600 dark:text-green-400">strong</span>
      <span className="font-medium text-sky-600 dark:text-sky-400">good</span>
      <span className="font-medium text-cyan-600 dark:text-cyan-400">light</span>
      <span className="font-medium text-yellow-600 dark:text-yellow-400">caution</span>
      <span className="font-medium text-red-600 dark:text-red-400">weak/tail</span>
    </div>
  );
}

function EquityCard<T extends EquityRankingItem>({
  item,
  rowNumber,
  onStockClick,
  showChange,
  showValuation,
  showLiquidity,
  showSectorStrength,
  formatLargeValue,
  labels,
}: {
  item: T;
  rowNumber: number;
  onStockClick: (code: string) => void;
  showChange: boolean;
  showValuation: boolean;
  showLiquidity: boolean;
  showSectorStrength: boolean;
  formatLargeValue: (value: number | null | undefined) => string;
  labels: EquityRankingLabels;
}) {
  return (
    <button
      type="button"
      onClick={() => onStockClick(item.code)}
      className="min-h-[7.5rem] w-full rounded-lg border border-border/60 bg-background/80 p-3 text-left shadow-sm transition-colors hover:bg-[var(--app-surface-muted)]"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <span className="rounded-full bg-[var(--app-surface-muted)] px-2 py-0.5 text-[11px] font-semibold tabular-nums text-muted-foreground">
              {rowNumber}
            </span>
            <span className="font-mono text-sm font-semibold text-primary">{item.code}</span>
          </div>
          <p className="mt-1 truncate text-sm font-semibold text-foreground">{item.companyName}</p>
          <div className="mt-0.5 flex items-center gap-2">
            <p className="truncate text-xs text-muted-foreground">{item.sector33Name}</p>
            {showSectorStrength ? <SectorStrengthScoreChip value={item.sectorStrengthScore} /> : null}
          </div>
        </div>
        {showChange ? (
          <span className="shrink-0 text-sm font-semibold tabular-nums">
            <DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.changePercentage} />
          </span>
        ) : null}
      </div>
      <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
        <Metric
          label={labels.price}
          value={<DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.currentPrice} />}
        />
        <Metric
          label={showChange ? labels.change : labels.tradingValue}
          value={
            showChange ? (
              <DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.changePercentage} />
            ) : (
              formatLargeValue(item.tradingValue ?? item.tradingValueAverage)
            )
          }
        />
        {showValuation ? (
          <>
            <Metric
              label={DAILY_RANKING_METRICS_BY_KEY.sma5AboveCount5d.label}
              value={<DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.sma5AboveCount5d} />}
            />
            <Metric
              label={DAILY_RANKING_METRICS_BY_KEY.per.label}
              value={<DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.per} />}
            />
            <Metric
              label={DAILY_RANKING_METRICS_BY_KEY.forwardPer.label}
              value={<DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.forwardPer} />}
            />
            <Metric
              label={DAILY_RANKING_METRICS_BY_KEY.forecastOperatingProfitGrowthRatio.label}
              value={
                <DailyRankingMetricValue
                  item={item}
                  metric={DAILY_RANKING_METRICS_BY_KEY.forecastOperatingProfitGrowthRatio}
                />
              }
            />
            <Metric
              label={DAILY_RANKING_METRICS_BY_KEY.psr.label}
              value={<DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.psr} />}
            />
            <Metric
              label={DAILY_RANKING_METRICS_BY_KEY.forwardPsr.label}
              value={<DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.forwardPsr} />}
            />
            <Metric
              label={DAILY_RANKING_METRICS_BY_KEY.pbr.label}
              value={<DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.pbr} />}
            />
            <Metric
              label={DAILY_RANKING_METRICS_BY_KEY.valueCompositeScore.label}
              value={<DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.valueCompositeScore} />}
            />
          </>
        ) : null}
        {showLiquidity ? (
          <Metric
            label={DAILY_RANKING_METRICS_BY_KEY.liquidityResidualZ.label}
            value={<DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.liquidityResidualZ} />}
          />
        ) : null}
      </div>
    </button>
  );
}

function Metric({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="rounded-lg bg-[var(--app-surface-muted)] px-2.5 py-2">
      <p className="text-[10px] font-semibold uppercase text-muted-foreground">{label}</p>
      <p className="mt-0.5 font-semibold tabular-nums text-foreground">{value}</p>
    </div>
  );
}

function EquityCardList<T extends EquityRankingItem>({
  items,
  onStockClick,
  showChange,
  showValuation,
  showLiquidity,
  showSectorStrength,
  formatLargeValue,
  labels,
  paddingTop,
  paddingBottom,
  startIndex,
  shouldVirtualize,
}: {
  items: T[];
  onStockClick: (code: string) => void;
  showChange: boolean;
  showValuation: boolean;
  showLiquidity: boolean;
  showSectorStrength: boolean;
  formatLargeValue: (value: number | null | undefined) => string;
  labels: EquityRankingLabels;
  paddingTop: number;
  paddingBottom: number;
  startIndex: number;
  shouldVirtualize: boolean;
}) {
  return (
    <div className="flex flex-col gap-2 p-3">
      {shouldVirtualize ? <VirtualSpacer height={paddingTop} /> : null}
      {items.map((item, index) => (
        <EquityCard
          key={`${item.code}-${item.rank}`}
          item={item}
          rowNumber={startIndex + index + 1}
          onStockClick={onStockClick}
          showChange={showChange}
          showValuation={showValuation}
          showLiquidity={showLiquidity}
          showSectorStrength={showSectorStrength}
          formatLargeValue={formatLargeValue}
          labels={labels}
        />
      ))}
      {shouldVirtualize ? <VirtualSpacer height={paddingBottom} /> : null}
    </div>
  );
}

function DesktopEquityTable<T extends EquityRankingItem>({
  items,
  onStockClick,
  showChange,
  showMarket,
  showValuation,
  showLiquidity,
  showSectorStrength,
  formatLargeValue,
  labels,
  sortState,
  columnCount,
  paddingTop,
  paddingBottom,
  startIndex,
  shouldVirtualize,
}: {
  items: T[];
  onStockClick: (code: string) => void;
  showChange: boolean;
  showMarket: boolean;
  showValuation: boolean;
  showLiquidity: boolean;
  showSectorStrength: boolean;
  formatLargeValue: (value: number | null | undefined) => string;
  labels: EquityRankingLabels;
  sortState?: EquityRankingTableProps<T>['sortState'];
  columnCount: number;
  paddingTop: number;
  paddingBottom: number;
  startIndex: number;
  shouldVirtualize: boolean;
}) {
  return (
    <table className="w-full text-xs">
      <DesktopEquityHeader
        showChange={showChange}
        showMarket={showMarket}
        showValuation={showValuation}
        showLiquidity={showLiquidity}
        showSectorStrength={showSectorStrength}
        labels={labels}
        sortState={sortState}
      />
      <tbody>
        {shouldVirtualize && paddingTop > 0 ? (
          <tr>
            <td colSpan={columnCount} className="p-0" style={{ height: paddingTop }} />
          </tr>
        ) : null}
        {items.map((item, index) => (
          <DesktopEquityRow
            key={`${item.code}-${item.rank}`}
            item={item}
            rowNumber={startIndex + index + 1}
            onStockClick={onStockClick}
            showChange={showChange}
            showMarket={showMarket}
            showValuation={showValuation}
            showLiquidity={showLiquidity}
            showSectorStrength={showSectorStrength}
            formatLargeValue={formatLargeValue}
          />
        ))}
        {shouldVirtualize && paddingBottom > 0 ? (
          <tr>
            <td colSpan={columnCount} className="p-0" style={{ height: paddingBottom }} />
          </tr>
        ) : null}
      </tbody>
    </table>
  );
}

function DesktopEquityHeader<T extends EquityRankingItem>({
  showChange,
  showMarket,
  showValuation,
  showLiquidity,
  showSectorStrength,
  labels,
  sortState,
}: {
  showChange: boolean;
  showMarket: boolean;
  showValuation: boolean;
  showLiquidity: boolean;
  showSectorStrength: boolean;
  labels: EquityRankingLabels;
  sortState?: EquityRankingTableProps<T>['sortState'];
}) {
  return (
    <thead className="sticky top-0 z-10 border-b bg-[var(--app-surface-muted)]">
      <tr>
        <th className="w-12 px-2 py-1.5 text-center">行</th>
        <th className="w-20 px-2 py-1.5 text-left">
          <SortHeader field="code" sortState={sortState}>
            {labels.code}
          </SortHeader>
        </th>
        {showMarket ? <th className="w-16 px-2 py-1.5 text-center">{labels.market}</th> : null}
        <th className="w-32 max-w-32 px-2 py-1.5 text-left">{labels.company}</th>
        <th className="w-24 px-2 py-1.5 text-left">{labels.sector}</th>
        {showSectorStrength ? (
          <th className="w-20 px-2 py-1.5 text-right">
            <SortHeader field="sectorStrengthScore" sortState={sortState} align="right">
              {labels.sectorScore}
            </SortHeader>
          </th>
        ) : null}
        <th className="w-24 px-2 py-1.5 text-right">
          <SortHeader field="currentPrice" sortState={sortState} align="right">
            {labels.price}
          </SortHeader>
        </th>
        {showValuation ? <ValuationHeaders sortState={sortState} /> : null}
        {showLiquidity ? <LiquidityHeaders sortState={sortState} /> : null}
        <th className="w-28 px-2 py-1.5 text-right">
          <SortHeader field="tradingValue" sortState={sortState} align="right">
            {labels.tradingValue}
          </SortHeader>
        </th>
        {showValuation ? (
          <th className="w-28 px-2 py-1.5 text-right">
            <SortHeader field="marketCap" sortState={sortState} align="right">
              {labels.marketCap}
            </SortHeader>
          </th>
        ) : null}
        {showChange ? (
          <th className="w-24 px-2 py-1.5 text-right">
            <SortHeader field="changePercentage" sortState={sortState} align="right">
              {labels.change}
            </SortHeader>
          </th>
        ) : null}
      </tr>
    </thead>
  );
}

function LiquidityHeaders<T extends EquityRankingItem>({
  sortState,
}: {
  sortState?: EquityRankingTableProps<T>['sortState'];
}) {
  return (
    <>
      <th className="w-20 px-2 py-1.5 text-right">
        <SortHeader field="liquidityResidualZ" sortState={sortState} align="right">
          {DAILY_RANKING_METRICS_BY_KEY.liquidityResidualZ.label}
        </SortHeader>
      </th>
      <th className="w-28 px-2 py-1.5 text-center">Regime</th>
      <th className="w-40 px-2 py-1.5 text-center">Signals</th>
    </>
  );
}

function ValuationHeaders<T extends EquityRankingItem>({
  sortState,
}: {
  sortState?: EquityRankingTableProps<T>['sortState'];
}) {
  return (
    <>
      <th className="w-20 px-2 py-1.5 text-right">
        <SortHeader field="sma5AboveCount5d" sortState={sortState} align="right">
          {DAILY_RANKING_METRICS_BY_KEY.sma5AboveCount5d.label}
        </SortHeader>
      </th>
      <th className="w-20 px-2 py-1.5 text-right">
        <SortHeader field="per" sortState={sortState} align="right">
          {DAILY_RANKING_METRICS_BY_KEY.per.label}
        </SortHeader>
      </th>
      <th className="w-24 px-2 py-1.5 text-right">
        <SortHeader field="forwardPer" sortState={sortState} align="right">
          {DAILY_RANKING_METRICS_BY_KEY.forwardPer.label}
        </SortHeader>
      </th>
      <th
        className="w-24 px-2 py-1.5 text-right"
        title={DAILY_RANKING_METRICS_BY_KEY.forecastOperatingProfitGrowthRatio.title}
      >
        <SortHeader field="forecastOperatingProfitGrowthRatio" sortState={sortState} align="right">
          {DAILY_RANKING_METRICS_BY_KEY.forecastOperatingProfitGrowthRatio.label}
        </SortHeader>
      </th>
      <th className="w-20 px-2 py-1.5 text-right">
        <SortHeader field="psr" sortState={sortState} align="right">
          {DAILY_RANKING_METRICS_BY_KEY.psr.label}
        </SortHeader>
      </th>
      <th className="w-24 px-2 py-1.5 text-right">
        <SortHeader field="forwardPsr" sortState={sortState} align="right">
          {DAILY_RANKING_METRICS_BY_KEY.forwardPsr.label}
        </SortHeader>
      </th>
      <th className="w-20 px-2 py-1.5 text-right">
        <SortHeader field="pbr" sortState={sortState} align="right">
          {DAILY_RANKING_METRICS_BY_KEY.pbr.label}
        </SortHeader>
      </th>
      <th className="w-24 px-2 py-1.5 text-right" title={DAILY_RANKING_METRICS_BY_KEY.valueCompositeScore.title}>
        <SortHeader field="valueCompositeScore" sortState={sortState} align="right">
          {DAILY_RANKING_METRICS_BY_KEY.valueCompositeScore.label}
        </SortHeader>
      </th>
    </>
  );
}

function DesktopEquityRow<T extends EquityRankingItem>({
  item,
  rowNumber,
  onStockClick,
  showChange,
  showMarket,
  showValuation,
  showLiquidity,
  showSectorStrength,
  formatLargeValue,
}: {
  item: T;
  rowNumber: number;
  onStockClick: (code: string) => void;
  showChange: boolean;
  showMarket: boolean;
  showValuation: boolean;
  showLiquidity: boolean;
  showSectorStrength: boolean;
  formatLargeValue: (value: number | null | undefined) => string;
}) {
  return (
    <tr
      className="cursor-pointer border-b border-border/30 transition-colors hover:bg-[var(--app-surface-muted)]"
      onClick={() => onStockClick(item.code)}
    >
      <td className="px-2 py-1.5 text-center font-medium tabular-nums text-muted-foreground">{rowNumber}</td>
      <td className="px-2 py-1.5 font-medium">{item.code}</td>
      {showMarket ? <td className="px-2 py-1.5 text-center text-muted-foreground">{item.marketCode}</td> : null}
      <td className="w-32 max-w-32 px-2 py-1.5">
        <span className="block w-32 truncate">{item.companyName}</span>
      </td>
      <td className="max-w-[120px] truncate px-2 py-1.5 text-muted-foreground">{item.sector33Name}</td>
      {showSectorStrength ? (
        <td className="px-2 py-1.5 text-right">
          <SectorStrengthScoreChip value={item.sectorStrengthScore} />
        </td>
      ) : null}
      <td className="px-2 py-1.5 text-right tabular-nums">
        <DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.currentPrice} />
      </td>
      {showValuation ? (
        <>
          <td className="px-2 py-1.5 text-right tabular-nums">
            <DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.sma5AboveCount5d} />
          </td>
          <td className="px-2 py-1.5 text-right tabular-nums">
            <DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.per} />
          </td>
          <td className="px-2 py-1.5 text-right font-medium tabular-nums">
            <DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.forwardPer} />
          </td>
          <td
            className="px-2 py-1.5 text-right tabular-nums"
            title={DAILY_RANKING_METRICS_BY_KEY.forecastOperatingProfitGrowthRatio.title}
          >
            <DailyRankingMetricValue
              item={item}
              metric={DAILY_RANKING_METRICS_BY_KEY.forecastOperatingProfitGrowthRatio}
            />
          </td>
          <td className="px-2 py-1.5 text-right tabular-nums">
            <DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.psr} />
          </td>
          <td className="px-2 py-1.5 text-right tabular-nums">
            <DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.forwardPsr} />
          </td>
          <td className="px-2 py-1.5 text-right tabular-nums">
            <DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.pbr} />
          </td>
          <td
            className="px-2 py-1.5 text-right font-medium tabular-nums"
            title={DAILY_RANKING_METRICS_BY_KEY.valueCompositeScore.title}
          >
            <DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.valueCompositeScore} />
          </td>
        </>
      ) : null}
      {showLiquidity ? (
        <>
          <td className="px-2 py-1.5 text-right tabular-nums">
            <DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.liquidityResidualZ} />
          </td>
          <td className="px-2 py-1.5 text-center">
            <DailyRankingRegimeChip item={item} />
          </td>
          <td className="px-2 py-1.5 text-center">
            <DailyRankingSignalChips item={item} />
          </td>
        </>
      ) : null}
      <td className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">
        {formatLargeValue(item.tradingValue ?? item.tradingValueAverage)}
      </td>
      {showValuation ? (
        <td className="px-2 py-1.5 text-right tabular-nums">{formatLargeValue(item.marketCap)}</td>
      ) : null}
      {showChange ? (
        <td className="px-2 py-1.5 text-right font-medium tabular-nums">
          <DailyRankingMetricValue item={item} metric={DAILY_RANKING_METRICS_BY_KEY.changePercentage} />
        </td>
      ) : null}
    </tr>
  );
}

export function EquityRankingTable<T extends EquityRankingItem>({
  items,
  isLoading,
  error,
  onStockClick,
  showChange = false,
  showMarket = false,
  showValuation = false,
  showLiquidity = false,
  emptyMessage = 'No ranking data available',
  emptySubMessage = 'Try a different date or market',
  formatLargeValue = formatDailyRankingTradingValue,
  labels,
  sortState,
  scrollRestorationKey,
}: EquityRankingTableProps<T>) {
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const restoredScrollKeyRef = useRef<string | null>(null);
  const isMobileLayout = useIsMobileLayout();
  const shouldVirtualize = items.length >= VIRTUALIZATION_THRESHOLD;
  const resolvedLabels = { ...DEFAULT_EQUITY_RANKING_LABELS, ...labels };
  const showSectorStrength = items.some((item) => item.sectorStrengthScore != null);
  const virtual = useVirtualizedRows(items, {
    enabled: shouldVirtualize,
    rowHeight: isMobileLayout ? CARD_ROW_HEIGHT : ROW_HEIGHT,
    viewportHeight: VIEWPORT_HEIGHT,
  });
  const columnCount =
    6 +
    (showChange ? 1 : 0) +
    (showMarket ? 1 : 0) +
    (showSectorStrength ? 1 : 0) +
    (showValuation ? 8 : 0) +
    (showLiquidity ? 4 : 0);
  const handleScroll = useCallback(
    (event: UIEvent<HTMLDivElement>) => {
      virtual.onScroll?.(event);
      if (scrollRestorationKey) {
        writeStoredScrollTop(scrollRestorationKey, event.currentTarget.scrollTop);
      }
    },
    [scrollRestorationKey, virtual.onScroll]
  );

  useEffect(() => {
    if (!scrollRestorationKey || restoredScrollKeyRef.current === scrollRestorationKey || items.length === 0) return;
    const storedScrollTop = readStoredScrollTop(scrollRestorationKey);
    const scrollTop = storedScrollTop ?? 0;
    restoredScrollKeyRef.current = scrollRestorationKey;

    const scrollContainer = scrollContainerRef.current;
    if (!scrollContainer) return;

    const restoreScrollTop = () => {
      scrollContainer.scrollTop = scrollTop;
      virtual.setScrollTop(scrollTop);
    };
    restoreScrollTop();
    if (storedScrollTop == null) return;
    if (typeof window === 'undefined' || typeof window.requestAnimationFrame !== 'function') return;
    const animationFrame = window.requestAnimationFrame(restoreScrollTop);
    return () => window.cancelAnimationFrame(animationFrame);
  }, [items.length, scrollRestorationKey, virtual.setScrollTop]);

  return (
    <div ref={scrollContainerRef} className="min-h-0 flex-1 overflow-auto" onScroll={handleScroll}>
      {showValuation || showLiquidity ? <EvidenceColorLegend /> : null}
      <DataStateWrapper
        isLoading={isLoading}
        error={error}
        isEmpty={items.length === 0}
        emptyMessage={emptyMessage}
        emptySubMessage={emptySubMessage}
        emptyIcon={<TrendingUp className="h-8 w-8" />}
        height="h-full min-h-[18rem]"
      >
        {isMobileLayout ? (
          <EquityCardList
            items={virtual.visibleItems}
            onStockClick={onStockClick}
            showChange={showChange}
            showValuation={showValuation}
            showLiquidity={showLiquidity}
            showSectorStrength={showSectorStrength}
            formatLargeValue={formatLargeValue}
            labels={resolvedLabels}
            paddingTop={virtual.paddingTop}
            paddingBottom={virtual.paddingBottom}
            startIndex={virtual.startIndex}
            shouldVirtualize={shouldVirtualize}
          />
        ) : (
          <DesktopEquityTable
            items={virtual.visibleItems}
            onStockClick={onStockClick}
            showChange={showChange}
            showMarket={showMarket}
            showValuation={showValuation}
            showLiquidity={showLiquidity}
            showSectorStrength={showSectorStrength}
            formatLargeValue={formatLargeValue}
            labels={resolvedLabels}
            sortState={sortState}
            columnCount={columnCount}
            paddingTop={virtual.paddingTop}
            paddingBottom={virtual.paddingBottom}
            startIndex={virtual.startIndex}
            shouldVirtualize={shouldVirtualize}
          />
        )}
      </DataStateWrapper>
    </div>
  );
}
