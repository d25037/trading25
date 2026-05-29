import { ArrowDown, ArrowUp, ArrowUpDown, TrendingUp } from 'lucide-react';
import type { ReactNode } from 'react';
import { useEffect, useState } from 'react';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { useVirtualizedRows } from '@/hooks/useVirtualizedRows';
import { cn } from '@/lib/utils';
import { formatPriceJPY, formatTradingValue } from '@/utils/formatters';
import {
  type EvidenceColorTier,
  getCheapValuationPercentileTier,
  getForwardPerEvidenceTier,
  getForwardPOpEvidenceTier,
  getLiquidityEvidenceTier,
  getPerEvidenceTier,
} from './rankingEvidenceTiers';
import { type EquityRiskFlag, formatRiskFlag } from './rankingState';

export type EquitySortField =
  | 'tradingValue'
  | 'changePercentage'
  | 'code'
  | 'currentPrice'
  | 'sectorStrengthScore'
  | 'per'
  | 'forwardPer'
  | 'forwardPOp'
  | 'pbr'
  | 'marketCap'
  | 'liquidityResidualZ'
  | 'adv60ToFreeFloatPct';
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
  per?: number | null;
  perPercentile?: number | null;
  forwardPer?: number | null;
  forwardPerPercentile?: number | null;
  pOp?: number | null;
  forwardPOp?: number | null;
  forwardPOpPercentile?: number | null;
  pbr?: number | null;
  pbrPercentile?: number | null;
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
}

const VIRTUALIZATION_THRESHOLD = 120;
const ROW_HEIGHT = 36;
const CARD_ROW_HEIGHT = 160;
const VIEWPORT_HEIGHT = 520;
const DEFAULT_EQUITY_RANKING_LABELS: EquityRankingLabels = {
  code: 'コード',
  market: '市場',
  company: '銘柄名',
  sector: '業種',
  sectorScore: 'Sector Score',
  price: '現在値',
  marketCap: '時価総額',
  tradingValue: '売買代金',
  change: '騰落率',
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

function formatNullableTradingValue(value: number | null | undefined): string {
  return value == null ? '-' : formatTradingValue(value);
}

function formatRatio(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toFixed(2)}x`;
}

function formatChangePercentage(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}%`;
}

function formatSignedNumber(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}`;
}

function formatSectorStrengthScore(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return value.toFixed(2);
}

function getSectorStrengthScoreClass(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return 'text-muted-foreground';
  if (value >= 0.8) return 'bg-green-50 text-green-700 dark:bg-green-950/40 dark:text-green-300';
  if (value <= 0.2) return 'bg-amber-50 text-amber-700 dark:bg-amber-950/40 dark:text-amber-300';
  return 'bg-[var(--app-surface-muted)] text-muted-foreground';
}

function SectorStrengthScoreChip({ value }: { value: number | null | undefined }) {
  return (
    <span
      className={cn(
        'inline-flex min-w-[3rem] justify-center rounded px-1.5 py-0.5 text-[10px] font-semibold tabular-nums',
        getSectorStrengthScoreClass(value)
      )}
    >
      {formatSectorStrengthScore(value)}
    </span>
  );
}

function formatLiquidityRegime(value: EquityRankingItem['liquidityRegime']): string {
  if (value === 'neutral_rerating') return 'Neutral Rerating';
  if (value === 'crowded_rerating') return 'Crowded Rerating';
  if (value === 'distribution_stress') return 'Stress';
  if (value === 'stale_liquidity') return 'Stale';
  if (value === 'neutral') return 'Neutral';
  return '-';
}

function getEvidenceTierChipClass(tier: EvidenceColorTier): string {
  if (tier === 'excellent') return 'bg-green-50 text-green-700 dark:bg-green-950/40 dark:text-green-300';
  if (tier === 'good') return 'bg-sky-50 text-sky-700 dark:bg-sky-950/40 dark:text-sky-300';
  if (tier === 'light_good') return 'bg-cyan-50 text-cyan-700 dark:bg-cyan-950/40 dark:text-cyan-300';
  if (tier === 'bad') return 'bg-yellow-50 text-yellow-800 dark:bg-yellow-950/40 dark:text-yellow-300';
  if (tier === 'very_bad') return 'bg-red-50 text-red-700 dark:bg-red-950/40 dark:text-red-300';
  return 'bg-[var(--app-surface-muted)] text-muted-foreground';
}

function getRiskFlagClass(value: EquityRiskFlag): string {
  if (value === 'overheat') return 'bg-purple-50 text-purple-700 dark:bg-purple-950/40 dark:text-purple-300';
  if (value === 'stale_rally_fade') return 'bg-red-50 text-red-700 dark:bg-red-950/40 dark:text-red-300';
  return 'bg-[var(--app-surface-muted)] text-muted-foreground';
}

function LiquidityStateChips({ item }: { item: EquityRankingItem }) {
  return (
    <div className="flex flex-wrap justify-center gap-1">
      <span
        className={cn(
          'inline-flex min-w-[4.5rem] justify-center rounded px-1.5 py-0.5 text-[10px] font-semibold',
          getEvidenceTierChipClass(getLiquidityEvidenceTier(item))
        )}
      >
        {formatLiquidityRegime(item.liquidityRegime)}
      </span>
      {item.riskFlags?.map((flag) => (
        <span
          key={flag}
          className={cn(
            'inline-flex min-w-[4.5rem] justify-center rounded px-1.5 py-0.5 text-[10px] font-semibold',
            getRiskFlagClass(flag)
          )}
        >
          {formatRiskFlag(flag)}
        </span>
      ))}
    </div>
  );
}

function getEvidenceTierClass(tier: EvidenceColorTier): string | undefined {
  if (tier === 'excellent') return 'text-green-600 dark:text-green-400';
  if (tier === 'good') return 'text-sky-600 dark:text-sky-400';
  if (tier === 'light_good') return 'text-cyan-600 dark:text-cyan-400';
  if (tier === 'bad') return 'text-yellow-600 dark:text-yellow-400';
  if (tier === 'very_bad') return 'text-red-600 dark:text-red-400';
  return undefined;
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
  const isPositive = (item.changePercentage ?? 0) >= 0;
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
          <span
            className={cn(
              'shrink-0 text-sm font-semibold tabular-nums',
              isPositive ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'
            )}
          >
            {formatChangePercentage(item.changePercentage)}
          </span>
        ) : null}
      </div>
      <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
        <Metric label={labels.price} value={formatPriceJPY(item.currentPrice)} />
        <Metric
          label={showChange ? labels.change : labels.tradingValue}
          value={
            showChange
              ? formatChangePercentage(item.changePercentage)
              : formatLargeValue(item.tradingValue ?? item.tradingValueAverage)
          }
        />
        {showValuation ? (
          <>
            <Metric
              label="PER"
              value={formatRatio(item.per)}
              valueClassName={getEvidenceTierClass(getPerEvidenceTier(item.perPercentile))}
            />
            <Metric
              label="Fwd PER"
              value={formatRatio(item.forwardPer)}
              valueClassName={getEvidenceTierClass(getForwardPerEvidenceTier(item))}
            />
            <Metric
              label="Fwd P/OP"
              value={formatRatio(item.forwardPOp)}
              valueClassName={getEvidenceTierClass(
                getForwardPOpEvidenceTier(
                  item.forwardPOpPercentile,
                  item.forwardPerPercentile,
                  item.perPercentile,
                  item.forwardPOp,
                  item.per
                )
              )}
            />
            <Metric
              label="PBR"
              value={formatRatio(item.pbr)}
              valueClassName={getEvidenceTierClass(getCheapValuationPercentileTier(item.pbrPercentile))}
            />
          </>
        ) : null}
        {showLiquidity ? (
          <Metric
            label="流動性Z"
            value={formatSignedNumber(item.liquidityResidualZ)}
            valueClassName={getEvidenceTierClass(getLiquidityEvidenceTier(item))}
          />
        ) : null}
      </div>
    </button>
  );
}

function Metric({ label, value, valueClassName }: { label: string; value: string; valueClassName?: string }) {
  return (
    <div className="rounded-lg bg-[var(--app-surface-muted)] px-2.5 py-2">
      <p className="text-[10px] font-semibold uppercase text-muted-foreground">{label}</p>
      <p className={cn('mt-0.5 font-semibold tabular-nums text-foreground', valueClassName)}>{value}</p>
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
        <th className="px-2 py-1.5 text-left">{labels.company}</th>
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
          流動性Z
        </SortHeader>
      </th>
      <th className="w-24 px-2 py-1.5 text-center">状態</th>
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
        <SortHeader field="per" sortState={sortState} align="right">
          PER
        </SortHeader>
      </th>
      <th className="w-24 px-2 py-1.5 text-right">
        <SortHeader field="forwardPer" sortState={sortState} align="right">
          Fwd PER
        </SortHeader>
      </th>
      <th className="w-24 px-2 py-1.5 text-right">
        <SortHeader field="forwardPOp" sortState={sortState} align="right">
          Fwd P/OP
        </SortHeader>
      </th>
      <th className="w-20 px-2 py-1.5 text-right">
        <SortHeader field="pbr" sortState={sortState} align="right">
          PBR
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
  const isPositive = (item.changePercentage ?? 0) >= 0;
  return (
    <tr
      className="cursor-pointer border-b border-border/30 transition-colors hover:bg-[var(--app-surface-muted)]"
      onClick={() => onStockClick(item.code)}
    >
      <td className="px-2 py-1.5 text-center font-medium tabular-nums text-muted-foreground">{rowNumber}</td>
      <td className="px-2 py-1.5 font-medium">{item.code}</td>
      {showMarket ? <td className="px-2 py-1.5 text-center text-muted-foreground">{item.marketCode}</td> : null}
      <td className="max-w-[200px] truncate px-2 py-1.5">{item.companyName}</td>
      <td className="max-w-[120px] truncate px-2 py-1.5 text-muted-foreground">{item.sector33Name}</td>
      {showSectorStrength ? (
        <td className="px-2 py-1.5 text-right">
          <SectorStrengthScoreChip value={item.sectorStrengthScore} />
        </td>
      ) : null}
      <td className="px-2 py-1.5 text-right tabular-nums">{formatPriceJPY(item.currentPrice)}</td>
      {showValuation ? (
        <>
          <td
            className={cn(
              'px-2 py-1.5 text-right tabular-nums',
              getEvidenceTierClass(getPerEvidenceTier(item.perPercentile))
            )}
          >
            {formatRatio(item.per)}
          </td>
          <td
            className={cn(
              'px-2 py-1.5 text-right font-medium tabular-nums',
              getEvidenceTierClass(getForwardPerEvidenceTier(item))
            )}
          >
            {formatRatio(item.forwardPer)}
          </td>
          <td
            className={cn(
              'px-2 py-1.5 text-right tabular-nums',
              getEvidenceTierClass(
                getForwardPOpEvidenceTier(
                  item.forwardPOpPercentile,
                  item.forwardPerPercentile,
                  item.perPercentile,
                  item.forwardPOp,
                  item.per
                )
              )
            )}
          >
            {formatRatio(item.forwardPOp)}
          </td>
          <td
            className={cn(
              'px-2 py-1.5 text-right tabular-nums',
              getEvidenceTierClass(getCheapValuationPercentileTier(item.pbrPercentile))
            )}
          >
            {formatRatio(item.pbr)}
          </td>
        </>
      ) : null}
      {showLiquidity ? (
        <>
          <td
            className={cn('px-2 py-1.5 text-right tabular-nums', getEvidenceTierClass(getLiquidityEvidenceTier(item)))}
          >
            {formatSignedNumber(item.liquidityResidualZ)}
          </td>
          <td className="px-2 py-1.5 text-center">
            <LiquidityStateChips item={item} />
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
        <td
          className={cn(
            'px-2 py-1.5 text-right font-medium tabular-nums',
            isPositive ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'
          )}
        >
          {formatChangePercentage(item.changePercentage)}
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
  formatLargeValue = formatNullableTradingValue,
  labels,
  sortState,
}: EquityRankingTableProps<T>) {
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
    (showValuation ? 5 : 0) +
    (showLiquidity ? 3 : 0);

  return (
    <div className="min-h-0 flex-1 overflow-auto" onScroll={shouldVirtualize ? virtual.onScroll : undefined}>
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
