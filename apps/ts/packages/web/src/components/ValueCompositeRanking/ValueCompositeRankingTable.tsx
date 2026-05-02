import { TrendingUp } from 'lucide-react';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { useVirtualizedRows } from '@/hooks/useVirtualizedRows';
import type { ValueCompositeRankingItem, ValueCompositeRankingResponse } from '@/types/valueCompositeRanking';
import { formatPriceJPY } from '@/utils/formatters';

interface ValueCompositeRankingTableProps {
  data: ValueCompositeRankingResponse | undefined;
  isLoading: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
}

const VIRTUALIZATION_THRESHOLD = 120;
const VALUE_ROW_HEIGHT = 36;
const VALUE_VIEWPORT_HEIGHT = 560;

function formatScore(value: number): string {
  if (!Number.isFinite(value)) return '-';
  return value.toLocaleString('ja-JP', { maximumFractionDigits: 3 });
}

function formatRatio(value: number): string {
  if (!Number.isFinite(value)) return '-';
  return `${value.toLocaleString('ja-JP', { maximumFractionDigits: 2 })}x`;
}

function formatMarketCapBil(value: number): string {
  if (!Number.isFinite(value)) return '-';
  return `${value.toLocaleString('ja-JP', { maximumFractionDigits: 1 })}bn`;
}

type TechnicalMetricKey = Exclude<keyof NonNullable<ValueCompositeRankingItem['technicalMetrics']>, 'featureDate'>;

interface TechnicalMetricColumn {
  key: TechnicalMetricKey;
  label: string;
  signed: boolean;
}

const STANDARD_TECHNICAL_COLUMNS: TechnicalMetricColumn[] = [
  { key: 'reboundFrom252dLowPct', label: '252d Low Reb', signed: true },
  { key: 'return252dPct', label: '252d Ret', signed: true },
];

const PRIME_TECHNICAL_COLUMNS: TechnicalMetricColumn[] = [
  { key: 'volatility20dPct', label: 'Vol 20d', signed: false },
  { key: 'volatility60dPct', label: 'Vol 60d', signed: false },
  { key: 'downsideVolatility60dPct', label: 'Down Vol 60d', signed: false },
];

function normalizeMarket(value: string | null | undefined): 'prime' | 'standard' | 'other' {
  if (value === 'prime' || value === '0111') return 'prime';
  if (value === 'standard' || value === '0112') return 'standard';
  return 'other';
}

function resolveTechnicalColumns(markets: string[], items: ValueCompositeRankingItem[]): TechnicalMetricColumn[] {
  const marketSet = new Set(markets.map(normalizeMarket));
  if (marketSet.size === 1 && marketSet.has('standard')) return STANDARD_TECHNICAL_COLUMNS;
  if (marketSet.size === 1 && marketSet.has('prime')) return PRIME_TECHNICAL_COLUMNS;

  const itemMarketSet = new Set(items.map((item) => normalizeMarket(item.marketCode)));
  if (itemMarketSet.size === 1 && itemMarketSet.has('standard')) return STANDARD_TECHNICAL_COLUMNS;
  if (itemMarketSet.size === 1 && itemMarketSet.has('prime')) return PRIME_TECHNICAL_COLUMNS;
  return [...STANDARD_TECHNICAL_COLUMNS, ...PRIME_TECHNICAL_COLUMNS];
}

function formatTechnicalPercent(value: number | null | undefined, signed: boolean): string {
  if (value == null || !Number.isFinite(value)) return '-';
  const formatted = Math.abs(value).toLocaleString('ja-JP', {
    maximumFractionDigits: 1,
    minimumFractionDigits: 1,
  });
  if (!signed) return `${formatted}%`;
  return `${value >= 0 ? '+' : '-'}${formatted}%`;
}

function ValueCompositeRankingRow({
  item,
  technicalColumns,
  onStockClick,
}: {
  item: ValueCompositeRankingItem;
  technicalColumns: TechnicalMetricColumn[];
  onStockClick: (code: string) => void;
}) {
  return (
    <tr
      className="cursor-pointer border-b border-border/30 transition-colors hover:bg-[var(--app-surface-muted)]"
      onClick={() => onStockClick(item.code)}
    >
      <td className="px-2 py-1.5 text-center font-medium tabular-nums">{item.rank}</td>
      <td className="px-2 py-1.5 font-medium">{item.code}</td>
      <td className="px-2 py-1.5 truncate max-w-[180px]">{item.companyName}</td>
      <td className="px-2 py-1.5 truncate max-w-[100px] text-muted-foreground">{item.sector33Name}</td>
      <td className="px-2 py-1.5 text-right font-medium tabular-nums">{formatScore(item.score)}</td>
      {technicalColumns.map((column) => (
        <td key={column.key} className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">
          {formatTechnicalPercent(item.technicalMetrics?.[column.key], column.signed)}
        </td>
      ))}
      <td className="px-2 py-1.5 text-right tabular-nums">{formatRatio(item.pbr)}</td>
      <td className="px-2 py-1.5 text-right tabular-nums">{formatRatio(item.forwardPer)}</td>
      <td className="px-2 py-1.5 text-right tabular-nums">{formatMarketCapBil(item.marketCapBilJpy)}</td>
      <td className="px-2 py-1.5 text-right tabular-nums">{formatPriceJPY(item.currentPrice)}</td>
    </tr>
  );
}

export function ValueCompositeRankingTable({ data, isLoading, error, onStockClick }: ValueCompositeRankingTableProps) {
  const items = data?.items ?? [];
  const technicalColumns = resolveTechnicalColumns(data?.markets ?? [], items);
  const columnCount = 9 + technicalColumns.length;
  const shouldVirtualize = items.length >= VIRTUALIZATION_THRESHOLD;
  const virtual = useVirtualizedRows(items, {
    enabled: shouldVirtualize,
    rowHeight: VALUE_ROW_HEIGHT,
    viewportHeight: VALUE_VIEWPORT_HEIGHT,
  });

  return (
    <Surface className="flex min-h-[26rem] flex-1 flex-col overflow-hidden">
      <div className="space-y-1 border-b border-border/70 px-4 py-3">
        <SectionEyebrow>Results</SectionEyebrow>
        <h2 className="text-base font-semibold text-foreground">
          Value Composite Scores
          {items.length > 0 && <span className="text-sm font-normal text-muted-foreground ml-2">({items.length})</span>}
        </h2>
      </div>
      <div className="min-h-0 flex-1 overflow-auto" onScroll={shouldVirtualize ? virtual.onScroll : undefined}>
        <DataStateWrapper
          isLoading={isLoading}
          error={error}
          isEmpty={items.length === 0}
          emptyMessage="No value score data available"
          emptySubMessage="Try a different date or market filter"
          emptyIcon={<TrendingUp className="h-8 w-8" />}
          height="h-full min-h-[20rem]"
        >
          <table className="w-full text-xs">
            <thead className="sticky top-0 z-10 border-b bg-[var(--app-surface-muted)]">
              <tr>
                <th className="text-center px-2 py-1.5 w-12">#</th>
                <th className="text-left px-2 py-1.5 w-16">Code</th>
                <th className="text-left px-2 py-1.5">Company</th>
                <th className="text-left px-2 py-1.5 w-24">Sector</th>
                <th className="text-right px-2 py-1.5 w-20">Score</th>
                {technicalColumns.map((column) => (
                  <th key={column.key} className="text-right px-2 py-1.5 w-24">
                    {column.label}
                  </th>
                ))}
                <th className="text-right px-2 py-1.5 w-20">PBR</th>
                <th className="text-right px-2 py-1.5 w-24">Fwd PER</th>
                <th className="text-right px-2 py-1.5 w-24">Mkt Cap</th>
                <th className="text-right px-2 py-1.5 w-24">Price</th>
              </tr>
            </thead>
            <tbody>
              {shouldVirtualize && virtual.paddingTop > 0 && (
                <tr>
                  <td colSpan={columnCount} className="p-0" style={{ height: virtual.paddingTop }} />
                </tr>
              )}
              {virtual.visibleItems.map((item) => (
                <ValueCompositeRankingRow
                  key={`${item.code}-${item.rank}-${data?.date ?? ''}`}
                  item={item}
                  technicalColumns={technicalColumns}
                  onStockClick={onStockClick}
                />
              ))}
              {shouldVirtualize && virtual.paddingBottom > 0 && (
                <tr>
                  <td colSpan={columnCount} className="p-0" style={{ height: virtual.paddingBottom }} />
                </tr>
              )}
            </tbody>
          </table>
        </DataStateWrapper>
      </div>
    </Surface>
  );
}
