import { TrendingUp } from 'lucide-react';
import { useState } from 'react';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useVirtualizedRows } from '@/hooks/useVirtualizedRows';
import type { FundamentalRankingItem, FundamentalRankings } from '@/types/fundamentalRanking';
import { formatPriceJPY } from '@/utils/formatters';

type FundamentalRankingType = 'ratioHigh' | 'ratioLow';
const VIRTUALIZATION_THRESHOLD = 120;

function formatRatio(value: number): string {
  if (!Number.isFinite(value)) return '-';
  return `${value.toLocaleString('ja-JP', { maximumFractionDigits: 4 })}x`;
}

function FundamentalRankingRow({
  item,
  onStockClick,
}: {
  item: FundamentalRankingItem;
  onStockClick: (code: string) => void;
}) {
  return (
    <tr
      className="cursor-pointer border-b border-border/30 transition-colors hover:bg-[var(--app-surface-muted)]"
      onClick={() => onStockClick(item.code)}
    >
      <td className="px-2 py-1.5 text-center font-medium tabular-nums">{item.rank}</td>
      <td className="px-2 py-1.5 font-medium">{item.code}</td>
      <td className="max-w-[180px] truncate px-2 py-1.5">{item.companyName}</td>
      <td className="max-w-[100px] truncate px-2 py-1.5 text-muted-foreground">{item.sector33Name}</td>
      <td className="px-2 py-1.5 text-right tabular-nums">{formatPriceJPY(item.currentPrice)}</td>
      <td className="px-2 py-1.5 text-right tabular-nums">{formatRatio(item.epsValue)}</td>
      <td className="px-2 py-1.5 text-muted-foreground tabular-nums">
        {item.disclosedDate}
        <span className="ml-1 text-[10px] uppercase">{item.source}</span>
      </td>
    </tr>
  );
}

export function FundamentalRankingTable({
  rankings,
  isLoading,
  error,
  onStockClick,
}: {
  rankings: FundamentalRankings | undefined;
  isLoading: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
}) {
  const [activeRankingType, setActiveRankingType] = useState<FundamentalRankingType>('ratioHigh');
  const currentItems = rankings?.[activeRankingType] ?? [];
  const shouldVirtualize = currentItems.length >= VIRTUALIZATION_THRESHOLD;
  const virtual = useVirtualizedRows(currentItems, {
    enabled: shouldVirtualize,
    rowHeight: 36,
    viewportHeight: 520,
  });

  return (
    <Surface className="flex min-h-[24rem] flex-1 flex-col overflow-hidden">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border/70 px-4 py-3">
        <div className="space-y-1">
          <SectionEyebrow>Results</SectionEyebrow>
          <h2 className="text-base font-semibold text-foreground">Fundamental Rankings</h2>
        </div>
        <Select
          value={activeRankingType}
          onValueChange={(value) => setActiveRankingType(value as FundamentalRankingType)}
        >
          <SelectTrigger className="h-8 w-40 text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="ratioHigh">Ratio High</SelectItem>
            <SelectItem value="ratioLow">Ratio Low</SelectItem>
          </SelectContent>
        </Select>
      </div>
      <div className="min-h-0 flex-1 overflow-auto" onScroll={shouldVirtualize ? virtual.onScroll : undefined}>
        <DataStateWrapper
          isLoading={isLoading}
          error={error}
          isEmpty={currentItems.length === 0}
          emptyMessage="No fundamental ranking data available"
          emptySubMessage="Try a different market filter"
          emptyIcon={<TrendingUp className="h-8 w-8" />}
          height="h-full min-h-[18rem]"
        >
          <table className="w-full text-xs">
            <thead className="sticky top-0 z-10 border-b bg-[var(--app-surface-muted)]">
              <tr>
                <th className="w-12 px-2 py-1.5 text-center">#</th>
                <th className="w-16 px-2 py-1.5 text-left">Code</th>
                <th className="px-2 py-1.5 text-left">Company</th>
                <th className="w-24 px-2 py-1.5 text-left">Sector</th>
                <th className="w-24 px-2 py-1.5 text-right">Price</th>
                <th className="w-36 px-2 py-1.5 text-right">Forecast/Actual EPS</th>
                <th className="w-36 px-2 py-1.5 text-left">Disclosed</th>
              </tr>
            </thead>
            <tbody>
              {shouldVirtualize && virtual.paddingTop > 0 ? (
                <tr>
                  <td colSpan={7} className="p-0" style={{ height: virtual.paddingTop }} />
                </tr>
              ) : null}
              {virtual.visibleItems.map((item) => (
                <FundamentalRankingRow
                  key={`${item.code}-${item.rank}-${item.disclosedDate}-${item.source}`}
                  item={item}
                  onStockClick={onStockClick}
                />
              ))}
              {shouldVirtualize && virtual.paddingBottom > 0 ? (
                <tr>
                  <td colSpan={7} className="p-0" style={{ height: virtual.paddingBottom }} />
                </tr>
              ) : null}
            </tbody>
          </table>
        </DataStateWrapper>
      </div>
    </Surface>
  );
}
