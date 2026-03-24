import { ArrowDownCircle, ArrowUpCircle, TrendingUp } from 'lucide-react';
import { useState } from 'react';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useVirtualizedRows } from '@/hooks/useVirtualizedRows';
import type { FundamentalRankingItem, FundamentalRankings } from '@/types/fundamentalRanking';
import { formatPriceJPY } from '@/utils/formatters';

interface FundamentalRankingTableProps {
  rankings: FundamentalRankings | undefined;
  isLoading: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
}

type FundamentalRankingType = 'ratioHigh' | 'ratioLow';

const rankingTabs: { id: FundamentalRankingType; label: string; icon: typeof TrendingUp }[] = [
  { id: 'ratioHigh', label: 'Ratio High', icon: ArrowUpCircle },
  { id: 'ratioLow', label: 'Ratio Low', icon: ArrowDownCircle },
];

const VIRTUALIZATION_THRESHOLD = 120;
const FUNDAMENTAL_ROW_HEIGHT = 36;
const FUNDAMENTAL_VIEWPORT_HEIGHT = 520;

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
      <td className="px-2 py-1.5 truncate max-w-[180px]">{item.companyName}</td>
      <td className="px-2 py-1.5 truncate max-w-[100px] text-muted-foreground">{item.sector33Name}</td>
      <td className="px-2 py-1.5 text-right tabular-nums">{formatPriceJPY(item.currentPrice)}</td>
      <td className="px-2 py-1.5 text-right tabular-nums">{formatRatio(item.epsValue)}</td>
      <td className="px-2 py-1.5 text-muted-foreground tabular-nums">
        {item.disclosedDate}
        <span className="ml-1 text-[10px] uppercase">{item.source}</span>
      </td>
    </tr>
  );
}

export function FundamentalRankingTable({ rankings, isLoading, error, onStockClick }: FundamentalRankingTableProps) {
  const [activeRankingType, setActiveRankingType] = useState<FundamentalRankingType>('ratioHigh');
  const currentItems = rankings?.[activeRankingType] ?? [];
  const shouldVirtualize = currentItems.length >= VIRTUALIZATION_THRESHOLD;
  const virtual = useVirtualizedRows(currentItems, {
    enabled: shouldVirtualize,
    rowHeight: FUNDAMENTAL_ROW_HEIGHT,
    viewportHeight: FUNDAMENTAL_VIEWPORT_HEIGHT,
  });

  return (
    <Surface className="flex min-h-[24rem] flex-1 flex-col overflow-hidden">
      <div className="space-y-4 border-b border-border/70 px-4 py-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="space-y-1">
            <SectionEyebrow>Results</SectionEyebrow>
            <h2 className="text-base font-semibold text-foreground">
              Fundamental Rankings
              {currentItems.length > 0 && (
                <span className="text-sm font-normal text-muted-foreground ml-2">({currentItems.length})</span>
              )}
            </h2>
          </div>
          <Select
            value={activeRankingType}
            onValueChange={(value) => setActiveRankingType(value as FundamentalRankingType)}
          >
            <SelectTrigger className="h-8 w-[10rem] text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {rankingTabs.map((tab) => (
                <SelectItem key={tab.id} value={tab.id} className="text-xs">
                  {tab.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
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
                <th className="text-center px-2 py-1.5 w-12">#</th>
                <th className="text-left px-2 py-1.5 w-16">Code</th>
                <th className="text-left px-2 py-1.5">Company</th>
                <th className="text-left px-2 py-1.5 w-24">Sector</th>
                <th className="text-right px-2 py-1.5 w-24">Price</th>
                <th className="text-right px-2 py-1.5 w-36">Forecast/Actual EPS</th>
                <th className="text-left px-2 py-1.5 w-36">Disclosed</th>
              </tr>
            </thead>
            <tbody>
              {shouldVirtualize && virtual.paddingTop > 0 && (
                <tr>
                  <td colSpan={7} className="p-0" style={{ height: virtual.paddingTop }} />
                </tr>
              )}
              {virtual.visibleItems.map((item) => (
                <FundamentalRankingRow
                  key={`${item.code}-${item.rank}-${item.disclosedDate}-${item.source}`}
                  item={item}
                  onStockClick={onStockClick}
                />
              ))}
              {shouldVirtualize && virtual.paddingBottom > 0 && (
                <tr>
                  <td colSpan={7} className="p-0" style={{ height: virtual.paddingBottom }} />
                </tr>
              )}
            </tbody>
          </table>
        </DataStateWrapper>
      </div>
    </Surface>
  );
}
