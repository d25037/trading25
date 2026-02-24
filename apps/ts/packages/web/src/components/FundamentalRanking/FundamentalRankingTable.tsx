import { ArrowDownCircle, ArrowUpCircle, TrendingDown, TrendingUp } from 'lucide-react';
import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { cn } from '@/lib/utils';
import type { FundamentalRankingItem, FundamentalRankings } from '@/types/fundamentalRanking';
import { formatPriceJPY } from '@/utils/formatters';

interface FundamentalRankingTableProps {
  rankings: FundamentalRankings | undefined;
  isLoading: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
}

type FundamentalRankingType = 'forecastHigh' | 'forecastLow' | 'actualHigh' | 'actualLow';

const rankingTabs: { id: FundamentalRankingType; label: string; icon: typeof TrendingUp }[] = [
  { id: 'forecastHigh', label: 'Forecast High', icon: ArrowUpCircle },
  { id: 'forecastLow', label: 'Forecast Low', icon: ArrowDownCircle },
  { id: 'actualHigh', label: 'Actual High', icon: TrendingUp },
  { id: 'actualLow', label: 'Actual Low', icon: TrendingDown },
];

function formatEps(value: number): string {
  if (!Number.isFinite(value)) return '-';
  return value.toLocaleString('ja-JP', { maximumFractionDigits: 2 });
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
      className="border-b border-border/30 hover:bg-accent/30 cursor-pointer transition-colors"
      onClick={() => onStockClick(item.code)}
    >
      <td className="p-2 text-center font-medium tabular-nums">{item.rank}</td>
      <td className="p-2 font-medium">{item.code}</td>
      <td className="p-2 truncate max-w-[180px]">{item.companyName}</td>
      <td className="p-2 truncate max-w-[100px] text-muted-foreground">{item.sector33Name}</td>
      <td className="p-2 text-right tabular-nums">{formatPriceJPY(item.currentPrice)}</td>
      <td className="p-2 text-right tabular-nums">{formatEps(item.epsValue)}</td>
      <td className="p-2 text-muted-foreground tabular-nums">
        {item.disclosedDate}
        <span className="ml-1 text-[10px] uppercase">{item.source}</span>
      </td>
    </tr>
  );
}

export function FundamentalRankingTable({ rankings, isLoading, error, onStockClick }: FundamentalRankingTableProps) {
  const [activeRankingType, setActiveRankingType] = useState<FundamentalRankingType>('forecastHigh');
  const currentItems = rankings?.[activeRankingType] ?? [];

  return (
    <Card className="glass-panel overflow-hidden flex-1">
      <CardHeader className="border-b border-border/30 py-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base">
            Fundamental Rankings
            {currentItems.length > 0 && (
              <span className="text-sm font-normal text-muted-foreground ml-2">({currentItems.length})</span>
            )}
          </CardTitle>
          <div className="flex gap-1 flex-wrap">
            {rankingTabs.map((tab) => {
              const Icon = tab.icon;
              const isActive = activeRankingType === tab.id;
              return (
                <Button
                  key={tab.id}
                  variant={isActive ? 'default' : 'ghost'}
                  size="sm"
                  className={cn('h-7 text-xs gap-1', isActive && 'shadow-sm')}
                  onClick={() => setActiveRankingType(tab.id)}
                >
                  <Icon className="h-3 w-3" />
                  {tab.label}
                </Button>
              );
            })}
          </div>
        </div>
      </CardHeader>
      <CardContent className="p-0 overflow-auto" style={{ maxHeight: 'calc(100vh - 280px)' }}>
        <DataStateWrapper
          isLoading={isLoading}
          error={error}
          isEmpty={currentItems.length === 0}
          emptyMessage="No fundamental ranking data available"
          emptySubMessage="Try a different market filter"
          emptyIcon={<TrendingUp className="h-8 w-8" />}
        >
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-background border-b z-10">
              <tr>
                <th className="text-center p-2 w-12">#</th>
                <th className="text-left p-2 w-16">Code</th>
                <th className="text-left p-2">Company</th>
                <th className="text-left p-2 w-24">Sector</th>
                <th className="text-right p-2 w-24">Price</th>
                <th className="text-right p-2 w-24">EPS</th>
                <th className="text-left p-2 w-36">Disclosed</th>
              </tr>
            </thead>
            <tbody>
              {currentItems.map((item) => (
                <FundamentalRankingRow key={`${item.code}-${item.rank}`} item={item} onStockClick={onStockClick} />
              ))}
            </tbody>
          </table>
        </DataStateWrapper>
      </CardContent>
    </Card>
  );
}
