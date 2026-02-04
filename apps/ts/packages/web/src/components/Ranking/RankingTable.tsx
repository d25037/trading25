import { ArrowDownCircle, ArrowUpCircle, DollarSign, TrendingDown, TrendingUp } from 'lucide-react';
import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { cn } from '@/lib/utils';
import type { RankingItem, Rankings, RankingType } from '@/types/ranking';
import { formatPercentage, formatPriceJPY, formatTradingValue } from '@/utils/formatters';

interface RankingTableProps {
  rankings: Rankings | undefined;
  isLoading: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
  periodDays?: number;
}

const rankingTabs: { id: RankingType; label: string; icon: typeof DollarSign }[] = [
  { id: 'tradingValue', label: 'Trading Value', icon: DollarSign },
  { id: 'gainers', label: 'Gainers', icon: TrendingUp },
  { id: 'losers', label: 'Losers', icon: TrendingDown },
  { id: 'periodHigh', label: 'Period High', icon: ArrowUpCircle },
  { id: 'periodLow', label: 'Period Low', icon: ArrowDownCircle },
];

function RankingRow({
  item,
  onStockClick,
  showChange,
}: {
  item: RankingItem;
  onStockClick: (code: string) => void;
  showChange: boolean;
}) {
  const isPositive = (item.changePercentage ?? 0) >= 0;

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
      {showChange && (
        <td
          className={cn(
            'p-2 text-right tabular-nums font-medium',
            isPositive ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'
          )}
        >
          {formatPercentage(item.changePercentage)}
        </td>
      )}
      <td className="p-2 text-right tabular-nums text-muted-foreground">
        {formatTradingValue(item.tradingValue ?? item.tradingValueAverage)}
      </td>
    </tr>
  );
}

export function RankingTable({ rankings, isLoading, error, onStockClick, periodDays }: RankingTableProps) {
  const [activeRankingType, setActiveRankingType] = useState<RankingType>('tradingValue');

  const currentItems = rankings?.[activeRankingType] ?? [];
  const showChange = activeRankingType !== 'tradingValue';
  const isPeriodType = activeRankingType === 'periodHigh' || activeRankingType === 'periodLow';

  // Dynamic label for period tabs
  const getTabLabel = (tab: (typeof rankingTabs)[number]) => {
    if (tab.id === 'periodHigh') return `${periodDays || 250}D High`;
    if (tab.id === 'periodLow') return `${periodDays || 250}D Low`;
    return tab.label;
  };

  return (
    <Card className="glass-panel overflow-hidden flex-1">
      <CardHeader className="border-b border-border/30 py-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base">
            Market Rankings
            {currentItems.length > 0 && (
              <span className="text-sm font-normal text-muted-foreground ml-2">({currentItems.length})</span>
            )}
          </CardTitle>

          {/* Sub-tabs for ranking type */}
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
                  {getTabLabel(tab)}
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
          emptyMessage="No ranking data available"
          emptySubMessage="Try a different date or market"
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
                {showChange && <th className="text-right p-2 w-20">{isPeriodType ? 'Break %' : 'Change'}</th>}
                <th className="text-right p-2 w-24">Trading Value</th>
              </tr>
            </thead>
            <tbody>
              {currentItems.map((item) => (
                <RankingRow
                  key={`${item.code}-${item.rank}`}
                  item={item}
                  onStockClick={onStockClick}
                  showChange={showChange}
                />
              ))}
            </tbody>
          </table>
        </DataStateWrapper>
      </CardContent>
    </Card>
  );
}
