import { Loader2, TrendingUp } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import type { ScreeningResultItem } from '@/types/screening';
import { formatDateShort } from '@/utils/formatters';

interface ScreeningTableProps {
  results: ScreeningResultItem[];
  isLoading: boolean;
  isFetching?: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
}

export function ScreeningTable({ results, isLoading, isFetching = false, error, onStockClick }: ScreeningTableProps) {
  return (
    <Card className="glass-panel overflow-hidden flex flex-1 min-h-0 flex-col">
      <CardHeader className="border-b border-border/30 py-3">
        <CardTitle className="text-base flex items-center gap-2">
          Screening Results
          {results.length > 0 && (
            <span className="text-sm font-normal text-muted-foreground ml-2">({results.length})</span>
          )}
          {isFetching && (
            <span className="inline-flex items-center gap-1 text-xs font-normal text-muted-foreground ml-2">
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
              Updating...
            </span>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent className="p-0 flex-1 min-h-0 overflow-auto">
        <DataStateWrapper
          isLoading={isLoading}
          error={error}
          isEmpty={results.length === 0}
          emptyMessage="No matches found"
          emptySubMessage="Try adjusting filters or recent days"
          emptyIcon={<TrendingUp className="h-8 w-8" />}
        >
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-background border-b z-10">
              <tr>
                <th className="text-left p-2 w-24">Date</th>
                <th className="text-left p-2 w-16">Code</th>
                <th className="text-left p-2">Company</th>
                <th className="text-left p-2 w-24">Sector</th>
                <th className="text-right p-2 w-16">Matches</th>
                <th className="text-left p-2">Matched Strategies</th>
              </tr>
            </thead>
            <tbody>
              {results.map((result) => (
                <tr
                  key={result.stockCode}
                  className="border-b border-border/30 hover:bg-accent/30 cursor-pointer transition-colors"
                  onClick={() => onStockClick(result.stockCode)}
                >
                  <td className="p-2 text-muted-foreground tabular-nums">{formatDateShort(result.matchedDate)}</td>
                  <td className="p-2 font-medium">{result.stockCode}</td>
                  <td className="p-2 truncate max-w-[200px]">{result.companyName}</td>
                  <td className="p-2 truncate max-w-[120px] text-muted-foreground">{result.sector33Name || '-'}</td>
                  <td className="p-2 text-right tabular-nums">{result.matchStrategyCount}</td>
                  <td className="p-2 text-muted-foreground truncate max-w-[260px]">
                    {result.matchedStrategies.map((strategy) => strategy.strategyName).join(', ')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </DataStateWrapper>
      </CardContent>
    </Card>
  );
}
