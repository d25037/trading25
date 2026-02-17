import { TrendingUp } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import type { ScreeningResultItem } from '@/types/screening';
import { formatDateShort } from '@/utils/formatters';

interface ScreeningTableProps {
  results: ScreeningResultItem[];
  isLoading: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
}

function formatScore(score: number | null): string {
  if (score === null || score === undefined) {
    return '-';
  }
  return score.toFixed(3);
}

export function ScreeningTable({ results, isLoading, error, onStockClick }: ScreeningTableProps) {
  return (
    <Card className="glass-panel overflow-hidden flex-1">
      <CardHeader className="border-b border-border/30 py-3">
        <CardTitle className="text-base">
          Screening Results
          {results.length > 0 && <span className="text-sm font-normal text-muted-foreground ml-2">({results.length})</span>}
        </CardTitle>
      </CardHeader>
      <CardContent className="p-0 overflow-auto" style={{ maxHeight: 'calc(100vh - 280px)' }}>
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
                <th className="text-left p-2 w-16">Date</th>
                <th className="text-left p-2 w-16">Code</th>
                <th className="text-left p-2">Company</th>
                <th className="text-left p-2 w-24">Sector</th>
                <th className="text-left p-2 w-40">Best Strategy</th>
                <th className="text-right p-2 w-20">Score</th>
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
                  <td className="p-2 truncate max-w-[180px]">{result.bestStrategyName}</td>
                  <td className="p-2 text-right tabular-nums">{formatScore(result.bestStrategyScore)}</td>
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
