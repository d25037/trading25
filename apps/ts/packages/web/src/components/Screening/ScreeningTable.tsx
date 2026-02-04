import { TrendingUp, Zap } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { cn } from '@/lib/utils';
import type { ScreeningResultItem } from '@/types/screening';
import { getReturnColor } from '@/utils/color-schemes';
import { formatDateShort, formatPercentage, formatReturnPercent, formatVolumeRatio } from '@/utils/formatters';

interface ScreeningTableProps {
  results: ScreeningResultItem[];
  isLoading: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
}

function hasFutureReturns(results: ScreeningResultItem[]): boolean {
  return results.some((r) => r.futureReturns !== undefined);
}

function ScreeningTypeIcon({ type }: { type: ScreeningResultItem['screeningType'] }) {
  if (type === 'rangeBreakFast') {
    return (
      <span className="inline-flex items-center gap-1 text-amber-600 dark:text-amber-400">
        <Zap className="h-3 w-3" />
        <span className="text-xs">Fast</span>
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 text-blue-600 dark:text-blue-400">
      <TrendingUp className="h-3 w-3" />
      <span className="text-xs">Slow</span>
    </span>
  );
}

export function ScreeningTable({ results, isLoading, error, onStockClick }: ScreeningTableProps) {
  return (
    <Card className="glass-panel overflow-hidden flex-1">
      <CardHeader className="border-b border-border/30 py-3">
        <CardTitle className="text-base">
          Screening Results
          {results.length > 0 && (
            <span className="text-sm font-normal text-muted-foreground ml-2">({results.length})</span>
          )}
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
                <th className="text-center p-2 w-16">Type</th>
                <th className="text-right p-2 w-20">Vol Ratio</th>
                <th className="text-right p-2 w-20">Break %</th>
                {hasFutureReturns(results) && (
                  <>
                    <th className="text-right p-2 w-16">+5日</th>
                    <th className="text-right p-2 w-16">+20日</th>
                    <th className="text-right p-2 w-16">+60日</th>
                  </>
                )}
              </tr>
            </thead>
            <tbody>
              {results.map((result, index) => {
                const details = result.details.rangeBreak;
                const showFutureReturns = hasFutureReturns(results);
                return (
                  <tr
                    key={`${result.stockCode}-${result.matchedDate}-${index}`}
                    className="border-b border-border/30 hover:bg-accent/30 cursor-pointer transition-colors"
                    onClick={() => onStockClick(result.stockCode)}
                  >
                    <td className="p-2 text-muted-foreground tabular-nums">{formatDateShort(result.matchedDate)}</td>
                    <td className="p-2 font-medium">{result.stockCode}</td>
                    <td className="p-2 truncate max-w-[200px]">{result.companyName}</td>
                    <td className="p-2 truncate max-w-[120px] text-muted-foreground">{result.sector33Name || '-'}</td>
                    <td className="p-2 text-center">
                      <ScreeningTypeIcon type={result.screeningType} />
                    </td>
                    <td
                      className={cn(
                        'p-2 text-right tabular-nums',
                        details && details.volumeRatio >= 2 && 'text-green-600 dark:text-green-400 font-medium'
                      )}
                    >
                      {formatVolumeRatio(details?.volumeRatio)}
                    </td>
                    <td
                      className={cn(
                        'p-2 text-right tabular-nums',
                        details && details.breakPercentage >= 5 && 'text-green-600 dark:text-green-400 font-medium'
                      )}
                    >
                      {formatPercentage(details?.breakPercentage, { showSign: false, decimals: 1 })}
                    </td>
                    {showFutureReturns && (
                      <>
                        <td
                          className={cn(
                            'p-2 text-right tabular-nums',
                            getReturnColor(result.futureReturns?.day5?.changePercent)
                          )}
                        >
                          {formatReturnPercent(result.futureReturns?.day5?.changePercent)}
                        </td>
                        <td
                          className={cn(
                            'p-2 text-right tabular-nums',
                            getReturnColor(result.futureReturns?.day20?.changePercent)
                          )}
                        >
                          {formatReturnPercent(result.futureReturns?.day20?.changePercent)}
                        </td>
                        <td
                          className={cn(
                            'p-2 text-right tabular-nums',
                            getReturnColor(result.futureReturns?.day60?.changePercent)
                          )}
                        >
                          {formatReturnPercent(result.futureReturns?.day60?.changePercent)}
                        </td>
                      </>
                    )}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </DataStateWrapper>
      </CardContent>
    </Card>
  );
}
