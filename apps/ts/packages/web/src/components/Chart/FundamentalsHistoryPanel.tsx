import { hasActualFinancialData, isFiscalYear } from '@/utils/fundamental-analysis';
import { useMemo } from 'react';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { useFundamentals } from '@/hooks/useFundamentals';
import { cn } from '@/lib/utils';
import { getFundamentalColor } from '@/utils/color-schemes';
import { formatFundamentalValue } from '@/utils/formatters';

interface FundamentalsHistoryPanelProps {
  symbol: string | null;
  enabled?: boolean;
}

interface ForecastEpsFields {
  forecastEps?: number | null;
  adjustedForecastEps?: number | null;
  revisedForecastEps?: number | null;
  revisedForecastSource?: string | null;
}

/**
 * Format date string (YYYY-MM-DD) to FY label (YYYY/M期).
 */
function formatFyLabel(date: string): string {
  const parts = date.split('-');
  const year = Number.parseInt(parts[0] ?? '', 10);
  const month = Number.parseInt(parts[1] ?? '', 10);
  return `${year}/${month}期`;
}

function renderForecastEps(fy: ForecastEpsFields): React.ReactNode {
  const { forecastEps, adjustedForecastEps, revisedForecastEps, revisedForecastSource } = fy;
  const displayForecastEps = adjustedForecastEps ?? forecastEps ?? null;

  const revisionBadge =
    revisedForecastEps != null ? (
      <span
        className={cn(
          'text-xs ml-1',
          displayForecastEps != null && revisedForecastEps > displayForecastEps ? 'text-green-500' : 'text-red-500'
        )}
      >
        ({revisedForecastSource}: {formatFundamentalValue(revisedForecastEps, 'yen')})
      </span>
    ) : null;

  if (displayForecastEps != null) {
    return (
      <span>
        {formatFundamentalValue(displayForecastEps, 'yen')}
        {revisionBadge}
      </span>
    );
  }

  if (revisedForecastEps != null) {
    return (
      <span className="text-xs text-muted-foreground">
        ({revisedForecastSource}: {formatFundamentalValue(revisedForecastEps, 'yen')})
      </span>
    );
  }

  return formatFundamentalValue(null, 'yen');
}

export function FundamentalsHistoryPanel({ symbol, enabled = true }: FundamentalsHistoryPanelProps) {
  const { data, isLoading, error } = useFundamentals(symbol, { enabled });

  const fyHistory = useMemo(() => {
    if (!data?.data) return [];
    return data.data
      .filter((d) => isFiscalYear(d.periodType) && hasActualFinancialData(d))
      .sort((a, b) => b.date.localeCompare(a.date))
      .slice(0, 5);
  }, [data]);

  if (!symbol) {
    return (
      <div className="h-full flex items-center justify-center">
        <p className="text-sm text-muted-foreground">銘柄を選択してください</p>
      </div>
    );
  }

  function normalizeError(err: unknown): Error | null {
    if (err instanceof Error) return err;
    if (err) return new Error('Failed to load fundamentals data');
    return null;
  }

  const normalizedError = normalizeError(error);

  return (
    <DataStateWrapper
      isLoading={isLoading}
      error={normalizedError}
      isEmpty={fyHistory.length === 0 && !isLoading}
      emptyMessage="過去のFYデータがありません"
      loadingMessage="Loading fundamentals history..."
      height="h-full"
    >
      <div className="h-full overflow-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border/30 text-xs text-muted-foreground">
              <th className="text-left py-2 px-3 font-medium">FY期</th>
              <th className="text-right py-2 px-3 font-medium">EPS</th>
              <th className="text-right py-2 px-3 font-medium">来期予想EPS</th>
              <th className="text-right py-2 px-3 font-medium">BPS</th>
              <th className="text-right py-2 px-3 font-medium">ROE</th>
              <th className="text-right py-2 px-3 font-medium">営業CF</th>
              <th className="text-right py-2 px-3 font-medium">投資CF</th>
              <th className="text-right py-2 px-3 font-medium">財務CF</th>
            </tr>
          </thead>
          <tbody>
            {fyHistory.map((fy) => (
              <tr key={fy.date} className="border-b border-border/20 hover:bg-background/40">
                <td className="py-2.5 px-3 font-medium text-foreground">{formatFyLabel(fy.date)}</td>
                <td className="py-2.5 px-3 text-right text-foreground">
                  {formatFundamentalValue(fy.adjustedEps ?? fy.eps, 'yen')}
                </td>
                <td className="py-2.5 px-3 text-right text-muted-foreground">
                  {renderForecastEps(fy)}
                </td>
                <td className="py-2.5 px-3 text-right text-foreground">
                  {formatFundamentalValue(fy.adjustedBps ?? fy.bps, 'yen')}
                </td>
                <td className="py-2.5 px-3 text-right text-foreground">{formatFundamentalValue(fy.roe, 'percent')}</td>
                <td className={cn('py-2.5 px-3 text-right', getFundamentalColor(fy.cashFlowOperating, 'cashFlow'))}>
                  {formatFundamentalValue(fy.cashFlowOperating, 'millions')}
                </td>
                <td className={cn('py-2.5 px-3 text-right', getFundamentalColor(fy.cashFlowInvesting, 'cashFlow'))}>
                  {formatFundamentalValue(fy.cashFlowInvesting, 'millions')}
                </td>
                <td className={cn('py-2.5 px-3 text-right', getFundamentalColor(fy.cashFlowFinancing, 'cashFlow'))}>
                  {formatFundamentalValue(fy.cashFlowFinancing, 'millions')}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </DataStateWrapper>
  );
}
