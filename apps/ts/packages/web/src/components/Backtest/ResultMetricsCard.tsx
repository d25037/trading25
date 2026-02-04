import { Card, CardContent } from '@/components/ui/card';
import type { BacktestResultSummary } from '@/types/backtest';
import { formatPercentage } from '@/utils/formatters';

interface ResultMetricsCardProps {
  summary: BacktestResultSummary | null | undefined;
}

interface MetricItemProps {
  label: string;
  value: string;
  colorClass?: string;
}

function MetricItem({ label, value, colorClass = 'text-foreground' }: MetricItemProps) {
  return (
    <Card className="glass-panel">
      <CardContent className="p-4 text-center">
        <div className="text-xs text-muted-foreground uppercase tracking-wider">{label}</div>
        <div className={`text-xl font-bold mt-1 ${colorClass}`}>{value}</div>
      </CardContent>
    </Card>
  );
}

export function ResultMetricsCard({ summary }: ResultMetricsCardProps) {
  if (!summary) return null;

  const formatRatio = (value: number) => value.toFixed(2);

  const returnColor = summary.total_return >= 0 ? 'text-green-500' : 'text-red-500';
  const winRateColor = summary.win_rate >= 50 ? 'text-green-500' : 'text-yellow-500';

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
      <MetricItem label="Return" value={formatPercentage(summary.total_return)} colorClass={returnColor} />
      <MetricItem label="Sharpe" value={formatRatio(summary.sharpe_ratio)} />
      <MetricItem
        label="Max DD"
        value={formatPercentage(summary.max_drawdown, { showSign: false })}
        colorClass="text-red-500"
      />
      <MetricItem label="Win Rate" value={formatPercentage(summary.win_rate)} colorClass={winRateColor} />
      <MetricItem label="Calmar" value={formatRatio(summary.calmar_ratio)} />
      <MetricItem label="Trades" value={summary.trade_count.toString()} />
    </div>
  );
}
