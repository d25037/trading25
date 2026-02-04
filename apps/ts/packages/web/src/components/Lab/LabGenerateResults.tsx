import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import type { LabGenerateResult } from '@/types/backtest';
import { formatPercentage } from '@/utils/formatters';

interface LabGenerateResultsProps {
  result: LabGenerateResult;
}

export function LabGenerateResults({ result }: LabGenerateResultsProps) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h4 className="text-sm font-medium">Generated Strategies</h4>
        <span className="text-xs text-muted-foreground">{result.total_generated} generated</span>
      </div>

      {result.saved_strategy_path && (
        <p className="text-xs text-muted-foreground">Saved to: {result.saved_strategy_path}</p>
      )}

      <div className="rounded-md border overflow-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="text-xs">Strategy</TableHead>
              <TableHead className="text-xs text-right">Score</TableHead>
              <TableHead className="text-xs text-right">Sharpe</TableHead>
              <TableHead className="text-xs text-right">Calmar</TableHead>
              <TableHead className="text-xs text-right">Return</TableHead>
              <TableHead className="text-xs text-right">Max DD</TableHead>
              <TableHead className="text-xs text-right">Win Rate</TableHead>
              <TableHead className="text-xs text-right">Trades</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {result.results.map((item) => (
              <TableRow key={item.strategy_id}>
                <TableCell className="text-xs font-mono">{item.strategy_id}</TableCell>
                <TableCell className="text-xs text-right">{item.score.toFixed(2)}</TableCell>
                <TableCell className="text-xs text-right">{item.sharpe_ratio.toFixed(2)}</TableCell>
                <TableCell className="text-xs text-right">{item.calmar_ratio.toFixed(2)}</TableCell>
                <TableCell
                  className={`text-xs text-right ${item.total_return >= 0 ? 'text-green-500' : 'text-red-500'}`}
                >
                  {formatPercentage(item.total_return)}
                </TableCell>
                <TableCell className="text-xs text-right text-red-500">
                  {formatPercentage(item.max_drawdown, { showSign: false })}
                </TableCell>
                <TableCell className="text-xs text-right">
                  {formatPercentage(item.win_rate, { showSign: false })}
                </TableCell>
                <TableCell className="text-xs text-right">{item.trade_count}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
