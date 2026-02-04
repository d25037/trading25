import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import type { LabOptimizeResult } from '@/types/backtest';

interface LabOptimizeResultsProps {
  result: LabOptimizeResult;
}

export function LabOptimizeResults({ result }: LabOptimizeResultsProps) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h4 className="text-sm font-medium">Optimization Results</h4>
        <span className="text-xs text-muted-foreground">{result.total_trials} trials</span>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Best Parameters</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-1">
            <p className="text-xs">
              <span className="text-muted-foreground">Best score:</span>{' '}
              <span className="font-medium">{result.best_score.toFixed(4)}</span>
            </p>
            {Object.entries(result.best_params).map(([key, value]) => (
              <p key={key} className="text-xs">
                <span className="text-muted-foreground">{key}:</span> <span className="font-mono">{String(value)}</span>
              </p>
            ))}
            {result.saved_strategy_path && (
              <p className="text-xs text-muted-foreground">Saved to: {result.saved_strategy_path}</p>
            )}
          </div>
        </CardContent>
      </Card>

      <div className="rounded-md border overflow-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="text-xs">Trial</TableHead>
              <TableHead className="text-xs text-right">Score</TableHead>
              <TableHead className="text-xs">Params</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {result.history.map((item) => (
              <TableRow key={item.trial}>
                <TableCell className="text-xs">{item.trial}</TableCell>
                <TableCell className="text-xs text-right font-medium">{item.score.toFixed(4)}</TableCell>
                <TableCell className="text-xs font-mono text-muted-foreground">
                  {Object.entries(item.params)
                    .map(([k, v]) => `${k}=${String(v)}`)
                    .join(', ')}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
