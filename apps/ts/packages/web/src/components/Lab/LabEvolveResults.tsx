import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import type { LabEvolveResult } from '@/types/backtest';

interface LabEvolveResultsProps {
  result: LabEvolveResult;
}

export function LabEvolveResults({ result }: LabEvolveResultsProps) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h4 className="text-sm font-medium">Evolution Results</h4>
        <span className="text-xs text-muted-foreground">Best score: {result.best_score.toFixed(4)}</span>
      </div>

      <div className="text-xs text-muted-foreground space-y-1">
        <p>
          Best strategy: <span className="font-mono">{result.best_strategy_id}</span>
        </p>
        {result.saved_strategy_path && <p>Saved to: {result.saved_strategy_path}</p>}
      </div>

      <div className="rounded-md border overflow-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="text-xs">Generation</TableHead>
              <TableHead className="text-xs text-right">Best Score</TableHead>
              <TableHead className="text-xs text-right">Avg Score</TableHead>
              <TableHead className="text-xs text-right">Worst Score</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {result.history.map((item) => (
              <TableRow key={item.generation}>
                <TableCell className="text-xs">{item.generation}</TableCell>
                <TableCell className="text-xs text-right font-medium">{item.best_score.toFixed(4)}</TableCell>
                <TableCell className="text-xs text-right">{item.avg_score.toFixed(4)}</TableCell>
                <TableCell className="text-xs text-right text-muted-foreground">
                  {item.worst_score.toFixed(4)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
