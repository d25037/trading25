import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import type { LabImproveResult } from '@/types/backtest';
import { formatPercentage } from '@/utils/formatters';

interface LabImproveResultsProps {
  result: LabImproveResult;
}

export function LabImproveResults({ result }: LabImproveResultsProps) {
  const allImprovements = [
    ...result.suggested_improvements.map((item) => ({ ...item, applied: false })),
    ...result.improvements.map((item) => ({ ...item, applied: true })),
  ];

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h4 className="text-sm font-medium">Improvement Analysis</h4>
        <span className="text-xs text-muted-foreground">{result.strategy_name}</span>
      </div>

      <div className="grid grid-cols-2 gap-3 text-xs">
        <div>
          <span className="text-muted-foreground">Max Drawdown:</span>{' '}
          <span className="text-red-500">{formatPercentage(result.max_drawdown, { showSign: false })}</span>
        </div>
        <div>
          <span className="text-muted-foreground">DD Duration:</span>{' '}
          <span>{result.max_drawdown_duration_days} days</span>
        </div>
      </div>

      {result.saved_strategy_path && (
        <p className="text-xs text-muted-foreground">Saved to: {result.saved_strategy_path}</p>
      )}

      <div className="space-y-2">
        {allImprovements.map((item, index) => (
          <Card key={`${item.improvement_type}-${item.target}-${index}`}>
            <CardHeader className="pb-2">
              <div className="flex items-center gap-2">
                <CardTitle className="text-sm">{item.improvement_type}</CardTitle>
                {item.applied && (
                  <span className="rounded-full bg-green-500/10 px-2 py-0.5 text-xs text-green-500">Applied</span>
                )}
              </div>
            </CardHeader>
            <CardContent className="space-y-1 text-xs">
              <p>
                <span className="text-muted-foreground">Target:</span> {item.target}
              </p>
              <p>
                <span className="text-muted-foreground">Signal:</span>{' '}
                <span className="font-mono">{item.signal_name}</span>
              </p>
              <p>
                <span className="text-muted-foreground">Reason:</span> {item.reason}
              </p>
              <p>
                <span className="text-muted-foreground">Expected Impact:</span> {item.expected_impact}
              </p>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}
