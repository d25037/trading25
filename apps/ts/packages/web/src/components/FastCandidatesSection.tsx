import type { CanonicalExecutionMetrics, FastCandidateSummary } from '@trading25/api-clients/backtest';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';

interface FastCandidatesSectionProps {
  fastCandidates?: FastCandidateSummary[] | null;
  className?: string;
}

function formatMetric(value: number | null | undefined, digits = 2): string {
  if (value == null || Number.isNaN(value)) return '-';
  return value.toFixed(digits);
}

function formatMetricsLabel(metrics?: CanonicalExecutionMetrics | null): string {
  if (!metrics) return 'metrics unavailable';
  return [
    `ret ${formatMetric(metrics.total_return)}%`,
    `sh ${formatMetric(metrics.sharpe_ratio)}`,
    `dd ${formatMetric(metrics.max_drawdown)}%`,
    `tr ${formatMetric(metrics.trade_count, 0)}`,
  ].join(' / ');
}

export function FastCandidatesSection({ fastCandidates, className }: FastCandidatesSectionProps) {
  if (!fastCandidates || fastCandidates.length === 0) return null;

  return (
    <Card className={className}>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm">Fast Ranking</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="overflow-auto rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="text-xs">Rank</TableHead>
                <TableHead className="text-xs">Candidate</TableHead>
                <TableHead className="text-right text-xs">Score</TableHead>
                <TableHead className="text-xs">Metrics</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {fastCandidates.map((candidate) => (
                <TableRow key={candidate.candidate_id}>
                  <TableCell className="text-xs">{candidate.rank}</TableCell>
                  <TableCell className="font-mono text-xs">{candidate.candidate_id}</TableCell>
                  <TableCell className="text-right text-xs font-medium">{candidate.score.toFixed(4)}</TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {formatMetricsLabel(candidate.metrics)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}
