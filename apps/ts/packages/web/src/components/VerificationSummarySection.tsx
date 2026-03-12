import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import type {
  CanonicalExecutionMetrics,
  FastCandidateSummary,
  VerificationCandidate,
  VerificationDelta,
  VerificationSummary,
} from '@/types/backtest';

interface VerificationSummarySectionProps {
  fastCandidates?: FastCandidateSummary[] | null;
  verification?: VerificationSummary | null;
  className?: string;
}

function formatMetric(value: number | null | undefined, digits = 2): string {
  if (value == null || Number.isNaN(value)) return '-';
  return value.toFixed(digits);
}

function formatDelta(value: number | null | undefined, digits = 2): string {
  if (value == null || Number.isNaN(value)) return '-';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(digits)}`;
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

function statusClass(status: VerificationCandidate['verification_status']): string {
  switch (status) {
    case 'verified':
      return 'text-green-600';
    case 'failed':
      return 'text-red-500';
    case 'running':
      return 'text-blue-500';
    default:
      return 'text-muted-foreground';
  }
}

const DELTA_FIELDS: Array<{
  label: string;
  key: keyof VerificationDelta;
  digits?: number;
  suffix?: string;
}> = [
  { label: 'ret', key: 'total_return_delta', suffix: '%' },
  { label: 'sh', key: 'sharpe_ratio_delta' },
  { label: 'dd', key: 'max_drawdown_delta', suffix: '%' },
  { label: 'tr', key: 'trade_count_delta', digits: 0 },
];

export function VerificationSummarySection({
  fastCandidates,
  verification,
  className,
}: VerificationSummarySectionProps) {
  if ((!fastCandidates || fastCandidates.length === 0) && !verification) {
    return null;
  }

  const verificationStats = verification
    ? [
        { label: 'Overall', value: verification.overall_status },
        { label: 'Top K', value: verification.requested_top_k },
        { label: 'Completed', value: verification.completed_count },
        { label: 'Mismatch', value: verification.mismatch_count },
        { label: 'Winner Changed', value: verification.winner_changed ? 'Yes' : 'No' },
        { label: 'Authoritative', value: verification.authoritative_candidate_id ?? 'none', mono: true },
      ]
    : [];

  return (
    <div className={className ?? 'space-y-3'}>
      {fastCandidates && fastCandidates.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Fast Ranking</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="rounded-md border overflow-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="text-xs">Rank</TableHead>
                    <TableHead className="text-xs">Candidate</TableHead>
                    <TableHead className="text-xs text-right">Score</TableHead>
                    <TableHead className="text-xs">Metrics</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {fastCandidates.map((candidate) => (
                    <TableRow key={candidate.candidate_id}>
                      <TableCell className="text-xs">{candidate.rank}</TableCell>
                      <TableCell className="text-xs font-mono">{candidate.candidate_id}</TableCell>
                      <TableCell className="text-xs text-right font-medium">{candidate.score.toFixed(4)}</TableCell>
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
      )}

      {verification && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Verification</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
              {verificationStats.map((stat) => (
                <div key={stat.label} className="rounded-md border p-2">
                  <div className="text-[11px] uppercase text-muted-foreground">{stat.label}</div>
                  <div className={`text-sm font-medium ${stat.mono ? 'font-mono' : ''}`}>{stat.value}</div>
                </div>
              ))}
            </div>

            {verification.candidates && verification.candidates.length > 0 && (
              <div className="rounded-md border overflow-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="text-xs">Candidate</TableHead>
                      <TableHead className="text-xs text-right">Fast</TableHead>
                      <TableHead className="text-xs">Status</TableHead>
                      <TableHead className="text-xs">Verified</TableHead>
                      <TableHead className="text-xs">Delta</TableHead>
                      <TableHead className="text-xs">Mismatch</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {verification.candidates.map((candidate) => (
                      <TableRow
                        key={candidate.candidate_id}
                        className={
                          verification.authoritative_candidate_id === candidate.candidate_id ? 'bg-green-500/5' : ''
                        }
                      >
                        <TableCell className="text-xs">
                          <div className="font-mono">{candidate.candidate_id}</div>
                          <div className="text-muted-foreground">rank {candidate.fast_rank}</div>
                        </TableCell>
                        <TableCell className="text-xs text-right">
                          <div className="font-medium">{candidate.fast_score.toFixed(4)}</div>
                          <div className="text-muted-foreground">{formatMetricsLabel(candidate.fast_metrics)}</div>
                        </TableCell>
                        <TableCell className={`text-xs font-medium ${statusClass(candidate.verification_status)}`}>
                          {candidate.verification_status}
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {formatMetricsLabel(candidate.verified_metrics)}
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {DELTA_FIELDS.map((field) => (
                            <div key={field.key}>
                              {field.label} {formatDelta(candidate.delta?.[field.key], field.digits)}
                              {field.suffix ?? ''}
                            </div>
                          ))}
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {candidate.mismatch_reasons && candidate.mismatch_reasons.length > 0
                            ? candidate.mismatch_reasons.join(', ')
                            : '-'}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
