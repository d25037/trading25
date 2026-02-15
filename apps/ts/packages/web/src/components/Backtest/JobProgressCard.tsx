import { AlertCircle, Ban, CheckCircle2, Loader2, XCircle } from 'lucide-react';
import { useEffect, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import type { BacktestJobResponse, JobStatus } from '@/types/backtest';
import { formatPercentage } from '@/utils/formatters';

interface JobProgressCardProps {
  job: BacktestJobResponse | null | undefined;
  isLoading?: boolean;
  onCancel?: () => void;
  isCancelling?: boolean;
}

type BacktestResultWithSortino = NonNullable<BacktestJobResponse['result']> & {
  sortino_ratio?: number | null;
};

function StatusIcon({ status }: { status: JobStatus }) {
  switch (status) {
    case 'pending':
    case 'running':
      return <Loader2 className="h-5 w-5 animate-spin text-blue-500" />;
    case 'completed':
      return <CheckCircle2 className="h-5 w-5 text-green-500" />;
    case 'failed':
      return <XCircle className="h-5 w-5 text-red-500" />;
    case 'cancelled':
      return <Ban className="h-5 w-5 text-orange-500" />;
    default:
      return <AlertCircle className="h-5 w-5 text-yellow-500" />;
  }
}

function StatusLabel({ status }: { status: JobStatus }) {
  const labels: Record<JobStatus, string> = {
    pending: 'Pending',
    running: 'Running',
    completed: 'Completed',
    failed: 'Failed',
    cancelled: 'Cancelled',
  };
  return <span className="font-medium capitalize">{labels[status]}</span>;
}

export function JobProgressCard({ job, isLoading, onCancel, isCancelling }: JobProgressCardProps) {
  const isActive = job?.status === 'pending' || job?.status === 'running';

  const [elapsed, setElapsed] = useState(0);
  useEffect(() => {
    if (!isActive || !job) return;
    const startTime = job.started_at ?? job.created_at;
    if (!startTime) return;
    const start = new Date(startTime).getTime();
    const update = () => setElapsed(Math.floor((Date.now() - start) / 1000));
    update();
    const id = setInterval(update, 1000);
    return () => clearInterval(id);
  }, [isActive, job?.started_at, job?.created_at, job]);

  if (!job && !isLoading) return null;

  if (isLoading && !job) {
    return (
      <Card className="mt-4">
        <CardHeader className="pb-3">
          <div className="flex items-center gap-2">
            <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
            <CardTitle className="text-lg">Submitting...</CardTitle>
          </div>
        </CardHeader>
      </Card>
    );
  }

  if (!job) return null;

  const formatRatio = (value: number | null | undefined) =>
    typeof value === 'number' && Number.isFinite(value) ? value.toFixed(2) : '-';
  const completedResult =
    job.status === 'completed' && job.result ? (job.result as BacktestResultWithSortino) : null;

  const formatElapsed = (s: number) => {
    const m = Math.floor(s / 60);
    const sec = s % 60;
    return `${m}:${String(sec).padStart(2, '0')}`;
  };

  return (
    <Card className="mt-4">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <StatusIcon status={job.status} />
            <CardTitle className="text-lg">
              <StatusLabel status={job.status} />
            </CardTitle>
          </div>
          {isActive && (
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">⏱ {formatElapsed(elapsed)}</span>
              {onCancel && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={onCancel}
                  disabled={isCancelling}
                  className="h-7 gap-1 text-xs text-muted-foreground hover:text-red-500"
                >
                  <XCircle className="h-3.5 w-3.5" />
                  Cancel
                </Button>
              )}
            </div>
          )}
        </div>
      </CardHeader>
      <CardContent>
        {/* Progress bar (indeterminate) */}
        {isActive && (
          <div className="space-y-2">
            <div className="h-2 w-full rounded-full bg-secondary overflow-hidden">
              <div className="h-full rounded-full bg-blue-500 animate-progress-indeterminate" />
            </div>
            {job.message && <p className="text-xs text-muted-foreground">{job.message}</p>}
          </div>
        )}

        {/* Completed result summary */}
        {completedResult && (
          <div className="space-y-2 text-sm">
            <div className="grid grid-cols-2 gap-2">
              <div>
                <span className="text-muted-foreground">Return:</span>
                <span
                  className={`ml-2 font-medium ${completedResult.total_return >= 0 ? 'text-green-500' : 'text-red-500'}`}
                >
                  {formatPercentage(completedResult.total_return)}
                </span>
              </div>
              <div>
                <span className="text-muted-foreground">Sharpe:</span>
                <span className="ml-2 font-medium">{formatRatio(completedResult.sharpe_ratio)}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Sortino:</span>
                <span className="ml-2 font-medium">{formatRatio(completedResult.sortino_ratio)}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Max DD:</span>
                <span className="ml-2 font-medium text-red-500">
                  {formatPercentage(completedResult.max_drawdown, { showSign: false })}
                </span>
              </div>
              <div>
                <span className="text-muted-foreground">Trades:</span>
                <span className="ml-2 font-medium">{completedResult.trade_count}</span>
              </div>
            </div>
          </div>
        )}

        {/* Failed error */}
        {job.status === 'failed' && job.error && (
          <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{job.error}</div>
        )}

        {/* Cancelled */}
        {job.status === 'cancelled' && (
          <div className="rounded-md bg-orange-500/10 p-3 text-sm text-orange-500">
            {job.message ?? 'Backtest was cancelled'}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
