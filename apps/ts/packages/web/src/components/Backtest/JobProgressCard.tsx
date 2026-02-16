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

function formatRatio(value: number | null | undefined) {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(2) : '-';
}

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

function RunningProgress({ isActive, message }: { isActive: boolean; message: string | null }) {
  if (!isActive) return null;

  return (
    <div className="space-y-2">
      <div className="h-2 w-full rounded-full bg-secondary overflow-hidden">
        <div className="h-full rounded-full bg-blue-500 animate-progress-indeterminate" />
      </div>
      {message && <p className="text-xs text-muted-foreground">{message}</p>}
    </div>
  );
}

function CompletedSummary({ result }: { result: BacktestResultWithSortino | null }) {
  if (!result) return null;

  return (
    <div className="space-y-2 text-sm">
      <div className="grid grid-cols-2 gap-2">
        <div>
          <span className="text-muted-foreground">Return:</span>
          <span className={`ml-2 font-medium ${result.total_return >= 0 ? 'text-green-500' : 'text-red-500'}`}>
            {formatPercentage(result.total_return)}
          </span>
        </div>
        <div>
          <span className="text-muted-foreground">Sharpe:</span>
          <span className="ml-2 font-medium">{formatRatio(result.sharpe_ratio)}</span>
        </div>
        <div>
          <span className="text-muted-foreground">Sortino:</span>
          <span className="ml-2 font-medium">{formatRatio(result.sortino_ratio)}</span>
        </div>
        <div>
          <span className="text-muted-foreground">Max DD:</span>
          <span className="ml-2 font-medium text-red-500">{formatPercentage(result.max_drawdown, { showSign: false })}</span>
        </div>
        <div>
          <span className="text-muted-foreground">Trades:</span>
          <span className="ml-2 font-medium">{result.trade_count}</span>
        </div>
      </div>
    </div>
  );
}

function StatusAlert({ job }: { job: BacktestJobResponse }) {
  if (job.status === 'failed' && job.error) {
    return <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{job.error}</div>;
  }
  if (job.status === 'cancelled') {
    return <div className="rounded-md bg-orange-500/10 p-3 text-sm text-orange-500">{job.message ?? 'Backtest was cancelled'}</div>;
  }
  return null;
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
        <RunningProgress isActive={isActive} message={job.message} />
        <CompletedSummary result={completedResult} />
        <StatusAlert job={job} />
      </CardContent>
    </Card>
  );
}
