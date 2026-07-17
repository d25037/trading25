import type { BacktestJobResponse, JobStatus } from '@trading25/api-clients/backtest';
import { isActiveJobStatus } from '@trading25/api-clients/base/job-status';
import { Loader2, XCircle } from 'lucide-react';
import { JobStatusIcon } from '@/components/Jobs/JobStatusIcon';
import { Surface } from '@/components/Layout/Workspace';
import { Button } from '@/components/ui/button';
import { useElapsedSeconds } from '@/hooks/useElapsedSeconds';
import { formatElapsedSeconds, formatPercentage } from '@/utils/formatters';

interface JobProgressCardProps {
  job: BacktestJobResponse | null | undefined;
  isLoading?: boolean;
  onCancel?: () => void;
  isCancelling?: boolean;
}

function formatRatio(value: number | null | undefined) {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(2) : '-';
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
      <div className="h-2 w-full overflow-hidden rounded-full bg-secondary">
        <div className="h-full animate-progress-indeterminate rounded-full bg-blue-500" />
      </div>
      {message && <p className="text-xs text-muted-foreground">{message}</p>}
    </div>
  );
}

function CompletedSummary({ result }: { result: BacktestJobResponse['result'] }) {
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
          <span className="ml-2 font-medium text-red-500">
            {formatPercentage(result.max_drawdown, { showSign: false })}
          </span>
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
    return (
      <div className="rounded-md bg-orange-500/10 p-3 text-sm text-orange-500">
        {job.message ?? 'Backtest was cancelled'}
      </div>
    );
  }
  return null;
}

export function JobProgressCard({ job, isLoading, onCancel, isCancelling }: JobProgressCardProps) {
  const isActive = isActiveJobStatus(job?.status);
  const elapsed = useElapsedSeconds(isActive, job?.started_at ?? job?.created_at);

  if (!job && !isLoading) return null;

  if (isLoading && !job) {
    return (
      <Surface className="mt-4 p-4 sm:p-5">
        <div className="flex items-center gap-2">
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
          <h3 className="text-lg font-semibold tracking-tight text-foreground">Submitting...</h3>
        </div>
      </Surface>
    );
  }

  if (!job) return null;

  const completedResult = job.status === 'completed' ? job.result : null;
  return (
    <Surface className="mt-4 p-4 sm:p-5">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <JobStatusIcon status={job.status} showUnknown />
          <h3 className="text-lg font-semibold tracking-tight text-foreground">
            <StatusLabel status={job.status} />
          </h3>
        </div>
        {isActive ? (
          <div className="flex items-center gap-2">
            <span className="text-sm text-muted-foreground">⏱ {formatElapsedSeconds(elapsed)}</span>
            {onCancel ? (
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
            ) : null}
          </div>
        ) : null}
      </div>
      <div className="mt-4">
        <RunningProgress isActive={isActive} message={job.message ?? null} />
        <CompletedSummary result={completedResult} />
        <StatusAlert job={job} />
      </div>
    </Surface>
  );
}
