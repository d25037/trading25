import { AlertCircle, Ban, CheckCircle2, Loader2, XCircle } from 'lucide-react';
import { useEffect, useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import type { JobStatus, OptimizationJobResponse } from '@/types/backtest';

interface OptimizationJobProgressCardProps {
  job: OptimizationJobResponse | null | undefined;
  isLoading?: boolean;
}

function formatScore(score: number | null | undefined): string {
  return score != null ? score.toFixed(4) : '-';
}

function stringifyParams(params: Record<string, unknown> | null | undefined): string | null {
  if (!params || Object.keys(params).length === 0) {
    return null;
  }
  return JSON.stringify(params, null, 2);
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
    running: 'Optimizing',
    completed: 'Completed',
    failed: 'Failed',
    cancelled: 'Cancelled',
  };
  return <span className="font-medium capitalize">{labels[status]}</span>;
}

export function OptimizationJobProgressCard({ job, isLoading }: OptimizationJobProgressCardProps) {
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
      <Card className="mt-2">
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

  const formatElapsed = (s: number) => {
    const m = Math.floor(s / 60);
    const sec = s % 60;
    return `${m}:${String(sec).padStart(2, '0')}`;
  };

  const bestParamsText = stringifyParams(job.best_params);
  const worstParamsText = stringifyParams(job.worst_params);

  return (
    <Card className="mt-2">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <StatusIcon status={job.status} />
            <CardTitle className="text-lg">
              <StatusLabel status={job.status} />
            </CardTitle>
          </div>
          {isActive && <span className="text-sm text-muted-foreground">⏱ {formatElapsed(elapsed)}</span>}
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
        {job.status === 'completed' && (
          <div className="space-y-3 text-sm">
            {job.total_combinations != null && (
              <div>
                <span className="text-muted-foreground">Combinations:</span>
                <span className="ml-2 font-medium">{job.total_combinations}</span>
              </div>
            )}

            {bestParamsText && (
              <div className="space-y-1">
                <div className="flex items-center gap-2">
                  <span className="text-muted-foreground">Best Params</span>
                  <span className="font-medium">score: {formatScore(job.best_score)}</span>
                </div>
                <pre className="max-h-40 overflow-auto rounded-md bg-muted/50 p-2 text-xs whitespace-pre-wrap break-all">
                  {bestParamsText}
                </pre>
              </div>
            )}

            {worstParamsText && (
              <div className="space-y-1">
                <div className="flex items-center gap-2">
                  <span className="text-muted-foreground">Worst Params</span>
                  <span className="font-medium">score: {formatScore(job.worst_score)}</span>
                </div>
                <pre className="max-h-40 overflow-auto rounded-md bg-muted/50 p-2 text-xs whitespace-pre-wrap break-all">
                  {worstParamsText}
                </pre>
              </div>
            )}

            {!bestParamsText && job.best_score != null && (
              <div>
                <span className="text-muted-foreground">Best Score:</span>
                <span className="ml-2 font-medium">{formatScore(job.best_score)}</span>
              </div>
            )}
            {!worstParamsText && job.worst_score != null && (
              <div>
                <span className="text-muted-foreground">Worst Score:</span>
                <span className="ml-2 font-medium">{formatScore(job.worst_score)}</span>
              </div>
            )}
          </div>
        )}

        {/* Failed error */}
        {job.status === 'failed' && job.error && (
          <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{job.error}</div>
        )}
      </CardContent>
    </Card>
  );
}
