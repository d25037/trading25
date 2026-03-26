import { AlertCircle, Ban, CheckCircle2, Loader2, XCircle } from 'lucide-react';
import { useEffect, useState } from 'react';
import { Surface } from '@/components/Layout/Workspace';
import { VerificationSummarySection } from '@/components/VerificationSummarySection';
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

function CompletedSummary({
  job,
  bestParamsText,
  worstParamsText,
}: {
  job: OptimizationJobResponse;
  bestParamsText: string | null;
  worstParamsText: string | null;
}) {
  return (
    <div className="space-y-3 text-sm">
      {job.total_combinations != null ? (
        <div>
          <span className="text-muted-foreground">Combinations:</span>
          <span className="ml-2 font-medium">{job.total_combinations}</span>
        </div>
      ) : null}

      {bestParamsText ? (
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground">Best Params</span>
            <span className="font-medium">score: {formatScore(job.best_score)}</span>
          </div>
          <pre className="max-h-40 overflow-auto rounded-md bg-muted/50 p-2 text-xs whitespace-pre-wrap break-all">
            {bestParamsText}
          </pre>
        </div>
      ) : null}

      {worstParamsText ? (
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground">Worst Params</span>
            <span className="font-medium">score: {formatScore(job.worst_score)}</span>
          </div>
          <pre className="max-h-40 overflow-auto rounded-md bg-muted/50 p-2 text-xs whitespace-pre-wrap break-all">
            {worstParamsText}
          </pre>
        </div>
      ) : null}

      {!bestParamsText && job.best_score != null ? (
        <div>
          <span className="text-muted-foreground">Best Score:</span>
          <span className="ml-2 font-medium">{formatScore(job.best_score)}</span>
        </div>
      ) : null}
      {!worstParamsText && job.worst_score != null ? (
        <div>
          <span className="text-muted-foreground">Worst Score:</span>
          <span className="ml-2 font-medium">{formatScore(job.worst_score)}</span>
        </div>
      ) : null}
    </div>
  );
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

function resolveStageLabel(job: OptimizationJobResponse): string | null {
  if (job.status !== 'pending' && job.status !== 'running') return null;
  if (job.message?.toLowerCase().includes('nautilus verification')) return 'Verification stage';
  return 'Fast stage';
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
      <Surface className="mt-2 p-4 sm:p-5">
        <div className="flex items-center gap-2">
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
          <h3 className="text-lg font-semibold tracking-tight text-foreground">Submitting...</h3>
        </div>
      </Surface>
    );
  }

  if (!job) return null;

  const formatElapsed = (seconds: number) => {
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    return `${minutes}:${String(remainingSeconds).padStart(2, '0')}`;
  };

  const bestParamsText = stringifyParams(job.best_params);
  const worstParamsText = stringifyParams(job.worst_params);
  const stageLabel = resolveStageLabel(job);

  return (
    <Surface className="mt-2 p-4 sm:p-5">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <StatusIcon status={job.status} />
          <h3 className="text-lg font-semibold tracking-tight text-foreground">
            <StatusLabel status={job.status} />
          </h3>
        </div>
        {isActive ? <span className="text-sm text-muted-foreground">⏱ {formatElapsed(elapsed)}</span> : null}
      </div>
      <div className="mt-4">
        {isActive ? (
          <div className="space-y-2">
            {stageLabel ? <p className="text-xs font-medium text-blue-600">{stageLabel}</p> : null}
            <div className="h-2 w-full overflow-hidden rounded-full bg-secondary">
              <div className="h-full animate-progress-indeterminate rounded-full bg-blue-500" />
            </div>
            {job.message ? <p className="text-xs text-muted-foreground">{job.message}</p> : null}
          </div>
        ) : null}

        {job.status === 'completed' ? (
          <div className="space-y-3">
            <CompletedSummary job={job} bestParamsText={bestParamsText} worstParamsText={worstParamsText} />
            <VerificationSummarySection fastCandidates={job.fast_candidates} verification={job.verification} />
          </div>
        ) : null}

        {job.status === 'failed' && job.error ? (
          <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{job.error}</div>
        ) : null}
      </div>
    </Surface>
  );
}
