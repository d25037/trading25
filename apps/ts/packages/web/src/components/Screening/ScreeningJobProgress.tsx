import { JobStatusIcon } from '@/components/Jobs/JobStatusIcon';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { Button } from '@/components/ui/button';
import { useElapsedSeconds } from '@/hooks/useElapsedSeconds';
import type { ScreeningJobResponse } from '@/types/screening';
import { formatElapsedSeconds } from '@/utils/formatters';
import { isActiveJobStatus } from '@trading25/api-clients/base/job-status';

interface ScreeningJobProgressProps {
  job: ScreeningJobResponse | null;
  onCancel?: () => void;
  isCancelling?: boolean;
}

interface ScreeningJobStatusInlineProps {
  job: ScreeningJobResponse;
}

export function ScreeningJobStatusInline({ job }: ScreeningJobStatusInlineProps) {
  return (
    <div className="inline-flex items-center gap-2 rounded-lg border border-border/70 bg-[var(--app-surface-muted)] px-3 py-2 text-sm">
      <JobStatusIcon status={job.status} size="sm" />
      <span className="font-medium">Screening Job: {job.status}</span>
    </div>
  );
}

export function ScreeningJobProgress({ job, onCancel, isCancelling = false }: ScreeningJobProgressProps) {
  const startTime = job?.started_at ?? job?.created_at ?? null;
  const isActive = isActiveJobStatus(job?.status);
  const elapsed = useElapsedSeconds(isActive, startTime);

  if (!job) return null;

  const progress = job.progress == null ? null : Math.round(job.progress * 100);

  return (
    <Surface className="p-3">
      <div className="space-y-3">
        <div className="flex items-center justify-between gap-3">
          <div className="space-y-1">
            <SectionEyebrow>Current Job</SectionEyebrow>
            <div className="flex items-center gap-2 text-sm font-medium text-foreground">
              <JobStatusIcon status={job.status} size="sm" />
              <span>Screening Job: {job.status}</span>
            </div>
          </div>
          {isActive && (
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground tabular-nums">{formatElapsedSeconds(elapsed)}</span>
              {onCancel && (
                <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={onCancel} disabled={isCancelling}>
                  Cancel
                </Button>
              )}
            </div>
          )}
        </div>

        {isActive && (
          <>
            <div className="h-2 w-full overflow-hidden rounded-full bg-[var(--app-surface-muted)]">
              {progress == null ? (
                <div className="h-full rounded-full bg-primary animate-progress-indeterminate" />
              ) : (
                <div
                  className="h-full rounded-full bg-primary transition-all duration-300"
                  style={{ width: `${progress}%` }}
                />
              )}
            </div>
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span>{job.message ?? 'Running...'}</span>
              {progress != null && <span>{progress}%</span>}
            </div>
          </>
        )}

        {job.status === 'failed' && <p className="text-xs text-red-500">{job.error ?? 'Screening failed'}</p>}
        {job.status === 'cancelled' && <p className="text-xs text-orange-500">Screening was cancelled.</p>}
      </div>
    </Surface>
  );
}
