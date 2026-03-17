import { useId } from 'react';
import { JobHistoryTable, type JobHistoryColumn } from '@/components/Jobs/JobHistoryTable';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import type { ScreeningJobResponse, ScreeningMode } from '@/types/screening';

interface ScreeningJobHistoryTableProps {
  mode: ScreeningMode;
  jobs: ScreeningJobResponse[] | undefined;
  isLoading: boolean;
  showHistory: boolean;
  onShowHistoryChange: (showHistory: boolean) => void;
  selectedJobId?: string | null;
  onSelectJob: (job: ScreeningJobResponse) => void;
}

function formatDate(dateString: string | null | undefined): string {
  if (!dateString) return '-';
  return new Date(dateString).toLocaleString('ja-JP', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function actionLabel(status: ScreeningJobResponse['status']): string {
  if (status === 'pending' || status === 'running') return 'Monitor';
  return 'View';
}

function truncateJobId(jobId: string): string {
  return jobId.length <= 8 ? jobId : `${jobId.slice(0, 8)}...`;
}

export function ScreeningJobHistoryTable({
  mode,
  jobs,
  isLoading,
  showHistory,
  onShowHistoryChange,
  selectedJobId,
  onSelectJob,
}: ScreeningJobHistoryTableProps) {
  const switchId = useId();
  const allStrategiesLabel = mode === 'same_day' ? '(all same-day production)' : '(all standard production)';

  const columns: JobHistoryColumn<ScreeningJobResponse>[] = [
    {
      key: 'jobId',
      header: 'Job ID',
      render: (job) => <span className="font-mono text-xs">{truncateJobId(job.job_id)}</span>,
    },
    {
      key: 'markets',
      header: 'Markets',
      render: (job) => <span className="text-xs">{job.markets}</span>,
    },
    {
      key: 'strategies',
      header: 'Strategies',
      render: (job) => <span className="text-xs truncate max-w-[220px]">{job.strategies ?? allStrategiesLabel}</span>,
    },
    {
      key: 'createdAt',
      header: 'Created',
      render: (job) => <span className="text-sm">{formatDate(job.created_at)}</span>,
    },
  ];

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h4 className="text-sm font-medium">Job History</h4>
        <div className="flex items-center gap-2">
          <Label htmlFor={switchId} className="cursor-pointer text-xs text-muted-foreground">
            Show History
          </Label>
          <Switch id={switchId} checked={showHistory} onCheckedChange={onShowHistoryChange} />
        </div>
      </div>

      {showHistory ? (
        <JobHistoryTable
          jobs={jobs}
          isLoading={isLoading}
          selectedJobId={selectedJobId}
          emptyMessage="No screening jobs found"
          columns={columns}
          getJobId={(job) => job.job_id}
          getStatus={(job) => job.status}
          getActionLabel={(job) => actionLabel(job.status)}
          onSelectJob={onSelectJob}
        />
      ) : null}
    </div>
  );
}
