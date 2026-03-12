import { JobHistoryTable, type JobHistoryColumn } from '@/components/Jobs/JobHistoryTable';
import type { JobStatus, LabJobResponse } from '@/types/backtest';

interface LabJobHistoryTableProps {
  jobs: LabJobResponse[] | undefined;
  isLoading: boolean;
  isRefreshing: boolean;
  selectedJobId?: string | null;
  onSelectJob: (job: LabJobResponse) => void;
  onRefresh: () => void;
}

function formatDate(dateString?: string): string {
  if (!dateString) return '-';
  const date = new Date(dateString);
  return date.toLocaleString('ja-JP', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function actionLabel(status: JobStatus): string {
  if (status === 'pending' || status === 'running') return 'Monitor';
  return 'View';
}

function resolveVerificationLabel(job: LabJobResponse): string {
  const result = job.result_data;
  if (result && 'verification' in result && result.verification) {
    return result.verification.overall_status;
  }
  if ((job.message ?? '').toLowerCase().includes('nautilus verification')) {
    return 'verifying';
  }
  if (
    (job.lab_type === 'generate' || job.lab_type === 'evolve' || job.lab_type === 'optimize') &&
    (job.status === 'pending' || job.status === 'running')
  ) {
    return 'fast path';
  }
  return '-';
}

export function LabJobHistoryTable({
  jobs,
  isLoading,
  isRefreshing,
  selectedJobId,
  onSelectJob,
  onRefresh,
}: LabJobHistoryTableProps) {
  const columns: JobHistoryColumn<LabJobResponse>[] = [
    {
      key: 'type',
      header: 'Type',
      render: (job) => <span className="text-xs uppercase">{job.lab_type ?? '-'}</span>,
    },
    {
      key: 'strategy',
      header: 'Strategy',
      render: (job) => <span className="text-xs font-mono">{job.strategy_name ?? '-'}</span>,
    },
    {
      key: 'createdAt',
      header: 'Created',
      render: (job) => <span className="text-sm">{formatDate(job.created_at)}</span>,
    },
    {
      key: 'verification',
      header: 'Verification',
      render: (job) => <span className="text-xs text-muted-foreground">{resolveVerificationLabel(job)}</span>,
    },
  ];

  return (
    <JobHistoryTable
      jobs={jobs}
      isLoading={isLoading}
      isRefreshing={isRefreshing}
      selectedJobId={selectedJobId}
      title="Job History"
      emptyMessage="No lab jobs found"
      columns={columns}
      getJobId={(job) => job.job_id}
      getStatus={(job) => job.status as JobStatus}
      getActionLabel={(job) => actionLabel(job.status)}
      onSelectJob={onSelectJob}
      onRefresh={onRefresh}
    />
  );
}
