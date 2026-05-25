import type { JobStatus, LabJobResponse } from '@trading25/api-clients/backtest';
import { isActiveJobStatus } from '@trading25/api-clients/base/job-status';
import { type JobHistoryColumn, JobHistoryTable } from '@/components/Jobs/JobHistoryTable';
import { formatDateTimeShort } from '@/utils/formatters';

interface LabJobHistoryTableProps {
  jobs: LabJobResponse[] | undefined;
  isLoading: boolean;
  isRefreshing: boolean;
  selectedJobId?: string | null;
  onSelectJob: (job: LabJobResponse) => void;
  onRefresh: () => void;
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
    isActiveJobStatus(job.status)
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
      render: (job) => <span className="text-sm">{formatDateTimeShort(job.created_at)}</span>,
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
      onSelectJob={onSelectJob}
      onRefresh={onRefresh}
    />
  );
}
