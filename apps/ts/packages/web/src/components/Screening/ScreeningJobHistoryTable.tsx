import { JobHistoryTable, type JobHistoryColumn } from '@/components/Jobs/JobHistoryTable';
import type { ScreeningJobResponse } from '@/types/screening';

interface ScreeningJobHistoryTableProps {
  jobs: ScreeningJobResponse[] | undefined;
  isLoading: boolean;
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

export function ScreeningJobHistoryTable({ jobs, isLoading, selectedJobId, onSelectJob }: ScreeningJobHistoryTableProps) {
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
      render: (job) => <span className="text-xs truncate max-w-[220px]">{job.strategies ?? '(all production)'}</span>,
    },
    {
      key: 'createdAt',
      header: 'Created',
      render: (job) => <span className="text-sm">{formatDate(job.created_at)}</span>,
    },
  ];

  return (
    <JobHistoryTable
      jobs={jobs}
      isLoading={isLoading}
      selectedJobId={selectedJobId}
      title="Job History"
      emptyMessage="No screening jobs found"
      columns={columns}
      getJobId={(job) => job.job_id}
      getStatus={(job) => job.status}
      getActionLabel={(job) => actionLabel(job.status)}
      onSelectJob={onSelectJob}
    />
  );
}
