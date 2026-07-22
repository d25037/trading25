import type { JobStatus, LabJobResponse } from '@trading25/api-clients/backtest';
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
