import type { BacktestJobResponse, JobStatus } from '@trading25/api-clients/backtest';
import { type JobHistoryColumn, JobHistoryTable } from '@/components/Jobs/JobHistoryTable';
import { formatDateTimeShort, formatPercentage, formatShortId } from '@/utils/formatters';

interface JobsTableProps {
  jobs: BacktestJobResponse[] | undefined;
  isLoading: boolean;
  onSelectJob: (jobId: string) => void;
  selectedJobId?: string | null;
}

function formatReturn(result: BacktestJobResponse['result']): string {
  if (!result) return '-';
  return formatPercentage(result.total_return);
}

function returnColorClass(result: BacktestJobResponse['result']): string {
  if (!result) return '';
  return result.total_return >= 0 ? 'text-green-500' : 'text-red-500';
}

export function JobsTable({ jobs, isLoading, onSelectJob, selectedJobId }: JobsTableProps) {
  const columns: JobHistoryColumn<BacktestJobResponse>[] = [
    {
      key: 'jobId',
      header: 'Job ID',
      render: (job) => <span className="font-mono text-xs">{formatShortId(job.job_id)}</span>,
    },
    {
      key: 'startedAt',
      header: 'Started',
      render: (job) => <span className="text-sm">{formatDateTimeShort(job.started_at)}</span>,
    },
    {
      key: 'totalReturn',
      header: 'Return',
      render: (job) => <span className={`text-sm ${returnColorClass(job.result)}`}>{formatReturn(job.result)}</span>,
    },
  ];

  return (
    <JobHistoryTable
      jobs={jobs}
      isLoading={isLoading}
      selectedJobId={selectedJobId}
      emptyMessage="No jobs found"
      columns={columns}
      getJobId={(job) => job.job_id}
      getStatus={(job) => job.status as JobStatus}
      canSelectJob={(job) => job.status === 'completed'}
      getActionLabel={() => 'View'}
      onSelectJob={(job) => onSelectJob(job.job_id)}
    />
  );
}
