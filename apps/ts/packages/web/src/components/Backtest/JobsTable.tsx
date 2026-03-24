import { type JobHistoryColumn, JobHistoryTable } from '@/components/Jobs/JobHistoryTable';
import type { BacktestJobResponse, JobStatus } from '@/types/backtest';
import { formatPercentage } from '@/utils/formatters';

interface JobsTableProps {
  jobs: BacktestJobResponse[] | undefined;
  isLoading: boolean;
  onSelectJob: (jobId: string) => void;
  selectedJobId?: string | null;
}

function formatDate(dateString: string | null | undefined): string {
  if (!dateString) return '-';
  const date = new Date(dateString);
  return date.toLocaleString('ja-JP', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
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
      render: (job) => <span className="font-mono text-xs">{job.job_id.slice(0, 8)}...</span>,
    },
    {
      key: 'startedAt',
      header: 'Started',
      render: (job) => <span className="text-sm">{formatDate(job.started_at)}</span>,
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
