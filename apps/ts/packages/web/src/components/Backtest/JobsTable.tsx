import { CheckCircle2, Clock, Loader2, XCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import type { BacktestJobResponse, JobStatus } from '@/types/backtest';
import { formatPercentage } from '@/utils/formatters';

interface JobsTableProps {
  jobs: BacktestJobResponse[] | undefined;
  isLoading: boolean;
  onSelectJob: (jobId: string) => void;
  selectedJobId?: string | null;
}

function StatusIcon({ status }: { status: JobStatus }) {
  switch (status) {
    case 'pending':
      return <Clock className="h-4 w-4 text-yellow-500" />;
    case 'running':
      return <Loader2 className="h-4 w-4 animate-spin text-blue-500" />;
    case 'completed':
      return <CheckCircle2 className="h-4 w-4 text-green-500" />;
    case 'failed':
      return <XCircle className="h-4 w-4 text-red-500" />;
    default:
      return null;
  }
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
  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-48">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!jobs || jobs.length === 0) {
    return <div className="flex items-center justify-center h-48 text-muted-foreground">No jobs found</div>;
  }

  return (
    <div className="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-12">Status</TableHead>
            <TableHead>Job ID</TableHead>
            <TableHead>Started</TableHead>
            <TableHead>Return</TableHead>
            <TableHead className="w-24">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {jobs.map((job) => (
            <TableRow key={job.job_id} className={selectedJobId === job.job_id ? 'bg-accent/50' : ''}>
              <TableCell>
                <StatusIcon status={job.status} />
              </TableCell>
              <TableCell className="font-mono text-xs">{job.job_id.slice(0, 8)}...</TableCell>
              <TableCell className="text-sm">{formatDate(job.started_at)}</TableCell>
              <TableCell className={`text-sm ${returnColorClass(job.result)}`}>{formatReturn(job.result)}</TableCell>
              <TableCell>
                {job.status === 'completed' && (
                  <Button variant="outline" size="sm" onClick={() => onSelectJob(job.job_id)}>
                    View
                  </Button>
                )}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
