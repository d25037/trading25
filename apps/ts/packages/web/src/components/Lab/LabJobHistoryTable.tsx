import { CheckCircle2, Clock, Loader2, RotateCw, XCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import type { JobStatus, LabJobResponse } from '@/types/backtest';

interface LabJobHistoryTableProps {
  jobs: LabJobResponse[] | undefined;
  isLoading: boolean;
  isRefreshing: boolean;
  selectedJobId?: string | null;
  onSelectJob: (job: LabJobResponse) => void;
  onRefresh: () => void;
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
    case 'cancelled':
      return <XCircle className="h-4 w-4 text-red-500" />;
    default:
      return null;
  }
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

export function LabJobHistoryTable({
  jobs,
  isLoading,
  isRefreshing,
  selectedJobId,
  onSelectJob,
  onRefresh,
}: LabJobHistoryTableProps) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h4 className="text-sm font-medium">Job History</h4>
        <Button variant="outline" size="sm" onClick={onRefresh} disabled={isRefreshing}>
          <RotateCw className={`h-3.5 w-3.5 ${isRefreshing ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </div>

      {isLoading ? (
        <div className="flex h-40 items-center justify-center">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      ) : !jobs || jobs.length === 0 ? (
        <div className="flex h-40 items-center justify-center rounded-md border text-sm text-muted-foreground">
          No lab jobs found
        </div>
      ) : (
        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-12">Status</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Strategy</TableHead>
                <TableHead>Created</TableHead>
                <TableHead className="w-24">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {jobs.map((job) => (
                <TableRow key={job.job_id} className={selectedJobId === job.job_id ? 'bg-accent/50' : ''}>
                  <TableCell>
                    <StatusIcon status={job.status} />
                  </TableCell>
                  <TableCell className="text-xs uppercase">{job.lab_type ?? '-'}</TableCell>
                  <TableCell className="text-xs font-mono">{job.strategy_name ?? '-'}</TableCell>
                  <TableCell className="text-sm">{formatDate(job.created_at)}</TableCell>
                  <TableCell>
                    <Button variant="outline" size="sm" onClick={() => onSelectJob(job)}>
                      {actionLabel(job.status)}
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
