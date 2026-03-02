import { Ban, CheckCircle2, Clock, Loader2, RotateCw, XCircle } from 'lucide-react';
import type { ReactNode } from 'react';
import { Button } from '@/components/ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';

export type JobHistoryStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled' | string;

export interface JobHistoryColumn<TJob> {
  key: string;
  header: string;
  className?: string;
  render: (job: TJob) => ReactNode;
}

interface JobHistoryTableProps<TJob> {
  jobs: TJob[] | undefined;
  isLoading: boolean;
  isRefreshing?: boolean;
  selectedJobId?: string | null;
  title?: string;
  emptyMessage: string;
  columns: JobHistoryColumn<TJob>[];
  getJobId: (job: TJob) => string;
  getStatus: (job: TJob) => JobHistoryStatus;
  onSelectJob: (job: TJob) => void;
  canSelectJob?: (job: TJob) => boolean;
  getActionLabel?: (job: TJob) => string;
  onRefresh?: () => void;
}

function StatusIcon({ status }: { status: JobHistoryStatus }) {
  switch (status) {
    case 'pending':
      return <Clock className="h-4 w-4 text-yellow-500" />;
    case 'running':
      return <Loader2 className="h-4 w-4 animate-spin text-blue-500" />;
    case 'completed':
      return <CheckCircle2 className="h-4 w-4 text-green-500" />;
    case 'cancelled':
      return <Ban className="h-4 w-4 text-orange-500" />;
    case 'failed':
      return <XCircle className="h-4 w-4 text-red-500" />;
    default:
      return null;
  }
}

function defaultActionLabel(status: JobHistoryStatus): string {
  if (status === 'pending' || status === 'running') return 'Monitor';
  return 'View';
}

export function JobHistoryTable<TJob>({
  jobs,
  isLoading,
  isRefreshing = false,
  selectedJobId,
  title,
  emptyMessage,
  columns,
  getJobId,
  getStatus,
  onSelectJob,
  canSelectJob,
  getActionLabel,
  onRefresh,
}: JobHistoryTableProps<TJob>) {
  const resolvedCanSelect = canSelectJob ?? (() => true);
  const resolvedGetActionLabel = getActionLabel ?? ((job: TJob) => defaultActionLabel(getStatus(job)));

  return (
    <div className="space-y-3">
      {(title || onRefresh) && (
        <div className="flex items-center justify-between">
          {title ? <h4 className="text-sm font-medium">{title}</h4> : <span />}
          {onRefresh && (
            <Button variant="outline" size="sm" onClick={onRefresh} disabled={isRefreshing}>
              <RotateCw className={`h-3.5 w-3.5 ${isRefreshing ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
          )}
        </div>
      )}

      {isLoading ? (
        <div className="flex h-40 items-center justify-center">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      ) : !jobs || jobs.length === 0 ? (
        <div className="flex h-40 items-center justify-center rounded-md border text-sm text-muted-foreground">
          {emptyMessage}
        </div>
      ) : (
        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-12">Status</TableHead>
                {columns.map((column) => (
                  <TableHead key={column.key} className={column.className}>
                    {column.header}
                  </TableHead>
                ))}
                <TableHead className="w-24">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {jobs.map((job) => {
                const jobId = getJobId(job);
                const status = getStatus(job);
                const canSelect = resolvedCanSelect(job);
                return (
                  <TableRow key={jobId} className={selectedJobId === jobId ? 'bg-accent/50' : ''}>
                    <TableCell>
                      <StatusIcon status={status} />
                    </TableCell>
                    {columns.map((column) => (
                      <TableCell key={`${jobId}-${column.key}`} className={column.className}>
                        {column.render(job)}
                      </TableCell>
                    ))}
                    <TableCell>
                      {canSelect ? (
                        <Button variant="outline" size="sm" onClick={() => onSelectJob(job)}>
                          {resolvedGetActionLabel(job)}
                        </Button>
                      ) : (
                        <span className="text-muted-foreground">-</span>
                      )}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
