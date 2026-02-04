import { AlertCircle, CheckCircle2, Loader2, XCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import type { SyncJobResponse } from '@/types/sync';

interface SyncStatusCardProps {
  job: SyncJobResponse | null | undefined;
  isLoading: boolean;
  onCancel: () => void;
  isCancelling: boolean;
}

function StatusIcon({ status }: { status: SyncJobResponse['status'] }) {
  switch (status) {
    case 'pending':
    case 'running':
      return <Loader2 className="h-5 w-5 animate-spin text-blue-500" />;
    case 'completed':
      return <CheckCircle2 className="h-5 w-5 text-green-500" />;
    case 'failed':
      return <XCircle className="h-5 w-5 text-red-500" />;
    case 'cancelled':
      return <AlertCircle className="h-5 w-5 text-yellow-500" />;
    default:
      return null;
  }
}

function StatusLabel({ status }: { status: SyncJobResponse['status'] }) {
  const labels: Record<SyncJobResponse['status'], string> = {
    pending: 'Pending',
    running: 'Running',
    completed: 'Completed',
    failed: 'Failed',
    cancelled: 'Cancelled',
  };
  return <span className="font-medium">{labels[status]}</span>;
}

export function SyncStatusCard({ job, isLoading, onCancel, isCancelling }: SyncStatusCardProps) {
  if (!job) return null;

  const isActive = job.status === 'pending' || job.status === 'running';
  const progress = job.progress;
  const result = job.result;

  return (
    <Card className="mt-4">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <StatusIcon status={job.status} />
            <CardTitle className="text-lg">
              <StatusLabel status={job.status} />
            </CardTitle>
          </div>
          {isActive && (
            <Button variant="outline" size="sm" onClick={onCancel} disabled={isCancelling || isLoading}>
              {isCancelling ? <Loader2 className="h-4 w-4 animate-spin" /> : 'Cancel'}
            </Button>
          )}
        </div>
        <CardDescription>Mode: {job.mode}</CardDescription>
      </CardHeader>
      <CardContent>
        {/* Progress bar */}
        {isActive && progress && (
          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span className="text-muted-foreground">{progress.stage}</span>
              <span className="font-medium">{progress.percentage.toFixed(1)}%</span>
            </div>
            <div className="h-2 w-full rounded-full bg-secondary">
              <div
                className="h-full rounded-full bg-blue-500 transition-all duration-300"
                style={{ width: `${progress.percentage}%` }}
              />
            </div>
            <p className="text-xs text-muted-foreground">{progress.message}</p>
          </div>
        )}

        {/* Completed result */}
        {job.status === 'completed' && result && (
          <div className="space-y-2 text-sm">
            <div className="grid grid-cols-2 gap-2">
              <div>
                <span className="text-muted-foreground">API Calls:</span>
                <span className="ml-2 font-medium">{result.totalApiCalls}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Stocks Updated:</span>
                <span className="ml-2 font-medium">{result.stocksUpdated}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Dates Processed:</span>
                <span className="ml-2 font-medium">{result.datesProcessed}</span>
              </div>
              {result.failedDates.length > 0 && (
                <div>
                  <span className="text-muted-foreground">Failed Dates:</span>
                  <span className="ml-2 font-medium text-red-500">{result.failedDates.length}</span>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Failed error */}
        {job.status === 'failed' && job.error && (
          <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{job.error}</div>
        )}

        {/* Cancelled message */}
        {job.status === 'cancelled' && <div className="text-sm text-muted-foreground">Sync was cancelled by user.</div>}
      </CardContent>
    </Card>
  );
}
