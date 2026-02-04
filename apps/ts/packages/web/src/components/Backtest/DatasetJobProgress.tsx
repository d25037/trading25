import { useQueryClient } from '@tanstack/react-query';
import { AlertCircle, CheckCircle2, Loader2, XCircle } from 'lucide-react';
import { useEffect, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { datasetKeys, useCancelDatasetJob, useDatasetJobStatus } from '@/hooks/useDataset';
import { useBacktestStore } from '@/stores/backtestStore';

function formatElapsed(seconds: number): string {
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}:${String(s).padStart(2, '0')}`;
}

export function DatasetJobProgress() {
  const { activeDatasetJobId, setActiveDatasetJobId } = useBacktestStore();
  const { data: job } = useDatasetJobStatus(activeDatasetJobId);
  const cancelJob = useCancelDatasetJob();
  const queryClient = useQueryClient();

  const isActive = job?.status === 'pending' || job?.status === 'running';
  const isTerminal = job?.status === 'completed' || job?.status === 'failed' || job?.status === 'cancelled';

  const [elapsed, setElapsed] = useState(0);
  const startedAt = job?.startedAt;
  useEffect(() => {
    if (!isActive || !startedAt) return;
    const start = new Date(startedAt).getTime();
    const update = () => setElapsed(Math.floor((Date.now() - start) / 1000));
    update();
    const id = setInterval(update, 1000);
    return () => clearInterval(id);
  }, [isActive, startedAt]);

  // Invalidate dataset list when job completes and auto-clear after delay
  useEffect(() => {
    if (isTerminal && activeDatasetJobId) {
      queryClient.invalidateQueries({ queryKey: datasetKeys.list() });
      const jobIdToClean = activeDatasetJobId;
      const timer = setTimeout(() => {
        // Only clear if the active ID hasn't changed since we set this timer
        const current = useBacktestStore.getState().activeDatasetJobId;
        if (current === jobIdToClean) {
          setActiveDatasetJobId(null);
        }
      }, 5000);
      return () => clearTimeout(timer);
    }
  }, [isTerminal, activeDatasetJobId, setActiveDatasetJobId, queryClient]);

  if (!activeDatasetJobId || !job) return null;

  const handleCancel = () => {
    if (activeDatasetJobId) {
      cancelJob.mutate(activeDatasetJobId, {
        onSuccess: () => setActiveDatasetJobId(null),
      });
    }
  };

  return (
    <Card className="mt-4">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            {isActive && <Loader2 className="h-5 w-5 animate-spin text-blue-500" />}
            {job.status === 'completed' && <CheckCircle2 className="h-5 w-5 text-green-500" />}
            {job.status === 'failed' && <XCircle className="h-5 w-5 text-red-500" />}
            {job.status === 'cancelled' && <AlertCircle className="h-5 w-5 text-yellow-500" />}
            <CardTitle className="text-lg capitalize">{job.status}</CardTitle>
          </div>
          <div className="flex items-center gap-2">
            {isActive && <span className="text-sm text-muted-foreground">{formatElapsed(elapsed)}</span>}
            {isActive && (
              <Button variant="outline" size="sm" onClick={handleCancel} disabled={cancelJob.isPending}>
                キャンセル
              </Button>
            )}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {job.progress && (
          <div className="space-y-2">
            <div className="flex justify-between text-xs text-muted-foreground">
              <span>{job.progress.stage}</span>
              <span>{job.progress.percentage.toFixed(1)}%</span>
            </div>
            <div className="h-2 w-full rounded-full bg-secondary overflow-hidden">
              <div
                className="h-full rounded-full bg-blue-500 transition-all duration-300"
                style={{ width: `${Math.min(job.progress.percentage, 100)}%` }}
              />
            </div>
            <p className="text-xs text-muted-foreground">{job.progress.message}</p>
          </div>
        )}

        {isActive && !job.progress && (
          <div className="h-2 w-full rounded-full bg-secondary overflow-hidden">
            <div className="h-full rounded-full bg-blue-500 animate-progress-indeterminate" />
          </div>
        )}

        {job.status === 'completed' && job.result && (
          <div className="space-y-1 text-sm">
            <p>
              {job.result.processedStocks}/{job.result.totalStocks} 銘柄処理完了
            </p>
            {job.result.warnings.length > 0 && (
              <p className="text-xs text-yellow-500">{job.result.warnings.length} warnings</p>
            )}
          </div>
        )}

        {job.status === 'failed' && job.error && (
          <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{job.error}</div>
        )}
      </CardContent>
    </Card>
  );
}
