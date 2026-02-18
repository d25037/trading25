import { Ban, CheckCircle2, Loader2, XCircle } from 'lucide-react';
import { useEffect, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import type { ScreeningJobResponse } from '@/types/screening';

interface ScreeningJobProgressProps {
  job: ScreeningJobResponse | null;
  onCancel?: () => void;
  isCancelling?: boolean;
}

function StatusIcon({ status }: { status: ScreeningJobResponse['status'] }) {
  switch (status) {
    case 'pending':
    case 'running':
      return <Loader2 className="h-4 w-4 animate-spin text-blue-500" />;
    case 'completed':
      return <CheckCircle2 className="h-4 w-4 text-green-500" />;
    case 'failed':
      return <XCircle className="h-4 w-4 text-red-500" />;
    case 'cancelled':
      return <Ban className="h-4 w-4 text-orange-500" />;
    default:
      return null;
  }
}

function formatElapsedSeconds(seconds: number): string {
  const minutes = Math.floor(seconds / 60);
  const sec = seconds % 60;
  return `${minutes}:${String(sec).padStart(2, '0')}`;
}

export function ScreeningJobProgress({ job, onCancel, isCancelling = false }: ScreeningJobProgressProps) {
  const startTime = job?.started_at ?? job?.created_at ?? null;
  const isActive = job?.status === 'pending' || job?.status === 'running';

  const [elapsed, setElapsed] = useState(0);
  useEffect(() => {
    if (!isActive || !startTime) {
      setElapsed(0);
      return;
    }

    const start = new Date(startTime).getTime();
    const update = () => setElapsed(Math.floor((Date.now() - start) / 1000));

    update();
    const timerId = setInterval(update, 1000);
    return () => clearInterval(timerId);
  }, [isActive, startTime]);

  if (!job) return null;

  const progress = job.progress == null ? null : Math.round(job.progress * 100);

  return (
    <Card className="glass-panel mb-4">
      <CardHeader className="py-3">
        <div className="flex items-center justify-between gap-3">
          <CardTitle className="text-sm flex items-center gap-2">
            <StatusIcon status={job.status} />
            Screening Job: {job.status}
          </CardTitle>
          {isActive && (
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground tabular-nums">{formatElapsedSeconds(elapsed)}</span>
              {onCancel && (
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 text-xs"
                  onClick={onCancel}
                  disabled={isCancelling}
                >
                  Cancel
                </Button>
              )}
            </div>
          )}
        </div>
      </CardHeader>
      <CardContent className="space-y-2 pt-0">
        {isActive && (
          <>
            <div className="h-2 w-full rounded-full bg-secondary overflow-hidden">
              {progress == null ? (
                <div className="h-full rounded-full bg-blue-500 animate-progress-indeterminate" />
              ) : (
                <div className="h-full rounded-full bg-blue-500 transition-all duration-300" style={{ width: `${progress}%` }} />
              )}
            </div>
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span>{job.message ?? 'Running...'}</span>
              {progress != null && <span>{progress}%</span>}
            </div>
          </>
        )}

        {job.status === 'failed' && <p className="text-xs text-red-500">{job.error ?? 'Screening failed'}</p>}
        {job.status === 'cancelled' && <p className="text-xs text-orange-500">Screening was cancelled.</p>}
      </CardContent>
    </Card>
  );
}
