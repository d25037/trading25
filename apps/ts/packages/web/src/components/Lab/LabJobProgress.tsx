import { Ban, CheckCircle2, Loader2, XCircle } from 'lucide-react';
import { useEffect, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import type { JobStatus } from '@/types/backtest';

interface LabJobProgressProps {
  status: JobStatus | null;
  progress: number | null;
  message: string | null;
  error?: string | null;
  createdAt?: string;
  startedAt?: string | null;
  onCancel?: () => void;
  isCancelling?: boolean;
}

function ProgressStatusIcon({ status }: { status: JobStatus }) {
  switch (status) {
    case 'pending':
    case 'running':
      return <Loader2 className="h-5 w-5 animate-spin text-blue-500" />;
    case 'completed':
      return <CheckCircle2 className="h-5 w-5 text-green-500" />;
    case 'failed':
      return <XCircle className="h-5 w-5 text-red-500" />;
    case 'cancelled':
      return <Ban className="h-5 w-5 text-orange-500" />;
    default:
      return null;
  }
}

export function LabJobProgress({
  status,
  progress,
  message,
  error,
  createdAt,
  startedAt,
  onCancel,
  isCancelling,
}: LabJobProgressProps) {
  const isActive = status === 'pending' || status === 'running';

  const [elapsed, setElapsed] = useState(0);
  useEffect(() => {
    if (!isActive) return;
    const startTime = startedAt ?? createdAt;
    if (!startTime) return;
    const start = new Date(startTime).getTime();
    const update = () => setElapsed(Math.floor((Date.now() - start) / 1000));
    update();
    const id = setInterval(update, 1000);
    return () => clearInterval(id);
  }, [isActive, startedAt, createdAt]);

  if (!status) return null;

  const formatElapsed = (s: number) => {
    const m = Math.floor(s / 60);
    const sec = s % 60;
    return `${m}:${String(sec).padStart(2, '0')}`;
  };

  const progressPercent = progress != null ? Math.round(progress * 100) : null;

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <ProgressStatusIcon status={status} />
            <CardTitle className="text-lg capitalize">{status}</CardTitle>
          </div>
          {isActive && (
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">{formatElapsed(elapsed)}</span>
              {onCancel && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={onCancel}
                  disabled={isCancelling}
                  className="h-7 gap-1 text-xs text-muted-foreground hover:text-red-500"
                >
                  <XCircle className="h-3.5 w-3.5" />
                  Cancel
                </Button>
              )}
            </div>
          )}
        </div>
      </CardHeader>
      <CardContent>
        {isActive && (
          <div className="space-y-2">
            <div className="h-2 w-full rounded-full bg-secondary overflow-hidden">
              {progressPercent != null ? (
                <div
                  className="h-full rounded-full bg-blue-500 transition-all duration-300"
                  style={{ width: `${progressPercent}%` }}
                />
              ) : (
                <div className="h-full rounded-full bg-blue-500 animate-progress-indeterminate" />
              )}
            </div>
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              {message && <span>{message}</span>}
              {progressPercent != null && <span>{progressPercent}%</span>}
            </div>
          </div>
        )}

        {status === 'failed' && error && (
          <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{error}</div>
        )}

        {status === 'cancelled' && (
          <div className="rounded-md bg-orange-500/10 p-3 text-sm text-orange-500">Job was cancelled</div>
        )}

        {status === 'completed' && (
          <div className="rounded-md bg-green-500/10 p-3 text-sm text-green-500">
            {message ?? 'Job completed successfully'}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
