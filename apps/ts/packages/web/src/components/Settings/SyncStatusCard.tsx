import { AlertCircle, CheckCircle2, Loader2, XCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import type { SyncFetchDetail, SyncFetchDetailsResponse, SyncJobResponse } from '@/types/sync';

interface SyncStatusCardProps {
  job: SyncJobResponse | null | undefined;
  fetchDetails?: SyncFetchDetailsResponse | null;
  isLoading: boolean;
  onCancel: () => void;
  isCancelling: boolean;
}

interface FetchProgressInfo {
  endpoint: string | null;
  method: 'BULK' | 'REST' | null;
}

const FETCH_ENDPOINT_PATTERN = /\/[a-z0-9-]+(?:\/[a-z0-9-]+)+/i;
const FETCH_METHOD_PATTERN = /\b(BULK|REST)\b/i;

function parseFetchProgressInfo(message: string): FetchProgressInfo {
  const endpointMatch = message.match(FETCH_ENDPOINT_PATTERN);
  const methodMatch = message.match(FETCH_METHOD_PATTERN);

  const endpoint = endpointMatch?.[0] ?? null;
  const methodRaw = methodMatch?.[1]?.toUpperCase();
  const method = methodRaw === 'BULK' || methodRaw === 'REST' ? methodRaw : null;

  return { endpoint, method };
}

function getMethodBadgeClass(method: FetchProgressInfo['method']): string {
  if (method === 'BULK') {
    return 'bg-emerald-500/15 text-emerald-700';
  }
  return 'bg-blue-500/15 text-blue-700';
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

function toDisplayMethod(method: SyncFetchDetail['method'] | null | undefined): FetchProgressInfo['method'] {
  if (method === 'bulk') {
    return 'BULK';
  }
  if (method === 'rest') {
    return 'REST';
  }
  return null;
}

export function SyncStatusCard({ job, fetchDetails, isLoading, onCancel, isCancelling }: SyncStatusCardProps) {
  if (!job) return null;

  const isActive = job.status === 'pending' || job.status === 'running';
  const progress = job.progress;
  const result = job.result;
  const parsedFetchInfo = progress ? parseFetchProgressInfo(progress.message) : { endpoint: null, method: null };
  const latestFetchDetail = fetchDetails?.latest;
  const fetchInfo = {
    endpoint: latestFetchDetail?.endpoint ?? parsedFetchInfo.endpoint,
    method: toDisplayMethod(latestFetchDetail?.method) ?? parsedFetchInfo.method,
  };
  const recentFetchDetails = fetchDetails?.items.slice(-5).reverse() ?? [];

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
            {(fetchInfo.method || fetchInfo.endpoint) && (
              <div className="flex flex-wrap items-center gap-2 text-xs">
                <span className="text-muted-foreground">Fetch</span>
                {fetchInfo.method && (
                  <span className={`rounded px-2 py-0.5 font-medium ${getMethodBadgeClass(fetchInfo.method)}`}>
                    {fetchInfo.method}
                  </span>
                )}
                {fetchInfo.endpoint && (
                  <code className="rounded bg-muted px-1.5 py-0.5 text-[11px]">{fetchInfo.endpoint}</code>
                )}
              </div>
            )}
            {latestFetchDetail && (
              <div className="space-y-1 rounded-md border bg-muted/20 p-2 text-xs">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="text-muted-foreground">Detail</span>
                  <span className="rounded bg-muted px-1.5 py-0.5 uppercase">{latestFetchDetail.eventType}</span>
                  <span>{new Date(latestFetchDetail.timestamp).toLocaleTimeString()}</span>
                </div>
                {(latestFetchDetail.reason || latestFetchDetail.reasonDetail) && (
                  <p className="text-muted-foreground">
                    {latestFetchDetail.reason}
                    {latestFetchDetail.reasonDetail ? ` (${latestFetchDetail.reasonDetail})` : ''}
                  </p>
                )}
                {(latestFetchDetail.estimatedRestCalls !== undefined || latestFetchDetail.estimatedBulkCalls !== undefined) && (
                  <p className="text-muted-foreground">
                    REST est: {latestFetchDetail.estimatedRestCalls ?? 'n/a'}, BULK est:{' '}
                    {latestFetchDetail.estimatedBulkCalls ?? 'n/a'}
                  </p>
                )}
                {latestFetchDetail.fallback && (
                  <p className="text-amber-600">
                    bulk fallback{latestFetchDetail.fallbackReason ? `: ${latestFetchDetail.fallbackReason}` : ''}
                  </p>
                )}
              </div>
            )}
            {recentFetchDetails.length > 0 && (
              <div className="space-y-1 rounded-md border bg-muted/20 p-2 text-xs">
                <p className="text-muted-foreground">Recent Fetch Events</p>
                {recentFetchDetails.map((item) => {
                  const displayMethod = toDisplayMethod(item.method) ?? 'REST';
                  return (
                    <div
                      key={`${item.timestamp}-${item.stage}-${item.endpoint}-${item.eventType}`}
                      className="flex flex-wrap items-center gap-2"
                    >
                      <span className="rounded bg-muted px-1 py-0.5 uppercase">{item.eventType}</span>
                      <span className={`rounded px-1 py-0.5 ${getMethodBadgeClass(displayMethod)}`}>{displayMethod}</span>
                      <code className="rounded bg-muted px-1 py-0.5">{item.endpoint}</code>
                      <span className="text-muted-foreground">{item.stage}</span>
                    </div>
                  );
                })}
              </div>
            )}
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
