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

function FetchInfoRow({ endpoint, method }: FetchProgressInfo) {
  if (!method && !endpoint) {
    return null;
  }

  return (
    <div className="flex flex-wrap items-center gap-2 text-xs">
      <span className="text-muted-foreground">Fetch</span>
      {method && <span className={`rounded px-2 py-0.5 font-medium ${getMethodBadgeClass(method)}`}>{method}</span>}
      {endpoint && <code className="rounded bg-muted px-1.5 py-0.5 text-[11px]">{endpoint}</code>}
    </div>
  );
}

function LatestFetchDetailCard({ detail }: { detail: SyncFetchDetail | null | undefined }) {
  if (!detail) {
    return null;
  }

  return (
    <div className="space-y-1 rounded-md border bg-muted/20 p-2 text-xs">
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-muted-foreground">Detail</span>
        <span className="rounded bg-muted px-1.5 py-0.5 uppercase">{detail.eventType}</span>
        <span>{new Date(detail.timestamp).toLocaleTimeString()}</span>
      </div>
      {(detail.reason || detail.reasonDetail) && (
        <p className="text-muted-foreground">
          {detail.reason}
          {detail.reasonDetail ? ` (${detail.reasonDetail})` : ''}
        </p>
      )}
      {(detail.estimatedRestCalls !== undefined || detail.estimatedBulkCalls !== undefined) && (
        <p className="text-muted-foreground">
          REST est: {detail.estimatedRestCalls ?? 'n/a'}, BULK est: {detail.estimatedBulkCalls ?? 'n/a'}
        </p>
      )}
      {detail.fallback && (
        <p className="text-amber-600">bulk fallback{detail.fallbackReason ? `: ${detail.fallbackReason}` : ''}</p>
      )}
    </div>
  );
}

function RecentFetchEvents({ items }: { items: SyncFetchDetail[] }) {
  if (items.length === 0) {
    return null;
  }

  return (
    <div className="space-y-1 rounded-md border bg-muted/20 p-2 text-xs">
      <p className="text-muted-foreground">Recent Fetch Events</p>
      {items.map((item) => {
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
  );
}

function ActiveProgressSection({
  isActive,
  progress,
  fetchInfo,
  latestFetchDetail,
  recentFetchDetails,
}: {
  isActive: boolean;
  progress: SyncJobResponse['progress'];
  fetchInfo: FetchProgressInfo;
  latestFetchDetail: SyncFetchDetail | null | undefined;
  recentFetchDetails: SyncFetchDetail[];
}) {
  if (!isActive || !progress) {
    return null;
  }

  return (
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
      <FetchInfoRow endpoint={fetchInfo.endpoint} method={fetchInfo.method} />
      <LatestFetchDetailCard detail={latestFetchDetail} />
      <RecentFetchEvents items={recentFetchDetails} />
      <p className="text-xs text-muted-foreground">{progress.message}</p>
    </div>
  );
}

function CompletedResultSection({
  mode,
  status,
  result,
}: {
  mode: SyncJobResponse['mode'];
  status: SyncJobResponse['status'];
  result: SyncJobResponse['result'];
}) {
  if (status !== 'completed' || !result) {
    return null;
  }

  const stocksLabel = mode === 'repair' ? 'Stocks Refreshed:' : 'Stocks Updated:';
  const hasErrors = result.errors.length > 0;
  const visibleErrors = result.errors.slice(0, 3);

  return (
    <div className="space-y-2 text-sm">
      <div className="grid grid-cols-2 gap-2">
        <div>
          <span className="text-muted-foreground">API Calls:</span>
          <span className="ml-2 font-medium">{result.totalApiCalls}</span>
        </div>
        <div>
          <span className="text-muted-foreground">{stocksLabel}</span>
          <span className="ml-2 font-medium">{result.stocksUpdated}</span>
        </div>
        <div>
          <span className="text-muted-foreground">Dates Processed:</span>
          <span className="ml-2 font-medium">{result.datesProcessed}</span>
        </div>
        <div>
          <span className="text-muted-foreground">Fundamentals Updated:</span>
          <span className="ml-2 font-medium">{result.fundamentalsUpdated}</span>
        </div>
        {result.failedDates.length > 0 && (
          <div>
            <span className="text-muted-foreground">Failed Dates:</span>
            <span className="ml-2 font-medium text-red-500">{result.failedDates.length}</span>
          </div>
        )}
        {hasErrors && (
          <div>
            <span className="text-muted-foreground">Errors:</span>
            <span className="ml-2 font-medium text-red-500">{result.errors.length}</span>
          </div>
        )}
      </div>
      {hasErrors && (
        <div className="rounded-md bg-red-500/10 p-3 text-xs text-red-600">
          {visibleErrors.join(' | ')}
        </div>
      )}
    </div>
  );
}

function SyncErrorSection({ status, error }: { status: SyncJobResponse['status']; error: string | null | undefined }) {
  if (status !== 'failed' || !error) {
    return null;
  }

  return <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{error}</div>;
}

function CancelledSection({ status }: { status: SyncJobResponse['status'] }) {
  if (status !== 'cancelled') {
    return null;
  }
  return <div className="text-sm text-muted-foreground">Sync was cancelled by user.</div>;
}

export function SyncStatusCard({ job, fetchDetails, isLoading, onCancel, isCancelling }: SyncStatusCardProps) {
  if (!job) return null;

  const isActive = job.status === 'pending' || job.status === 'running';
  const progress = job.progress;
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
        <ActiveProgressSection
          isActive={isActive}
          progress={progress}
          fetchInfo={fetchInfo}
          latestFetchDetail={latestFetchDetail}
          recentFetchDetails={recentFetchDetails}
        />
        <CompletedResultSection mode={job.mode} status={job.status} result={job.result} />
        <SyncErrorSection status={job.status} error={job.error} />
        <CancelledSection status={job.status} />
      </CardContent>
    </Card>
  );
}
