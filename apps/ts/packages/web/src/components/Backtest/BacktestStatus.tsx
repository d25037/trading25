import { CheckCircle2, Loader2, RefreshCw, Server, XCircle } from 'lucide-react';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { Button } from '@/components/ui/button';
import { useBacktestHealth, useJobs } from '@/hooks/useBacktest';
import { JobsTable } from './JobsTable';

interface BacktestStatusProps {
  onViewJob: (jobId: string) => void;
}

export function BacktestStatus({ onViewJob }: BacktestStatusProps) {
  const { data: health, isLoading: isLoadingHealth, refetch: refetchHealth } = useBacktestHealth();
  const { data: jobs, isLoading: isLoadingJobs, refetch: refetchJobs } = useJobs(20);

  const handleRefresh = () => {
    refetchHealth();
    refetchJobs();
  };

  return (
    <div className="space-y-3">
      <Surface className="p-4 sm:p-5">
        <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
          <div>
            <SectionEyebrow>Operations</SectionEyebrow>
            <h2 className="mt-2 flex items-center gap-2 text-base font-semibold tracking-tight text-foreground">
              <Server className="h-4 w-4" />
              Backtest Server
            </h2>
            <p className="mt-1 text-sm text-muted-foreground">trading25-bt FastAPI server status</p>
          </div>
          <Button variant="outline" size="sm" onClick={handleRefresh}>
            <RefreshCw className="mr-2 h-4 w-4" />
            Refresh
          </Button>
        </div>

        <div className="mt-4">
          {isLoadingHealth ? (
            <div className="flex items-center gap-2">
              <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
              <span className="text-sm text-muted-foreground">Checking...</span>
            </div>
          ) : health ? (
            <div className="space-y-2">
              <div className="flex items-center gap-2">
                <CheckCircle2 className="h-5 w-5 text-green-500" />
                <span className="font-medium text-green-500">Connected</span>
              </div>
              <div className="grid grid-cols-2 gap-2 text-sm">
                <div>
                  <span className="text-muted-foreground">Service:</span>
                  <span className="ml-2">{health.service}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Version:</span>
                  <span className="ml-2">{health.version}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Status:</span>
                  <span className="ml-2">{health.status}</span>
                </div>
              </div>
            </div>
          ) : (
            <div className="flex items-center gap-2">
              <XCircle className="h-5 w-5 text-red-500" />
              <span className="font-medium text-red-500">Disconnected</span>
              <span className="ml-2 text-sm text-muted-foreground">Make sure bt server is running on port 3002</span>
            </div>
          )}
        </div>
      </Surface>

      <Surface className="p-4 sm:p-5">
        <div className="space-y-1">
          <SectionEyebrow>History</SectionEyebrow>
          <h2 className="text-base font-semibold tracking-tight text-foreground">Recent Jobs</h2>
          <p className="text-sm text-muted-foreground">Last 20 backtest jobs</p>
        </div>
        <div className="mt-4">
          <JobsTable jobs={jobs} isLoading={isLoadingJobs} onSelectJob={onViewJob} />
        </div>
      </Surface>
    </div>
  );
}
