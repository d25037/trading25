import { CheckCircle2, Loader2, RefreshCw, Server, XCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { useBacktestHealth, useJobs } from '@/hooks/useBacktest';
import { useBacktestStore } from '@/stores/backtestStore';
import { JobsTable } from './JobsTable';

export function BacktestStatus() {
  const { setActiveSubTab, setSelectedResultJobId } = useBacktestStore();
  const { data: health, isLoading: isLoadingHealth, refetch: refetchHealth } = useBacktestHealth();
  const { data: jobs, isLoading: isLoadingJobs, refetch: refetchJobs } = useJobs(20);

  const handleViewJob = (jobId: string) => {
    setSelectedResultJobId(jobId);
    setActiveSubTab('results');
  };

  const handleRefresh = () => {
    refetchHealth();
    refetchJobs();
  };

  return (
    <div className="space-y-6">
      {/* Server Health */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between pb-2">
          <div>
            <CardTitle className="text-base flex items-center gap-2">
              <Server className="h-4 w-4" />
              Backtest Server
            </CardTitle>
            <CardDescription>trading25-bt FastAPI server status</CardDescription>
          </div>
          <Button variant="outline" size="sm" onClick={handleRefresh}>
            <RefreshCw className="h-4 w-4 mr-2" />
            Refresh
          </Button>
        </CardHeader>
        <CardContent>
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
              <span className="text-sm text-muted-foreground ml-2">Make sure bt server is running on port 3002</span>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Recent Jobs */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Recent Jobs</CardTitle>
          <CardDescription>Last 20 backtest jobs</CardDescription>
        </CardHeader>
        <CardContent>
          <JobsTable jobs={jobs} isLoading={isLoadingJobs} onSelectJob={handleViewJob} />
        </CardContent>
      </Card>
    </div>
  );
}
