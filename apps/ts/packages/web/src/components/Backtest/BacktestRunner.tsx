import { useQueryClient } from '@tanstack/react-query';
import { Play, Settings, Settings2 } from 'lucide-react';
import { useEffect, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import {
  backtestKeys,
  useCancelBacktest,
  useJobStatus,
  useRunBacktest,
  useStrategies,
  useStrategy,
} from '@/hooks/useBacktest';
import {
  optimizationKeys,
  useOptimizationGridConfig,
  useOptimizationJobStatus,
  useRunOptimization,
} from '@/hooks/useOptimization';
import { useBacktestStore } from '@/stores/backtestStore';
import { DefaultConfigEditor } from './DefaultConfigEditor';
import { JobProgressCard } from './JobProgressCard';
import { OptimizationJobProgressCard } from './OptimizationJobProgressCard';
import { StrategySelector } from './StrategySelector';

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: UI component with conditional rendering
export function BacktestRunner() {
  const [defaultConfigOpen, setDefaultConfigOpen] = useState(false);
  const queryClient = useQueryClient();
  const {
    selectedStrategy,
    setSelectedStrategy,
    activeJobId,
    setActiveJobId,
    activeOptimizationJobId,
    setActiveOptimizationJobId,
  } = useBacktestStore();
  const { data: strategiesData, isLoading: isLoadingStrategies } = useStrategies();
  const { data: strategyDetail } = useStrategy(selectedStrategy);
  const { data: jobStatus, isLoading: isLoadingJob } = useJobStatus(activeJobId);
  const runBacktest = useRunBacktest();
  const cancelBacktest = useCancelBacktest();

  // Optimization
  const { data: optimizationJobStatus, isLoading: isLoadingOptJob } = useOptimizationJobStatus(activeOptimizationJobId);
  const runOptimization = useRunOptimization();
  const { data: gridConfig } = useOptimizationGridConfig(
    selectedStrategy ? (selectedStrategy.split('/').pop() ?? selectedStrategy) : null
  );

  useEffect(() => {
    if (jobStatus?.status === 'completed' || jobStatus?.status === 'failed' || jobStatus?.status === 'cancelled') {
      queryClient.invalidateQueries({ queryKey: backtestKeys.htmlFiles() });
    }
  }, [jobStatus?.status, queryClient]);

  useEffect(() => {
    if (optimizationJobStatus?.status === 'completed' || optimizationJobStatus?.status === 'failed') {
      queryClient.invalidateQueries({ queryKey: optimizationKeys.htmlFiles() });
    }
  }, [optimizationJobStatus?.status, queryClient]);

  const handleRunBacktest = async () => {
    if (!selectedStrategy) return;

    const result = await runBacktest.mutateAsync({
      strategy_name: selectedStrategy,
    });
    setActiveJobId(result.job_id);
  };

  const handleRunOptimization = async () => {
    if (!selectedStrategy) return;

    const result = await runOptimization.mutateAsync({
      strategy_name: selectedStrategy,
    });
    setActiveOptimizationJobId(result.job_id);
  };

  const isRunning = runBacktest.isPending || jobStatus?.status === 'running' || jobStatus?.status === 'pending';

  const isOptRunning =
    runOptimization.isPending ||
    optimizationJobStatus?.status === 'running' ||
    optimizationJobStatus?.status === 'pending';

  return (
    <div className="space-y-4">
      {/* Strategy Selection */}
      <div className="space-y-2">
        <span className="text-sm font-medium">Strategy</span>
        <StrategySelector
          strategies={strategiesData?.strategies}
          isLoading={isLoadingStrategies}
          value={selectedStrategy}
          onChange={setSelectedStrategy}
          disabled={isRunning || isOptRunning}
        />
      </div>

      {/* Strategy Info */}
      {strategyDetail && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">{strategyDetail.display_name || strategyDetail.name}</CardTitle>
            <CardDescription>Category: {strategyDetail.category}</CardDescription>
          </CardHeader>
          <CardContent>
            {strategyDetail.description && (
              <p className="text-sm text-muted-foreground">{strategyDetail.description}</p>
            )}
          </CardContent>
        </Card>
      )}

      {/* Default Config */}
      <Button variant="outline" size="sm" className="gap-2" onClick={() => setDefaultConfigOpen(true)}>
        <Settings className="h-4 w-4" />
        Default Config
      </Button>
      <DefaultConfigEditor open={defaultConfigOpen} onOpenChange={setDefaultConfigOpen} />

      {/* Run Button */}
      <Button onClick={handleRunBacktest} disabled={!selectedStrategy || isRunning} className="w-full gap-2">
        <Play className="h-4 w-4" />
        {isRunning ? 'Running...' : 'Run Backtest'}
      </Button>

      {/* Progress Card */}
      <JobProgressCard
        job={jobStatus}
        isLoading={isLoadingJob || runBacktest.isPending}
        onCancel={activeJobId ? () => cancelBacktest.mutate(activeJobId) : undefined}
        isCancelling={cancelBacktest.isPending}
      />

      {/* Error Message */}
      {runBacktest.isError && (
        <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{runBacktest.error.message}</div>
      )}

      {/* Optimization Section */}
      <div className="border-t" />

      <div className="space-y-3">
        <div className="flex items-center gap-2">
          <Settings2 className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium">Optimization</span>
        </div>

        {gridConfig ? (
          <p className="text-xs text-muted-foreground">
            Grid config: {gridConfig.param_count} params, {gridConfig.combinations} combinations
          </p>
        ) : (
          <p className="text-xs text-muted-foreground">
            No grid config found. Configure in Strategies &gt; Optimize tab.
          </p>
        )}

        <Button
          onClick={handleRunOptimization}
          disabled={!selectedStrategy || !gridConfig || isOptRunning}
          variant="outline"
          className="w-full gap-2"
        >
          <Settings2 className="h-4 w-4" />
          {isOptRunning ? 'Optimizing...' : 'Run Optimization'}
        </Button>

        <OptimizationJobProgressCard
          job={optimizationJobStatus}
          isLoading={isLoadingOptJob || runOptimization.isPending}
        />

        {runOptimization.isError && (
          <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{runOptimization.error.message}</div>
        )}
      </div>
    </div>
  );
}
