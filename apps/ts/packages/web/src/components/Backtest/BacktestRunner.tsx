import { useQueryClient } from '@tanstack/react-query';
import { Play, Settings, Settings2 } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { buildEnginePolicy, EnginePolicySelector } from '@/components/EnginePolicySelector';
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
import type { EnginePolicyMode } from '@/types/backtest';
import { DefaultConfigEditor } from './DefaultConfigEditor';
import { JobProgressCard } from './JobProgressCard';
import { OptimizationJobProgressCard } from './OptimizationJobProgressCard';
import { extractGridParameterEntries, formatGridParameterValue } from './optimizationGridParams';
import { StrategySelector } from './StrategySelector';

type BacktestJobStatusData = ReturnType<typeof useJobStatus>['data'];
type OptimizationJobStatusData = ReturnType<typeof useOptimizationJobStatus>['data'];
type RunBacktestMutation = ReturnType<typeof useRunBacktest>;
type RunOptimizationMutation = ReturnType<typeof useRunOptimization>;
type CancelBacktestMutation = ReturnType<typeof useCancelBacktest>;

function isRunningStatus(status: string | null | undefined): boolean {
  return status === 'running' || status === 'pending';
}

function isBacktestTerminalStatus(status: string | null | undefined): boolean {
  return status === 'completed' || status === 'failed' || status === 'cancelled';
}

function isOptimizationTerminalStatus(status: string | null | undefined): boolean {
  return status === 'completed' || status === 'failed';
}

function StrategyInfoCard({
  displayName,
  name,
  category,
  description,
}: {
  displayName?: string | null;
  name: string;
  category: string;
  description?: string | null;
}) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base">{displayName || name}</CardTitle>
        <CardDescription>Category: {category}</CardDescription>
      </CardHeader>
      <CardContent>{description && <p className="text-sm text-muted-foreground">{description}</p>}</CardContent>
    </Card>
  );
}

function GridConfigSummary({
  paramCount,
  combinations,
  entries,
}: {
  paramCount: number;
  combinations: number;
  entries: Array<{ path: string; values: unknown[] }>;
}) {
  return (
    <div className="space-y-1">
      <p className="text-xs text-muted-foreground">
        Grid config: {paramCount} params, {combinations} combinations
      </p>
      {entries.length > 0 && (
        <div className="max-h-32 overflow-auto rounded-md border border-border/50 bg-muted/20 p-2">
          <ul className="space-y-1">
            {entries.map((entry) => (
              <li key={entry.path} className="text-xs font-mono text-muted-foreground break-all">
                {entry.path}: [{entry.values.map((value) => formatGridParameterValue(value)).join(', ')}]
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

function useInvalidateBacktestHtmlOnTerminalStatus(status: string | null | undefined): void {
  const queryClient = useQueryClient();

  useEffect(() => {
    if (!isBacktestTerminalStatus(status)) return;
    queryClient.invalidateQueries({ queryKey: backtestKeys.htmlFiles() });
  }, [status, queryClient]);
}

function useInvalidateOptimizationHtmlOnTerminalStatus(status: string | null | undefined): void {
  const queryClient = useQueryClient();

  useEffect(() => {
    if (!isOptimizationTerminalStatus(status)) return;
    queryClient.invalidateQueries({ queryKey: optimizationKeys.htmlFiles() });
  }, [status, queryClient]);
}

async function runBacktestForSelectedStrategy({
  selectedStrategy,
  runBacktest,
  setActiveJobId,
}: {
  selectedStrategy: string | null;
  runBacktest: RunBacktestMutation;
  setActiveJobId: (jobId: string | null) => void;
}): Promise<void> {
  if (!selectedStrategy) return;
  const result = await runBacktest.mutateAsync({
    strategy_name: selectedStrategy,
    engine_family: 'vectorbt',
  });
  setActiveJobId(result.job_id);
}

async function runOptimizationForSelectedStrategy({
  selectedStrategy,
  runOptimization,
  setActiveOptimizationJobId,
  enginePolicyMode,
  verificationTopK,
}: {
  selectedStrategy: string | null;
  runOptimization: RunOptimizationMutation;
  setActiveOptimizationJobId: (jobId: string | null) => void;
  enginePolicyMode: EnginePolicyMode;
  verificationTopK: string;
}): Promise<void> {
  if (!selectedStrategy) return;
  const result = await runOptimization.mutateAsync({
    strategy_name: selectedStrategy,
    engine_policy: buildEnginePolicy(enginePolicyMode, verificationTopK),
  });
  setActiveOptimizationJobId(result.job_id);
}

function BacktestExecutionSection({
  selectedStrategy,
  isRunning,
  runBacktest,
  activeJobId,
  jobStatus,
  isLoadingJob,
  cancelBacktest,
  onRunBacktest,
}: {
  selectedStrategy: string | null;
  isRunning: boolean;
  runBacktest: RunBacktestMutation;
  activeJobId: string | null;
  jobStatus: BacktestJobStatusData;
  isLoadingJob: boolean;
  cancelBacktest: CancelBacktestMutation;
  onRunBacktest: () => Promise<void>;
}) {
  return (
    <>
      <Button onClick={onRunBacktest} disabled={!selectedStrategy || isRunning} className="w-full gap-2">
        <Play className="h-4 w-4" />
        {isRunning ? 'Running...' : 'Run Backtest'}
      </Button>

      <JobProgressCard
        job={jobStatus}
        isLoading={isLoadingJob || runBacktest.isPending}
        onCancel={activeJobId ? () => cancelBacktest.mutate(activeJobId) : undefined}
        isCancelling={cancelBacktest.isPending}
      />

      {runBacktest.isError && (
        <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{runBacktest.error.message}</div>
      )}
    </>
  );
}

function OptimizationSection({
  gridConfig,
  gridParameterEntries,
  selectedStrategy,
  isOptRunning,
  onRunOptimization,
  optimizationJobStatus,
  isLoadingOptJob,
  runOptimization,
  enginePolicyMode,
  onEnginePolicyModeChange,
  optimizationVerificationTopK,
  onOptimizationVerificationTopKChange,
}: {
  gridConfig: ReturnType<typeof useOptimizationGridConfig>['data'];
  gridParameterEntries: Array<{ path: string; values: unknown[] }>;
  selectedStrategy: string | null;
  isOptRunning: boolean;
  onRunOptimization: () => Promise<void>;
  optimizationJobStatus: OptimizationJobStatusData;
  isLoadingOptJob: boolean;
  runOptimization: RunOptimizationMutation;
  enginePolicyMode: EnginePolicyMode;
  onEnginePolicyModeChange: (value: EnginePolicyMode) => void;
  optimizationVerificationTopK: string;
  onOptimizationVerificationTopKChange: (value: string) => void;
}) {
  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2">
        <Settings2 className="h-4 w-4 text-muted-foreground" />
        <span className="text-sm font-medium">Optimization</span>
      </div>

      {gridConfig ? (
        <GridConfigSummary
          paramCount={gridConfig.param_count}
          combinations={gridConfig.combinations}
          entries={gridParameterEntries}
        />
      ) : (
        <p className="text-xs text-muted-foreground">
          No grid config found. Configure in Strategies &gt; Optimize tab.
        </p>
      )}

      <EnginePolicySelector
        mode={enginePolicyMode}
        onModeChange={onEnginePolicyModeChange}
        verificationTopK={optimizationVerificationTopK}
        onVerificationTopKChange={onOptimizationVerificationTopKChange}
        disabled={isOptRunning}
      />

      <Button
        onClick={onRunOptimization}
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
  );
}

interface BacktestRunnerProps {
  selectedStrategy: string | null;
  onSelectedStrategyChange: (strategy: string | null) => void;
}

export function BacktestRunner({ selectedStrategy, onSelectedStrategyChange }: BacktestRunnerProps) {
  const [defaultConfigOpen, setDefaultConfigOpen] = useState(false);
  const [enginePolicyMode, setEnginePolicyMode] = useState<EnginePolicyMode>('fast_only');
  const [optimizationVerificationTopK, setOptimizationVerificationTopK] = useState('5');
  const { activeJobId, setActiveJobId, activeOptimizationJobId, setActiveOptimizationJobId } = useBacktestStore();
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
  const gridParameterEntries = useMemo(
    () => (gridConfig ? extractGridParameterEntries(gridConfig.content) : []),
    [gridConfig]
  );

  useInvalidateBacktestHtmlOnTerminalStatus(jobStatus?.status);
  useInvalidateOptimizationHtmlOnTerminalStatus(optimizationJobStatus?.status);

  const handleRunBacktest = () =>
    runBacktestForSelectedStrategy({
      selectedStrategy,
      runBacktest,
      setActiveJobId,
    });

  const handleRunOptimization = () =>
    runOptimizationForSelectedStrategy({
      selectedStrategy,
      runOptimization,
      setActiveOptimizationJobId,
      enginePolicyMode,
      verificationTopK: optimizationVerificationTopK,
    });

  const isRunning = runBacktest.isPending || isRunningStatus(jobStatus?.status);
  const isOptRunning = runOptimization.isPending || isRunningStatus(optimizationJobStatus?.status);

  return (
    <div className="space-y-4">
      {/* Strategy Selection */}
      <div className="space-y-2">
        <span className="text-sm font-medium">Strategy</span>
        <StrategySelector
          strategies={strategiesData?.strategies}
          isLoading={isLoadingStrategies}
          value={selectedStrategy}
          onChange={onSelectedStrategyChange}
          disabled={isRunning || isOptRunning}
        />
      </div>

      {/* Strategy Info */}
      {strategyDetail && (
        <StrategyInfoCard
          displayName={strategyDetail.display_name}
          name={strategyDetail.name}
          category={strategyDetail.category}
          description={strategyDetail.description}
        />
      )}

      {/* Default Config */}
      <Button variant="outline" size="sm" className="gap-2" onClick={() => setDefaultConfigOpen(true)}>
        <Settings className="h-4 w-4" />
        Default Config
      </Button>
      <DefaultConfigEditor open={defaultConfigOpen} onOpenChange={setDefaultConfigOpen} />

      <BacktestExecutionSection
        selectedStrategy={selectedStrategy}
        isRunning={isRunning}
        runBacktest={runBacktest}
        activeJobId={activeJobId}
        jobStatus={jobStatus}
        isLoadingJob={isLoadingJob}
        cancelBacktest={cancelBacktest}
        onRunBacktest={handleRunBacktest}
      />

      {/* Optimization Section */}
      <div className="border-t" />

      <OptimizationSection
        gridConfig={gridConfig}
        gridParameterEntries={gridParameterEntries}
        selectedStrategy={selectedStrategy}
        isOptRunning={isOptRunning}
        onRunOptimization={handleRunOptimization}
        optimizationJobStatus={optimizationJobStatus}
        isLoadingOptJob={isLoadingOptJob}
        runOptimization={runOptimization}
        enginePolicyMode={enginePolicyMode}
        onEnginePolicyModeChange={setEnginePolicyMode}
        optimizationVerificationTopK={optimizationVerificationTopK}
        onOptimizationVerificationTopKChange={setOptimizationVerificationTopK}
      />
    </div>
  );
}
