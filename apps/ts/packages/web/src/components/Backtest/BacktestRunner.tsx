import { useQueryClient } from '@tanstack/react-query';
import { Play, Settings, Settings2 } from 'lucide-react';
import { type ComponentProps, useEffect, useMemo, useState } from 'react';
import { buildEnginePolicy, EnginePolicySelector } from '@/components/EnginePolicySelector';
import {
  SectionEyebrow,
  SectionHeading,
  Surface,
} from '@/components/Layout/Workspace';
import { Button } from '@/components/ui/button';
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

type RunBacktestMutation = ReturnType<typeof useRunBacktest>;
type RunOptimizationMutation = ReturnType<typeof useRunOptimization>;
type StrategyOptions = ComponentProps<typeof StrategySelector>['strategies'];

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
    <Surface className="p-4 sm:p-5">
      <div className="space-y-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div className="space-y-1">
            <SectionEyebrow>Selected Strategy</SectionEyebrow>
            <h2 className="text-lg font-semibold tracking-tight text-foreground">{displayName || name}</h2>
            <p className="text-sm text-muted-foreground">Category: {category}</p>
          </div>
          <div className="rounded-full border border-border/70 bg-[var(--app-surface-muted)] px-3 py-1.5 text-xs font-medium capitalize text-foreground">
            {category}
          </div>
        </div>
        {description ? <p className="text-sm text-muted-foreground">{description}</p> : null}
      </div>
    </Surface>
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
    <div className="space-y-2">
      <p className="text-sm text-muted-foreground">
        Grid config: {paramCount} params, {combinations} combinations
      </p>
      {entries.length > 0 ? (
        <div className="max-h-32 overflow-auto rounded-md border border-border/50 bg-muted/20 p-2">
          <ul className="space-y-1">
            {entries.map((entry) => (
              <li key={entry.path} className="break-all font-mono text-xs text-muted-foreground">
                {entry.path}: [{entry.values.map((value) => formatGridParameterValue(value)).join(', ')}]
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
}

function InlineErrorBanner({ message }: { message: string }) {
  return <div className="rounded-xl border border-red-500/20 bg-red-500/8 px-3 py-2 text-sm text-destructive">{message}</div>;
}

function WorkspaceEmptyState({ title, description }: { title: string; description: string }) {
  return (
    <div className="rounded-2xl border border-dashed border-border/70 bg-[var(--app-surface-muted)] px-4 py-10 text-center">
      <h3 className="text-sm font-semibold text-foreground">{title}</h3>
      <p className="mx-auto mt-2 max-w-xl text-sm text-muted-foreground">{description}</p>
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

interface BacktestRunnerProps {
  selectedStrategy: string | null;
  onSelectedStrategyChange: (strategy: string | null) => void;
}

function SelectedStrategyPanel({
  strategyDetail,
  selectedStrategy,
  selectedStrategyShortName,
}: {
  strategyDetail: ReturnType<typeof useStrategy>['data'];
  selectedStrategy: string | null;
  selectedStrategyShortName: string | null;
}) {
  return (
    <div className="lg:col-span-2 lg:row-start-1">
      {strategyDetail ? (
        <StrategyInfoCard
          displayName={strategyDetail.display_name}
          name={strategyDetail.name}
          category={strategyDetail.category}
          description={strategyDetail.description}
        />
      ) : (
        <Surface className="p-4 sm:p-5">
          <div className="space-y-1">
            <SectionEyebrow>Selected Strategy</SectionEyebrow>
            <h2 className="text-lg font-semibold tracking-tight text-foreground">
              {selectedStrategyShortName ?? 'Choose a strategy to begin'}
            </h2>
            <p className="text-sm text-muted-foreground">
              {selectedStrategy
                ? 'Strategy detail will appear here once the metadata is available.'
                : 'Use the control panel to load a strategy before running backtest or optimization jobs.'}
            </p>
          </div>
        </Surface>
      )}
    </div>
  );
}

function BacktestWorkspacePanel({
  selectedStrategy,
  jobStatus,
  isLoadingJob,
  isSubmitting,
  onCancel,
  isCancelling,
}: {
  selectedStrategy: string | null;
  jobStatus: ReturnType<typeof useJobStatus>['data'];
  isLoadingJob: boolean;
  isSubmitting: boolean;
  onCancel?: () => void;
  isCancelling: boolean;
}) {
  const showProgressCard = Boolean(jobStatus) || isLoadingJob || isSubmitting;

  return (
    <Surface className="p-4 sm:p-5 lg:col-start-1 lg:row-start-2">
      <SectionHeading
        eyebrow="Workspace"
        title="Run Status"
        description="Backtest execution stays visible here while controls stay in the right panel."
      />
      <div className="mt-4">
        {showProgressCard ? (
          <JobProgressCard
            job={jobStatus}
            isLoading={isLoadingJob || isSubmitting}
            onCancel={onCancel}
            isCancelling={isCancelling}
          />
        ) : (
          <WorkspaceEmptyState
            title={selectedStrategy ? 'No active backtest run' : 'No strategy selected'}
            description={
              selectedStrategy
                ? 'Run the selected strategy to populate live status and terminal performance here.'
                : 'Choose a strategy in the right panel to enable backtest execution.'
            }
          />
        )}
      </div>
    </Surface>
  );
}

function OptimizationWorkspacePanel({
  selectedStrategy,
  gridConfig,
  gridParameterEntries,
  optimizationJobStatus,
  isLoadingOptJob,
  isSubmitting,
}: {
  selectedStrategy: string | null;
  gridConfig: ReturnType<typeof useOptimizationGridConfig>['data'];
  gridParameterEntries: Array<{ path: string; values: unknown[] }>;
  optimizationJobStatus: ReturnType<typeof useOptimizationJobStatus>['data'];
  isLoadingOptJob: boolean;
  isSubmitting: boolean;
}) {
  const showProgressCard = Boolean(optimizationJobStatus) || isLoadingOptJob || isSubmitting;

  return (
    <Surface className="p-4 sm:p-5 lg:col-start-2 lg:row-start-2">
      <SectionHeading
        eyebrow="Workspace"
        title="Optimization Status"
        description="Grid search details, verification stage, and terminal summaries stay in view here."
      />
      <div className="mt-4 space-y-4">
        {gridConfig ? (
          <div className="rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] p-4">
            <GridConfigSummary
              paramCount={gridConfig.param_count}
              combinations={gridConfig.combinations}
              entries={gridParameterEntries}
            />
          </div>
        ) : (
          <div className="rounded-2xl border border-dashed border-border/70 bg-[var(--app-surface-muted)] px-4 py-5">
            <p className="text-sm text-muted-foreground">No grid config found. Configure in Strategies &gt; Optimize tab.</p>
          </div>
        )}

        {showProgressCard ? (
          <OptimizationJobProgressCard job={optimizationJobStatus} isLoading={isLoadingOptJob || isSubmitting} />
        ) : (
          <WorkspaceEmptyState
            title={selectedStrategy ? 'No active optimization run' : 'Optimization stays idle until a strategy is selected'}
            description={
              selectedStrategy
                ? 'Launch optimization from the right panel to monitor stage progress and final candidates here.'
                : 'Choose a strategy and load grid settings before starting optimization.'
            }
          />
        )}
      </div>
    </Surface>
  );
}

function RunnerControlPanel({
  strategies,
  isLoadingStrategies,
  selectedStrategy,
  onSelectedStrategyChange,
  isRunning,
  isOptRunning,
  onOpenDefaultConfig,
  onRunBacktest,
  runBacktestErrorMessage,
  gridConfig,
  enginePolicyMode,
  onEnginePolicyModeChange,
  optimizationVerificationTopK,
  onOptimizationVerificationTopKChange,
  onRunOptimization,
  runOptimizationErrorMessage,
}: {
  strategies: StrategyOptions;
  isLoadingStrategies: boolean;
  selectedStrategy: string | null;
  onSelectedStrategyChange: (strategy: string | null) => void;
  isRunning: boolean;
  isOptRunning: boolean;
  onOpenDefaultConfig: () => void;
  onRunBacktest: () => Promise<void>;
  runBacktestErrorMessage: string | null;
  gridConfig: ReturnType<typeof useOptimizationGridConfig>['data'];
  enginePolicyMode: EnginePolicyMode;
  onEnginePolicyModeChange: (value: EnginePolicyMode) => void;
  optimizationVerificationTopK: string;
  onOptimizationVerificationTopKChange: (value: string) => void;
  onRunOptimization: () => Promise<void>;
  runOptimizationErrorMessage: string | null;
}) {
  return (
    <div className="lg:row-span-2 lg:col-start-3 lg:row-start-1 lg:sticky lg:top-0 lg:self-start">
      <Surface className="space-y-5 p-4 sm:p-5">
        <div className="space-y-1">
          <SectionEyebrow>Control Panel</SectionEyebrow>
          <h2 className="text-base font-semibold tracking-tight text-foreground">Run Setup</h2>
          <p className="text-sm text-muted-foreground">
            Choose a strategy, then send work to the backtest and optimization workspaces.
          </p>
        </div>

        <div className="space-y-2">
          <span className="text-sm font-medium">Strategy</span>
          <StrategySelector
            strategies={strategies}
            isLoading={isLoadingStrategies}
            value={selectedStrategy}
            onChange={onSelectedStrategyChange}
            disabled={isRunning || isOptRunning}
          />
        </div>

        <Button variant="outline" size="sm" className="w-full gap-2" onClick={onOpenDefaultConfig}>
          <Settings className="h-4 w-4" />
          Default Config
        </Button>

        <div className="space-y-3 border-t border-border/70 pt-4">
          <div className="space-y-1">
            <SectionEyebrow>Execution</SectionEyebrow>
            <h3 className="text-sm font-semibold text-foreground">Backtest</h3>
            <p className="text-xs text-muted-foreground">
              Launch the selected strategy and monitor the active job in the workspace pane.
            </p>
          </div>
          <Button onClick={onRunBacktest} disabled={!selectedStrategy || isRunning} className="w-full gap-2">
            <Play className="h-4 w-4" />
            {isRunning ? 'Running...' : 'Run Backtest'}
          </Button>
          {runBacktestErrorMessage ? <InlineErrorBanner message={runBacktestErrorMessage} /> : null}
        </div>

        <div className="space-y-3 border-t border-border/70 pt-4">
          <div className="space-y-1">
            <SectionEyebrow>Execution</SectionEyebrow>
            <h3 className="text-sm font-semibold text-foreground">Optimization</h3>
            <p className="text-xs text-muted-foreground">
              Keep grid search controls here and follow progress in the main workspace.
            </p>
          </div>

          {gridConfig ? (
            <p className="text-xs text-muted-foreground">
              Grid config: {gridConfig.param_count} params, {gridConfig.combinations} combinations
            </p>
          ) : (
            <p className="text-xs text-muted-foreground">No grid config found. Configure in Strategies &gt; Optimize tab.</p>
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
          {runOptimizationErrorMessage ? <InlineErrorBanner message={runOptimizationErrorMessage} /> : null}
        </div>
      </Surface>
    </div>
  );
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
  const selectedStrategyShortName = selectedStrategy ? (selectedStrategy.split('/').pop() ?? selectedStrategy) : null;
  const cancelBacktestAction = activeJobId ? () => cancelBacktest.mutate(activeJobId) : undefined;

  return (
    <>
      <div className="grid grid-cols-1 gap-3 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_minmax(19.5rem,22rem)] lg:items-start">
        <SelectedStrategyPanel
          strategyDetail={strategyDetail}
          selectedStrategy={selectedStrategy}
          selectedStrategyShortName={selectedStrategyShortName}
        />
        <BacktestWorkspacePanel
          selectedStrategy={selectedStrategy}
          jobStatus={jobStatus}
          isLoadingJob={isLoadingJob}
          isSubmitting={runBacktest.isPending}
          onCancel={cancelBacktestAction}
          isCancelling={cancelBacktest.isPending}
        />
        <OptimizationWorkspacePanel
          selectedStrategy={selectedStrategy}
          gridConfig={gridConfig}
          gridParameterEntries={gridParameterEntries}
          optimizationJobStatus={optimizationJobStatus}
          isLoadingOptJob={isLoadingOptJob}
          isSubmitting={runOptimization.isPending}
        />
        <RunnerControlPanel
          strategies={strategiesData?.strategies}
          isLoadingStrategies={isLoadingStrategies}
          selectedStrategy={selectedStrategy}
          onSelectedStrategyChange={onSelectedStrategyChange}
          isRunning={isRunning}
          isOptRunning={isOptRunning}
          onOpenDefaultConfig={() => setDefaultConfigOpen(true)}
          onRunBacktest={handleRunBacktest}
          runBacktestErrorMessage={runBacktest.isError ? runBacktest.error.message : null}
          gridConfig={gridConfig}
          enginePolicyMode={enginePolicyMode}
          onEnginePolicyModeChange={setEnginePolicyMode}
          optimizationVerificationTopK={optimizationVerificationTopK}
          onOptimizationVerificationTopKChange={setOptimizationVerificationTopK}
          onRunOptimization={handleRunOptimization}
          runOptimizationErrorMessage={runOptimization.isError ? runOptimization.error.message : null}
        />
      </div>

      <DefaultConfigEditor open={defaultConfigOpen} onOpenChange={setDefaultConfigOpen} />
    </>
  );
}
