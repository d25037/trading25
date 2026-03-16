import { FlaskConical } from 'lucide-react';
import { useState } from 'react';
import { StrategySelector } from '@/components/Backtest/StrategySelector';
import { useStrategies } from '@/hooks/useBacktest';
import {
  useCancelLabJob,
  useLabEvolve,
  useLabGenerate,
  useLabImprove,
  useLabJobStatus,
  useLabJobs,
  useLabOptimize,
} from '@/hooks/useLab';
import { useLabSSE } from '@/hooks/useLabSSE';
import { useBacktestStore } from '@/stores/backtestStore';
import type { LabType } from '@/types/backtest';
import { LabEvolveForm } from './LabEvolveForm';
import { LabGenerateForm } from './LabGenerateForm';
import { LabImproveForm } from './LabImproveForm';
import { LabJobHistoryTable } from './LabJobHistoryTable';
import { LabJobProgress } from './LabJobProgress';
import { LabOperationSelector } from './LabOperationSelector';
import { LabOptimizeForm } from './LabOptimizeForm';
import { LabResultSection } from './LabResultSection';

function tabButtonClass(isActive: boolean): string {
  return `px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
    isActive ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'
  }`;
}

function isRunningStatus(status: string | null | undefined): boolean {
  return status === 'pending' || status === 'running';
}

function resolveIsJobActive(params: {
  generatePending: boolean;
  evolvePending: boolean;
  optimizePending: boolean;
  improvePending: boolean;
  sseStatus: string | null | undefined;
  jobStatus: string | null | undefined;
}): boolean {
  return (
    params.generatePending ||
    params.evolvePending ||
    params.optimizePending ||
    params.improvePending ||
    isRunningStatus(params.sseStatus) ||
    isRunningStatus(params.jobStatus)
  );
}

interface LabOperationFormProps {
  operation: LabType;
  strategyName: string | null;
  disabled: boolean;
  onGenerateSubmit: (request: unknown) => Promise<void>;
  onEvolveSubmit: (request: unknown) => Promise<void>;
  onOptimizeSubmit: (request: unknown) => Promise<void>;
  onImproveSubmit: (request: unknown) => Promise<void>;
}

function LabOperationForm({
  operation,
  strategyName,
  disabled,
  onGenerateSubmit,
  onEvolveSubmit,
  onOptimizeSubmit,
  onImproveSubmit,
}: LabOperationFormProps) {
  switch (operation) {
    case 'generate':
      return <LabGenerateForm onSubmit={onGenerateSubmit} disabled={disabled} />;
    case 'evolve':
      return <LabEvolveForm strategyName={strategyName} onSubmit={onEvolveSubmit} disabled={disabled} />;
    case 'optimize':
      return <LabOptimizeForm strategyName={strategyName} onSubmit={onOptimizeSubmit} disabled={disabled} />;
    case 'improve':
      return <LabImproveForm strategyName={strategyName} onSubmit={onImproveSubmit} disabled={disabled} />;
    default:
      return null;
  }
}

interface LabPanelProps {
  selectedStrategy: string | null;
  onSelectedStrategyChange: (strategy: string | null) => void;
  operation: LabType;
  onOperationChange: (operation: LabType) => void;
}

export function LabPanel({
  selectedStrategy,
  onSelectedStrategyChange,
  operation,
  onOperationChange,
}: LabPanelProps) {
  const [activeTab, setActiveTab] = useState<'run' | 'history'>('run');
  const { activeLabJobId, setActiveLabJobId } = useBacktestStore();
  const { data: strategiesData, isLoading: isLoadingStrategies } = useStrategies();
  const {
    data: labJobs,
    isLoading: isLoadingLabJobs,
    isFetching: isFetchingLabJobs,
    refetch: refetchLabJobs,
  } = useLabJobs(30);

  const sse = useLabSSE(activeLabJobId);
  const { data: jobStatus } = useLabJobStatus(activeLabJobId, sse.isConnected);

  const labGenerate = useLabGenerate();
  const labEvolve = useLabEvolve();
  const labOptimize = useLabOptimize();
  const labImprove = useLabImprove();
  const cancelLabJob = useCancelLabJob();

  const isJobActive = resolveIsJobActive({
    generatePending: labGenerate.isPending,
    evolvePending: labEvolve.isPending,
    optimizePending: labOptimize.isPending,
    improvePending: labImprove.isPending,
    sseStatus: sse.status,
    jobStatus: jobStatus?.status,
  });

  const needsStrategy = operation !== 'generate';

  // Merge SSE and polling status
  const currentStatus = sse.status ?? jobStatus?.status ?? null;
  const currentProgress = sse.progress ?? jobStatus?.progress ?? null;
  const currentMessage = sse.message ?? jobStatus?.message ?? null;

  const resultData = jobStatus?.result_data ?? null;

  const handleJobStart = (jobId: string, type: LabType) => {
    setActiveLabJobId(jobId);
    onOperationChange(type);
    setActiveTab('run');
  };

  const handleSelectHistoryJob = (jobId: string, type: LabType | null) => {
    setActiveLabJobId(jobId);
    if (type) {
      onOperationChange(type);
    }
  };

  const mutationError = labGenerate.error ?? labEvolve.error ?? labOptimize.error ?? labImprove.error;
  const shouldShowProgress = !!activeLabJobId && !!currentStatus;
  const cancelHandler =
    activeLabJobId && isRunningStatus(currentStatus) ? () => cancelLabJob.mutate(activeLabJobId) : undefined;

  const handleGenerateSubmit = async (request: unknown) => {
    const result = await labGenerate.mutateAsync(request as Parameters<typeof labGenerate.mutateAsync>[0]);
    handleJobStart(result.job_id, 'generate');
  };
  const handleEvolveSubmit = async (request: unknown) => {
    const result = await labEvolve.mutateAsync(request as Parameters<typeof labEvolve.mutateAsync>[0]);
    handleJobStart(result.job_id, 'evolve');
  };
  const handleOptimizeSubmit = async (request: unknown) => {
    const result = await labOptimize.mutateAsync(request as Parameters<typeof labOptimize.mutateAsync>[0]);
    handleJobStart(result.job_id, 'optimize');
  };
  const handleImproveSubmit = async (request: unknown) => {
    const result = await labImprove.mutateAsync(request as Parameters<typeof labImprove.mutateAsync>[0]);
    handleJobStart(result.job_id, 'improve');
  };

  return (
    <div className="max-w-2xl space-y-4">
      <div className="flex items-center gap-2">
        <FlaskConical className="h-5 w-5 text-primary" />
        <div>
          <h2 className="text-lg font-semibold">Lab</h2>
          <p className="text-xs text-muted-foreground">
            Strategy generation, evolution, optimization, and AI improvement
          </p>
        </div>
      </div>

      <div className="flex border-b">
        <button type="button" onClick={() => setActiveTab('run')} className={tabButtonClass(activeTab === 'run')}>
          Run
        </button>
        <button
          type="button"
          onClick={() => setActiveTab('history')}
          className={tabButtonClass(activeTab === 'history')}
        >
          History
        </button>
      </div>

      {activeTab === 'run' ? (
        <>
          <LabOperationSelector value={operation} onChange={onOperationChange} disabled={isJobActive} />

          {needsStrategy && (
            <div className="space-y-1.5">
              <span className="text-sm font-medium">Strategy</span>
              <StrategySelector
                strategies={strategiesData?.strategies}
                isLoading={isLoadingStrategies}
                value={selectedStrategy}
                onChange={onSelectedStrategyChange}
                disabled={isJobActive}
              />
            </div>
          )}

          <LabOperationForm
            operation={operation}
            strategyName={selectedStrategy}
            disabled={isJobActive}
            onGenerateSubmit={handleGenerateSubmit}
            onEvolveSubmit={handleEvolveSubmit}
            onOptimizeSubmit={handleOptimizeSubmit}
            onImproveSubmit={handleImproveSubmit}
          />

          {mutationError && (
            <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{mutationError.message}</div>
          )}
        </>
      ) : (
        <LabJobHistoryTable
          jobs={labJobs}
          isLoading={isLoadingLabJobs}
          isRefreshing={isFetchingLabJobs}
          selectedJobId={activeLabJobId}
          onSelectJob={(job) => handleSelectHistoryJob(job.job_id, job.lab_type ?? null)}
          onRefresh={() => {
            void refetchLabJobs();
          }}
        />
      )}

      {shouldShowProgress && (
        <LabJobProgress
          status={currentStatus}
          progress={currentProgress}
          message={currentMessage}
          error={jobStatus?.error}
          createdAt={jobStatus?.created_at}
          startedAt={jobStatus?.started_at}
          onCancel={cancelHandler}
          isCancelling={cancelLabJob.isPending}
        />
      )}

      {resultData && currentStatus === 'completed' && <LabResultSection resultData={resultData} />}
    </div>
  );
}
