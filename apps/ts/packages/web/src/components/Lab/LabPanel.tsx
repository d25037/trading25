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
  useLabOptimize,
} from '@/hooks/useLab';
import { useLabSSE } from '@/hooks/useLabSSE';
import { useBacktestStore } from '@/stores/backtestStore';
import type { LabType } from '@/types/backtest';
import { LabEvolveForm } from './LabEvolveForm';
import { LabGenerateForm } from './LabGenerateForm';
import { LabImproveForm } from './LabImproveForm';
import { LabJobProgress } from './LabJobProgress';
import { LabOperationSelector } from './LabOperationSelector';
import { LabOptimizeForm } from './LabOptimizeForm';
import { LabResultSection } from './LabResultSection';

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: UI component with conditional rendering
export function LabPanel() {
  const [operation, setOperation] = useState<LabType>('generate');
  const { selectedStrategy, setSelectedStrategy, activeLabJobId, setActiveLabJobId, setActiveLabType } =
    useBacktestStore();
  const { data: strategiesData, isLoading: isLoadingStrategies } = useStrategies();

  const sse = useLabSSE(activeLabJobId);
  const { data: jobStatus } = useLabJobStatus(activeLabJobId, sse.isConnected);

  const labGenerate = useLabGenerate();
  const labEvolve = useLabEvolve();
  const labOptimize = useLabOptimize();
  const labImprove = useLabImprove();
  const cancelLabJob = useCancelLabJob();

  const isJobActive =
    labGenerate.isPending ||
    labEvolve.isPending ||
    labOptimize.isPending ||
    labImprove.isPending ||
    sse.status === 'pending' ||
    sse.status === 'running' ||
    jobStatus?.status === 'pending' ||
    jobStatus?.status === 'running';

  const needsStrategy = operation !== 'generate';

  // Merge SSE and polling status
  const currentStatus = sse.status ?? jobStatus?.status ?? null;
  const currentProgress = sse.progress ?? jobStatus?.progress ?? null;
  const currentMessage = sse.message ?? jobStatus?.message ?? null;

  const resultData = jobStatus?.result_data ?? null;

  const handleJobStart = (jobId: string, type: LabType) => {
    setActiveLabJobId(jobId);
    setActiveLabType(type);
  };

  const mutationError = labGenerate.error ?? labEvolve.error ?? labOptimize.error ?? labImprove.error;

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

      <LabOperationSelector value={operation} onChange={setOperation} disabled={isJobActive} />

      {needsStrategy && (
        <div className="space-y-1.5">
          <span className="text-sm font-medium">Strategy</span>
          <StrategySelector
            strategies={strategiesData?.strategies}
            isLoading={isLoadingStrategies}
            value={selectedStrategy}
            onChange={setSelectedStrategy}
            disabled={isJobActive}
          />
        </div>
      )}

      {operation === 'generate' && (
        <LabGenerateForm
          onSubmit={async (req) => {
            const result = await labGenerate.mutateAsync(req);
            handleJobStart(result.job_id, 'generate');
          }}
          disabled={isJobActive}
        />
      )}

      {operation === 'evolve' && (
        <LabEvolveForm
          strategyName={selectedStrategy}
          onSubmit={async (req) => {
            const result = await labEvolve.mutateAsync(req);
            handleJobStart(result.job_id, 'evolve');
          }}
          disabled={isJobActive}
        />
      )}

      {operation === 'optimize' && (
        <LabOptimizeForm
          strategyName={selectedStrategy}
          onSubmit={async (req) => {
            const result = await labOptimize.mutateAsync(req);
            handleJobStart(result.job_id, 'optimize');
          }}
          disabled={isJobActive}
        />
      )}

      {operation === 'improve' && (
        <LabImproveForm
          strategyName={selectedStrategy}
          onSubmit={async (req) => {
            const result = await labImprove.mutateAsync(req);
            handleJobStart(result.job_id, 'improve');
          }}
          disabled={isJobActive}
        />
      )}

      {mutationError && (
        <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{mutationError.message}</div>
      )}

      {activeLabJobId && currentStatus && (
        <LabJobProgress
          status={currentStatus}
          progress={currentProgress}
          message={currentMessage}
          error={jobStatus?.error}
          createdAt={jobStatus?.created_at}
          startedAt={jobStatus?.started_at}
          onCancel={
            currentStatus === 'pending' || currentStatus === 'running'
              ? () => cancelLabJob.mutate(activeLabJobId)
              : undefined
          }
          isCancelling={cancelLabJob.isPending}
        />
      )}

      {resultData && currentStatus === 'completed' && <LabResultSection resultData={resultData} />}
    </div>
  );
}
