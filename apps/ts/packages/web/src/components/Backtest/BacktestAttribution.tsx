import { AlertCircle, Ban, CheckCircle2, ChevronDown, GitBranch, Loader2, XCircle } from 'lucide-react';
import { type ComponentProps, useEffect, useMemo, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import {
  useCancelSignalAttribution,
  useRunSignalAttribution,
  useSignalAttributionJobStatus,
  useSignalAttributionResult,
  useStrategies,
} from '@/hooks/useBacktest';
import { useBacktestStore } from '@/stores/backtestStore';
import type {
  JobStatus,
  SignalAttributionJobResponse,
  SignalAttributionResult,
  SignalAttributionSignalResult,
} from '@/types/backtest';
import { formatRate } from '@/utils/formatters';
import { AttributionArtifactBrowser } from './AttributionArtifactBrowser';
import { StrategySelector } from './StrategySelector';

const DEFAULT_TOP_N = 5;
const DEFAULT_PERMUTATIONS = 128;

type StrategyOptions = ComponentProps<typeof StrategySelector>['strategies'];

type ParsedRunParameters = {
  topN: number;
  permutations: number;
  randomSeed: number | null;
};

function StatusIcon({ status }: { status: JobStatus }) {
  switch (status) {
    case 'pending':
    case 'running':
      return <Loader2 className="h-4 w-4 animate-spin text-blue-500" />;
    case 'completed':
      return <CheckCircle2 className="h-4 w-4 text-green-500" />;
    case 'cancelled':
      return <Ban className="h-4 w-4 text-orange-500" />;
    case 'failed':
      return <XCircle className="h-4 w-4 text-red-500" />;
    default:
      return <AlertCircle className="h-4 w-4 text-yellow-500" />;
  }
}

function formatSigned(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  const sign = value >= 0 ? '+' : '';
  return `${sign}${value.toFixed(4)}`;
}

function formatReturn(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return formatRate(value);
}

function parsePositiveInt(value: string, fallback: number): number {
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function isActiveStatus(status: JobStatus | undefined): boolean {
  return status === 'pending' || status === 'running';
}

function clampProgressPercentage(progress: number | null | undefined): number | null {
  if (progress == null || !Number.isFinite(progress)) return null;
  return Math.min(100, Math.max(0, progress * 100));
}

function formatElapsed(seconds: number): string {
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}:${String(s).padStart(2, '0')}`;
}

function parseRunParameters(
  topNInput: string,
  permutationsInput: string,
  randomSeedInput: string
): { value: ParsedRunParameters | null; error: string | null } {
  const topN = parsePositiveInt(topNInput, DEFAULT_TOP_N);
  const permutations = parsePositiveInt(permutationsInput, DEFAULT_PERMUTATIONS);

  if (randomSeedInput.trim().length === 0) {
    return { value: { topN, permutations, randomSeed: null }, error: null };
  }

  const seedNumber = Number(randomSeedInput);
  if (!Number.isInteger(seedNumber)) {
    return { value: null, error: 'Random seed must be an integer.' };
  }

  return {
    value: {
      topN,
      permutations,
      randomSeed: seedNumber,
    },
    error: null,
  };
}

function renderSignalRow(signal: SignalAttributionSignalResult, selectedForShapley: Set<string>) {
  const isSelected = selectedForShapley.has(signal.signal_id);
  return (
    <TableRow key={signal.signal_id} className={isSelected ? 'bg-primary/5' : undefined}>
      <TableCell className="font-mono text-xs">{signal.signal_id}</TableCell>
      <TableCell>{signal.scope}</TableCell>
      <TableCell>{signal.loo.status}</TableCell>
      <TableCell>{formatReturn(signal.loo.delta_total_return)}</TableCell>
      <TableCell>{formatSigned(signal.loo.delta_sharpe_ratio)}</TableCell>
      <TableCell>{signal.shapley?.status ?? '-'}</TableCell>
      <TableCell>{formatReturn(signal.shapley?.total_return ?? null)}</TableCell>
      <TableCell>{formatSigned(signal.shapley?.sharpe_ratio ?? null)}</TableCell>
    </TableRow>
  );
}

function ErrorBanner({ message }: { message: string }) {
  return <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{message}</div>;
}

function AdvancedParameterFields({
  topN,
  permutations,
  randomSeed,
  isRunning,
  onTopNChange,
  onPermutationsChange,
  onRandomSeedChange,
}: {
  topN: string;
  permutations: string;
  randomSeed: string;
  isRunning: boolean;
  onTopNChange: (value: string) => void;
  onPermutationsChange: (value: string) => void;
  onRandomSeedChange: (value: string) => void;
}) {
  return (
    <div className="grid gap-3 md:grid-cols-3">
      <div className="space-y-1">
        <Label htmlFor="attr-top-n">Shapley Top N</Label>
        <Input
          id="attr-top-n"
          type="number"
          min={1}
          value={topN}
          onChange={(e) => onTopNChange(e.target.value)}
          disabled={isRunning}
        />
      </div>
      <div className="space-y-1">
        <Label htmlFor="attr-permutations">Shapley Permutations</Label>
        <Input
          id="attr-permutations"
          type="number"
          min={1}
          value={permutations}
          onChange={(e) => onPermutationsChange(e.target.value)}
          disabled={isRunning}
        />
      </div>
      <div className="space-y-1">
        <Label htmlFor="attr-random-seed">Random Seed (optional)</Label>
        <Input
          id="attr-random-seed"
          type="number"
          value={randomSeed}
          onChange={(e) => onRandomSeedChange(e.target.value)}
          disabled={isRunning}
        />
      </div>
    </div>
  );
}

function AttributionRunCard({
  strategies,
  isLoadingStrategies,
  selectedStrategy,
  isRunning,
  advancedOpen,
  topN,
  permutations,
  randomSeed,
  validationError,
  runErrorMessage,
  onStrategyChange,
  onToggleAdvanced,
  onTopNChange,
  onPermutationsChange,
  onRandomSeedChange,
  onRun,
}: {
  strategies: StrategyOptions;
  isLoadingStrategies: boolean;
  selectedStrategy: string | null;
  isRunning: boolean;
  advancedOpen: boolean;
  topN: string;
  permutations: string;
  randomSeed: string;
  validationError: string | null;
  runErrorMessage: string | null;
  onStrategyChange: (strategy: string | null) => void;
  onToggleAdvanced: () => void;
  onTopNChange: (value: string) => void;
  onPermutationsChange: (value: string) => void;
  onRandomSeedChange: (value: string) => void;
  onRun: () => void;
}) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Run Attribution</CardTitle>
        <CardDescription>Run async signal attribution for the selected strategy.</CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="space-y-1.5">
          <span className="text-sm font-medium">Strategy</span>
          <StrategySelector
            strategies={strategies}
            isLoading={isLoadingStrategies}
            value={selectedStrategy}
            onChange={onStrategyChange}
            disabled={isRunning}
          />
        </div>

        <Button type="button" variant="outline" size="sm" className="gap-2" onClick={onToggleAdvanced}>
          <ChevronDown className={`h-4 w-4 transition-transform ${advancedOpen ? 'rotate-180' : ''}`} />
          Advanced Parameters
        </Button>

        {advancedOpen && (
          <AdvancedParameterFields
            topN={topN}
            permutations={permutations}
            randomSeed={randomSeed}
            isRunning={isRunning}
            onTopNChange={onTopNChange}
            onPermutationsChange={onPermutationsChange}
            onRandomSeedChange={onRandomSeedChange}
          />
        )}

        {validationError && <ErrorBanner message={validationError} />}
        {runErrorMessage && <ErrorBanner message={runErrorMessage} />}

        <Button onClick={onRun} disabled={!selectedStrategy || isRunning} className="w-full">
          {isRunning ? 'Running...' : 'Run Signal Attribution'}
        </Button>
      </CardContent>
    </Card>
  );
}

function AttributionJobCard({
  activeJob,
  cancelPending,
  cancelErrorMessage,
  onCancel,
}: {
  activeJob: SignalAttributionJobResponse | null;
  cancelPending: boolean;
  cancelErrorMessage: string | null;
  onCancel: (jobId: string) => void;
}) {
  const isActive = isActiveStatus(activeJob?.status);
  const progressPercent = clampProgressPercentage(activeJob?.progress);
  const progressValue = progressPercent == null ? undefined : Math.round(progressPercent);
  const progressLabel = progressPercent == null ? null : `${progressPercent.toFixed(1)}%`;
  const startTime = activeJob?.started_at ?? activeJob?.created_at ?? null;
  const [elapsed, setElapsed] = useState(0);

  useEffect(() => {
    if (!isActive || !startTime) {
      setElapsed(0);
      return;
    }
    const start = new Date(startTime).getTime();
    const update = () => setElapsed(Math.floor((Date.now() - start) / 1000));
    update();
    const id = setInterval(update, 1000);
    return () => clearInterval(id);
  }, [isActive, startTime]);

  if (!activeJob) {
    return null;
  }

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2 text-base">
            <StatusIcon status={activeJob.status} />
            <span className="capitalize">{activeJob.status}</span>
          </CardTitle>
          {isActive && (
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground">⏱ {formatElapsed(elapsed)}</span>
              <Button variant="ghost" size="sm" onClick={() => onCancel(activeJob.job_id)} disabled={cancelPending}>
                Cancel
              </Button>
            </div>
          )}
        </div>
      </CardHeader>
      <CardContent className="space-y-2">
        <div className="text-xs text-muted-foreground">Job ID: {activeJob.job_id}</div>
        {activeJob.message && <div className="text-sm">{activeJob.message}</div>}
        {(isActive || progressPercent != null) && (
          <div className="space-y-2">
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span>Progress</span>
              <span className="font-medium">{progressLabel ?? 'Tracking...'}</span>
            </div>
            <div
              role="progressbar"
              aria-valuemin={0}
              aria-valuemax={100}
              aria-valuenow={progressValue}
              aria-valuetext={progressLabel ?? 'In progress'}
              className="h-2 w-full rounded-full bg-secondary overflow-hidden"
            >
              {progressPercent != null ? (
                <div
                  className="h-full rounded-full bg-blue-500 transition-all duration-300"
                  style={{ width: `${progressPercent}%` }}
                />
              ) : (
                <div className="h-full rounded-full bg-blue-500 animate-progress-indeterminate" />
              )}
            </div>
          </div>
        )}
        {activeJob.error && <ErrorBanner message={activeJob.error} />}
        {cancelErrorMessage && <ErrorBanner message={cancelErrorMessage} />}
      </CardContent>
    </Card>
  );
}

function AttributionResultCards({
  resultData,
  selectedForShapley,
  resultErrorMessage,
}: {
  resultData: SignalAttributionResult | null;
  selectedForShapley: Set<string>;
  resultErrorMessage: string | null;
}) {
  return (
    <>
      {resultData && (
        <>
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base">Summary</CardTitle>
            </CardHeader>
            <CardContent className="grid gap-2 text-sm md:grid-cols-2">
              <div>Total Return: {formatReturn(resultData.baseline_metrics.total_return)}</div>
              <div>Sharpe Ratio: {formatSigned(resultData.baseline_metrics.sharpe_ratio)}</div>
              <div>Top N Requested: {resultData.top_n_selection.top_n_requested}</div>
              <div>Top N Effective: {resultData.top_n_selection.top_n_effective}</div>
              <div>Shapley Method: {resultData.shapley.method ?? '-'}</div>
              <div>Shapley Evaluations: {resultData.shapley.evaluations ?? '-'}</div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base">Signals</CardTitle>
              <CardDescription>Top-N signals used for Shapley are highlighted.</CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Signal</TableHead>
                    <TableHead>Scope</TableHead>
                    <TableHead>LOO</TableHead>
                    <TableHead>LOO Return</TableHead>
                    <TableHead>LOO Sharpe</TableHead>
                    <TableHead>Shapley</TableHead>
                    <TableHead>Shapley Return</TableHead>
                    <TableHead>Shapley Sharpe</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>{resultData.signals.map((signal) => renderSignalRow(signal, selectedForShapley))}</TableBody>
              </Table>
            </CardContent>
          </Card>
        </>
      )}

      {resultErrorMessage && <ErrorBanner message={resultErrorMessage} />}
    </>
  );
}

export function BacktestAttribution() {
  const [activeTab, setActiveTab] = useState<'run' | 'history'>('run');
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [topN, setTopN] = useState(String(DEFAULT_TOP_N));
  const [permutations, setPermutations] = useState(String(DEFAULT_PERMUTATIONS));
  const [randomSeed, setRandomSeed] = useState('');
  const [validationError, setValidationError] = useState<string | null>(null);

  const { data: strategiesData, isLoading: isLoadingStrategies } = useStrategies();
  const { selectedStrategy, setSelectedStrategy, activeAttributionJobId, setActiveAttributionJobId } =
    useBacktestStore();
  const runSignalAttribution = useRunSignalAttribution();
  const cancelSignalAttribution = useCancelSignalAttribution();
  const jobStatus = useSignalAttributionJobStatus(activeAttributionJobId);
  const resultDetail = useSignalAttributionResult(
    jobStatus.data?.status === 'completed' && !jobStatus.data?.result_data ? activeAttributionJobId : null
  );

  const activeJob = jobStatus.data ?? null;
  const isRunning = runSignalAttribution.isPending || isActiveStatus(activeJob?.status);
  const runErrorMessage = runSignalAttribution.isError ? runSignalAttribution.error.message : null;
  const cancelErrorMessage = cancelSignalAttribution.isError ? cancelSignalAttribution.error.message : null;
  const resultErrorMessage = resultDetail.isError ? resultDetail.error.message : null;

  const resultData = useMemo(
    () => resultDetail.data?.result ?? activeJob?.result_data ?? null,
    [activeJob?.result_data, resultDetail.data?.result]
  );
  const selectedForShapley = useMemo(
    () => new Set(resultData?.top_n_selection.selected_signal_ids ?? []),
    [resultData?.top_n_selection.selected_signal_ids]
  );

  const handleRun = async () => {
    if (!selectedStrategy) {
      return;
    }

    const parsed = parseRunParameters(topN, permutations, randomSeed);
    if (!parsed.value) {
      setValidationError(parsed.error);
      return;
    }

    setValidationError(null);
    const started = await runSignalAttribution.mutateAsync({
      strategy_name: selectedStrategy,
      shapley_top_n: parsed.value.topN,
      shapley_permutations: parsed.value.permutations,
      random_seed: parsed.value.randomSeed,
    });
    setActiveAttributionJobId(started.job_id);
  };

  return (
    <div className="max-w-6xl space-y-4">
      <div className="flex items-center gap-2">
        <GitBranch className="h-5 w-5 text-primary" />
        <div>
          <h2 className="text-lg font-semibold">Signal Attribution</h2>
          <p className="text-xs text-muted-foreground">LOO + Shapley top-N contribution analysis</p>
        </div>
      </div>

      <div className="flex border-b">
        <button
          type="button"
          onClick={() => setActiveTab('run')}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
            activeTab === 'run'
              ? 'border-primary text-primary'
              : 'border-transparent text-muted-foreground hover:text-foreground'
          }`}
        >
          Run
        </button>
        <button
          type="button"
          onClick={() => setActiveTab('history')}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
            activeTab === 'history'
              ? 'border-primary text-primary'
              : 'border-transparent text-muted-foreground hover:text-foreground'
          }`}
        >
          History
        </button>
      </div>

      {activeTab === 'run' ? (
        <>
          <AttributionRunCard
            strategies={strategiesData?.strategies}
            isLoadingStrategies={isLoadingStrategies}
            selectedStrategy={selectedStrategy}
            isRunning={isRunning}
            advancedOpen={advancedOpen}
            topN={topN}
            permutations={permutations}
            randomSeed={randomSeed}
            validationError={validationError}
            runErrorMessage={runErrorMessage}
            onStrategyChange={setSelectedStrategy}
            onToggleAdvanced={() => setAdvancedOpen((value) => !value)}
            onTopNChange={setTopN}
            onPermutationsChange={setPermutations}
            onRandomSeedChange={setRandomSeed}
            onRun={handleRun}
          />

          <AttributionJobCard
            activeJob={activeJob}
            cancelPending={cancelSignalAttribution.isPending}
            cancelErrorMessage={cancelErrorMessage}
            onCancel={(jobId) => cancelSignalAttribution.mutate(jobId)}
          />

          <AttributionResultCards
            resultData={resultData}
            selectedForShapley={selectedForShapley}
            resultErrorMessage={resultErrorMessage}
          />
        </>
      ) : (
        <AttributionArtifactBrowser />
      )}
    </div>
  );
}
