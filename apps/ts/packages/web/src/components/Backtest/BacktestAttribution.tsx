import { AlertCircle, Ban, CheckCircle2, ChevronDown, GitBranch, Loader2, XCircle } from 'lucide-react';
import { useMemo, useState } from 'react';
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
import type { JobStatus, SignalAttributionSignalResult } from '@/types/backtest';
import { formatRate } from '@/utils/formatters';
import { StrategySelector } from './StrategySelector';

const DEFAULT_TOP_N = 5;
const DEFAULT_PERMUTATIONS = 128;

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

export function BacktestAttribution() {
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [topN, setTopN] = useState(String(DEFAULT_TOP_N));
  const [permutations, setPermutations] = useState(String(DEFAULT_PERMUTATIONS));
  const [randomSeed, setRandomSeed] = useState('');
  const [validationError, setValidationError] = useState<string | null>(null);

  const { data: strategiesData, isLoading: isLoadingStrategies } = useStrategies();
  const { selectedStrategy, setSelectedStrategy, activeAttributionJobId, setActiveAttributionJobId } = useBacktestStore();
  const runSignalAttribution = useRunSignalAttribution();
  const cancelSignalAttribution = useCancelSignalAttribution();
  const jobStatus = useSignalAttributionJobStatus(activeAttributionJobId);
  const resultDetail = useSignalAttributionResult(
    jobStatus.data?.status === 'completed' && !jobStatus.data?.result_data ? activeAttributionJobId : null
  );

  const activeJob = jobStatus.data;
  const isRunning = runSignalAttribution.isPending || isActiveStatus(activeJob?.status);

  const resultData = useMemo(
    () => resultDetail.data?.result ?? activeJob?.result_data ?? null,
    [activeJob?.result_data, resultDetail.data?.result]
  );
  const selectedForShapley = useMemo(
    () => new Set(resultData?.top_n_selection.selected_signal_ids ?? []),
    [resultData?.top_n_selection.selected_signal_ids]
  );

  const handleRun = async () => {
    if (!selectedStrategy) return;

    const parsedTopN = parsePositiveInt(topN, DEFAULT_TOP_N);
    const parsedPermutations = parsePositiveInt(permutations, DEFAULT_PERMUTATIONS);
    let parsedSeed: number | null = null;

    if (randomSeed.trim().length > 0) {
      const seedNumber = Number(randomSeed);
      if (!Number.isInteger(seedNumber)) {
        setValidationError('Random seed must be an integer.');
        return;
      }
      parsedSeed = seedNumber;
    }

    setValidationError(null);
    const started = await runSignalAttribution.mutateAsync({
      strategy_name: selectedStrategy,
      shapley_top_n: parsedTopN,
      shapley_permutations: parsedPermutations,
      random_seed: parsedSeed,
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

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Run Attribution</CardTitle>
          <CardDescription>Run async signal attribution for the selected strategy.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="space-y-1.5">
            <span className="text-sm font-medium">Strategy</span>
            <StrategySelector
              strategies={strategiesData?.strategies}
              isLoading={isLoadingStrategies}
              value={selectedStrategy}
              onChange={setSelectedStrategy}
              disabled={isRunning}
            />
          </div>

          <Button type="button" variant="outline" size="sm" className="gap-2" onClick={() => setAdvancedOpen((v) => !v)}>
            <ChevronDown className={`h-4 w-4 transition-transform ${advancedOpen ? 'rotate-180' : ''}`} />
            Advanced Parameters
          </Button>

          {advancedOpen && (
            <div className="grid gap-3 md:grid-cols-3">
              <div className="space-y-1">
                <Label htmlFor="attr-top-n">Shapley Top N</Label>
                <Input
                  id="attr-top-n"
                  type="number"
                  min={1}
                  value={topN}
                  onChange={(e) => setTopN(e.target.value)}
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
                  onChange={(e) => setPermutations(e.target.value)}
                  disabled={isRunning}
                />
              </div>
              <div className="space-y-1">
                <Label htmlFor="attr-random-seed">Random Seed (optional)</Label>
                <Input
                  id="attr-random-seed"
                  type="number"
                  value={randomSeed}
                  onChange={(e) => setRandomSeed(e.target.value)}
                  disabled={isRunning}
                />
              </div>
            </div>
          )}

          {validationError && <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{validationError}</div>}
          {runSignalAttribution.isError && (
            <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{runSignalAttribution.error.message}</div>
          )}

          <Button onClick={handleRun} disabled={!selectedStrategy || isRunning} className="w-full">
            {isRunning ? 'Running...' : 'Run Signal Attribution'}
          </Button>
        </CardContent>
      </Card>

      {activeJob && (
        <Card>
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="flex items-center gap-2 text-base">
                <StatusIcon status={activeJob.status} />
                <span className="capitalize">{activeJob.status}</span>
              </CardTitle>
              {isActiveStatus(activeJob.status) && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => cancelSignalAttribution.mutate(activeJob.job_id)}
                  disabled={cancelSignalAttribution.isPending}
                >
                  Cancel
                </Button>
              )}
            </div>
          </CardHeader>
          <CardContent className="space-y-2">
            <div className="text-xs text-muted-foreground">Job ID: {activeJob.job_id}</div>
            {activeJob.message && <div className="text-sm">{activeJob.message}</div>}
            {activeJob.progress != null && (
              <div className="text-sm text-muted-foreground">Progress: {(activeJob.progress * 100).toFixed(0)}%</div>
            )}
            {activeJob.error && <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{activeJob.error}</div>}
            {cancelSignalAttribution.isError && (
              <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{cancelSignalAttribution.error.message}</div>
            )}
          </CardContent>
        </Card>
      )}

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

      {resultDetail.isError && (
        <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{resultDetail.error.message}</div>
      )}
    </div>
  );
}
