import { useNavigate } from '@tanstack/react-router';
import { Filter } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
import {
  ModeSwitcherPanel,
  SectionEyebrow,
  SplitLayout,
  SplitMain,
  SplitSidebar,
  Surface,
} from '@/components/Layout/Workspace';
import { ScreeningFilters } from '@/components/Screening/ScreeningFilters';
import { ScreeningJobHistoryTable } from '@/components/Screening/ScreeningJobHistoryTable';
import { ScreeningJobProgress, ScreeningJobStatusInline } from '@/components/Screening/ScreeningJobProgress';
import { ScreeningSummary } from '@/components/Screening/ScreeningSummary';
import { ScreeningTable } from '@/components/Screening/ScreeningTable';
import { Button } from '@/components/ui/button';
import { useStrategies } from '@/hooks/useBacktest';
import { useMigrateScreeningRouteState, useScreeningRouteState } from '@/hooks/usePageRouteState';
import {
  useCancelScreeningJob,
  useRunScreeningJob,
  useScreeningJobSSE,
  useScreeningJobStatus,
  useScreeningResult,
} from '@/hooks/useScreening';
import { ApiError } from '@/lib/api-client';
import { unionMarkets } from '@/lib/marketUtils';
import type { ScreeningSubTab } from '@/stores/screeningStore';
import { useScreeningStore } from '@/stores/screeningStore';
import type { StrategyMetadata } from '@/types/backtest';
import type {
  EntryDecidability,
  MarketScreeningResponse,
  ScreeningJobResponse,
  ScreeningParams,
  ScreeningResultItem,
} from '@/types/screening';

const subTabs = [
  { value: 'preOpenScreening' as ScreeningSubTab, label: 'Pre-Open Decidable', icon: Filter },
  { value: 'inSessionScreening' as ScreeningSubTab, label: 'Requires In-Session Observation', icon: Filter },
];

function isScreeningSubTab(tab: ScreeningSubTab): tab is 'preOpenScreening' | 'inSessionScreening' {
  return tab === 'preOpenScreening' || tab === 'inSessionScreening';
}

function sanitizeStrategies(
  strategies: string | undefined,
  allowedStrategies: string[] | undefined
): string | undefined {
  if (!strategies) return undefined;
  if (!allowedStrategies) return strategies;
  const allowed = new Set(allowedStrategies);
  const sanitized = strategies
    .split(',')
    .map((value) => value.trim())
    .filter((value) => value.length > 0 && allowed.has(value));

  return sanitized.length > 0 ? sanitized.join(',') : undefined;
}

function sanitizeScreeningParams(
  params: ScreeningParams,
  allowedStrategies: string[] | undefined,
  entryDecidability: EntryDecidability
): ScreeningParams {
  return {
    ...params,
    entry_decidability: entryDecidability,
    strategies: sanitizeStrategies(params.strategies, allowedStrategies),
  };
}

function areScreeningParamsEqual(left: ScreeningParams, right: ScreeningParams): boolean {
  return (
    left.entry_decidability === right.entry_decidability &&
    left.markets === right.markets &&
    left.strategies === right.strategies &&
    left.recentDays === right.recentDays &&
    left.date === right.date &&
    left.sortBy === right.sortBy &&
    left.order === right.order &&
    left.limit === right.limit
  );
}

function parseCsvTokens(value: string | undefined): string[] {
  if (!value) {
    return [];
  }
  return value
    .split(',')
    .map((token) => token.trim())
    .filter(Boolean);
}

function isPreOpenScreeningStrategy(strategy: StrategyMetadata): boolean {
  return (
    strategy.category === 'production' &&
    strategy.screening_support === 'supported' &&
    (strategy.entry_decidability ?? 'pre_open_decidable') === 'pre_open_decidable'
  );
}

function isInSessionScreeningStrategy(strategy: StrategyMetadata): boolean {
  return (
    strategy.category === 'production' &&
    strategy.screening_support === 'supported' &&
    strategy.entry_decidability === 'requires_same_session_observation'
  );
}

function isEligibleScreeningStrategy(strategy: StrategyMetadata, entryDecidability: EntryDecidability): boolean {
  return entryDecidability === 'requires_same_session_observation'
    ? isInSessionScreeningStrategy(strategy)
    : isPreOpenScreeningStrategy(strategy);
}

function selectStrategyNames(
  strategies: StrategyMetadata[] | undefined,
  predicate: (strategy: StrategyMetadata) => boolean
): string[] | undefined {
  if (!strategies) {
    return undefined;
  }

  return strategies
    .filter(predicate)
    .map((strategy) => strategy.name)
    .sort((left, right) => left.localeCompare(right));
}

function resolveAutoScreeningMarkets(
  strategies: StrategyMetadata[] | undefined,
  params: ScreeningParams,
  entryDecidability: EntryDecidability
): string[] {
  if (!strategies) {
    return [];
  }

  const eligibleStrategies = strategies.filter((strategy) => isEligibleScreeningStrategy(strategy, entryDecidability));
  const eligibleByName = new Map(eligibleStrategies.map((strategy) => [strategy.name, strategy]));
  const selectedStrategies = parseCsvTokens(params.strategies)
    .map((name) => eligibleByName.get(name))
    .filter((strategy): strategy is StrategyMetadata => strategy !== undefined);
  const targetStrategies = selectedStrategies.length > 0 ? selectedStrategies : eligibleStrategies;

  return unionMarkets(targetStrategies.map((strategy) => strategy.screening_default_markets));
}

function useSanitizedScreeningParams(
  params: ScreeningParams,
  setParams: (params: ScreeningParams) => void,
  allowedStrategies: string[] | undefined,
  entryDecidability: EntryDecidability
): void {
  useEffect(() => {
    if (!allowedStrategies) {
      return;
    }

    const sanitized = sanitizeScreeningParams(params, allowedStrategies, entryDecidability);
    if (!areScreeningParamsEqual(sanitized, params)) {
      setParams(sanitized);
    }
  }, [allowedStrategies, entryDecidability, params, setParams]);
}

interface ScreeningSidebarProps {
  activeSubTab: ScreeningSubTab;
  entryDecidability: EntryDecidability;
  screeningParams: ScreeningParams;
  screeningAutoMarkets: string[];
  setScreeningParams: (params: ScreeningParams) => void;
  productionStrategies: string[];
  isLoadingStrategies: boolean;
}

function ScreeningSidebar({
  activeSubTab,
  entryDecidability,
  screeningParams,
  screeningAutoMarkets,
  setScreeningParams,
  productionStrategies,
  isLoadingStrategies,
}: ScreeningSidebarProps) {
  if (isScreeningSubTab(activeSubTab)) {
    return (
      <ScreeningFilters
        entryDecidability={entryDecidability}
        params={screeningParams}
        onChange={setScreeningParams}
        strategyOptions={productionStrategies}
        autoMarkets={screeningAutoMarkets}
        strategiesLoading={isLoadingStrategies}
      />
    );
  }
  return null;
}

interface ScreeningMainContentProps {
  activeSubTab: ScreeningSubTab;
  entryDecidability: EntryDecidability;
  handleRunScreening: () => Promise<void>;
  screeningIsRunning: boolean;
  screeningJob: ScreeningJobResponse | null;
  handleCancelScreening: () => void;
  cancelScreeningPending: boolean;
  screeningJobHistory: ScreeningJobResponse[];
  showScreeningJobHistory: boolean;
  onShowScreeningJobHistoryChange: (showHistory: boolean) => void;
  onSelectScreeningJob: (job: ScreeningJobResponse) => void;
  screeningSummary: MarketScreeningResponse['summary'] | undefined;
  screeningMarkets: string[];
  screeningRecentDays: number;
  screeningReferenceDate: string | undefined;
  screeningResults: ScreeningResultItem[];
  screeningTableLoading: boolean;
  screeningError: Error | null;
  onStockClick: (code: string) => void;
}

function ScreeningMainContent({
  activeSubTab,
  entryDecidability,
  handleRunScreening,
  screeningIsRunning,
  screeningJob,
  handleCancelScreening,
  cancelScreeningPending,
  screeningJobHistory,
  showScreeningJobHistory,
  onShowScreeningJobHistoryChange,
  onSelectScreeningJob,
  screeningSummary,
  screeningMarkets,
  screeningRecentDays,
  screeningReferenceDate,
  screeningResults,
  screeningTableLoading,
  screeningError,
  onStockClick,
}: ScreeningMainContentProps) {
  if (isScreeningSubTab(activeSubTab)) {
    const completedScreeningJob = screeningJob?.status === 'completed' ? screeningJob : null;
    const runButtonLabel =
      entryDecidability === 'requires_same_session_observation' ? 'Run In-Session Screening' : 'Run Pre-Open Screening';
    const modeLabel =
      entryDecidability === 'requires_same_session_observation'
        ? 'Requires In-Session Observation'
        : 'Pre-Open Decidable';

    return (
      <>
        <Surface className="mb-4 flex flex-wrap items-center gap-3 px-4 py-3">
          <div className="min-w-0 flex-1">
            <SectionEyebrow>{modeLabel}</SectionEyebrow>
            {completedScreeningJob ? <ScreeningJobStatusInline job={completedScreeningJob} /> : null}
          </div>
          <Button onClick={() => void handleRunScreening()} disabled={screeningIsRunning}>
            {runButtonLabel}
          </Button>
        </Surface>

        {completedScreeningJob ? null : (
          <ScreeningJobProgress
            job={screeningJob}
            onCancel={screeningIsRunning ? handleCancelScreening : undefined}
            isCancelling={cancelScreeningPending}
          />
        )}

        <ScreeningSummary
          summary={screeningSummary}
          markets={screeningMarkets}
          recentDays={screeningRecentDays}
          referenceDate={screeningReferenceDate}
        />
        <ScreeningTable
          results={screeningResults}
          isLoading={screeningTableLoading}
          isFetching={screeningIsRunning}
          error={screeningError}
          onStockClick={onStockClick}
        />

        <ScreeningJobHistoryTable
          entryDecidability={entryDecidability}
          jobs={screeningJobHistory}
          isLoading={false}
          showHistory={showScreeningJobHistory}
          onShowHistoryChange={onShowScreeningJobHistoryChange}
          selectedJobId={screeningJob?.job_id ?? null}
          onSelectJob={onSelectScreeningJob}
        />
      </>
    );
  }
  return null;
}

interface ScreeningControllerArgs {
  entryDecidability: EntryDecidability;
  params: ScreeningParams;
  setParams: (params: ScreeningParams) => void;
  allowedStrategies: string[] | undefined;
  activeJobId: string | null;
  setActiveJobId: (jobId: string | null) => void;
  history: ScreeningJobResponse[];
  upsertHistory: (job: ScreeningJobResponse) => void;
}

interface ScreeningController {
  allowedStrategies: string[];
  params: ScreeningParams;
  setParams: (params: ScreeningParams) => void;
  result: MarketScreeningResponse | null;
  history: ScreeningJobResponse[];
  job: ScreeningJobResponse | null;
  isRunning: boolean;
  error: Error | null;
  cancelPending: boolean;
  handleRun: () => Promise<void>;
  handleSelectJob: (job: ScreeningJobResponse) => void;
  handleCancel: () => void;
}

function isStaleScreeningJobError(error: unknown): boolean {
  return error instanceof ApiError && error.status === 404;
}

function resolveScreeningError(
  runError: Error | null,
  staleJob: boolean,
  statusError: Error | null,
  resultError: Error | null
): Error | null {
  return (runError ?? (staleJob ? null : statusError) ?? resultError) as Error | null;
}

function useScreeningHistorySync(
  entries: Array<ScreeningJobResponse | null | undefined>,
  upsertHistory: (job: ScreeningJobResponse) => void
): void {
  const [runEntry, statusEntry, cancelEntry] = entries;

  useEffect(() => {
    for (const entry of [runEntry, statusEntry, cancelEntry]) {
      if (entry) {
        upsertHistory(entry);
      }
    }
  }, [cancelEntry, runEntry, statusEntry, upsertHistory]);
}

function useStaleScreeningJobReset(shouldReset: boolean, setActiveJobId: (jobId: string | null) => void): void {
  useEffect(() => {
    if (!shouldReset) {
      return;
    }
    setActiveJobId(null);
  }, [setActiveJobId, shouldReset]);
}

function useScreeningController({
  entryDecidability,
  params,
  setParams,
  allowedStrategies,
  activeJobId,
  setActiveJobId,
  history,
  upsertHistory,
}: ScreeningControllerArgs): ScreeningController {
  const runScreeningJob = useRunScreeningJob();
  const cancelScreeningJob = useCancelScreeningJob();
  const screeningSse = useScreeningJobSSE(activeJobId);
  const screeningJobStatus = useScreeningJobStatus(activeJobId, screeningSse.isConnected);
  const shouldFetchResult = screeningJobStatus.data?.status === 'completed';
  const screeningResultQuery = useScreeningResult(activeJobId, shouldFetchResult);
  const statusError = screeningJobStatus.error as Error | null;
  const staleJob = isStaleScreeningJobError(statusError);

  useSanitizedScreeningParams(params, setParams, allowedStrategies, entryDecidability);
  useScreeningHistorySync([runScreeningJob.data, screeningJobStatus.data, cancelScreeningJob.data], upsertHistory);
  useStaleScreeningJobReset(Boolean(activeJobId) && staleJob, setActiveJobId);

  const job = screeningJobStatus.data ?? runScreeningJob.data ?? null;
  const status = job?.status ?? null;
  const isRunning = runScreeningJob.isPending || status === 'pending' || status === 'running';
  const error = resolveScreeningError(
    runScreeningJob.error as Error | null,
    staleJob,
    statusError,
    screeningResultQuery.error as Error | null
  );

  const handleRun = useCallback(async () => {
    const job = await runScreeningJob.mutateAsync(
      sanitizeScreeningParams(params, allowedStrategies, entryDecidability)
    );
    setActiveJobId(job.job_id);
    upsertHistory(job);
  }, [allowedStrategies, entryDecidability, params, runScreeningJob, setActiveJobId, upsertHistory]);

  const handleSelectJob = useCallback(
    (job: ScreeningJobResponse) => {
      setActiveJobId(job.job_id);
    },
    [setActiveJobId]
  );

  const handleCancel = useCallback(() => {
    if (!activeJobId) {
      return;
    }
    cancelScreeningJob.mutate(activeJobId);
  }, [activeJobId, cancelScreeningJob]);

  return {
    allowedStrategies: allowedStrategies ?? [],
    params,
    setParams,
    result: screeningResultQuery.data ?? null,
    history,
    job,
    isRunning,
    error,
    cancelPending: cancelScreeningJob.isPending,
    handleRun,
    handleSelectJob,
    handleCancel,
  };
}

export function ScreeningPage() {
  useMigrateScreeningRouteState();
  const {
    activeSubTab,
    preOpenScreeningParams,
    inSessionScreeningParams,
    setActiveSubTab,
    setPreOpenScreeningParams,
    setInSessionScreeningParams,
  } = useScreeningRouteState();
  const activePreOpenScreeningJobId = useScreeningStore((state) => state.activePreOpenScreeningJobId);
  const activeInSessionScreeningJobId = useScreeningStore((state) => state.activeInSessionScreeningJobId);
  const preOpenScreeningJobHistory = useScreeningStore((state) => state.preOpenScreeningJobHistory);
  const inSessionScreeningJobHistory = useScreeningStore((state) => state.inSessionScreeningJobHistory);
  const setActivePreOpenScreeningJobId = useScreeningStore((state) => state.setActivePreOpenScreeningJobId);
  const setActiveInSessionScreeningJobId = useScreeningStore((state) => state.setActiveInSessionScreeningJobId);
  const upsertPreOpenScreeningJobHistory = useScreeningStore((state) => state.upsertPreOpenScreeningJobHistory);
  const upsertInSessionScreeningJobHistory = useScreeningStore((state) => state.upsertInSessionScreeningJobHistory);

  const navigate = useNavigate();
  const { data: strategiesData, isLoading: isLoadingStrategies } = useStrategies();
  const [screeningJobHistoryVisibility, setScreeningJobHistoryVisibility] = useState<
    Record<EntryDecidability, boolean>
  >({
    pre_open_decidable: false,
    requires_same_session_observation: false,
  });

  const productionStrategies = strategiesData?.strategies?.filter((strategy) => strategy.category === 'production');
  const preOpenProductionStrategies = selectStrategyNames(productionStrategies, isPreOpenScreeningStrategy);
  const inSessionProductionStrategies = selectStrategyNames(productionStrategies, isInSessionScreeningStrategy);
  const preOpenAutoMarkets = resolveAutoScreeningMarkets(
    productionStrategies,
    preOpenScreeningParams,
    'pre_open_decidable'
  );
  const inSessionAutoMarkets = resolveAutoScreeningMarkets(
    productionStrategies,
    inSessionScreeningParams,
    'requires_same_session_observation'
  );
  const activeEntryDecidability: EntryDecidability =
    activeSubTab === 'inSessionScreening' ? 'requires_same_session_observation' : 'pre_open_decidable';
  const preOpenScreening = useScreeningController({
    entryDecidability: 'pre_open_decidable',
    params: preOpenScreeningParams,
    setParams: setPreOpenScreeningParams,
    allowedStrategies: preOpenProductionStrategies,
    activeJobId: activePreOpenScreeningJobId,
    setActiveJobId: setActivePreOpenScreeningJobId,
    history: preOpenScreeningJobHistory,
    upsertHistory: upsertPreOpenScreeningJobHistory,
  });
  const inSessionScreening = useScreeningController({
    entryDecidability: 'requires_same_session_observation',
    params: inSessionScreeningParams,
    setParams: setInSessionScreeningParams,
    allowedStrategies: inSessionProductionStrategies,
    activeJobId: activeInSessionScreeningJobId,
    setActiveJobId: setActiveInSessionScreeningJobId,
    history: inSessionScreeningJobHistory,
    upsertHistory: upsertInSessionScreeningJobHistory,
  });
  const activeScreening =
    activeEntryDecidability === 'requires_same_session_observation' ? inSessionScreening : preOpenScreening;
  const activeScreeningAutoMarkets =
    activeEntryDecidability === 'requires_same_session_observation' ? inSessionAutoMarkets : preOpenAutoMarkets;
  const activeScreeningJobHistoryVisible = screeningJobHistoryVisibility[activeEntryDecidability];

  const handleStockClick = useCallback(
    (code: string, strategy?: string, matchedDate?: string) => {
      void navigate({
        to: '/charts',
        search: {
          symbol: code,
          ...(strategy ? { strategy } : {}),
          ...(matchedDate ? { matchedDate } : {}),
        },
      });
    },
    [navigate]
  );
  const handleScreeningHistoryVisibilityChange = useCallback(
    (showHistory: boolean) => {
      setScreeningJobHistoryVisibility((current) => ({
        ...current,
        [activeEntryDecidability]: showHistory,
      }));
    },
    [activeEntryDecidability]
  );

  return (
    <div className="flex h-full min-h-0 flex-col p-4">
      <ModeSwitcherPanel label="Screening Mode" items={subTabs} value={activeSubTab} onChange={setActiveSubTab} />

      <SplitLayout className="mt-4 gap-4">
        <SplitSidebar className="w-64">
          <ScreeningSidebar
            activeSubTab={activeSubTab}
            entryDecidability={activeEntryDecidability}
            screeningParams={activeScreening.params}
            screeningAutoMarkets={activeScreeningAutoMarkets}
            setScreeningParams={activeScreening.setParams}
            productionStrategies={activeScreening.allowedStrategies}
            isLoadingStrategies={isLoadingStrategies}
          />
        </SplitSidebar>

        <SplitMain>
          <ScreeningMainContent
            activeSubTab={activeSubTab}
            entryDecidability={activeEntryDecidability}
            handleRunScreening={activeScreening.handleRun}
            screeningIsRunning={activeScreening.isRunning}
            screeningJob={activeScreening.job}
            handleCancelScreening={activeScreening.handleCancel}
            cancelScreeningPending={activeScreening.cancelPending}
            screeningJobHistory={activeScreening.history}
            showScreeningJobHistory={activeScreeningJobHistoryVisible}
            onShowScreeningJobHistoryChange={handleScreeningHistoryVisibilityChange}
            onSelectScreeningJob={activeScreening.handleSelectJob}
            screeningSummary={activeScreening.result?.summary}
            screeningMarkets={activeScreening.result?.markets || []}
            screeningRecentDays={activeScreening.result?.recentDays || (activeScreening.params.recentDays ?? 0)}
            screeningReferenceDate={activeScreening.result?.referenceDate}
            screeningResults={activeScreening.result?.results || []}
            screeningTableLoading={!activeScreening.result && activeScreening.isRunning}
            screeningError={activeScreening.error}
            onStockClick={handleStockClick}
          />
        </SplitMain>
      </SplitLayout>
    </div>
  );
}
