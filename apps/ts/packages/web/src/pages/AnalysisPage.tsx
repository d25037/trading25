import { useNavigate } from '@tanstack/react-router';
import { BarChart3, Filter } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
import {
  FundamentalRankingFilters,
  FundamentalRankingSummary,
  FundamentalRankingTable,
} from '@/components/FundamentalRanking';
import { RankingFilters, RankingSummary, RankingTable } from '@/components/Ranking';
import { ScreeningFilters } from '@/components/Screening/ScreeningFilters';
import { ScreeningJobHistoryTable } from '@/components/Screening/ScreeningJobHistoryTable';
import { ScreeningJobProgress, ScreeningJobStatusInline } from '@/components/Screening/ScreeningJobProgress';
import { ScreeningSummary } from '@/components/Screening/ScreeningSummary';
import { ScreeningTable } from '@/components/Screening/ScreeningTable';
import { Button } from '@/components/ui/button';
import { useStrategies } from '@/hooks/useBacktest';
import { useAnalysisRouteState, useMigrateAnalysisRouteState } from '@/hooks/usePageRouteState';
import { useFundamentalRanking } from '@/hooks/useFundamentalRanking';
import { useRanking } from '@/hooks/useRanking';
import {
  useCancelScreeningJob,
  useRunScreeningJob,
  useScreeningJobSSE,
  useScreeningJobStatus,
  useScreeningResult,
} from '@/hooks/useScreening';
import { ApiError } from '@/lib/api-client';
import { cn } from '@/lib/utils';
import type { AnalysisSubTab } from '@/stores/analysisStore';
import { useAnalysisStore } from '@/stores/analysisStore';
import type { StrategyMetadata } from '@/types/backtest';
import type { FundamentalRankingParams, MarketFundamentalRankingResponse } from '@/types/fundamentalRanking';
import type { MarketRankingResponse, RankingParams } from '@/types/ranking';
import type {
  EntryDecidability,
  MarketScreeningResponse,
  ScreeningJobResponse,
  ScreeningParams,
  ScreeningResultItem,
} from '@/types/screening';

const subTabs: { id: AnalysisSubTab; label: string; icon: typeof BarChart3 }[] = [
  { id: 'preOpenScreening', label: 'Pre-Open Decidable', icon: Filter },
  { id: 'inSessionScreening', label: 'Requires In-Session Observation', icon: Filter },
  { id: 'ranking', label: 'Daily Ranking', icon: BarChart3 },
  { id: 'fundamentalRanking', label: 'Fundamental Ranking', icon: BarChart3 },
];

function isScreeningSubTab(tab: AnalysisSubTab): tab is 'preOpenScreening' | 'inSessionScreening' {
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

interface AnalysisSidebarProps {
  activeSubTab: AnalysisSubTab;
  entryDecidability: EntryDecidability;
  screeningParams: ScreeningParams;
  rankingParams: RankingParams;
  fundamentalRankingParams: FundamentalRankingParams;
  setScreeningParams: (params: ScreeningParams) => void;
  setRankingParams: (params: RankingParams) => void;
  setFundamentalRankingParams: (params: FundamentalRankingParams) => void;
  productionStrategies: string[];
  isLoadingStrategies: boolean;
}

function AnalysisSidebar({
  activeSubTab,
  entryDecidability,
  screeningParams,
  rankingParams,
  fundamentalRankingParams,
  setScreeningParams,
  setRankingParams,
  setFundamentalRankingParams,
  productionStrategies,
  isLoadingStrategies,
}: AnalysisSidebarProps) {
  if (isScreeningSubTab(activeSubTab)) {
    return (
      <ScreeningFilters
        entryDecidability={entryDecidability}
        params={screeningParams}
        onChange={setScreeningParams}
        strategyOptions={productionStrategies}
        strategiesLoading={isLoadingStrategies}
      />
    );
  }

  if (activeSubTab === 'ranking') {
    return <RankingFilters params={rankingParams} onChange={setRankingParams} />;
  }

  return <FundamentalRankingFilters params={fundamentalRankingParams} onChange={setFundamentalRankingParams} />;
}

interface AnalysisMainContentProps {
  activeSubTab: AnalysisSubTab;
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
  rankingData: MarketRankingResponse | undefined;
  rankingLoading: boolean;
  rankingError: Error | null;
  rankingPeriodDays: number | undefined;
  fundamentalData: MarketFundamentalRankingResponse | undefined;
  fundamentalLoading: boolean;
  fundamentalError: Error | null;
}

function AnalysisMainContent({
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
  rankingData,
  rankingLoading,
  rankingError,
  rankingPeriodDays,
  fundamentalData,
  fundamentalLoading,
  fundamentalError,
}: AnalysisMainContentProps) {
  if (isScreeningSubTab(activeSubTab)) {
    const completedScreeningJob = screeningJob?.status === 'completed' ? screeningJob : null;
    const runButtonLabel =
      entryDecidability === 'requires_same_session_observation'
        ? 'Run In-Session Screening'
        : 'Run Pre-Open Screening';

    return (
      <>
        <div className="mb-3 flex flex-wrap items-center gap-3">
          <div className="min-w-0 flex-1">
            {completedScreeningJob ? <ScreeningJobStatusInline job={completedScreeningJob} /> : null}
          </div>
          <Button onClick={() => void handleRunScreening()} disabled={screeningIsRunning}>
            {runButtonLabel}
          </Button>
        </div>

        {completedScreeningJob ? null : (
          <ScreeningJobProgress
            job={screeningJob}
            onCancel={screeningIsRunning ? handleCancelScreening : undefined}
            isCancelling={cancelScreeningPending}
          />
        )}

        <ScreeningJobHistoryTable
          entryDecidability={entryDecidability}
          jobs={screeningJobHistory}
          isLoading={false}
          showHistory={showScreeningJobHistory}
          onShowHistoryChange={onShowScreeningJobHistoryChange}
          selectedJobId={screeningJob?.job_id ?? null}
          onSelectJob={onSelectScreeningJob}
        />

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
      </>
    );
  }

  if (activeSubTab === 'ranking') {
    return (
      <>
        <RankingSummary data={rankingData} />
        <RankingTable
          rankings={rankingData?.rankings}
          isLoading={rankingLoading}
          error={rankingError}
          onStockClick={onStockClick}
          periodDays={rankingPeriodDays}
        />
      </>
    );
  }

  return (
    <>
      <FundamentalRankingSummary data={fundamentalData} />
      <FundamentalRankingTable
        rankings={fundamentalData?.rankings}
        isLoading={fundamentalLoading}
        error={fundamentalError}
        onStockClick={onStockClick}
      />
    </>
  );
}

interface ScreeningControllerArgs {
  entryDecidability: EntryDecidability;
  params: ScreeningParams;
  setParams: (params: ScreeningParams) => void;
  allowedStrategies: string[] | undefined;
  activeJobId: string | null;
  setActiveJobId: (jobId: string | null) => void;
  result: MarketScreeningResponse | null;
  setResult: (result: MarketScreeningResponse) => void;
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

function useScreeningResultSync(
  result: MarketScreeningResponse | undefined,
  setResult: (result: MarketScreeningResponse) => void
): void {
  useEffect(() => {
    if (!result) {
      return;
    }
    setResult(result);
  }, [result, setResult]);
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
  result,
  setResult,
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
  useScreeningResultSync(screeningResultQuery.data, setResult);
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
    result,
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

export function AnalysisPage() {
  useMigrateAnalysisRouteState();
  const {
    activeSubTab,
    preOpenScreeningParams,
    inSessionScreeningParams,
    rankingParams,
    fundamentalRankingParams,
    setActiveSubTab,
    setPreOpenScreeningParams,
    setInSessionScreeningParams,
    setRankingParams,
    setFundamentalRankingParams,
  } = useAnalysisRouteState();
  const activePreOpenScreeningJobId = useAnalysisStore((state) => state.activePreOpenScreeningJobId);
  const activeInSessionScreeningJobId = useAnalysisStore((state) => state.activeInSessionScreeningJobId);
  const preOpenScreeningResult = useAnalysisStore((state) => state.preOpenScreeningResult);
  const inSessionScreeningResult = useAnalysisStore((state) => state.inSessionScreeningResult);
  const preOpenScreeningJobHistory = useAnalysisStore((state) => state.preOpenScreeningJobHistory);
  const inSessionScreeningJobHistory = useAnalysisStore((state) => state.inSessionScreeningJobHistory);
  const setActivePreOpenScreeningJobId = useAnalysisStore((state) => state.setActivePreOpenScreeningJobId);
  const setActiveInSessionScreeningJobId = useAnalysisStore((state) => state.setActiveInSessionScreeningJobId);
  const setPreOpenScreeningResult = useAnalysisStore((state) => state.setPreOpenScreeningResult);
  const setInSessionScreeningResult = useAnalysisStore((state) => state.setInSessionScreeningResult);
  const upsertPreOpenScreeningJobHistory = useAnalysisStore((state) => state.upsertPreOpenScreeningJobHistory);
  const upsertInSessionScreeningJobHistory = useAnalysisStore((state) => state.upsertInSessionScreeningJobHistory);

  const navigate = useNavigate();
  const { data: strategiesData, isLoading: isLoadingStrategies } = useStrategies();
  const [screeningJobHistoryVisibility, setScreeningJobHistoryVisibility] = useState<
    Record<EntryDecidability, boolean>
  >({
    pre_open_decidable: true,
    requires_same_session_observation: true,
  });

  const rankingQuery = useRanking(rankingParams, activeSubTab === 'ranking');
  const fundamentalRankingQuery = useFundamentalRanking(
    fundamentalRankingParams,
    activeSubTab === 'fundamentalRanking'
  );

  const productionStrategies = strategiesData?.strategies?.filter((strategy) => strategy.category === 'production');
  const preOpenProductionStrategies = selectStrategyNames(productionStrategies, isPreOpenScreeningStrategy);
  const inSessionProductionStrategies = selectStrategyNames(productionStrategies, isInSessionScreeningStrategy);
  const activeEntryDecidability: EntryDecidability =
    activeSubTab === 'inSessionScreening'
      ? 'requires_same_session_observation'
      : 'pre_open_decidable';
  const preOpenScreening = useScreeningController({
    entryDecidability: 'pre_open_decidable',
    params: preOpenScreeningParams,
    setParams: setPreOpenScreeningParams,
    allowedStrategies: preOpenProductionStrategies,
    activeJobId: activePreOpenScreeningJobId,
    setActiveJobId: setActivePreOpenScreeningJobId,
    result: preOpenScreeningResult,
    setResult: setPreOpenScreeningResult,
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
    result: inSessionScreeningResult,
    setResult: setInSessionScreeningResult,
    history: inSessionScreeningJobHistory,
    upsertHistory: upsertInSessionScreeningJobHistory,
  });
  const activeScreening =
    activeEntryDecidability === 'requires_same_session_observation'
      ? inSessionScreening
      : preOpenScreening;
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
    <div className="flex h-full flex-col p-4">
      {/* Sub-tab navigation */}
      <div className="flex gap-2 mb-4">
        {subTabs.map((tab) => {
          const Icon = tab.icon;
          const isActive = activeSubTab === tab.id;
          return (
            <Button
              key={tab.id}
              variant={isActive ? 'default' : 'outline'}
              size="sm"
              className={cn('gap-2', isActive && 'shadow-md')}
              onClick={() => setActiveSubTab(tab.id)}
            >
              <Icon className="h-4 w-4" />
              {tab.label}
            </Button>
          );
        })}
      </div>

      {/* Content area */}
      <div className="flex flex-1 gap-4 min-h-0">
        {/* Sidebar */}
        <div className="w-64 flex-shrink-0">
          <AnalysisSidebar
            activeSubTab={activeSubTab}
            entryDecidability={activeEntryDecidability}
            screeningParams={activeScreening.params}
            rankingParams={rankingParams}
            fundamentalRankingParams={fundamentalRankingParams}
            setScreeningParams={activeScreening.setParams}
            setRankingParams={setRankingParams}
            setFundamentalRankingParams={setFundamentalRankingParams}
            productionStrategies={activeScreening.allowedStrategies}
            isLoadingStrategies={isLoadingStrategies}
          />
        </div>

        {/* Main Content */}
        <div className="flex-1 flex flex-col min-w-0 min-h-0">
          <AnalysisMainContent
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
            rankingData={rankingQuery.data}
            rankingLoading={rankingQuery.isLoading}
            rankingError={rankingQuery.error}
            rankingPeriodDays={rankingParams.periodDays}
            fundamentalData={fundamentalRankingQuery.data}
            fundamentalLoading={fundamentalRankingQuery.isLoading}
            fundamentalError={fundamentalRankingQuery.error}
          />
        </div>
      </div>
    </div>
  );
}
