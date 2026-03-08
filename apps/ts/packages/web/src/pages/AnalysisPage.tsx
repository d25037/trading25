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
import { useFundamentalRanking } from '@/hooks/useFundamentalRanking';
import { useRanking } from '@/hooks/useRanking';
import {
  useCancelScreeningJob,
  useRunScreeningJob,
  useScreeningJobStatus,
  useScreeningResult,
} from '@/hooks/useScreening';
import { ApiError } from '@/lib/api-client';
import { cn } from '@/lib/utils';
import type { AnalysisSubTab } from '@/stores/analysisStore';
import { useAnalysisStore } from '@/stores/analysisStore';
import { useChartStore } from '@/stores/chartStore';
import type { StrategyMetadata } from '@/types/backtest';
import type { FundamentalRankingParams, MarketFundamentalRankingResponse } from '@/types/fundamentalRanking';
import type { MarketRankingResponse, RankingParams } from '@/types/ranking';
import type {
  MarketScreeningResponse,
  ScreeningMode,
  ScreeningJobResponse,
  ScreeningParams,
  ScreeningResultItem,
} from '@/types/screening';

const subTabs: { id: AnalysisSubTab; label: string; icon: typeof BarChart3 }[] = [
  { id: 'screening', label: 'Screening', icon: Filter },
  { id: 'oracleScreening', label: 'Oracle Screening', icon: Filter },
  { id: 'ranking', label: 'Daily Ranking', icon: BarChart3 },
  { id: 'fundamentalRanking', label: 'Fundamental Ranking', icon: BarChart3 },
];

function isScreeningSubTab(tab: AnalysisSubTab): tab is 'screening' | 'oracleScreening' {
  return tab === 'screening' || tab === 'oracleScreening';
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
  mode: ScreeningMode
): ScreeningParams {
  return {
    ...params,
    mode,
    strategies: sanitizeStrategies(params.strategies, allowedStrategies),
  };
}

function areScreeningParamsEqual(left: ScreeningParams, right: ScreeningParams): boolean {
  return (
    left.mode === right.mode &&
    left.markets === right.markets &&
    left.strategies === right.strategies &&
    left.recentDays === right.recentDays &&
    left.date === right.date &&
    left.sortBy === right.sortBy &&
    left.order === right.order &&
    left.limit === right.limit
  );
}

function isStandardScreeningStrategy(strategy: StrategyMetadata): boolean {
  return strategy.category === 'production' && (strategy.screening_mode ?? 'standard') === 'standard';
}

function isOracleScreeningStrategy(strategy: StrategyMetadata): boolean {
  return strategy.category === 'production' && strategy.screening_mode === 'oracle';
}

function selectStrategyNames(
  strategies: StrategyMetadata[] | undefined,
  predicate: (strategy: StrategyMetadata) => boolean
): string[] | undefined {
  if (!strategies) {
    return undefined;
  }

  return strategies.filter(predicate).map((strategy) => strategy.name).sort((left, right) => left.localeCompare(right));
}

function useSanitizedScreeningParams(
  params: ScreeningParams,
  setParams: (params: ScreeningParams) => void,
  allowedStrategies: string[] | undefined,
  mode: ScreeningMode
): void {
  useEffect(() => {
    if (!allowedStrategies) {
      return;
    }

    const sanitized = sanitizeScreeningParams(params, allowedStrategies, mode);
    if (!areScreeningParamsEqual(sanitized, params)) {
      setParams(sanitized);
    }
  }, [allowedStrategies, mode, params, setParams]);
}

interface AnalysisSidebarProps {
  activeSubTab: AnalysisSubTab;
  screeningMode: ScreeningMode;
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
  screeningMode,
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
        mode={screeningMode}
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
  screeningMode: ScreeningMode;
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
  screeningMode,
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
    const runButtonLabel = screeningMode === 'oracle' ? 'Run Oracle Screening' : 'Run Screening';

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
          mode={screeningMode}
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
export function AnalysisPage() {
  const activeSubTab = useAnalysisStore((state) => state.activeSubTab);
  const screeningParams = useAnalysisStore((state) => state.screeningParams);
  const oracleScreeningParams = useAnalysisStore((state) => state.oracleScreeningParams);
  const rankingParams = useAnalysisStore((state) => state.rankingParams);
  const fundamentalRankingParams = useAnalysisStore((state) => state.fundamentalRankingParams);
  const activeScreeningJobId = useAnalysisStore((state) => state.activeScreeningJobId);
  const activeOracleScreeningJobId = useAnalysisStore((state) => state.activeOracleScreeningJobId);
  const screeningResult = useAnalysisStore((state) => state.screeningResult);
  const oracleScreeningResult = useAnalysisStore((state) => state.oracleScreeningResult);
  const screeningJobHistory = useAnalysisStore((state) => state.screeningJobHistory);
  const oracleScreeningJobHistory = useAnalysisStore((state) => state.oracleScreeningJobHistory);
  const setActiveSubTab = useAnalysisStore((state) => state.setActiveSubTab);
  const setScreeningParams = useAnalysisStore((state) => state.setScreeningParams);
  const setOracleScreeningParams = useAnalysisStore((state) => state.setOracleScreeningParams);
  const setRankingParams = useAnalysisStore((state) => state.setRankingParams);
  const setFundamentalRankingParams = useAnalysisStore((state) => state.setFundamentalRankingParams);
  const setActiveScreeningJobId = useAnalysisStore((state) => state.setActiveScreeningJobId);
  const setActiveOracleScreeningJobId = useAnalysisStore((state) => state.setActiveOracleScreeningJobId);
  const setScreeningResult = useAnalysisStore((state) => state.setScreeningResult);
  const setOracleScreeningResult = useAnalysisStore((state) => state.setOracleScreeningResult);
  const upsertScreeningJobHistory = useAnalysisStore((state) => state.upsertScreeningJobHistory);
  const upsertOracleScreeningJobHistory = useAnalysisStore((state) => state.upsertOracleScreeningJobHistory);

  const navigate = useNavigate();
  const { setSelectedSymbol } = useChartStore();
  const { data: strategiesData, isLoading: isLoadingStrategies } = useStrategies();
  const [screeningJobHistoryVisibility, setScreeningJobHistoryVisibility] = useState<Record<ScreeningMode, boolean>>({
    standard: true,
    oracle: true,
  });

  const runScreeningJob = useRunScreeningJob();
  const runOracleScreeningJob = useRunScreeningJob();
  const screeningJobStatus = useScreeningJobStatus(activeScreeningJobId);
  const oracleScreeningJobStatus = useScreeningJobStatus(activeOracleScreeningJobId);
  const cancelScreeningJob = useCancelScreeningJob();
  const cancelOracleScreeningJob = useCancelScreeningJob();

  const shouldFetchScreeningResult = screeningJobStatus.data?.status === 'completed';
  const shouldFetchOracleScreeningResult = oracleScreeningJobStatus.data?.status === 'completed';
  const screeningResultQuery = useScreeningResult(activeScreeningJobId, shouldFetchScreeningResult);
  const oracleScreeningResultQuery = useScreeningResult(
    activeOracleScreeningJobId,
    shouldFetchOracleScreeningResult
  );
  const screeningJobStatusError = screeningJobStatus.error;
  const oracleScreeningJobStatusError = oracleScreeningJobStatus.error;
  const isStaleScreeningJob = screeningJobStatusError instanceof ApiError && screeningJobStatusError.status === 404;
  const isStaleOracleScreeningJob =
    oracleScreeningJobStatusError instanceof ApiError && oracleScreeningJobStatusError.status === 404;
  const shouldResetStaleScreeningJobId = Boolean(activeScreeningJobId) && isStaleScreeningJob;
  const shouldResetStaleOracleScreeningJobId =
    Boolean(activeOracleScreeningJobId) && isStaleOracleScreeningJob;

  const rankingQuery = useRanking(rankingParams, activeSubTab === 'ranking');
  const fundamentalRankingQuery = useFundamentalRanking(
    fundamentalRankingParams,
    activeSubTab === 'fundamentalRanking'
  );

  const productionStrategies = strategiesData?.strategies?.filter((strategy) => strategy.category === 'production');
  const standardProductionStrategies = selectStrategyNames(productionStrategies, isStandardScreeningStrategy);
  const oracleProductionStrategies = selectStrategyNames(productionStrategies, isOracleScreeningStrategy);
  const activeScreeningMode: ScreeningMode = activeSubTab === 'oracleScreening' ? 'oracle' : 'standard';
  const activeStrategyOptions =
    activeSubTab === 'oracleScreening' ? (oracleProductionStrategies ?? []) : (standardProductionStrategies ?? []);
  const activeScreeningParams = activeSubTab === 'oracleScreening' ? oracleScreeningParams : screeningParams;
  const activeScreeningResult = activeSubTab === 'oracleScreening' ? oracleScreeningResult : screeningResult;
  const activeScreeningJobHistory = activeSubTab === 'oracleScreening' ? oracleScreeningJobHistory : screeningJobHistory;
  const activeScreeningJobHistoryVisible = screeningJobHistoryVisibility[activeScreeningMode];

  useSanitizedScreeningParams(screeningParams, setScreeningParams, standardProductionStrategies, 'standard');
  useSanitizedScreeningParams(
    oracleScreeningParams,
    setOracleScreeningParams,
    oracleProductionStrategies,
    'oracle'
  );

  useEffect(() => {
    if (!screeningResultQuery.data) return;
    setScreeningResult(screeningResultQuery.data);
  }, [screeningResultQuery.data, setScreeningResult]);

  useEffect(() => {
    if (!oracleScreeningResultQuery.data) return;
    setOracleScreeningResult(oracleScreeningResultQuery.data);
  }, [oracleScreeningResultQuery.data, setOracleScreeningResult]);

  useEffect(() => {
    if (!runScreeningJob.data) return;
    upsertScreeningJobHistory(runScreeningJob.data);
  }, [runScreeningJob.data, upsertScreeningJobHistory]);

  useEffect(() => {
    if (!runOracleScreeningJob.data) return;
    upsertOracleScreeningJobHistory(runOracleScreeningJob.data);
  }, [runOracleScreeningJob.data, upsertOracleScreeningJobHistory]);

  useEffect(() => {
    if (!screeningJobStatus.data) return;
    upsertScreeningJobHistory(screeningJobStatus.data);
  }, [screeningJobStatus.data, upsertScreeningJobHistory]);

  useEffect(() => {
    if (!oracleScreeningJobStatus.data) return;
    upsertOracleScreeningJobHistory(oracleScreeningJobStatus.data);
  }, [oracleScreeningJobStatus.data, upsertOracleScreeningJobHistory]);

  useEffect(() => {
    if (!cancelScreeningJob.data) return;
    upsertScreeningJobHistory(cancelScreeningJob.data);
  }, [cancelScreeningJob.data, upsertScreeningJobHistory]);

  useEffect(() => {
    if (!cancelOracleScreeningJob.data) return;
    upsertOracleScreeningJobHistory(cancelOracleScreeningJob.data);
  }, [cancelOracleScreeningJob.data, upsertOracleScreeningJobHistory]);

  useEffect(() => {
    if (!shouldResetStaleScreeningJobId) return;
    setActiveScreeningJobId(null);
  }, [shouldResetStaleScreeningJobId, setActiveScreeningJobId]);

  useEffect(() => {
    if (!shouldResetStaleOracleScreeningJobId) return;
    setActiveOracleScreeningJobId(null);
  }, [shouldResetStaleOracleScreeningJobId, setActiveOracleScreeningJobId]);

  const handleRunScreening = useCallback(async () => {
    if (activeScreeningMode === 'oracle') {
      const job = await runOracleScreeningJob.mutateAsync(
        sanitizeScreeningParams(oracleScreeningParams, oracleProductionStrategies, 'oracle')
      );
      setActiveOracleScreeningJobId(job.job_id);
      upsertOracleScreeningJobHistory(job);
      return;
    }

    const job = await runScreeningJob.mutateAsync(
      sanitizeScreeningParams(screeningParams, standardProductionStrategies, 'standard')
    );
    setActiveScreeningJobId(job.job_id);
    upsertScreeningJobHistory(job);
  }, [
    activeScreeningMode,
    oracleProductionStrategies,
    oracleScreeningParams,
    runOracleScreeningJob,
    runScreeningJob,
    screeningParams,
    setActiveOracleScreeningJobId,
    setActiveScreeningJobId,
    standardProductionStrategies,
    upsertOracleScreeningJobHistory,
    upsertScreeningJobHistory,
  ]);

  const handleSelectScreeningJob = useCallback(
    (job: ScreeningJobResponse) => {
      if (activeScreeningMode === 'oracle') {
        setActiveOracleScreeningJobId(job.job_id);
        return;
      }
      setActiveScreeningJobId(job.job_id);
    },
    [activeScreeningMode, setActiveOracleScreeningJobId, setActiveScreeningJobId]
  );

  const handleCancelScreening = useCallback(() => {
    if (activeScreeningMode === 'oracle') {
      if (!activeOracleScreeningJobId) return;
      cancelOracleScreeningJob.mutate(activeOracleScreeningJobId);
      return;
    }

    if (!activeScreeningJobId) return;
    cancelScreeningJob.mutate(activeScreeningJobId);
  }, [
    activeOracleScreeningJobId,
    activeScreeningJobId,
    activeScreeningMode,
    cancelOracleScreeningJob,
    cancelScreeningJob,
  ]);

  const handleStockClick = useCallback(
    (code: string) => {
      setSelectedSymbol(code);
      void navigate({ to: '/charts' });
    },
    [setSelectedSymbol, navigate]
  );
  const handleScreeningHistoryVisibilityChange = useCallback(
    (showHistory: boolean) => {
      setScreeningJobHistoryVisibility((current) => ({
        ...current,
        [activeScreeningMode]: showHistory,
      }));
    },
    [activeScreeningMode]
  );

  const screeningJob = screeningJobStatus.data ?? runScreeningJob.data ?? null;
  const screeningStatus = screeningJob?.status ?? null;
  const screeningIsRunning =
    runScreeningJob.isPending || screeningStatus === 'pending' || screeningStatus === 'running';
  const screeningError = (runScreeningJob.error ??
    (isStaleScreeningJob ? null : screeningJobStatusError) ??
    screeningResultQuery.error) as Error | null;
  const oracleScreeningJob = oracleScreeningJobStatus.data ?? runOracleScreeningJob.data ?? null;
  const oracleScreeningStatus = oracleScreeningJob?.status ?? null;
  const oracleScreeningIsRunning =
    runOracleScreeningJob.isPending ||
    oracleScreeningStatus === 'pending' ||
    oracleScreeningStatus === 'running';
  const oracleScreeningError = (runOracleScreeningJob.error ??
    (isStaleOracleScreeningJob ? null : oracleScreeningJobStatusError) ??
    oracleScreeningResultQuery.error) as Error | null;
  const activeScreeningJob = activeSubTab === 'oracleScreening' ? oracleScreeningJob : screeningJob;
  const activeScreeningIsRunning =
    activeSubTab === 'oracleScreening' ? oracleScreeningIsRunning : screeningIsRunning;
  const activeCancelPending =
    activeSubTab === 'oracleScreening' ? cancelOracleScreeningJob.isPending : cancelScreeningJob.isPending;
  const activeScreeningError =
    activeSubTab === 'oracleScreening' ? oracleScreeningError : screeningError;

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
            screeningMode={activeScreeningMode}
            screeningParams={activeScreeningParams}
            rankingParams={rankingParams}
            fundamentalRankingParams={fundamentalRankingParams}
            setScreeningParams={activeSubTab === 'oracleScreening' ? setOracleScreeningParams : setScreeningParams}
            setRankingParams={setRankingParams}
            setFundamentalRankingParams={setFundamentalRankingParams}
            productionStrategies={activeStrategyOptions}
            isLoadingStrategies={isLoadingStrategies}
          />
        </div>

        {/* Main Content */}
        <div className="flex-1 flex flex-col min-w-0 min-h-0">
          <AnalysisMainContent
            activeSubTab={activeSubTab}
            screeningMode={activeScreeningMode}
            handleRunScreening={handleRunScreening}
            screeningIsRunning={activeScreeningIsRunning}
            screeningJob={activeScreeningJob}
            handleCancelScreening={handleCancelScreening}
            cancelScreeningPending={activeCancelPending}
            screeningJobHistory={activeScreeningJobHistory}
            showScreeningJobHistory={activeScreeningJobHistoryVisible}
            onShowScreeningJobHistoryChange={handleScreeningHistoryVisibilityChange}
            onSelectScreeningJob={handleSelectScreeningJob}
            screeningSummary={activeScreeningResult?.summary}
            screeningMarkets={activeScreeningResult?.markets || []}
            screeningRecentDays={activeScreeningResult?.recentDays || (activeScreeningParams.recentDays ?? 0)}
            screeningReferenceDate={activeScreeningResult?.referenceDate}
            screeningResults={activeScreeningResult?.results || []}
            screeningTableLoading={!activeScreeningResult && activeScreeningIsRunning}
            screeningError={activeScreeningError}
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
