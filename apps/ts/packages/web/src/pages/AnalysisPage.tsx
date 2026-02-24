import { useNavigate } from '@tanstack/react-router';
import { BarChart3, Filter } from 'lucide-react';
import { useCallback, useEffect } from 'react';
import {
  FundamentalRankingFilters,
  FundamentalRankingSummary,
  FundamentalRankingTable,
} from '@/components/FundamentalRanking';
import { RankingFilters, RankingSummary, RankingTable } from '@/components/Ranking';
import { ScreeningFilters } from '@/components/Screening/ScreeningFilters';
import { ScreeningJobProgress } from '@/components/Screening/ScreeningJobProgress';
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
import type { FundamentalRankingParams, MarketFundamentalRankingResponse } from '@/types/fundamentalRanking';
import type { MarketRankingResponse, RankingParams } from '@/types/ranking';
import type {
  MarketScreeningResponse,
  ScreeningJobResponse,
  ScreeningParams,
  ScreeningResultItem,
} from '@/types/screening';

const subTabs: { id: AnalysisSubTab; label: string; icon: typeof BarChart3 }[] = [
  { id: 'screening', label: 'Screening', icon: Filter },
  { id: 'ranking', label: 'Daily Ranking', icon: BarChart3 },
  { id: 'fundamentalRanking', label: 'Fundamental Ranking', icon: BarChart3 },
];

interface AnalysisSidebarProps {
  activeSubTab: AnalysisSubTab;
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
  screeningParams,
  rankingParams,
  fundamentalRankingParams,
  setScreeningParams,
  setRankingParams,
  setFundamentalRankingParams,
  productionStrategies,
  isLoadingStrategies,
}: AnalysisSidebarProps) {
  if (activeSubTab === 'screening') {
    return (
      <ScreeningFilters
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
  handleRunScreening: () => Promise<void>;
  screeningIsRunning: boolean;
  screeningJob: ScreeningJobResponse | null;
  handleCancelScreening: () => void;
  cancelScreeningPending: boolean;
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
  handleRunScreening,
  screeningIsRunning,
  screeningJob,
  handleCancelScreening,
  cancelScreeningPending,
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
  if (activeSubTab === 'screening') {
    return (
      <>
        <div className="mb-3 flex justify-end">
          <Button onClick={() => void handleRunScreening()} disabled={screeningIsRunning}>
            Run Screening
          </Button>
        </div>

        <ScreeningJobProgress
          job={screeningJob}
          onCancel={screeningIsRunning ? handleCancelScreening : undefined}
          isCancelling={cancelScreeningPending}
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
  const rankingParams = useAnalysisStore((state) => state.rankingParams);
  const fundamentalRankingParams = useAnalysisStore((state) => state.fundamentalRankingParams);
  const activeScreeningJobId = useAnalysisStore((state) => state.activeScreeningJobId);
  const screeningResult = useAnalysisStore((state) => state.screeningResult);
  const setActiveSubTab = useAnalysisStore((state) => state.setActiveSubTab);
  const setScreeningParams = useAnalysisStore((state) => state.setScreeningParams);
  const setRankingParams = useAnalysisStore((state) => state.setRankingParams);
  const setFundamentalRankingParams = useAnalysisStore((state) => state.setFundamentalRankingParams);
  const setActiveScreeningJobId = useAnalysisStore((state) => state.setActiveScreeningJobId);
  const setScreeningResult = useAnalysisStore((state) => state.setScreeningResult);

  const navigate = useNavigate();
  const { setSelectedSymbol } = useChartStore();
  const { data: strategiesData, isLoading: isLoadingStrategies } = useStrategies();

  const runScreeningJob = useRunScreeningJob();
  const screeningJobStatus = useScreeningJobStatus(activeScreeningJobId);
  const cancelScreeningJob = useCancelScreeningJob();

  const shouldFetchScreeningResult = screeningJobStatus.data?.status === 'completed';
  const screeningResultQuery = useScreeningResult(activeScreeningJobId, shouldFetchScreeningResult);
  const screeningJobStatusError = screeningJobStatus.error;
  const isStaleScreeningJob = screeningJobStatusError instanceof ApiError && screeningJobStatusError.status === 404;
  const shouldResetStaleScreeningJobId = Boolean(activeScreeningJobId) && isStaleScreeningJob;

  const rankingQuery = useRanking(rankingParams, activeSubTab === 'ranking');
  const fundamentalRankingQuery = useFundamentalRanking(
    fundamentalRankingParams,
    activeSubTab === 'fundamentalRanking'
  );

  const productionStrategies = (strategiesData?.strategies ?? [])
    .filter((strategy) => strategy.category === 'production')
    .map((strategy) => strategy.name)
    .sort((a, b) => a.localeCompare(b));

  useEffect(() => {
    if (!screeningResultQuery.data) return;
    setScreeningResult(screeningResultQuery.data);
  }, [screeningResultQuery.data, setScreeningResult]);

  useEffect(() => {
    if (!shouldResetStaleScreeningJobId) return;
    setActiveScreeningJobId(null);
  }, [shouldResetStaleScreeningJobId, setActiveScreeningJobId]);

  const handleRunScreening = useCallback(async () => {
    const job = await runScreeningJob.mutateAsync(screeningParams);
    setActiveScreeningJobId(job.job_id);
  }, [runScreeningJob, screeningParams, setActiveScreeningJobId]);

  const handleCancelScreening = useCallback(() => {
    if (!activeScreeningJobId) return;
    cancelScreeningJob.mutate(activeScreeningJobId);
  }, [activeScreeningJobId, cancelScreeningJob]);

  const handleStockClick = useCallback(
    (code: string) => {
      setSelectedSymbol(code);
      void navigate({ to: '/charts' });
    },
    [setSelectedSymbol, navigate]
  );

  const screeningJob = screeningJobStatus.data ?? runScreeningJob.data ?? null;
  const screeningStatus = screeningJob?.status ?? null;
  const screeningIsRunning =
    runScreeningJob.isPending || screeningStatus === 'pending' || screeningStatus === 'running';
  const screeningError = (runScreeningJob.error ??
    (isStaleScreeningJob ? null : screeningJobStatusError) ??
    screeningResultQuery.error) as Error | null;

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
            screeningParams={screeningParams}
            rankingParams={rankingParams}
            fundamentalRankingParams={fundamentalRankingParams}
            setScreeningParams={setScreeningParams}
            setRankingParams={setRankingParams}
            setFundamentalRankingParams={setFundamentalRankingParams}
            productionStrategies={productionStrategies}
            isLoadingStrategies={isLoadingStrategies}
          />
        </div>

        {/* Main Content */}
        <div className="flex-1 flex flex-col min-w-0 min-h-0">
          <AnalysisMainContent
            activeSubTab={activeSubTab}
            handleRunScreening={handleRunScreening}
            screeningIsRunning={screeningIsRunning}
            screeningJob={screeningJob}
            handleCancelScreening={handleCancelScreening}
            cancelScreeningPending={cancelScreeningJob.isPending}
            screeningSummary={screeningResult?.summary}
            screeningMarkets={screeningResult?.markets || []}
            screeningRecentDays={screeningResult?.recentDays || (screeningParams.recentDays ?? 0)}
            screeningReferenceDate={screeningResult?.referenceDate}
            screeningResults={screeningResult?.results || []}
            screeningTableLoading={!screeningResult && screeningIsRunning}
            screeningError={screeningError}
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
