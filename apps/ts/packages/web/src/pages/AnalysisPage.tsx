import { useNavigate } from '@tanstack/react-router';
import { BarChart3, Filter } from 'lucide-react';
import { useCallback, useEffect } from 'react';
import { RankingFilters, RankingSummary, RankingTable } from '@/components/Ranking';
import { ScreeningFilters } from '@/components/Screening/ScreeningFilters';
import { ScreeningJobProgress } from '@/components/Screening/ScreeningJobProgress';
import { ScreeningSummary } from '@/components/Screening/ScreeningSummary';
import { ScreeningTable } from '@/components/Screening/ScreeningTable';
import { Button } from '@/components/ui/button';
import { useStrategies } from '@/hooks/useBacktest';
import { useRanking } from '@/hooks/useRanking';
import {
  useCancelScreeningJob,
  useRunScreeningJob,
  useScreeningJobStatus,
  useScreeningResult,
} from '@/hooks/useScreening';
import { cn } from '@/lib/utils';
import type { AnalysisSubTab } from '@/stores/analysisStore';
import { useAnalysisStore } from '@/stores/analysisStore';
import { useChartStore } from '@/stores/chartStore';

const subTabs: { id: AnalysisSubTab; label: string; icon: typeof BarChart3 }[] = [
  { id: 'screening', label: 'Screening', icon: Filter },
  { id: 'ranking', label: 'Ranking', icon: BarChart3 },
];

export function AnalysisPage() {
  const activeSubTab = useAnalysisStore((state) => state.activeSubTab);
  const screeningParams = useAnalysisStore((state) => state.screeningParams);
  const rankingParams = useAnalysisStore((state) => state.rankingParams);
  const activeScreeningJobId = useAnalysisStore((state) => state.activeScreeningJobId);
  const screeningResult = useAnalysisStore((state) => state.screeningResult);
  const setActiveSubTab = useAnalysisStore((state) => state.setActiveSubTab);
  const setScreeningParams = useAnalysisStore((state) => state.setScreeningParams);
  const setRankingParams = useAnalysisStore((state) => state.setRankingParams);
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

  const rankingQuery = useRanking(rankingParams, activeSubTab === 'ranking');

  const productionStrategies = (strategiesData?.strategies ?? [])
    .filter((strategy) => strategy.category === 'production')
    .map((strategy) => strategy.name)
    .sort((a, b) => a.localeCompare(b));

  useEffect(() => {
    if (!screeningResultQuery.data) return;
    setScreeningResult(screeningResultQuery.data);
  }, [screeningResultQuery.data, setScreeningResult]);

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
    screeningJobStatus.error ??
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
          {activeSubTab === 'screening' ? (
            <ScreeningFilters
              params={screeningParams}
              onChange={setScreeningParams}
              strategyOptions={productionStrategies}
              strategiesLoading={isLoadingStrategies}
            />
          ) : (
            <RankingFilters params={rankingParams} onChange={setRankingParams} />
          )}
        </div>

        {/* Main Content */}
        <div className="flex-1 flex flex-col min-w-0">
          {activeSubTab === 'screening' ? (
            <>
              <div className="mb-3 flex justify-end">
                <Button onClick={() => void handleRunScreening()} disabled={screeningIsRunning}>
                  Run Screening
                </Button>
              </div>

              <ScreeningJobProgress
                job={screeningJob}
                onCancel={screeningIsRunning ? handleCancelScreening : undefined}
                isCancelling={cancelScreeningJob.isPending}
              />

              <ScreeningSummary
                summary={screeningResult?.summary}
                markets={screeningResult?.markets || []}
                recentDays={screeningResult?.recentDays || (screeningParams.recentDays ?? 0)}
                referenceDate={screeningResult?.referenceDate}
              />
              <ScreeningTable
                results={screeningResult?.results || []}
                isLoading={!screeningResult && screeningIsRunning}
                isFetching={screeningIsRunning}
                error={screeningError}
                onStockClick={handleStockClick}
              />
            </>
          ) : (
            <>
              <RankingSummary data={rankingQuery.data} />
              <RankingTable
                rankings={rankingQuery.data?.rankings}
                isLoading={rankingQuery.isLoading}
                error={rankingQuery.error}
                onStockClick={handleStockClick}
                periodDays={rankingParams.periodDays}
              />
            </>
          )}
        </div>
      </div>
    </div>
  );
}
