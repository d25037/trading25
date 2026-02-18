import { BarChart3, Filter } from 'lucide-react';
import { useNavigate } from '@tanstack/react-router';
import { useCallback, useEffect, useState } from 'react';
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
import { useChartStore } from '@/stores/chartStore';
import type { RankingParams } from '@/types/ranking';
import type { MarketScreeningResponse, ScreeningParams } from '@/types/screening';

type AnalysisSubTab = 'screening' | 'ranking';

const DEFAULT_SCREENING_PARAMS: ScreeningParams = {
  markets: 'prime',
  recentDays: 10,
  sortBy: 'matchedDate',
  order: 'desc',
  limit: 50,
};

const DEFAULT_RANKING_PARAMS: RankingParams = {
  markets: 'prime',
  limit: 20,
  lookbackDays: 1,
  periodDays: 250,
};

const subTabs: { id: AnalysisSubTab; label: string; icon: typeof BarChart3 }[] = [
  { id: 'screening', label: 'Screening', icon: Filter },
  { id: 'ranking', label: 'Ranking', icon: BarChart3 },
];

export function AnalysisPage() {
  const [activeSubTab, setActiveSubTab] = useState<AnalysisSubTab>('screening');
  const [screeningParams, setScreeningParams] = useState<ScreeningParams>(DEFAULT_SCREENING_PARAMS);
  const [rankingParams, setRankingParams] = useState<RankingParams>(DEFAULT_RANKING_PARAMS);
  const [activeScreeningJobId, setActiveScreeningJobId] = useState<string | null>(null);
  const [screeningResult, setScreeningResult] = useState<MarketScreeningResponse | null>(null);

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
  }, [screeningResultQuery.data]);

  const handleRunScreening = useCallback(async () => {
    const job = await runScreeningJob.mutateAsync(screeningParams);
    setActiveScreeningJobId(job.job_id);
  }, [runScreeningJob, screeningParams]);

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
  const screeningError =
    (runScreeningJob.error ?? screeningJobStatus.error ?? screeningResultQuery.error) as Error | null;

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
