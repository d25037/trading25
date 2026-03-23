import { useNavigate } from '@tanstack/react-router';
import { BarChart3, TrendingUp } from 'lucide-react';
import { useCallback } from 'react';
import {
  FundamentalRankingFilters,
  FundamentalRankingSummary,
  FundamentalRankingTable,
} from '@/components/FundamentalRanking';
import {
  IndexPerformanceTable,
  RANKING_LOOKBACK_OPTIONS,
  RankingFilters,
  RankingSummary,
  RankingTable,
} from '@/components/Ranking';
import { DateInput, NumberSelect } from '@/components/shared/filters';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { useFundamentalRanking } from '@/hooks/useFundamentalRanking';
import { useRankingRouteState } from '@/hooks/usePageRouteState';
import { useRanking } from '@/hooks/useRanking';
import { cn } from '@/lib/utils';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import type { RankingDailyView, RankingPageTab, RankingParams } from '@/types/ranking';

const subTabs: { id: RankingPageTab; label: string; icon: typeof BarChart3 }[] = [
  { id: 'ranking', label: 'Daily Ranking', icon: BarChart3 },
  { id: 'fundamentalRanking', label: 'Fundamental Ranking', icon: TrendingUp },
];

const dailyViewTabs: { id: RankingDailyView; label: string }[] = [
  { id: 'stocks', label: 'Individual Stocks' },
  { id: 'indices', label: 'Indices' },
];

interface RankingSidebarProps {
  activeSubTab: RankingPageTab;
  activeDailyView: RankingDailyView;
  rankingParams: RankingParams;
  fundamentalRankingParams: FundamentalRankingParams;
  setRankingParams: (params: RankingParams) => void;
  setFundamentalRankingParams: (params: FundamentalRankingParams) => void;
}

interface IndexPerformanceSidebarProps {
  rankingParams: RankingParams;
  setRankingParams: (params: RankingParams) => void;
}

function IndexPerformanceSidebar({ rankingParams, setRankingParams }: IndexPerformanceSidebarProps) {
  const updateParam = <K extends keyof RankingParams>(key: K, value: RankingParams[K]) => {
    setRankingParams({ ...rankingParams, [key]: value });
  };

  return (
    <Card className="glass-panel">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Indices Filters</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <NumberSelect
          value={rankingParams.lookbackDays || 1}
          onChange={(lookbackDays) => updateParam('lookbackDays', lookbackDays)}
          options={RANKING_LOOKBACK_OPTIONS}
          id="index-performance-lookbackDays"
          label="Lookback Days"
        />
        <DateInput
          value={rankingParams.date}
          onChange={(date) => updateParam('date', date)}
          id="index-performance-date"
        />
        <p className="text-xs text-muted-foreground">
          Index performance compares each latest close with the selected trading sessions earlier.
        </p>
      </CardContent>
    </Card>
  );
}

function RankingSidebar({
  activeSubTab,
  activeDailyView,
  rankingParams,
  fundamentalRankingParams,
  setRankingParams,
  setFundamentalRankingParams,
}: RankingSidebarProps) {
  if (activeSubTab === 'ranking') {
    if (activeDailyView === 'indices') {
      return <IndexPerformanceSidebar rankingParams={rankingParams} setRankingParams={setRankingParams} />;
    }

    return <RankingFilters params={rankingParams} onChange={setRankingParams} />;
  }

  return <FundamentalRankingFilters params={fundamentalRankingParams} onChange={setFundamentalRankingParams} />;
}

export function RankingPage() {
  const {
    activeSubTab,
    activeDailyView,
    rankingParams,
    fundamentalRankingParams,
    setActiveSubTab,
    setActiveDailyView,
    setRankingParams,
    setFundamentalRankingParams,
  } = useRankingRouteState();
  const navigate = useNavigate();
  const rankingQuery = useRanking(rankingParams, true);
  const fundamentalRankingQuery = useFundamentalRanking(fundamentalRankingParams, activeSubTab === 'fundamentalRanking');

  const handleStockClick = useCallback(
    (code: string) => {
      void navigate({
        to: '/charts',
        search: { symbol: code },
      });
    },
    [navigate]
  );
  const handleIndexClick = useCallback(
    (code: string) => {
      void navigate({
        to: '/indices',
        search: { code },
      });
    },
    [navigate]
  );

  return (
    <div className="flex h-full flex-col p-4">
      <div className="mb-4 flex gap-2">
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

      <div className="flex min-h-0 flex-1 gap-4">
        <div className="w-64 flex-shrink-0">
          <RankingSidebar
            activeSubTab={activeSubTab}
            activeDailyView={activeDailyView}
            rankingParams={rankingParams}
            fundamentalRankingParams={fundamentalRankingParams}
            setRankingParams={setRankingParams}
            setFundamentalRankingParams={setFundamentalRankingParams}
          />
        </div>

        <div className="flex min-h-0 min-w-0 flex-1 flex-col gap-4">
          {activeSubTab === 'ranking' ? (
            <>
              <div className="flex gap-2">
                {dailyViewTabs.map((tab) => {
                  const isActive = activeDailyView === tab.id;
                  return (
                    <Button
                      key={tab.id}
                      variant={isActive ? 'default' : 'outline'}
                      size="sm"
                      className={cn('gap-2', isActive && 'shadow-md')}
                      onClick={() => setActiveDailyView(tab.id)}
                    >
                      {tab.label}
                    </Button>
                  );
                })}
              </div>

              {activeDailyView === 'indices' ? (
                <IndexPerformanceTable
                  items={rankingQuery.data?.indexPerformance}
                  isLoading={rankingQuery.isLoading}
                  error={rankingQuery.error}
                  onIndexClick={handleIndexClick}
                  lookbackDays={rankingParams.lookbackDays}
                />
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
            </>
          ) : (
            <>
              <FundamentalRankingSummary data={fundamentalRankingQuery.data} />
              <FundamentalRankingTable
                rankings={fundamentalRankingQuery.data?.rankings}
                isLoading={fundamentalRankingQuery.isLoading}
                error={fundamentalRankingQuery.error}
                onStockClick={handleStockClick}
              />
            </>
          )}
        </div>
      </div>
    </div>
  );
}
