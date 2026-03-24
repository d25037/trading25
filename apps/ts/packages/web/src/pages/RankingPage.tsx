import { useNavigate } from '@tanstack/react-router';
import { BarChart3, TrendingUp } from 'lucide-react';
import { useCallback } from 'react';
import {
  FundamentalRankingFilters,
  FundamentalRankingSummary,
  FundamentalRankingTable,
} from '@/components/FundamentalRanking';
import {
  ModeSwitcherPanel,
  SectionEyebrow,
  SegmentedTabs,
  SplitLayout,
  SplitMain,
  SplitSidebar,
  Surface,
} from '@/components/Layout/Workspace';
import {
  IndexPerformanceTable,
  RANKING_LOOKBACK_OPTIONS,
  RankingFilters,
  RankingSummary,
  RankingTable,
} from '@/components/Ranking';
import { DateInput, NumberSelect } from '@/components/shared/filters';
import { useFundamentalRanking } from '@/hooks/useFundamentalRanking';
import { useRankingRouteState } from '@/hooks/usePageRouteState';
import { useRanking } from '@/hooks/useRanking';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import type { RankingDailyView, RankingPageTab, RankingParams } from '@/types/ranking';

const subTabs = [
  { value: 'ranking' as RankingPageTab, label: 'Daily Ranking', icon: BarChart3 },
  { value: 'fundamentalRanking' as RankingPageTab, label: 'Fundamental Ranking', icon: TrendingUp },
];

const dailyViewTabs = [
  { value: 'stocks' as RankingDailyView, label: 'Individual Stocks' },
  { value: 'indices' as RankingDailyView, label: 'Indices' },
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
    <Surface className="glass-panel p-5">
      <div className="space-y-1 pb-4">
        <SectionEyebrow>Indices Filters</SectionEyebrow>
        <h2 className="text-base font-semibold text-foreground">Lookback and reference session</h2>
      </div>
      <div className="space-y-4">
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
      </div>
    </Surface>
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
  const fundamentalRankingQuery = useFundamentalRanking(
    fundamentalRankingParams,
    activeSubTab === 'fundamentalRanking'
  );

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
    <div className="flex h-full min-h-0 flex-col p-4">
      <ModeSwitcherPanel label="Ranking Mode" items={subTabs} value={activeSubTab} onChange={setActiveSubTab} />

      <SplitLayout className="mt-4 gap-4">
        <SplitSidebar className="w-64">
          <RankingSidebar
            activeSubTab={activeSubTab}
            activeDailyView={activeDailyView}
            rankingParams={rankingParams}
            fundamentalRankingParams={fundamentalRankingParams}
            setRankingParams={setRankingParams}
            setFundamentalRankingParams={setFundamentalRankingParams}
          />
        </SplitSidebar>

        <SplitMain className="gap-4">
          {activeSubTab === 'ranking' ? (
            <>
              <SegmentedTabs items={dailyViewTabs} value={activeDailyView} onChange={setActiveDailyView} />

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
        </SplitMain>
      </SplitLayout>
    </div>
  );
}
