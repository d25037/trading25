import { useNavigate } from '@tanstack/react-router';
import { BarChart3, TrendingUp } from 'lucide-react';
import { useCallback } from 'react';
import {
  FundamentalRankingFilters,
  FundamentalRankingSummary,
  FundamentalRankingTable,
} from '@/components/FundamentalRanking';
import {
  PageIntroMetaList,
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
  RankingTable,
} from '@/components/Ranking';
import { DateInput, NumberSelect } from '@/components/shared/filters';
import { useFundamentalRanking } from '@/hooks/useFundamentalRanking';
import { useRankingRouteState } from '@/hooks/usePageRouteState';
import { useRanking } from '@/hooks/useRanking';
import { formatMarketsLabel } from '@/lib/marketUtils';
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
  setActiveSubTab: (tab: RankingPageTab) => void;
  setActiveDailyView: (view: RankingDailyView) => void;
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
    <Surface className="p-4">
      <div className="space-y-1 pb-3">
        <SectionEyebrow>Filter Rail</SectionEyebrow>
        <h2 className="text-base font-semibold text-foreground">Indices Filters</h2>
        <p className="text-xs text-muted-foreground">
          Set the local benchmark window and the reference session for index performance.
        </p>
      </div>
      <div className="space-y-3">
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
  setActiveSubTab,
  setActiveDailyView,
  setRankingParams,
  setFundamentalRankingParams,
}: RankingSidebarProps) {
  return (
    <div className="space-y-3">
      <Surface className="p-3">
        <div className="space-y-3">
          <div className="space-y-1">
            <SectionEyebrow>Workspace</SectionEyebrow>
            <h2 className="text-sm font-semibold text-foreground">Result Scope</h2>
          </div>
          <SegmentedTabs
            items={subTabs}
            value={activeSubTab}
            onChange={setActiveSubTab}
            className="flex-col"
            itemClassName="h-8 justify-start rounded-lg px-3 py-1.5 text-xs"
          />
          {activeSubTab === 'ranking' ? (
            <div className="space-y-2 border-t border-border/60 pt-3">
              <SectionEyebrow className="mb-0">Daily View</SectionEyebrow>
              <SegmentedTabs
                items={dailyViewTabs}
                value={activeDailyView}
                onChange={setActiveDailyView}
                className="flex-col"
                itemClassName="h-8 justify-start rounded-lg px-3 py-1.5 text-xs"
              />
            </div>
          ) : null}
        </div>
      </Surface>

      {activeSubTab === 'ranking' ? (
        activeDailyView === 'indices' ? (
          <IndexPerformanceSidebar rankingParams={rankingParams} setRankingParams={setRankingParams} />
        ) : (
          <RankingFilters params={rankingParams} onChange={setRankingParams} />
        )
      ) : (
        <FundamentalRankingFilters params={fundamentalRankingParams} onChange={setFundamentalRankingParams} />
      )}
    </div>
  );
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
  const introMetaItems =
    activeSubTab === 'fundamentalRanking'
      ? [
          { label: 'Mode', value: 'Forecast / actual EPS' },
          { label: 'Markets', value: formatMarketsLabel((fundamentalRankingParams.markets ?? 'prime').split(',')) },
        ]
      : [
          {
            label: 'Mode',
            value: activeDailyView === 'indices' ? 'Index performance' : 'Daily market ranking',
          },
          { label: 'Markets', value: formatMarketsLabel((rankingParams.markets ?? 'prime').split(',')) },
        ];

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
    <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-4 lg:overflow-hidden">
      <Surface className="px-4 py-3">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
          <div className="space-y-2">
            <SectionEyebrow>Analytics Workspace</SectionEyebrow>
            <div className="space-y-1">
              <h1 className="text-2xl font-semibold tracking-tight text-foreground">Ranking</h1>
              <p className="max-w-2xl text-sm text-muted-foreground">
                Daily leaders, index moves, and forecast-to-actual EPS ratios.
              </p>
            </div>
          </div>
          <PageIntroMetaList items={introMetaItems} className="gap-x-4 gap-y-2" />
        </div>
      </Surface>

      <SplitLayout className="min-h-0 flex-1 flex-col gap-3 lg:flex-row lg:items-stretch">
        <SplitSidebar className="w-full lg:h-full lg:w-40 lg:overflow-auto xl:w-44 2xl:w-48">
          <RankingSidebar
            activeSubTab={activeSubTab}
            activeDailyView={activeDailyView}
            rankingParams={rankingParams}
            fundamentalRankingParams={fundamentalRankingParams}
            setActiveSubTab={setActiveSubTab}
            setActiveDailyView={setActiveDailyView}
            setRankingParams={setRankingParams}
            setFundamentalRankingParams={setFundamentalRankingParams}
          />
        </SplitSidebar>

        <SplitMain className="gap-3 lg:overflow-hidden">
          {activeSubTab === 'ranking' ? (
            activeDailyView === 'indices' ? (
              <IndexPerformanceTable
                items={rankingQuery.data?.indexPerformance}
                isLoading={rankingQuery.isLoading}
                error={rankingQuery.error}
                onIndexClick={handleIndexClick}
                lookbackDays={rankingParams.lookbackDays}
              />
            ) : (
              <RankingTable
                rankings={rankingQuery.data?.rankings}
                isLoading={rankingQuery.isLoading}
                error={rankingQuery.error}
                onStockClick={handleStockClick}
                periodDays={rankingParams.periodDays}
              />
            )
          ) : (
            <>
              <FundamentalRankingTable
                rankings={fundamentalRankingQuery.data?.rankings}
                isLoading={fundamentalRankingQuery.isLoading}
                error={fundamentalRankingQuery.error}
                onStockClick={handleStockClick}
              />
              <FundamentalRankingSummary data={fundamentalRankingQuery.data} />
            </>
          )}
        </SplitMain>
      </SplitLayout>
    </div>
  );
}
