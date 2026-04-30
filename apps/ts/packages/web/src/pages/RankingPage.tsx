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
  Topix100RankingFilters,
  Topix100RankingTable,
} from '@/components/Ranking';
import {
  getTopix100RankingMetricLabel,
  resolveTopix100PriceSmaWindow,
  resolveTopix100RankingMetric,
} from '@/components/Ranking/topix100RankingMetric';
import { DateInput, NumberSelect } from '@/components/shared/filters';
import {
  ValueCompositeRankingFilters,
  ValueCompositeRankingSummary,
  ValueCompositeRankingTable,
} from '@/components/ValueCompositeRanking';
import { useFundamentalRanking } from '@/hooks/useFundamentalRanking';
import { useRankingRouteState } from '@/hooks/usePageRouteState';
import { useRanking } from '@/hooks/useRanking';
import { useTopix100Ranking } from '@/hooks/useTopix100Ranking';
import { useValueCompositeRanking } from '@/hooks/useValueCompositeRanking';
import { formatMarketsLabel } from '@/lib/marketUtils';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import type { RankingDailyView, RankingPageTab, RankingParams } from '@/types/ranking';
import type { ValueCompositeRankingParams, ValueCompositeScoreMethod } from '@/types/valueCompositeRanking';

const subTabs = [
  { value: 'ranking' as RankingPageTab, label: 'Daily Ranking', icon: BarChart3 },
  { value: 'fundamentalRanking' as RankingPageTab, label: 'Fundamental Ranking', icon: TrendingUp },
  { value: 'valueComposite' as RankingPageTab, label: 'Value Scores', icon: TrendingUp },
];

const dailyViewTabs = [
  { value: 'stocks' as RankingDailyView, label: 'Individual Stocks' },
  { value: 'indices' as RankingDailyView, label: 'Indices' },
  { value: 'topix100' as RankingDailyView, label: 'TOPIX100 Study' },
];

function getValueCompositeScoreMethodLabel(method: ValueCompositeScoreMethod | undefined): string {
  if (method === 'equal_weight') {
    return 'Equal-weight value score';
  }
  if (method === 'prime_size_tilt') {
    return 'Prime size tilt score';
  }
  return 'Standard PBR tilt score';
}

function getValueCompositeForwardEpsModeLabel(mode: ValueCompositeRankingParams['forwardEpsMode']): string {
  return mode === 'fy' ? 'FY-only forecast EPS' : 'Latest revised EPS (previous default)';
}

interface RankingSidebarProps {
  activeSubTab: RankingPageTab;
  activeDailyView: RankingDailyView;
  rankingParams: RankingParams;
  fundamentalRankingParams: FundamentalRankingParams;
  valueCompositeRankingParams: ValueCompositeRankingParams;
  setActiveSubTab: (tab: RankingPageTab) => void;
  setActiveDailyView: (view: RankingDailyView) => void;
  setRankingParams: (params: RankingParams) => void;
  setFundamentalRankingParams: (params: FundamentalRankingParams) => void;
  setValueCompositeRankingParams: (params: ValueCompositeRankingParams) => void;
}

interface IndexPerformanceSidebarProps {
  rankingParams: RankingParams;
  setRankingParams: (params: RankingParams) => void;
}

interface RankingContentProps {
  activeSubTab: RankingPageTab;
  activeDailyView: RankingDailyView;
  rankingParams: RankingParams;
  setRankingParams: (params: RankingParams) => void;
  rankingQuery: ReturnType<typeof useRanking>;
  topix100RankingQuery: ReturnType<typeof useTopix100Ranking>;
  fundamentalRankingQuery: ReturnType<typeof useFundamentalRanking>;
  valueCompositeRankingQuery: ReturnType<typeof useValueCompositeRanking>;
  onStockClick: (code: string) => void;
  onIndexClick: (code: string) => void;
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
  valueCompositeRankingParams,
  setActiveSubTab,
  setActiveDailyView,
  setRankingParams,
  setFundamentalRankingParams,
  setValueCompositeRankingParams,
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
            className="overflow-x-auto lg:flex-col"
            itemClassName="h-8 shrink-0 justify-start rounded-lg px-3 py-1.5 text-xs"
          />
          {activeSubTab === 'ranking' ? (
            <div className="space-y-2 border-t border-border/60 pt-3">
              <SectionEyebrow className="mb-0">Daily View</SectionEyebrow>
              <SegmentedTabs
                items={dailyViewTabs}
                value={activeDailyView}
                onChange={setActiveDailyView}
                className="overflow-x-auto lg:flex-col"
                itemClassName="h-8 shrink-0 justify-start rounded-lg px-3 py-1.5 text-xs"
              />
            </div>
          ) : null}
        </div>
      </Surface>

      {activeSubTab === 'ranking' ? (
        activeDailyView === 'indices' ? (
          <IndexPerformanceSidebar rankingParams={rankingParams} setRankingParams={setRankingParams} />
        ) : activeDailyView === 'topix100' ? (
          <Topix100RankingFilters params={rankingParams} onChange={setRankingParams} />
        ) : (
          <RankingFilters params={rankingParams} onChange={setRankingParams} />
        )
      ) : activeSubTab === 'valueComposite' ? (
        <ValueCompositeRankingFilters params={valueCompositeRankingParams} onChange={setValueCompositeRankingParams} />
      ) : (
        <FundamentalRankingFilters params={fundamentalRankingParams} onChange={setFundamentalRankingParams} />
      )}
    </div>
  );
}

function buildIntroMetaItems(
  activeSubTab: RankingPageTab,
  activeDailyView: RankingDailyView,
  rankingParams: RankingParams,
  fundamentalRankingParams: FundamentalRankingParams,
  valueCompositeRankingParams: ValueCompositeRankingParams
) {
  if (activeSubTab === 'fundamentalRanking') {
    return [
      { label: 'Mode', value: 'Forecast / actual EPS' },
      { label: 'Markets', value: formatMarketsLabel((fundamentalRankingParams.markets ?? 'prime').split(',')) },
    ];
  }
  if (activeSubTab === 'valueComposite') {
    return [
      { label: 'Mode', value: getValueCompositeScoreMethodLabel(valueCompositeRankingParams.scoreMethod) },
      { label: 'EPS Basis', value: getValueCompositeForwardEpsModeLabel(valueCompositeRankingParams.forwardEpsMode) },
      { label: 'Markets', value: formatMarketsLabel((valueCompositeRankingParams.markets ?? 'standard').split(',')) },
    ];
  }
  if (activeDailyView === 'topix100') {
    const topix100Metric = resolveTopix100RankingMetric(rankingParams.topix100Metric);
    const topix100SmaWindow = resolveTopix100PriceSmaWindow(rankingParams.topix100SmaWindow);
    const topix100StudyMode = rankingParams.topix100StudyMode ?? 'swing_5d';
    return [
      { label: 'Metric', value: getTopix100RankingMetricLabel(topix100Metric, topix100SmaWindow) },
      {
        label: 'Read',
        value:
          topix100StudyMode === 'swing_5d'
            ? 'Leak-free X+1 open -> X+6 open, KPI vs TOPIX'
            : 'Decile-only intraday LightGBM score',
      },
    ];
  }
  return [
    {
      label: 'Mode',
      value: activeDailyView === 'indices' ? 'Index performance' : 'Daily market ranking',
    },
    { label: 'Markets', value: formatMarketsLabel((rankingParams.markets ?? 'prime').split(',')) },
  ];
}

function RankingContent({
  activeSubTab,
  activeDailyView,
  rankingParams,
  setRankingParams,
  rankingQuery,
  topix100RankingQuery,
  fundamentalRankingQuery,
  valueCompositeRankingQuery,
  onStockClick,
  onIndexClick,
}: RankingContentProps) {
  const topix100StudyMode = rankingParams.topix100StudyMode ?? 'swing_5d';
  const topix100Metric = resolveTopix100RankingMetric(rankingParams.topix100Metric);
  const topix100SmaWindow = resolveTopix100PriceSmaWindow(rankingParams.topix100SmaWindow);

  if (activeSubTab === 'fundamentalRanking') {
    return (
      <>
        <FundamentalRankingTable
          rankings={fundamentalRankingQuery.data?.rankings}
          isLoading={fundamentalRankingQuery.isLoading}
          error={fundamentalRankingQuery.error}
          onStockClick={onStockClick}
        />
        <FundamentalRankingSummary data={fundamentalRankingQuery.data} />
      </>
    );
  }

  if (activeSubTab === 'valueComposite') {
    return (
      <>
        <ValueCompositeRankingTable
          data={valueCompositeRankingQuery.data}
          isLoading={valueCompositeRankingQuery.isLoading}
          error={valueCompositeRankingQuery.error}
          onStockClick={onStockClick}
        />
        <ValueCompositeRankingSummary data={valueCompositeRankingQuery.data} />
      </>
    );
  }

  if (activeDailyView === 'indices') {
    return (
      <IndexPerformanceTable
        items={rankingQuery.data?.indexPerformance}
        isLoading={rankingQuery.isLoading}
        error={rankingQuery.error}
        onIndexClick={onIndexClick}
        lookbackDays={rankingParams.lookbackDays}
      />
    );
  }

  if (activeDailyView === 'topix100') {
    return (
      <Topix100RankingTable
        data={topix100RankingQuery.data}
        isLoading={topix100RankingQuery.isLoading}
        error={topix100RankingQuery.error}
        onStockClick={onStockClick}
        studyMode={topix100StudyMode}
        rankingMetric={topix100Metric}
        rankingSmaWindow={topix100SmaWindow}
        priceBucketFilter={rankingParams.topix100PriceBucket ?? 'all'}
        sortBy={rankingParams.topix100SortBy ?? 'rank'}
        sortOrder={rankingParams.topix100SortOrder ?? 'asc'}
        onSortChange={(sortBy, sortOrder) =>
          setRankingParams({
            ...rankingParams,
            topix100SortBy: sortBy,
            topix100SortOrder: sortOrder,
          })
        }
      />
    );
  }

  return (
    <RankingTable
      rankings={rankingQuery.data?.rankings}
      isLoading={rankingQuery.isLoading}
      error={rankingQuery.error}
      onStockClick={onStockClick}
      periodDays={rankingParams.periodDays}
    />
  );
}

export function RankingPage() {
  const {
    activeSubTab,
    activeDailyView,
    rankingParams,
    fundamentalRankingParams,
    valueCompositeRankingParams,
    setActiveSubTab,
    setActiveDailyView,
    setRankingParams,
    setFundamentalRankingParams,
    setValueCompositeRankingParams,
  } = useRankingRouteState();
  const navigate = useNavigate();
  const topix100StudyMode = rankingParams.topix100StudyMode ?? 'swing_5d';
  const topix100Metric = resolveTopix100RankingMetric(rankingParams.topix100Metric);
  const topix100SmaWindow = resolveTopix100PriceSmaWindow(rankingParams.topix100SmaWindow);
  const rankingQuery = useRanking(rankingParams, activeSubTab === 'ranking' && activeDailyView !== 'topix100');
  const topix100RankingQuery = useTopix100Ranking(
    rankingParams.date,
    topix100StudyMode,
    topix100Metric,
    topix100SmaWindow,
    activeSubTab === 'ranking' && activeDailyView === 'topix100'
  );
  const fundamentalRankingQuery = useFundamentalRanking(
    fundamentalRankingParams,
    activeSubTab === 'fundamentalRanking'
  );
  const valueCompositeRankingQuery = useValueCompositeRanking(
    valueCompositeRankingParams,
    activeSubTab === 'valueComposite'
  );
  const introMetaItems = buildIntroMetaItems(
    activeSubTab,
    activeDailyView,
    rankingParams,
    fundamentalRankingParams,
    valueCompositeRankingParams
  );

  const handleStockClick = useCallback(
    (code: string) => {
      void navigate({
        to: '/symbol-workbench',
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
    <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-3 lg:overflow-hidden">
      <Surface className="px-4 py-2">
        <div className="flex flex-col gap-2 xl:flex-row xl:items-end xl:justify-between">
          <div className="space-y-1">
            <SectionEyebrow>Analytics Workspace</SectionEyebrow>
            <div className="space-y-0.5">
              <h1 className="text-2xl font-semibold tracking-tight text-foreground">Ranking</h1>
              <p className="max-w-2xl text-xs text-muted-foreground sm:text-sm">
                Daily ranking, index performance, TOPIX100 SMA divergence, forecast/actual EPS ratios, and value scores.
              </p>
            </div>
          </div>
          <PageIntroMetaList items={introMetaItems} className="gap-x-2.5 gap-y-1 [&>div]:min-w-[6.5rem] [&>div]:pl-2" />
        </div>
      </Surface>

      <SplitLayout className="min-h-0 flex-1 flex-col gap-3 lg:flex-row lg:items-stretch lg:overflow-hidden">
        <SplitSidebar className="w-full lg:h-full lg:w-40 lg:overflow-auto xl:w-44 2xl:w-48">
          <RankingSidebar
            activeSubTab={activeSubTab}
            activeDailyView={activeDailyView}
            rankingParams={rankingParams}
            fundamentalRankingParams={fundamentalRankingParams}
            valueCompositeRankingParams={valueCompositeRankingParams}
            setActiveSubTab={setActiveSubTab}
            setActiveDailyView={setActiveDailyView}
            setRankingParams={setRankingParams}
            setFundamentalRankingParams={setFundamentalRankingParams}
            setValueCompositeRankingParams={setValueCompositeRankingParams}
          />
        </SplitSidebar>

        <SplitMain className="gap-3 lg:overflow-hidden">
          <RankingContent
            activeSubTab={activeSubTab}
            activeDailyView={activeDailyView}
            rankingParams={rankingParams}
            setRankingParams={setRankingParams}
            rankingQuery={rankingQuery}
            topix100RankingQuery={topix100RankingQuery}
            fundamentalRankingQuery={fundamentalRankingQuery}
            valueCompositeRankingQuery={valueCompositeRankingQuery}
            onStockClick={handleStockClick}
            onIndexClick={handleIndexClick}
          />
        </SplitMain>
      </SplitLayout>
    </div>
  );
}
