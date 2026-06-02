import { useNavigate } from '@tanstack/react-router';
import { useCallback, useMemo } from 'react';
import {
  PageIntroMetaList,
  SectionEyebrow,
  SegmentedTabs,
  SplitLayout,
  SplitMain,
  SplitSidebar,
  Surface,
} from '@/components/Layout/Workspace';
import { BubbleFootprintBanner } from '@/components/MarketRegime/BubbleFootprintBanner';
import {
  IndexPerformanceTable,
  RANKING_LOOKBACK_OPTIONS,
  RankingFilters,
  RankingTable,
  type RankingTableSortState,
  TechnicalEventFilters,
} from '@/components/Ranking';
import { DateInput, NumberSelect } from '@/components/shared/filters';
import { useMarketBubbleFootprint } from '@/hooks/useMarketBubbleFootprint';
import { useRankingRouteState } from '@/hooks/usePageRouteState';
import { useRanking } from '@/hooks/useRanking';
import { formatMarketsLabel } from '@/lib/marketUtils';
import type { RankingDailyView, RankingParams } from '@/types/ranking';

const dailyViewTabs = [
  { value: 'stocks' as RankingDailyView, label: 'Individual Stocks' },
  { value: 'technicalEvents' as RankingDailyView, label: 'Technical Events' },
  { value: 'indices' as RankingDailyView, label: 'Indices' },
];

interface IndexPerformanceSidebarProps {
  rankingParams: RankingParams;
  setRankingParams: (params: RankingParams) => void;
}

interface RankingSidebarProps {
  activeDailyView: RankingDailyView;
  rankingParams: RankingParams;
  setActiveDailyView: (view: RankingDailyView) => void;
  setRankingParams: (params: RankingParams) => void;
}

interface RankingContentProps {
  activeDailyView: RankingDailyView;
  rankingParams: RankingParams;
  rankingQuery: ReturnType<typeof useRanking>;
  rankingSortState: RankingTableSortState;
  onRankingSortChange: (state: RankingTableSortState) => void;
  onStockClick: (code: string) => void;
  onIndexClick: (code: string) => void;
}

function resolveRankingLimit(activeDailyView: RankingDailyView, rankingParams: RankingParams): number | undefined {
  if (activeDailyView === 'technicalEvents') return 50;
  if (activeDailyView === 'indices') return 20;
  return rankingParams.limit;
}

function buildRankingQueryParams(activeDailyView: RankingDailyView, rankingParams: RankingParams): RankingParams {
  const isStocksView = activeDailyView === 'stocks';
  return {
    date: rankingParams.date,
    markets: rankingParams.markets,
    lookbackDays: rankingParams.lookbackDays,
    periodDays: rankingParams.periodDays,
    technicalEventType: rankingParams.technicalEventType,
    sector33Name: rankingParams.sector33Name,
    sector17Name: rankingParams.sector17Name,
    limit: resolveRankingLimit(activeDailyView, rankingParams),
    includeValuation: activeDailyView !== 'indices',
    includeSectorStrength: activeDailyView !== 'technicalEvents',
    forwardEpsDisclosedWithinDays: isStocksView ? (rankingParams.forwardEpsDisclosedWithinDays ?? 0) : 0,
    liquidityState: isStocksView ? rankingParams.liquidityState : undefined,
    regimeState: isStocksView ? rankingParams.regimeState : undefined,
    riskState: isStocksView ? rankingParams.riskState : undefined,
    technicalState: isStocksView ? rankingParams.technicalState : undefined,
  };
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

function RankingSidebar({ activeDailyView, rankingParams, setActiveDailyView, setRankingParams }: RankingSidebarProps) {
  return (
    <div className="space-y-3">
      <Surface className="p-3">
        <div className="space-y-3">
          <div className="space-y-1">
            <SectionEyebrow>Workspace</SectionEyebrow>
            <h2 className="text-sm font-semibold text-foreground">Daily View</h2>
          </div>
          <SegmentedTabs
            items={dailyViewTabs}
            value={activeDailyView}
            onChange={setActiveDailyView}
            className="overflow-x-auto lg:flex-col"
            itemClassName="h-8 shrink-0 justify-start rounded-lg px-3 py-1.5 text-xs"
          />
        </div>
      </Surface>

      {activeDailyView === 'indices' ? (
        <IndexPerformanceSidebar rankingParams={rankingParams} setRankingParams={setRankingParams} />
      ) : activeDailyView === 'technicalEvents' ? (
        <TechnicalEventFilters params={rankingParams} onChange={setRankingParams} />
      ) : (
        <RankingFilters params={rankingParams} onChange={setRankingParams} />
      )}
    </div>
  );
}

function buildIntroMetaItems(activeDailyView: RankingDailyView, rankingParams: RankingParams) {
  return [
    {
      label: 'Mode',
      value:
        activeDailyView === 'indices'
          ? 'Index performance'
          : activeDailyView === 'technicalEvents'
            ? 'Technical events'
            : 'Daily market ranking',
    },
    { label: 'Markets', value: formatMarketsLabel((rankingParams.markets ?? 'prime').split(',')) },
  ];
}

function RankingContent({
  activeDailyView,
  rankingParams,
  rankingQuery,
  rankingSortState,
  onRankingSortChange,
  onStockClick,
  onIndexClick,
}: RankingContentProps) {
  const sector33IndexPerformance = useMemo(
    () => rankingQuery.data?.indexPerformance.filter((item) => item.category === 'sector33'),
    [rankingQuery.data?.indexPerformance]
  );

  if (activeDailyView === 'indices') {
    return (
      <IndexPerformanceTable
        items={sector33IndexPerformance}
        isLoading={rankingQuery.isLoading}
        error={rankingQuery.error}
        onIndexClick={onIndexClick}
        lookbackDays={rankingParams.lookbackDays}
        title="33業種指数"
        description={`Score: 20D/60D TOPIX超過 + 20D breadth。騰落率基準: ${rankingParams.lookbackDays ?? 5}営業日前`}
        emptyMessage="No 33-sector index performance data available"
        emptySubMessage="Run index sync or choose a date with sector index coverage"
      />
    );
  }

  if (activeDailyView === 'technicalEvents') {
    const eventType = rankingParams.technicalEventType ?? 'periodHigh';
    const isHigh = eventType === 'periodHigh';
    return (
      <RankingTable
        items={isHigh ? rankingQuery.data?.rankings.periodHigh : rankingQuery.data?.rankings.periodLow}
        isLoading={rankingQuery.isLoading}
        error={rankingQuery.error}
        onStockClick={onStockClick}
        title={`${rankingParams.periodDays ?? 250}日${isHigh ? '高値' : '安値'}`}
        eyebrow="Technical Events"
        showValuation
        showLiquidity
        showMarket
        showChangeForTradingValue
        enableColumnSort
        sortState={rankingSortState}
        onSortChange={onRankingSortChange}
      />
    );
  }

  return (
    <RankingTable
      items={rankingQuery.data?.rankings.tradingValue}
      isLoading={rankingQuery.isLoading}
      error={rankingQuery.error}
      onStockClick={onStockClick}
      showValuation
      showLiquidity
      showChangeForTradingValue
      enableColumnSort
      sortState={rankingSortState}
      onSortChange={onRankingSortChange}
    />
  );
}

export function RankingPage() {
  const { activeDailyView, rankingParams, setActiveDailyView, setRankingParams } = useRankingRouteState();
  const navigate = useNavigate();
  const rankingSortState = useMemo<RankingTableSortState>(
    () => ({
      field: rankingParams.sortBy ?? 'tradingValue',
      order: rankingParams.order ?? 'desc',
    }),
    [rankingParams.sortBy, rankingParams.order]
  );
  const handleRankingSortChange = useCallback(
    (sortState: RankingTableSortState) => {
      setRankingParams({
        ...rankingParams,
        sortBy: sortState.field,
        order: sortState.order,
      });
    },
    [rankingParams, setRankingParams]
  );
  const rankingQueryParams = useMemo(
    () => buildRankingQueryParams(activeDailyView, rankingParams),
    [activeDailyView, rankingParams]
  );
  const rankingQuery = useRanking(rankingQueryParams, true);
  const footprintQuery = useMarketBubbleFootprint({
    markets: rankingParams.markets ?? 'prime,standard,growth',
    date: rankingParams.date,
  });
  const introMetaItems = buildIntroMetaItems(activeDailyView, rankingParams);

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
        <div className="flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
          <div className="min-w-0 shrink-0 space-y-1">
            <SectionEyebrow>Analytics Workspace</SectionEyebrow>
            <div className="space-y-0.5">
              <h1 className="text-2xl font-semibold tracking-tight text-foreground">Ranking</h1>
              <p className="max-w-2xl text-xs text-muted-foreground sm:text-sm">
                Daily ranking, technical events, and index performance.
              </p>
            </div>
          </div>
          <div className="flex min-w-0 flex-col gap-2 lg:flex-row lg:items-center lg:justify-end">
            <PageIntroMetaList
              items={introMetaItems}
              className="shrink-0 gap-x-2.5 gap-y-1 [&>div]:min-w-[6.5rem] [&>div]:pl-2"
            />
            <BubbleFootprintBanner
              data={footprintQuery.data}
              isLoading={footprintQuery.isLoading}
              errorMessage={footprintQuery.error?.message ?? null}
            />
          </div>
        </div>
      </Surface>

      <SplitLayout className="min-h-0 flex-1 flex-col gap-3 lg:flex-row lg:items-stretch lg:overflow-hidden">
        <SplitSidebar className="w-full lg:h-full lg:w-40 lg:overflow-auto xl:w-44 2xl:w-48">
          <RankingSidebar
            activeDailyView={activeDailyView}
            rankingParams={rankingParams}
            setActiveDailyView={setActiveDailyView}
            setRankingParams={setRankingParams}
          />
        </SplitSidebar>

        <SplitMain className="gap-3 lg:overflow-hidden">
          <RankingContent
            activeDailyView={activeDailyView}
            rankingParams={rankingParams}
            rankingQuery={rankingQuery}
            rankingSortState={rankingSortState}
            onRankingSortChange={handleRankingSortChange}
            onStockClick={handleStockClick}
            onIndexClick={handleIndexClick}
          />
        </SplitMain>
      </SplitLayout>
    </div>
  );
}
