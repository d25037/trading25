import { useNavigate } from '@tanstack/react-router';
import { type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  PageIntroMetaList,
  SectionEyebrow,
  SegmentedTabs,
  SplitLayout,
  SplitMain,
  Surface,
} from '@/components/Layout/Workspace';
import { BubbleFootprintBanner } from '@/components/MarketRegime/BubbleFootprintBanner';
import {
  FORWARD_EPS_DISCLOSURE_OPTIONS,
  IndexPerformanceTable,
  PERIOD_OPTIONS,
  RANKING_LOOKBACK_OPTIONS,
  RANKING_MARKET_OPTIONS,
  RankingTable,
  type RankingTableSortState,
  SECTOR_STRENGTH_FAMILY_OPTIONS,
} from '@/components/Ranking';
import { RankingPresetInfoButton } from '@/components/Ranking/RankingPresetInfoButton';
import {
  applyRankingPreset,
  getRankingPreset,
  RANKING_PRESET_OPTIONS,
  type RankingPreset,
} from '@/components/Ranking/rankingState';
import { DateInput, MarketsSelect, NumberSelect } from '@/components/shared/filters';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useMarketBubbleFootprint } from '@/hooks/useMarketBubbleFootprint';
import { useRankingRouteState } from '@/hooks/usePageRouteState';
import { useRanking } from '@/hooks/useRanking';
import { useWatchlists, useWatchlistWithItems } from '@/hooks/useWatchlist';
import { formatMarketsLabel } from '@/lib/marketUtils';
import { cn } from '@/lib/utils';
import type { DailyRankingTableFilters, RankingDailyView, RankingParams } from '@/types/ranking';

const dailyViewTabs = [
  { value: 'stocks' as RankingDailyView, label: 'Individual Stocks' },
  { value: 'technicalEvents' as RankingDailyView, label: 'Technical Events' },
  { value: 'indices' as RankingDailyView, label: 'Indices' },
];
const rankingMoreControlsId = 'ranking-more-controls';

interface RankingHeaderControlsProps {
  activeDailyView: RankingDailyView;
  rankingParams: RankingParams;
  rankingTableFilters: DailyRankingTableFilters;
  setActiveDailyView: (view: RankingDailyView) => void;
  setRankingParams: (params: RankingParams) => void;
  setRankingTableFilters: (filters: DailyRankingTableFilters) => void;
}

interface RankingContentProps {
  activeDailyView: RankingDailyView;
  rankingParams: RankingParams;
  rankingQuery: ReturnType<typeof useRanking>;
  rankingSortState: RankingTableSortState;
  rankingTableFilters: DailyRankingTableFilters;
  watchlistsQuery: ReturnType<typeof useWatchlists>;
  selectedWatchlistQuery: ReturnType<typeof useWatchlistWithItems>;
  headerControls: ReactNode;
  onRankingSortChange: (state: RankingTableSortState) => void;
  onRankingTableFiltersChange: (filters: DailyRankingTableFilters) => void;
  scrollRestorationKey: string;
  onStockClick: (code: string) => void;
  onIndexClick: (code: string) => void;
}

interface SelectFieldOption<T extends string> {
  value: T;
  label: string;
}

type RankingParamUpdater = <K extends keyof RankingParams>(key: K, value: RankingParams[K]) => void;

function resolveRankingLimit(activeDailyView: RankingDailyView, rankingParams: RankingParams): number | undefined {
  if (activeDailyView === 'technicalEvents') return 50;
  if (activeDailyView === 'indices') return 20;
  return rankingParams.limit;
}

function buildRankingQueryParams(
  activeDailyView: RankingDailyView,
  rankingParams: RankingParams,
  rankingTableFilters: DailyRankingTableFilters
): RankingParams {
  const isStocksView = activeDailyView === 'stocks';
  const warningSignal = rankingTableFilters.warningSignal;
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
    sectorStrengthFamily:
      activeDailyView !== 'technicalEvents'
        ? (rankingParams.sectorStrengthFamily ?? 'balanced_sector_strength')
        : undefined,
    forwardEpsDisclosedWithinDays:
      activeDailyView === 'stocks' ? (rankingParams.forwardEpsDisclosedWithinDays ?? 0) : 0,
    regimeState: isStocksView ? rankingTableFilters.regimeState : undefined,
    fundamentalState: isStocksView ? rankingTableFilters.valuationSignal : undefined,
    riskState: isStocksView && warningSignal === 'overheat' ? 'overheat' : undefined,
    technicalState: isStocksView ? rankingTableFilters.technicalState : undefined,
  };
}

function sectorStrengthDescription(sectorStrengthFamily: RankingParams['sectorStrengthFamily']): string {
  if (sectorStrengthFamily === 'long_hybrid_leadership') {
    return 'Score: 120D/252D/504D index leadership + constituent/breadth leadership。';
  }
  return 'Balanced strength: 20D/60D TOPIX超過 + 20D breadth。';
}

function SelectField<T extends string>({
  id,
  label,
  value,
  options,
  onChange,
  className,
}: {
  id: string;
  label: string;
  value: T;
  options: readonly SelectFieldOption<T>[];
  onChange: (value: T) => void;
  className?: string;
}) {
  return (
    <div className={cn('space-y-1.5', className)}>
      <Label htmlFor={id} className="text-xs">
        {label}
      </Label>
      <Select value={value} onValueChange={onChange}>
        <SelectTrigger id={id} className="h-8 text-xs">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {options.map((option) => (
            <SelectItem key={option.value} value={option.value}>
              {option.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}

function SectorStrengthSelect({
  id,
  value,
  onChange,
}: {
  id: string;
  value: RankingParams['sectorStrengthFamily'] | undefined;
  onChange: (value: RankingParams['sectorStrengthFamily']) => void;
}) {
  return (
    <SelectField
      id={id}
      label="Sector Selector"
      value={value ?? 'balanced_sector_strength'}
      onChange={onChange}
      options={SECTOR_STRENGTH_FAMILY_OPTIONS}
    />
  );
}

function useDismissiblePopover() {
  const [isOpen, setIsOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    const handlePointerDown = (event: PointerEvent) => {
      const target = event.target instanceof Element ? event.target : null;
      const isPortaledSelectInteraction = target?.closest('[role="listbox"], [role="option"]') != null;
      if (!containerRef.current?.contains(event.target as Node) && !isPortaledSelectInteraction) {
        setIsOpen(false);
      }
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setIsOpen(false);
      }
    };

    document.addEventListener('pointerdown', handlePointerDown);
    document.addEventListener('keydown', handleKeyDown);

    return () => {
      document.removeEventListener('pointerdown', handlePointerDown);
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [isOpen]);

  return { containerRef, isOpen, setIsOpen };
}

function StockRankingMoreControls({
  commonMarketAndDateControls,
  rankingParams,
  updateParam,
}: {
  commonMarketAndDateControls: ReactNode;
  rankingParams: RankingParams;
  updateParam: RankingParamUpdater;
}) {
  return (
    <>
      {commonMarketAndDateControls}
      <NumberSelect
        value={rankingParams.lookbackDays || 1}
        onChange={(value) => updateParam('lookbackDays', value)}
        options={RANKING_LOOKBACK_OPTIONS}
        id="ranking-lookbackDays"
        label="Lookback Days"
      />
      <NumberSelect
        value={rankingParams.forwardEpsDisclosedWithinDays ?? 0}
        onChange={(value) => updateParam('forwardEpsDisclosedWithinDays', value)}
        options={FORWARD_EPS_DISCLOSURE_OPTIONS}
        id="ranking-forward-eps-disclosed-within-days"
        label="Fwd EPS Disclosure"
      />
      <SectorStrengthSelect
        id="ranking-sector-strength-family"
        value={rankingParams.sectorStrengthFamily}
        onChange={(value) => updateParam('sectorStrengthFamily', value)}
      />
    </>
  );
}

function TechnicalEventMoreControls({
  commonMarketAndDateControls,
  rankingParams,
  updateParam,
}: {
  commonMarketAndDateControls: ReactNode;
  rankingParams: RankingParams;
  updateParam: RankingParamUpdater;
}) {
  return (
    <>
      {commonMarketAndDateControls}
      <SelectField
        id="ranking-technical-event-type"
        label="Event Type"
        value={rankingParams.technicalEventType || 'periodHigh'}
        onChange={(value) => updateParam('technicalEventType', value as RankingParams['technicalEventType'])}
        options={[
          { value: 'periodHigh', label: 'New High' },
          { value: 'periodLow', label: 'New Low' },
        ]}
      />
      <NumberSelect
        value={rankingParams.periodDays || 250}
        onChange={(value) => updateParam('periodDays', value)}
        options={PERIOD_OPTIONS}
        id="ranking-technical-periodDays"
        label="Period Days"
      />
    </>
  );
}

function IndexMoreControls({
  commonMarketAndDateControls,
  rankingParams,
  updateParam,
}: {
  commonMarketAndDateControls: ReactNode;
  rankingParams: RankingParams;
  updateParam: RankingParamUpdater;
}) {
  return (
    <>
      {commonMarketAndDateControls}
      <NumberSelect
        value={rankingParams.lookbackDays || 1}
        onChange={(value) => updateParam('lookbackDays', value)}
        options={RANKING_LOOKBACK_OPTIONS}
        id="index-performance-lookbackDays"
        label="Lookback Days"
      />
      <SectorStrengthSelect
        id="index-performance-sector-strength-family"
        value={rankingParams.sectorStrengthFamily}
        onChange={(value) => updateParam('sectorStrengthFamily', value)}
      />
    </>
  );
}

function RankingMoreControlsPanel({
  activeDailyView,
  commonMarketAndDateControls,
  rankingParams,
  updateParam,
}: {
  activeDailyView: RankingDailyView;
  commonMarketAndDateControls: ReactNode;
  rankingParams: RankingParams;
  updateParam: RankingParamUpdater;
}) {
  if (activeDailyView === 'stocks') {
    return (
      <StockRankingMoreControls
        commonMarketAndDateControls={commonMarketAndDateControls}
        rankingParams={rankingParams}
        updateParam={updateParam}
      />
    );
  }

  if (activeDailyView === 'technicalEvents') {
    return (
      <TechnicalEventMoreControls
        commonMarketAndDateControls={commonMarketAndDateControls}
        rankingParams={rankingParams}
        updateParam={updateParam}
      />
    );
  }

  return (
    <IndexMoreControls
      commonMarketAndDateControls={commonMarketAndDateControls}
      rankingParams={rankingParams}
      updateParam={updateParam}
    />
  );
}

function RankingHeaderControls({
  activeDailyView,
  rankingParams,
  rankingTableFilters,
  setActiveDailyView,
  setRankingParams,
  setRankingTableFilters,
}: RankingHeaderControlsProps) {
  const {
    containerRef: moreControlsRef,
    isOpen: isMoreControlsOpen,
    setIsOpen: setIsMoreControlsOpen,
  } = useDismissiblePopover();
  const rankingPreset = getRankingPreset(rankingTableFilters);
  const updateParam = <K extends keyof RankingParams>(key: K, value: RankingParams[K]) => {
    setRankingParams({ ...rankingParams, [key]: value });
  };
  const updatePreset = (preset: RankingPreset) => {
    const next = applyRankingPreset(rankingParams, rankingTableFilters, preset);
    setRankingParams(next.rankingParams);
    setRankingTableFilters(next.rankingTableFilters);
  };
  const commonMarketAndDateControls = (
    <>
      <MarketsSelect
        value={rankingParams.markets || 'prime'}
        onChange={(value) => updateParam('markets', value)}
        options={RANKING_MARKET_OPTIONS}
        id={`ranking-${activeDailyView}-markets`}
      />
      <DateInput
        value={rankingParams.date}
        onChange={(date) => updateParam('date', date)}
        id={`ranking-${activeDailyView}-date`}
      />
    </>
  );

  return (
    <div className="flex min-w-0 flex-wrap items-end gap-2">
      <SegmentedTabs
        items={dailyViewTabs}
        value={activeDailyView}
        onChange={setActiveDailyView}
        className="max-w-full overflow-x-auto"
        itemClassName="h-8 shrink-0 rounded-lg px-2.5 py-1.5 text-xs"
      />

      {activeDailyView === 'stocks' ? (
        <div className="flex items-end gap-1.5">
          <SelectField
            id="ranking-preset"
            label="Preset"
            value={rankingPreset}
            onChange={updatePreset}
            options={RANKING_PRESET_OPTIONS}
            className="w-44"
          />
          <RankingPresetInfoButton className="mb-0.5" />
        </div>
      ) : null}

      <div ref={moreControlsRef} className="relative">
        <button
          type="button"
          aria-controls={rankingMoreControlsId}
          aria-expanded={isMoreControlsOpen}
          aria-haspopup="true"
          onClick={() => setIsMoreControlsOpen((current) => !current)}
          className="app-interactive flex h-8 items-center rounded-lg border border-border/70 px-3 text-xs font-medium text-muted-foreground hover:bg-[var(--app-surface-muted)] hover:text-foreground"
        >
          More
        </button>
        {isMoreControlsOpen ? (
          <div
            id={rankingMoreControlsId}
            className="absolute right-0 top-full z-30 mt-2 grid w-[calc(100vw-2rem)] gap-3 rounded-lg border border-border/70 bg-popover p-3 text-popover-foreground shadow-lg sm:w-[34rem] sm:grid-cols-2"
          >
            <RankingMoreControlsPanel
              activeDailyView={activeDailyView}
              commonMarketAndDateControls={commonMarketAndDateControls}
              rankingParams={rankingParams}
              updateParam={updateParam}
            />
          </div>
        ) : null}
      </div>
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

function buildRankingScrollRestorationKey(
  activeDailyView: RankingDailyView,
  rankingParams: RankingParams,
  rankingTableFilters: DailyRankingTableFilters
): string {
  return `ranking:daily-table-scroll:${activeDailyView}:${JSON.stringify({
    date: rankingParams.date,
    forwardEpsDisclosedWithinDays: rankingParams.forwardEpsDisclosedWithinDays,
    limit: rankingParams.limit,
    lookbackDays: rankingParams.lookbackDays,
    markets: rankingParams.markets,
    order: rankingParams.order,
    periodDays: rankingParams.periodDays,
    regimeState: rankingParams.regimeState,
    riskState: rankingParams.riskState,
    sector17Name: rankingParams.sector17Name,
    sector33Name: rankingParams.sector33Name,
    sectorStrengthFamily: rankingParams.sectorStrengthFamily,
    sortBy: rankingParams.sortBy,
    technicalEventType: rankingParams.technicalEventType,
    technicalState: rankingParams.technicalState,
    tableFilters: activeDailyView === 'stocks' ? rankingTableFilters : {},
  })}`;
}

function RankingContent({
  activeDailyView,
  rankingParams,
  rankingQuery,
  rankingSortState,
  rankingTableFilters,
  watchlistsQuery,
  selectedWatchlistQuery,
  headerControls,
  onRankingSortChange,
  onRankingTableFiltersChange,
  scrollRestorationKey,
  onStockClick,
  onIndexClick,
}: RankingContentProps) {
  const sector33IndexPerformance = useMemo(
    () => rankingQuery.data?.indexPerformance?.filter((item) => item.category === 'sector33'),
    [rankingQuery.data?.indexPerformance]
  );
  const selectedWatchlistCodes = useMemo(() => {
    if (!selectedWatchlistQuery.data?.items) return undefined;
    return new Set(selectedWatchlistQuery.data.items.map((item) => item.code));
  }, [selectedWatchlistQuery.data?.items]);

  if (activeDailyView === 'indices') {
    return (
      <IndexPerformanceTable
        items={sector33IndexPerformance}
        isLoading={rankingQuery.isLoading}
        error={rankingQuery.error}
        onIndexClick={onIndexClick}
        lookbackDays={rankingParams.lookbackDays}
        title="33業種指数"
        description={`${sectorStrengthDescription(rankingParams.sectorStrengthFamily)} Index return: ${
          rankingParams.lookbackDays ?? 5
        }営業日前`}
        headerActions={headerControls}
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
        className="flex min-h-[24rem] flex-1 flex-col overflow-visible"
        sortState={rankingSortState}
        onSortChange={onRankingSortChange}
        headerActions={headerControls}
        scrollRestorationKey={scrollRestorationKey}
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
      className="flex min-h-[24rem] flex-1 flex-col overflow-visible"
      sortState={rankingSortState}
      onSortChange={onRankingSortChange}
      headerActions={headerControls}
      enableTableFilters
      filterState={rankingTableFilters}
      filterWatchlists={watchlistsQuery.data?.watchlists ?? []}
      filterWatchlistsLoading={watchlistsQuery.isLoading}
      filterWatchlistsError={watchlistsQuery.error}
      filterWatchlistCodes={selectedWatchlistCodes}
      onFilterChange={onRankingTableFiltersChange}
      scrollRestorationKey={scrollRestorationKey}
    />
  );
}

export function RankingPage() {
  const {
    activeDailyView,
    rankingParams,
    rankingTableFilters,
    setActiveDailyView,
    setRankingParams,
    setRankingTableFilters,
  } = useRankingRouteState();
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
    () => buildRankingQueryParams(activeDailyView, rankingParams, rankingTableFilters),
    [activeDailyView, rankingParams, rankingTableFilters]
  );
  const rankingScrollRestorationKey = useMemo(
    () => buildRankingScrollRestorationKey(activeDailyView, rankingParams, rankingTableFilters),
    [activeDailyView, rankingParams, rankingTableFilters]
  );
  const rankingQuery = useRanking(rankingQueryParams, true);
  const watchlistsQuery = useWatchlists();
  const selectedWatchlistQuery = useWatchlistWithItems(
    activeDailyView === 'stocks' ? (rankingTableFilters.watchlistId ?? null) : null
  );
  const footprintQuery = useMarketBubbleFootprint({
    markets: rankingParams.markets ?? 'prime,standard,growth',
    date: rankingParams.date,
  });
  const introMetaItems = buildIntroMetaItems(activeDailyView, rankingParams);
  const headerControls = (
    <RankingHeaderControls
      activeDailyView={activeDailyView}
      rankingParams={rankingParams}
      rankingTableFilters={rankingTableFilters}
      setActiveDailyView={setActiveDailyView}
      setRankingParams={setRankingParams}
      setRankingTableFilters={setRankingTableFilters}
    />
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

      <SplitLayout className="min-h-0 flex-1 flex-col gap-3 lg:overflow-hidden">
        <SplitMain className="gap-3 lg:overflow-hidden">
          <RankingContent
            activeDailyView={activeDailyView}
            rankingParams={rankingParams}
            rankingQuery={rankingQuery}
            rankingSortState={rankingSortState}
            rankingTableFilters={activeDailyView === 'stocks' ? rankingTableFilters : {}}
            watchlistsQuery={watchlistsQuery}
            selectedWatchlistQuery={selectedWatchlistQuery}
            headerControls={headerControls}
            onRankingSortChange={handleRankingSortChange}
            onRankingTableFiltersChange={setRankingTableFilters}
            scrollRestorationKey={rankingScrollRestorationKey}
            onStockClick={handleStockClick}
            onIndexClick={handleIndexClick}
          />
        </SplitMain>
      </SplitLayout>
    </div>
  );
}
