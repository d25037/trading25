import { useNavigate } from '@tanstack/react-router';
import type { IndexItem } from '@trading25/contracts/types/api-response-types';
import { ChevronRight, Loader2, TrendingDown, TrendingUp } from 'lucide-react';
import { type CSSProperties, type RefObject, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { LinePriceChart } from '@/components/Chart/LinePriceChart';
import { StockChart } from '@/components/Chart/StockChart';
import { SectionEyebrow, SplitLayout, SplitMain, SplitSidebar, Surface } from '@/components/Layout/Workspace';
import { RankingTable, type RankingTableSortState } from '@/components/Ranking';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useIndexData, useIndicesList } from '@/hooks/useIndices';
import { useIndicesRouteState } from '@/hooks/usePageRouteState';
import { useRanking } from '@/hooks/useRanking';
import { INDEX_CATEGORY_LABELS, INDEX_CATEGORY_ORDER } from '@/lib/indexCategories';
import { cn } from '@/lib/utils';
import type { RankingParams } from '@/types/ranking';
import { formatDateShort, formatPercentage } from '@/utils/formatters';
import {
  buildTopixMultiTimeframeModeAnalysis,
  getTopixModeStateCopy,
  TOPIX_MODE_LONG_WINDOW_STREAKS,
  TOPIX_MODE_RECENT_POINT_LIMIT,
  TOPIX_MODE_SHORT_WINDOW_STREAKS,
  type TopixMode,
  type TopixModeAnalysis,
  type TopixModePoint,
} from '@/utils/topixMode';

const BENCHMARK_DISPLAY_ORDER: Record<string, number> = {
  N225_UNDERPX: 0,
  N225_VI: 1,
  NT_RATIO: 2,
};

const TWO_DECIMAL_INDEX_CODES = new Set(['NT_RATIO', 'N225_VI']);

const SYNTHETIC_INDEX_DESCRIPTIONS: Record<string, string> = {
  NT_RATIO: 'Nikkei 225 close / TOPIX close from local market snapshot',
  N225_VI: 'Daily BaseVol reference series derived from local N225 options snapshot',
  N225_UNDERPX: 'UnderPx derived daily reference series',
};
const DEFAULT_SYNTHETIC_DESCRIPTION = 'UnderPx derived daily reference series';

const NIKKEI_PARENT_INDEX_CODE = 'N225_UNDERPX';
const NIKKEI_VI_INDEX_CODE = 'N225_VI';
const TOPIX_PRIMARY_INDEX_CODES = new Set(['1321', 'TOPIX']);

const TOPIX_MODE_TONE_CLASSES: Record<TopixMode, string> = {
  bullish: 'border-emerald-500/25 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
  bearish: 'border-rose-500/25 bg-rose-500/10 text-rose-700 dark:text-rose-300',
};

const SECTOR_CHART_HEIGHT_RATIO = 0.7;
const SECTOR_CHART_BODY_BASE_HEIGHT = 400;
const SECTOR_CHART_BODY_HEIGHT = Math.round(SECTOR_CHART_BODY_BASE_HEIGHT * SECTOR_CHART_HEIGHT_RATIO);
const SECTOR_CHART_PANEL_MIN_HEIGHT = SECTOR_CHART_BODY_HEIGHT + 84;
const SECTOR_STOCK_FETCH_ALL_LIMIT = 0;
const SECTOR_TABLE_MIN_HEIGHT = 680;
const SECTOR_WORKSPACE_GAP = 12;

const TOPIX_MODE_BAR_CLASSES: Record<TopixMode, string> = {
  bullish: 'bg-emerald-500/85',
  bearish: 'bg-rose-500/80',
};

const TOPIX_STATE_TONE_CLASSES = {
  long_bullish__short_bullish: 'border-emerald-500/25 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
  long_bullish__short_bearish: 'border-amber-500/25 bg-amber-500/10 text-amber-700 dark:text-amber-300',
  long_bearish__short_bullish: 'border-orange-500/25 bg-orange-500/10 text-orange-700 dark:text-orange-300',
  long_bearish__short_bearish: 'border-rose-500/25 bg-rose-500/10 text-rose-700 dark:text-rose-300',
} as const;

function formatJapaneseLargeValue(value: number | null | undefined): string {
  if (value === undefined || value === null) return '-';
  if (value >= 1e12) return `${(value / 1e12).toFixed(2)}兆`;
  if (value >= 1e8) return `${(value / 1e8).toFixed(0)}億`;
  if (value >= 1e4) return `${(value / 1e4).toFixed(0)}万`;
  return value.toLocaleString();
}

function formatLatestIndexValue(value: number | null, code: string | null | undefined): string {
  if (value === undefined || value === null) return '-';
  if (code && TWO_DECIMAL_INDEX_CODES.has(code)) {
    return value.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }
  return value.toLocaleString();
}

function reorderCategoryIndices(category: string, indices: IndexItem[]): IndexItem[] {
  if (category !== 'synthetic' || indices.length < 2) {
    return indices;
  }

  return indices
    .map((index, position) => ({ index, position }))
    .sort((left, right) => {
      const leftPriority = BENCHMARK_DISPLAY_ORDER[left.index.code] ?? Number.MAX_SAFE_INTEGER;
      const rightPriority = BENCHMARK_DISPLAY_ORDER[right.index.code] ?? Number.MAX_SAFE_INTEGER;
      if (leftPriority !== rightPriority) {
        return leftPriority - rightPriority;
      }
      return left.position - right.position;
    })
    .map(({ index }) => index);
}

function getFlatIndicesList(indices: IndexItem[]): IndexItem[] {
  const groups: Record<string, IndexItem[]> = {};
  for (const index of indices) {
    const category = index.category;
    const group = groups[category];
    if (group) {
      group.push(index);
    } else {
      groups[category] = [index];
    }
  }

  const result: IndexItem[] = [];
  for (const category of INDEX_CATEGORY_ORDER) {
    const categoryGroup = groups[category];
    if (categoryGroup) {
      result.push(...reorderCategoryIndices(category, categoryGroup));
    }
  }
  return result;
}

function useObservedElementHeight<T extends HTMLElement>() {
  const ref = useRef<T | null>(null);
  const [height, setHeight] = useState<number | null>(null);

  useEffect(() => {
    const element = ref.current;
    if (!element) {
      return;
    }

    const updateHeight = () => {
      const nextHeight = Math.round(element.getBoundingClientRect().height);
      setHeight((current) => (current === nextHeight ? current : nextHeight));
    };

    updateHeight();

    if (typeof ResizeObserver === 'undefined') {
      return;
    }

    const observer = new ResizeObserver(() => {
      updateHeight();
    });

    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  return [ref, height] as const;
}

function CompactMetaStrip({
  items,
  className,
}: {
  items: readonly { label: string; value: string }[];
  className?: string;
}) {
  if (items.length === 0) {
    return null;
  }

  return (
    <dl className={cn('flex min-w-0 flex-nowrap items-center gap-2 overflow-x-auto pb-1', className)}>
      {items.map((item) => (
        <div
          key={item.label}
          className="flex shrink-0 items-baseline gap-2 rounded-full border border-border/70 bg-[var(--app-surface-muted)] px-3 py-1.5"
        >
          <dt className="text-[10px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">{item.label}</dt>
          <dd className="max-w-[14rem] truncate text-sm font-medium text-foreground">{item.value}</dd>
        </div>
      ))}
    </dl>
  );
}

function isTopixPrimaryIndex(code: string | null | undefined, name: string | null | undefined): boolean {
  if (code && TOPIX_PRIMARY_INDEX_CODES.has(code.toUpperCase())) {
    return true;
  }
  return name?.toUpperCase() === 'TOPIX';
}

function formatModeLabel(mode: TopixMode): string {
  return mode === 'bullish' ? 'Bullish' : 'Bearish';
}

function ModePill({ mode }: { mode: TopixMode }) {
  const isBullish = mode === 'bullish';

  return (
    <span
      className={cn(
        'inline-flex items-center gap-1 rounded-full border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.12em]',
        TOPIX_MODE_TONE_CLASSES[mode]
      )}
    >
      {isBullish ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
      {formatModeLabel(mode)}
    </span>
  );
}

function TopixStatePill({ point }: { point: TopixModePoint }) {
  return (
    <span
      className={cn(
        'inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.12em]',
        TOPIX_STATE_TONE_CLASSES[point.stateKey]
      )}
    >
      {point.stateLabel}
    </span>
  );
}

function TopixModeMetricCard({
  label,
  windowStreaks,
  mode,
  dominantReturn,
  dominantEventDate,
  dominantSegmentDayCount,
  modeSpanStreakCount,
}: {
  label: string;
  windowStreaks: number;
  mode: TopixMode;
  dominantReturn: number;
  dominantEventDate: string;
  dominantSegmentDayCount: number;
  modeSpanStreakCount: number;
}) {
  return (
    <article className="rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] px-4 py-3">
      <div className="flex items-start justify-between gap-2">
        <div className="space-y-1">
          <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">{label}</p>
          <p className="text-sm font-semibold text-foreground">Dominant streak over {windowStreaks} streaks</p>
        </div>
        <ModePill mode={mode} />
      </div>
      <p className="mt-3 text-xl font-semibold tracking-tight text-foreground tabular-nums">
        {formatPercentage(dominantReturn * 100)}
      </p>
      <p className="mt-1 text-xs text-muted-foreground">
        Shock ended {formatDateShort(dominantEventDate)} · dominant streak {dominantSegmentDayCount}d
      </p>
      <p className="mt-1 text-xs text-muted-foreground">Mode span {modeSpanStreakCount} streaks</p>
    </article>
  );
}

function TopixStateCard({ point }: { point: TopixModePoint }) {
  const stateCopy = getTopixModeStateCopy(point.stateKey);

  return (
    <article className="rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] px-4 py-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="space-y-1">
          <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">Current Lens</p>
          <p className="text-sm font-semibold text-foreground">{stateCopy.toneLabel}</p>
        </div>
        <TopixStatePill point={point} />
      </div>
      <p className="mt-3 text-xs font-medium text-foreground">4-state segment {point.stateSegmentLength} streaks</p>
      <p className="mt-1 text-xs leading-relaxed text-muted-foreground">{stateCopy.description}</p>
    </article>
  );
}

function TopixModeRibbon({
  label,
  points,
  currentMode,
  currentDominantReturn,
  currentDominantEventDate,
  currentModeSpanStreakCount,
  currentDominantSegmentDayCount,
  modeSelector,
}: {
  label: string;
  points: readonly TopixModePoint[];
  currentMode: TopixMode;
  currentDominantReturn: number;
  currentDominantEventDate: string;
  currentModeSpanStreakCount: number;
  currentDominantSegmentDayCount: number;
  modeSelector: (point: TopixModePoint) => TopixMode;
}) {
  const firstDate = points[0]?.date;
  const lastDate = points.at(-1)?.date;

  return (
    <div className="rounded-2xl border border-border/70 bg-background/80 px-3 py-3">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div className="space-y-1">
          <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">{label}</p>
          <p className="text-xs text-muted-foreground">
            {firstDate ? formatDateShort(firstDate) : '-'} to {lastDate ? formatDateShort(lastDate) : '-'}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
          <ModePill mode={currentMode} />
          <span className="tabular-nums">{formatPercentage(currentDominantReturn * 100)}</span>
          <span>Shock ended {formatDateShort(currentDominantEventDate)}</span>
          <span>Dominant streak {currentDominantSegmentDayCount}d</span>
          <span>Mode span {currentModeSpanStreakCount} streaks</span>
        </div>
      </div>
      <div
        aria-label={`${label} ribbon`}
        role="img"
        className="mt-3 grid h-3 gap-px overflow-hidden rounded-full bg-border/50"
        style={{ gridTemplateColumns: `repeat(${points.length}, minmax(0, 1fr))` }}
      >
        {points.map((point) => {
          const mode = modeSelector(point);
          return <span key={`${label}-${point.date}`} className={cn('h-full w-full', TOPIX_MODE_BAR_CLASSES[mode])} />;
        })}
      </div>
    </div>
  );
}

function TopixModePanel({ analysis }: { analysis: TopixModeAnalysis | null }) {
  const currentPoint = analysis?.currentPoint ?? null;
  if (!analysis || !currentPoint) {
    return null;
  }

  const recentPoints = analysis.points.slice(-TOPIX_MODE_RECENT_POINT_LIMIT);

  return (
    <div
      className="border-b border-border/70 px-4 py-4"
      style={{
        backgroundImage:
          'radial-gradient(circle at top left, rgba(16,185,129,0.08), transparent 32%), radial-gradient(circle at bottom right, rgba(244,63,94,0.08), transparent 34%)',
      }}
    >
      <div className="flex flex-col gap-4">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0 space-y-2">
            <SectionEyebrow>TOPIX Streak Mode</SectionEyebrow>
            <div className="flex flex-wrap items-center gap-2">
              <TopixStatePill point={currentPoint} />
              <div className="rounded-full border border-border/70 bg-background/85 px-3 py-1 text-xs text-muted-foreground">
                Long X={analysis.longWindowStreaks} streaks / Short X={analysis.shortWindowStreaks} streaks
              </div>
            </div>
            <p className="max-w-3xl text-xs leading-relaxed text-muted-foreground">
              Sign of the largest absolute synthesized streak candle inside the trailing streak window. The ribbons
              below show the latest {recentPoints.length} comparable TOPIX streak candles.
            </p>
          </div>
          <div className="grid gap-2 lg:grid-cols-3">
            <TopixModeMetricCard
              label="Long Mode"
              windowStreaks={analysis.longWindowStreaks}
              mode={currentPoint.longMode}
              dominantReturn={currentPoint.longDominantSegmentReturn}
              dominantEventDate={currentPoint.longDominantSegmentEndDate}
              dominantSegmentDayCount={currentPoint.longDominantSegmentDayCount}
              modeSpanStreakCount={currentPoint.longModeSpanStreakCount}
            />
            <TopixModeMetricCard
              label="Short Mode"
              windowStreaks={analysis.shortWindowStreaks}
              mode={currentPoint.shortMode}
              dominantReturn={currentPoint.shortDominantSegmentReturn}
              dominantEventDate={currentPoint.shortDominantSegmentEndDate}
              dominantSegmentDayCount={currentPoint.shortDominantSegmentDayCount}
              modeSpanStreakCount={currentPoint.shortModeSpanStreakCount}
            />
            <TopixStateCard point={currentPoint} />
          </div>
        </div>

        <div className="grid gap-3 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
          <TopixModeRibbon
            label={`Long regime (${analysis.longWindowStreaks} streaks)`}
            points={recentPoints}
            currentMode={currentPoint.longMode}
            currentDominantReturn={currentPoint.longDominantSegmentReturn}
            currentDominantEventDate={currentPoint.longDominantSegmentEndDate}
            currentModeSpanStreakCount={currentPoint.longModeSpanStreakCount}
            currentDominantSegmentDayCount={currentPoint.longDominantSegmentDayCount}
            modeSelector={(point) => point.longMode}
          />
          <TopixModeRibbon
            label={`Short regime (${analysis.shortWindowStreaks} streaks)`}
            points={recentPoints}
            currentMode={currentPoint.shortMode}
            currentDominantReturn={currentPoint.shortDominantSegmentReturn}
            currentDominantEventDate={currentPoint.shortDominantSegmentEndDate}
            currentModeSpanStreakCount={currentPoint.shortModeSpanStreakCount}
            currentDominantSegmentDayCount={currentPoint.shortDominantSegmentDayCount}
            modeSelector={(point) => point.shortMode}
          />
        </div>
      </div>
    </div>
  );
}

interface IndicesListProps {
  indices: IndexItem[];
  selectedCode: string | null;
  onSelect: (code: string) => void;
  isLoading: boolean;
  containerRef: RefObject<HTMLDivElement | null>;
}

function IndicesList({ indices, selectedCode, onSelect, isLoading, containerRef }: IndicesListProps) {
  const groupedIndices = useMemo(() => {
    const groups: Record<string, IndexItem[]> = {};
    for (const index of indices) {
      const category = index.category;
      if (!groups[category]) {
        groups[category] = [];
      }
      groups[category].push(index);
    }
    return groups;
  }, [indices]);

  if (isLoading) {
    return (
      <div ref={containerRef} className="min-h-0 flex-1 overflow-auto">
        <div className="flex h-full min-h-[20rem] items-center justify-center">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      </div>
    );
  }

  if (indices.length === 0) {
    return (
      <div ref={containerRef} className="min-h-0 flex-1 overflow-auto">
        <div className="flex min-h-[20rem] flex-col items-center justify-center px-4 text-center">
          <TrendingUp className="mb-4 h-10 w-10 text-muted-foreground" />
          <p className="text-sm font-medium text-foreground">No indices found</p>
          <p className="mt-1 text-xs text-muted-foreground">Run database sync to fetch index data.</p>
        </div>
      </div>
    );
  }

  return (
    <div ref={containerRef} className="min-h-0 flex-1 overflow-auto pr-1">
      <div className="space-y-4">
        {INDEX_CATEGORY_ORDER.filter((category) => groupedIndices[category]).map((category) => {
          const rawCategoryIndices = groupedIndices[category];
          if (!rawCategoryIndices) return null;
          const categoryIndices = reorderCategoryIndices(category, rawCategoryIndices);

          return (
            <section key={category} className="space-y-1.5">
              <SectionEyebrow className="px-1">{INDEX_CATEGORY_LABELS[category] ?? category}</SectionEyebrow>
              <div className="space-y-1">
                {categoryIndices.map((index) => {
                  const isSelected = selectedCode === index.code;

                  return (
                    <button
                      key={index.code}
                      type="button"
                      data-index-code={index.code}
                      onClick={() => onSelect(index.code)}
                      aria-label={`Select ${index.name}`}
                      aria-pressed={isSelected}
                      className={cn(
                        'app-interactive flex w-full items-center justify-between gap-2 rounded-xl border px-3 py-2 text-left text-xs transition-colors',
                        isSelected
                          ? 'border-border/70 bg-[var(--app-surface-emphasis)] text-foreground shadow-sm'
                          : 'border-transparent text-foreground hover:border-border/60 hover:bg-[var(--app-surface-muted)]'
                      )}
                    >
                      <span className="truncate font-medium">{index.name}</span>
                      <ChevronRight className="h-3 w-3 shrink-0 opacity-50" />
                    </button>
                  );
                })}
              </div>
            </section>
          );
        })}
      </div>
    </div>
  );
}

interface SectorStocksListProps {
  sectorName: string;
  sectorType: 'sector33' | 'sector17';
  onStockClick: (code: string) => void;
  panelMinHeight?: number | null;
  markets: string;
  lookbackDays: number;
  sortState: RankingTableSortState;
  onMarketsChange: (markets: string) => void;
  onLookbackDaysChange: (lookbackDays: number) => void;
  onSortChange: (sortState: RankingTableSortState) => void;
}

function SectorStocksList({
  sectorName,
  sectorType,
  onStockClick,
  panelMinHeight,
  markets,
  lookbackDays,
  sortState,
  onMarketsChange,
  onLookbackDaysChange,
  onSortChange,
}: SectorStocksListProps) {
  const resolvedPanelMinHeight = panelMinHeight ? Math.max(panelMinHeight, SECTOR_TABLE_MIN_HEIGHT) : 480;
  const panelStyle: CSSProperties = panelMinHeight
    ? {
        height: `${resolvedPanelMinHeight}px`,
        minHeight: `${resolvedPanelMinHeight}px`,
      }
    : {
        minHeight: `${resolvedPanelMinHeight}px`,
      };

  const rankingParams = useMemo<RankingParams>(
    () => ({
      ...(sectorType === 'sector33' ? { sector33Name: sectorName } : { sector17Name: sectorName }),
      markets,
      lookbackDays,
      limit: SECTOR_STOCK_FETCH_ALL_LIMIT,
      includeValuation: true,
    }),
    [lookbackDays, markets, sectorName, sectorType]
  );

  const { data, isLoading, error } = useRanking(rankingParams, true);

  const headerActions = (
    <>
      <div className="flex items-center gap-2">
        <span className="text-xs text-muted-foreground">市場</span>
        <Select value={markets} onValueChange={onMarketsChange}>
          <SelectTrigger className="h-8 w-36 text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="prime">プライム</SelectItem>
            <SelectItem value="standard">スタンダード</SelectItem>
            <SelectItem value="growth">グロース</SelectItem>
            <SelectItem value="prime,standard">P + S</SelectItem>
            <SelectItem value="prime,standard,growth">全市場</SelectItem>
          </SelectContent>
        </Select>
      </div>
      <div className="flex items-center gap-2">
        <span className="text-xs text-muted-foreground">比較期間</span>
        <Select value={lookbackDays.toString()} onValueChange={(value) => onLookbackDaysChange(Number(value))}>
          <SelectTrigger className="h-8 w-28 text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="1">前日比</SelectItem>
            <SelectItem value="5">5営業日</SelectItem>
            <SelectItem value="10">10営業日</SelectItem>
            <SelectItem value="20">20営業日</SelectItem>
          </SelectContent>
        </Select>
      </div>
    </>
  );

  return (
    <RankingTable
      items={data?.rankings.tradingValue}
      isLoading={isLoading}
      error={error}
      onStockClick={onStockClick}
      title="銘柄一覧"
      eyebrow="Daily Ranking"
      periodDays={data?.periodDays}
      showValuation
      showLiquidity
      showMarket
      showChangeForTradingValue
      enableColumnSort
      sortState={sortState}
      onSortChange={onSortChange}
      emptyMessage="銘柄が見つかりません"
      emptySubMessage="市場や比較期間を変更してください"
      formatLargeValue={formatJapaneseLargeValue}
      labels={{
        tradingValue: lookbackDays === 1 ? '売買代金' : `売買代金(${lookbackDays}日)`,
      }}
      headerActions={headerActions}
      className="flex flex-col overflow-hidden lg:min-h-0"
      style={panelStyle}
      testId="sector-stocks-panel"
    />
  );
}

interface IndexChartProps {
  code: string | null;
  indexInfo?: IndexItem;
  onStockClick: (code: string) => void;
  panelMinHeight?: number | null;
  sectorMarkets: string;
  sectorLookbackDays: number;
  sectorSortState: RankingTableSortState;
  onSectorMarketsChange: (markets: string) => void;
  onSectorLookbackDaysChange: (lookbackDays: number) => void;
  onSectorSortChange: (sortState: RankingTableSortState) => void;
}

interface LoadedIndexChartProps {
  code: string;
  data: {
    code: string;
    name: string;
    data: readonly {
      date: string;
      open: number;
      high: number;
      low: number;
      close: number;
    }[];
  };
  indexInfo?: IndexItem;
  onStockClick: (code: string) => void;
  chartData: {
    time: string;
    open: number;
    high: number;
    low: number;
    close: number;
  }[];
  lineData: {
    time: string;
    value: number;
  }[];
  showViSubChart: boolean;
  viLineData: {
    time: string;
    value: number;
  }[];
  viIsLoading: boolean;
  viErrorMessage?: string;
  workspaceStyle: CSSProperties;
  chartPanelStyle: CSSProperties;
  chartBodyHeight: number | null;
  sectorPanelMinHeight: number | null;
  sectorType?: 'sector33' | 'sector17';
  sectorName: string | null;
  isSectorIndex: boolean;
  isSyntheticIndex: boolean;
  sectorMarkets: string;
  sectorLookbackDays: number;
  sectorSortState: RankingTableSortState;
  onSectorMarketsChange: (markets: string) => void;
  onSectorLookbackDaysChange: (lookbackDays: number) => void;
  onSectorSortChange: (sortState: RankingTableSortState) => void;
}

function resolveIndexChartLayout(panelMinHeight: number | null | undefined, isSectorIndex: boolean) {
  const workspaceMinHeight = panelMinHeight ? Math.max(panelMinHeight, 432) : 432;
  const chartBodyHeight = isSectorIndex ? SECTOR_CHART_BODY_HEIGHT : null;
  const chartMinHeight = isSectorIndex ? SECTOR_CHART_PANEL_MIN_HEIGHT : workspaceMinHeight;
  const sectorMinHeight =
    isSectorIndex && panelMinHeight
      ? Math.max(SECTOR_TABLE_MIN_HEIGHT, workspaceMinHeight - chartMinHeight - SECTOR_WORKSPACE_GAP)
      : isSectorIndex
        ? SECTOR_TABLE_MIN_HEIGHT
        : null;

  return {
    workspaceMinHeight,
    chartPanelStyle: { minHeight: `${chartMinHeight}px` } satisfies CSSProperties,
    workspaceStyle: { minHeight: `${workspaceMinHeight}px` } satisfies CSSProperties,
    surfaceStyle: { minHeight: `${workspaceMinHeight}px` } satisfies CSSProperties,
    chartBodyHeight,
    sectorPanelMinHeight: sectorMinHeight,
  };
}

function resolveIndexChartCategoryLabel(indexInfo?: IndexItem): string {
  if (!indexInfo) {
    return 'Index';
  }
  if (indexInfo.category === 'synthetic') {
    return 'Synthetic';
  }
  return INDEX_CATEGORY_LABELS[indexInfo.category] ?? indexInfo.category;
}

function resolveSyntheticDescription(isSyntheticIndex: boolean, code: string | null | undefined): string | null {
  if (!isSyntheticIndex) {
    return null;
  }
  if (!code) {
    return DEFAULT_SYNTHETIC_DESCRIPTION;
  }
  return SYNTHETIC_INDEX_DESCRIPTIONS[code] ?? DEFAULT_SYNTHETIC_DESCRIPTION;
}

function toLinePriceData(data: { data?: readonly { date: string; close: number }[] } | null | undefined) {
  if (!data?.data) {
    return [];
  }
  return data.data.map((point) => ({
    time: point.date,
    value: point.close,
  }));
}

interface IndexMainChartAreaProps {
  isSectorIndex: boolean;
  isSyntheticIndex: boolean;
  chartData: {
    time: string;
    open: number;
    high: number;
    low: number;
    close: number;
  }[];
  lineData: {
    time: string;
    value: number;
  }[];
  chartBodyHeight: number | null;
}

function IndexMainChartArea({
  isSectorIndex,
  isSyntheticIndex,
  chartData,
  lineData,
  chartBodyHeight,
}: IndexMainChartAreaProps) {
  const chartBodyStyle = chartBodyHeight
    ? ({ height: `${chartBodyHeight}px`, minHeight: `${chartBodyHeight}px` } satisfies CSSProperties)
    : undefined;
  const chartBodyClassName = isSectorIndex ? 'shrink-0' : 'min-h-[24rem] flex-1';

  return (
    <>
      {!isSectorIndex ? (
        <div className="border-b border-border/70 px-4 py-3">
          <SectionEyebrow>Chart</SectionEyebrow>
          <h3 className="mt-1 text-sm font-semibold text-foreground">
            Price Chart ({isSyntheticIndex ? lineData.length : chartData.length} data points)
          </h3>
        </div>
      ) : null}

      <div className={cn('p-0', chartBodyClassName)} style={chartBodyStyle} data-testid="index-chart-body">
        <div className="h-full">
          {isSyntheticIndex ? (
            <LinePriceChart data={lineData} height={chartBodyHeight ?? undefined} />
          ) : (
            <StockChart data={chartData} height={chartBodyHeight ?? undefined} />
          )}
        </div>
      </div>
    </>
  );
}

function renderIndexChartState(
  kind: 'empty' | 'loading' | 'error',
  surfaceStyle: CSSProperties,
  errorMessage?: string
) {
  if (kind === 'empty') {
    return (
      <Surface className="flex flex-col overflow-hidden" style={surfaceStyle}>
        <div className="border-b border-border/70 px-4 py-3">
          <SectionEyebrow>Results</SectionEyebrow>
          <h2 className="mt-1 text-xl font-semibold tracking-tight text-foreground">Index Workspace</h2>
        </div>
        <div className="flex min-h-[24rem] flex-1 flex-col items-center justify-center px-6 text-center">
          <TrendingUp className="mb-4 h-14 w-14 text-muted-foreground" />
          <p className="text-lg font-medium text-foreground">Select an index to view chart</p>
        </div>
      </Surface>
    );
  }

  if (kind === 'loading') {
    return (
      <Surface className="flex flex-col overflow-hidden" style={surfaceStyle}>
        <div className="flex min-h-[24rem] flex-1 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      </Surface>
    );
  }

  return (
    <Surface className="flex flex-col overflow-hidden" style={surfaceStyle}>
      <div className="flex min-h-[24rem] flex-1 items-center justify-center px-6 text-center">
        <p className="text-sm text-destructive">Failed to load index data: {errorMessage}</p>
      </div>
    </Surface>
  );
}

function IndexViSubChart({
  lineData,
  isLoading,
  errorMessage,
}: {
  lineData: { time: string; value: number }[];
  isLoading: boolean;
  errorMessage?: string;
}) {
  const viDescription = SYNTHETIC_INDEX_DESCRIPTIONS[NIKKEI_VI_INDEX_CODE] ?? DEFAULT_SYNTHETIC_DESCRIPTION;
  const viLatestValue = lineData[lineData.length - 1]?.value ?? null;

  return (
    <>
      <div className="border-t border-border/70 px-4 py-3">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div className="space-y-1">
            <SectionEyebrow>Sub-chart</SectionEyebrow>
            <h3 className="text-sm font-semibold text-foreground">日経VI ({lineData.length} data points)</h3>
            <p className="text-xs text-muted-foreground">{viDescription}</p>
          </div>
          <div className="shrink-0 rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] px-4 py-3">
            <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">Latest VI</p>
            <p className="mt-1 whitespace-nowrap text-xl font-semibold tracking-tight text-foreground tabular-nums">
              {formatLatestIndexValue(viLatestValue, NIKKEI_VI_INDEX_CODE)}
            </p>
          </div>
        </div>
      </div>
      <div className="min-h-[14rem] border-t border-border/70 p-0">
        {isLoading ? (
          <div className="flex h-full min-h-[14rem] items-center justify-center">
            <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
          </div>
        ) : errorMessage ? (
          <div className="flex h-full min-h-[14rem] items-center justify-center px-6 text-center">
            <p className="text-sm text-destructive">Failed to load VI sub-chart: {errorMessage}</p>
          </div>
        ) : (
          <div className="h-full min-h-[14rem]">
            <LinePriceChart data={lineData} />
          </div>
        )}
      </div>
    </>
  );
}

function LoadedIndexChart({
  code,
  data,
  indexInfo,
  onStockClick,
  chartData,
  lineData,
  showViSubChart,
  viLineData,
  viIsLoading,
  viErrorMessage,
  workspaceStyle,
  chartPanelStyle,
  chartBodyHeight,
  sectorPanelMinHeight,
  sectorType,
  sectorName,
  isSectorIndex,
  isSyntheticIndex,
  sectorMarkets,
  sectorLookbackDays,
  sectorSortState,
  onSectorMarketsChange,
  onSectorLookbackDaysChange,
  onSectorSortChange,
}: LoadedIndexChartProps) {
  const lastDataPoint = chartData.length > 0 ? chartData[chartData.length - 1] : null;
  const latestPrice = lastDataPoint?.close ?? lineData[lineData.length - 1]?.value ?? null;
  const chartTypeLabel = isSyntheticIndex ? 'Line reference' : 'OHLC bars';
  const categoryLabel = resolveIndexChartCategoryLabel(indexInfo);
  const syntheticDescription = resolveSyntheticDescription(isSyntheticIndex, code);
  const showTopixModePanel = isTopixPrimaryIndex(code, data.name) && !isSyntheticIndex;
  const topixModeAnalysis = useMemo(() => {
    if (!showTopixModePanel) {
      return null;
    }
    return buildTopixMultiTimeframeModeAnalysis(
      data.data.map((point) => ({
        date: point.date,
        close: point.close,
      })),
      {
        shortWindowStreaks: TOPIX_MODE_SHORT_WINDOW_STREAKS,
        longWindowStreaks: TOPIX_MODE_LONG_WINDOW_STREAKS,
      }
    );
  }, [data.data, showTopixModePanel]);

  return (
    <div className="space-y-3" style={workspaceStyle}>
      <Surface
        className="flex flex-col overflow-hidden lg:min-h-0"
        style={chartPanelStyle}
        data-testid="index-chart-panel"
      >
        <div className={cn('border-b border-border/70 px-4', isSectorIndex ? 'py-2.5' : 'py-4')}>
          <div
            className={cn(
              'flex flex-col lg:flex-row lg:items-end lg:justify-between',
              isSectorIndex ? 'gap-2' : 'gap-4'
            )}
          >
            <div className="min-w-0 space-y-2">
              <SectionEyebrow>Results</SectionEyebrow>
              <div className="space-y-1">
                <h2
                  className={cn(
                    'truncate font-semibold tracking-tight text-foreground',
                    isSectorIndex ? 'text-lg' : 'text-2xl'
                  )}
                >
                  {data.name}
                </h2>
                <CompactMetaStrip
                  items={[
                    { label: 'Code', value: data.code },
                    { label: 'Series', value: chartTypeLabel },
                  ]}
                  className="max-w-full"
                />
              </div>
              {syntheticDescription ? <p className="text-xs text-muted-foreground">{syntheticDescription}</p> : null}
            </div>
            <div
              className={cn(
                'shrink-0 rounded-2xl border border-border/70 bg-[var(--app-surface-muted)]',
                isSectorIndex ? 'px-3 py-2' : 'px-4 py-3'
              )}
            >
              <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">Latest</p>
              <p
                className={cn(
                  'mt-1 whitespace-nowrap font-semibold tracking-tight text-foreground tabular-nums',
                  isSectorIndex ? 'text-lg' : 'text-xl'
                )}
              >
                {formatLatestIndexValue(latestPrice, data.code)}
              </p>
              <p className="mt-1 whitespace-nowrap text-xs text-muted-foreground">{categoryLabel}</p>
            </div>
          </div>
        </div>

        <TopixModePanel analysis={topixModeAnalysis} />

        <IndexMainChartArea
          isSectorIndex={isSectorIndex}
          isSyntheticIndex={isSyntheticIndex}
          chartData={chartData}
          lineData={lineData}
          chartBodyHeight={chartBodyHeight}
        />

        {showViSubChart ? (
          <IndexViSubChart lineData={viLineData} isLoading={viIsLoading} errorMessage={viErrorMessage} />
        ) : null}
      </Surface>

      {isSectorIndex && sectorType && sectorName ? (
        <SectorStocksList
          sectorName={sectorName}
          sectorType={sectorType}
          onStockClick={onStockClick}
          panelMinHeight={sectorPanelMinHeight}
          markets={sectorMarkets}
          lookbackDays={sectorLookbackDays}
          sortState={sectorSortState}
          onMarketsChange={onSectorMarketsChange}
          onLookbackDaysChange={onSectorLookbackDaysChange}
          onSortChange={onSectorSortChange}
        />
      ) : null}
    </div>
  );
}

function IndexChart({
  code,
  indexInfo,
  onStockClick,
  panelMinHeight,
  sectorMarkets,
  sectorLookbackDays,
  sectorSortState,
  onSectorMarketsChange,
  onSectorLookbackDaysChange,
  onSectorSortChange,
}: IndexChartProps) {
  const { data, isLoading, error } = useIndexData(code);
  const showViSubChart = code === NIKKEI_PARENT_INDEX_CODE;
  const {
    data: viData,
    isLoading: viIsLoading,
    error: viError,
  } = useIndexData(showViSubChart ? NIKKEI_VI_INDEX_CODE : null);

  const isSectorIndex = indexInfo?.category === 'sector33' || indexInfo?.category === 'sector17';
  const isSyntheticIndex = indexInfo?.category === 'synthetic';
  const sectorType = indexInfo?.category as 'sector33' | 'sector17' | undefined;
  const { workspaceStyle, chartPanelStyle, surfaceStyle, chartBodyHeight, sectorPanelMinHeight } =
    resolveIndexChartLayout(panelMinHeight, isSectorIndex);

  const chartData = useMemo(() => {
    if (!data?.data) return [];
    return data.data.map((point) => ({
      time: point.date,
      open: point.open,
      high: point.high,
      low: point.low,
      close: point.close,
    }));
  }, [data]);

  const lineData = useMemo(() => {
    return toLinePriceData(data);
  }, [data]);

  const viLineData = useMemo(() => toLinePriceData(viData), [viData]);

  const sectorName = useMemo(() => {
    if (!data?.name || !sectorType) return null;
    return sectorType === 'sector17' ? data.name.replace(/^TOPIX-17 /, '') : data.name;
  }, [data?.name, sectorType]);

  if (!code) {
    return renderIndexChartState('empty', surfaceStyle);
  }

  if (isLoading) {
    return renderIndexChartState('loading', surfaceStyle);
  }

  if (error) {
    return renderIndexChartState('error', surfaceStyle, error.message);
  }

  if (!data) {
    return null;
  }

  return (
    <LoadedIndexChart
      code={code}
      data={data}
      indexInfo={indexInfo}
      onStockClick={onStockClick}
      chartData={chartData}
      lineData={lineData}
      showViSubChart={showViSubChart}
      viLineData={viLineData}
      viIsLoading={viIsLoading}
      viErrorMessage={viError?.message}
      workspaceStyle={workspaceStyle}
      chartPanelStyle={chartPanelStyle}
      chartBodyHeight={chartBodyHeight}
      sectorPanelMinHeight={sectorPanelMinHeight}
      sectorType={sectorType}
      sectorName={sectorName}
      isSectorIndex={isSectorIndex}
      isSyntheticIndex={isSyntheticIndex}
      sectorMarkets={sectorMarkets}
      sectorLookbackDays={sectorLookbackDays}
      sectorSortState={sectorSortState}
      onSectorMarketsChange={onSectorMarketsChange}
      onSectorLookbackDaysChange={onSectorLookbackDaysChange}
      onSectorSortChange={onSectorSortChange}
    />
  );
}

export function IndicesPage() {
  const navigate = useNavigate();
  const {
    selectedIndexCode,
    setSelectedIndexCode,
    sectorMarkets,
    setSectorMarkets,
    sectorLookbackDays,
    setSectorLookbackDays,
    sectorSortBy,
    sectorOrder,
    setSectorSortState,
  } = useIndicesRouteState();
  const sectorSortState = useMemo<RankingTableSortState>(
    () => ({ field: sectorSortBy, order: sectorOrder }),
    [sectorOrder, sectorSortBy]
  );
  const handleSectorSortChange = useCallback(
    (sortState: RankingTableSortState) => {
      setSectorSortState(sortState.field, sortState.order);
    },
    [setSectorSortState]
  );
  const { data: indicesData, isLoading: indicesLoading, error: indicesError } = useIndicesList();
  const listContainerRef = useRef<HTMLDivElement>(null);
  const [sidebarRef, sidebarHeight] = useObservedElementHeight<HTMLDivElement>();

  const handleStockClick = useCallback(
    (code: string) => {
      void navigate({ to: '/symbol-workbench', search: { symbol: code } });
    },
    [navigate]
  );

  const selectedIndexInfo = useMemo(() => {
    if (!selectedIndexCode || !indicesData?.indices) return undefined;
    return indicesData.indices.find((index) => index.code === selectedIndexCode);
  }, [indicesData?.indices, selectedIndexCode]);

  const flatIndices = useMemo(() => {
    if (!indicesData?.indices) return [];
    return getFlatIndicesList(indicesData.indices);
  }, [indicesData?.indices]);

  const scrollToSelected = useCallback((code: string) => {
    const container = listContainerRef.current;
    if (!container) return;

    const button = container.querySelector(`[data-index-code="${code}"]`);
    if (button) {
      button.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
  }, []);

  const getNextIndex = useCallback((direction: 'up' | 'down', currentIndex: number, length: number): number => {
    if (direction === 'down') {
      return currentIndex < length - 1 ? currentIndex + 1 : 0;
    }
    return currentIndex > 0 ? currentIndex - 1 : length - 1;
  }, []);

  const handleKeyDown = useCallback(
    (event: KeyboardEvent) => {
      if (flatIndices.length === 0) return;
      if (event.key !== 'ArrowUp' && event.key !== 'ArrowDown') return;

      event.preventDefault();

      const currentIndex = selectedIndexCode ? flatIndices.findIndex((item) => item.code === selectedIndexCode) : -1;
      const direction = event.key === 'ArrowDown' ? 'down' : 'up';
      const nextIndex = getNextIndex(direction, currentIndex, flatIndices.length);
      const nextItem = flatIndices[nextIndex];

      if (nextItem) {
        setSelectedIndexCode(nextItem.code);
        scrollToSelected(nextItem.code);
      }
    },
    [flatIndices, getNextIndex, scrollToSelected, selectedIndexCode, setSelectedIndexCode]
  );

  useEffect(() => {
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [handleKeyDown]);

  const introMetaItems = selectedIndexInfo
    ? [{ label: 'Selected', value: selectedIndexInfo.code }]
    : [{ label: 'Selected', value: 'Choose an index' }];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-4 lg:overflow-hidden">
      <Surface className="px-4 py-3">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div className="space-y-2">
            <SectionEyebrow>Analytics Workspace</SectionEyebrow>
            <div className="space-y-1">
              <h1 className="text-2xl font-semibold tracking-tight text-foreground">Indices</h1>
              <p className="max-w-2xl text-sm text-muted-foreground">
                Benchmarks, TOPIX families, sector baskets, and style indices from the local market snapshot.
              </p>
            </div>
          </div>
          <CompactMetaStrip items={introMetaItems} />
        </div>
      </Surface>

      <SplitLayout className="min-h-0 flex-1 flex-col gap-3 lg:flex-row lg:items-stretch">
        <SplitSidebar className="w-full lg:flex lg:h-full lg:w-[18rem] lg:overflow-hidden xl:w-[19rem] 2xl:w-[20rem]">
          <div ref={sidebarRef} className="flex h-full min-h-0 flex-col">
            <Surface className="flex h-full min-h-[24rem] flex-col overflow-hidden p-3">
              <div className="space-y-1 border-b border-border/70 pb-3">
                <SectionEyebrow>Workspace</SectionEyebrow>
                <h2 className="text-sm font-semibold text-foreground">Index List</h2>
                <p className="text-xs text-muted-foreground">
                  Keyboard arrows move through the current category order and keep the selected index in view.
                </p>
              </div>

              <div className="flex min-h-0 flex-1 flex-col pt-3">
                <IndicesList
                  indices={indicesData?.indices || []}
                  selectedCode={selectedIndexCode}
                  onSelect={setSelectedIndexCode}
                  isLoading={indicesLoading}
                  containerRef={listContainerRef}
                />
              </div>

              {indicesError ? (
                <div className="mt-3 rounded-xl border border-red-500/20 bg-red-500/8 px-3 py-2 text-sm text-destructive">
                  Failed to load indices: {indicesError.message}
                </div>
              ) : null}
            </Surface>
          </div>
        </SplitSidebar>

        <SplitMain className="gap-3 lg:overflow-y-auto lg:pr-1">
          <IndexChart
            code={selectedIndexCode}
            indexInfo={selectedIndexInfo}
            onStockClick={handleStockClick}
            panelMinHeight={sidebarHeight}
            sectorMarkets={sectorMarkets}
            sectorLookbackDays={sectorLookbackDays}
            sectorSortState={sectorSortState}
            onSectorMarketsChange={setSectorMarkets}
            onSectorLookbackDaysChange={setSectorLookbackDays}
            onSectorSortChange={handleSectorSortChange}
          />
        </SplitMain>
      </SplitLayout>
    </div>
  );
}
