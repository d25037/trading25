import { CompactMetric, SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import type {
  SortOrder,
  Topix100PriceBucketFilter,
  Topix100PriceSmaWindow,
  Topix100RankingItem,
  Topix100RankingSortKey,
  Topix100RankingMetric,
  Topix100RankingResponse,
  Topix100StreakModeFilter,
  Topix100StudyMode,
  Topix100VolumeBucketFilter,
} from '@/types/ranking';
import { formatPriceJPY, formatRate, formatVolume, formatVolumeRatio } from '@/utils/formatters';
import {
  getTopix100RankingMetricLabel,
  getTopix100StreakModeLabel,
} from './topix100RankingMetric';

interface Topix100RankingTableProps {
  data: Topix100RankingResponse | undefined;
  isLoading: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
  studyMode: Topix100StudyMode;
  rankingMetric: Topix100RankingMetric;
  rankingSmaWindow: Topix100PriceSmaWindow;
  priceBucketFilter: Topix100PriceBucketFilter;
  volumeBucketFilter: Topix100VolumeBucketFilter;
  shortModeFilter: Topix100StreakModeFilter;
  longModeFilter: Topix100StreakModeFilter;
  sortBy: Topix100RankingSortKey;
  sortOrder: SortOrder;
  onSortChange: (sortBy: Topix100RankingSortKey, sortOrder: SortOrder) => void;
}

interface IntradaySnapshotSummary {
  topK: number;
  longReturnMean: number;
  shortEdgeMean: number;
  grossEdge: number;
  pairReturn: number;
  realizedDateLabel: string;
}

interface SwingSnapshotSummary {
  topK: number;
  selectedReturnMean: number;
  excessVsTopix: number | null;
  excessVsUniverse: number | null;
  entryDateLabel: string;
  exitDateLabel: string;
}

interface SnapshotBookRole {
  side: 'long' | 'short';
  rank: number;
}

interface RuntimeMetaSummary {
  studyMode: Topix100StudyMode;
  metricLabel: string;
  studyReadItems: string[];
  runtimeLabel: string;
  windowLabel: string;
}

const SNAPSHOT_SUMMARY_TOP_K_VALUES = [1, 3, 5] as const;
const SNAPSHOT_BOOK_TOP_K = 3;

function matchesFilters(
  item: Topix100RankingItem,
  priceBucketFilter: Topix100PriceBucketFilter,
  volumeBucketFilter: Topix100VolumeBucketFilter,
  shortModeFilter: Topix100StreakModeFilter,
  longModeFilter: Topix100StreakModeFilter
): boolean {
  if (priceBucketFilter !== 'all' && item.priceBucket !== priceBucketFilter) {
    return false;
  }
  if (volumeBucketFilter !== 'all' && item.volumeBucket !== volumeBucketFilter) {
    return false;
  }
  if (shortModeFilter !== 'all' && item.streakShortMode !== shortModeFilter) {
    return false;
  }
  if (longModeFilter !== 'all' && item.streakLongMode !== longModeFilter) {
    return false;
  }
  return true;
}

function streakModeToneClass(mode: Topix100RankingItem['streakShortMode']): string {
  if (mode === 'bullish') {
    return 'bg-emerald-500/12 text-emerald-700 dark:text-emerald-300';
  }
  if (mode === 'bearish') {
    return 'bg-rose-500/12 text-rose-700 dark:text-rose-300';
  }
  return 'bg-muted text-muted-foreground';
}

function getStudyReadItems(metric: Topix100RankingMetric, studyMode: Topix100StudyMode): string[] {
  if (studyMode === 'swing_5d') {
    if (metric === 'price_vs_sma_gap') {
      return ['KPI = vs TOPIX', 'Check = vs TOPIX100 EW', 'Entry = X+1 open', 'Exit = X+5 close'];
    }
    return ['Legacy metric', 'KPI = vs TOPIX', 'Check = vs TOPIX100 EW'];
  }

  if (metric === 'price_vs_sma_gap') {
    return ['Q10 = below SMA', 'Q2-4 = trough', 'Decile-only score', 'Volume/state = context'];
  }

  return ['Legacy comparison', 'Decile-only intraday score'];
}

function formatScore(value: number | null | undefined): string {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return '-';
  }
  return formatRate(value);
}

function isFiniteNumber(value: number | null | undefined): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function resolveStudyScore(item: Topix100RankingItem, studyMode: Topix100StudyMode): number | null | undefined {
  return studyMode === 'swing_5d' ? item.longScore5d : item.intradayScore;
}

function resolveStudyReturn(item: Topix100RankingItem, studyMode: Topix100StudyMode): number | null | undefined {
  return studyMode === 'swing_5d' ? item.openToClose5dReturn : item.nextSessionIntradayReturn;
}

function compareByScoreDesc(
  left: Topix100RankingItem,
  right: Topix100RankingItem,
  studyMode: Topix100StudyMode
): number {
  const leftScore = resolveStudyScore(left, studyMode);
  const rightScore = resolveStudyScore(right, studyMode);
  if (!isFiniteNumber(leftScore) && !isFiniteNumber(rightScore)) return left.code.localeCompare(right.code);
  if (!isFiniteNumber(leftScore)) return 1;
  if (!isFiniteNumber(rightScore)) return -1;
  if (leftScore !== rightScore) return rightScore - leftScore;
  return left.code.localeCompare(right.code);
}

function compareByScoreAsc(
  left: Topix100RankingItem,
  right: Topix100RankingItem,
  studyMode: Topix100StudyMode
): number {
  const leftScore = resolveStudyScore(left, studyMode);
  const rightScore = resolveStudyScore(right, studyMode);
  if (!isFiniteNumber(leftScore) && !isFiniteNumber(rightScore)) return left.code.localeCompare(right.code);
  if (!isFiniteNumber(leftScore)) return 1;
  if (!isFiniteNumber(rightScore)) return -1;
  if (leftScore !== rightScore) return leftScore - rightScore;
  return left.code.localeCompare(right.code);
}

function summarizeSingleDateLabel(values: Array<string | null | undefined>): string {
  const dates = values.map((value) => value?.trim()).filter((value): value is string => Boolean(value));
  if (dates.length === 0) {
    return '-';
  }
  const uniqueDates = [...new Set(dates)];
  return uniqueDates.length === 1 ? (uniqueDates[0] ?? '-') : 'Mixed dates';
}

function getEligibleIntradayItems(items: Topix100RankingItem[]): Topix100RankingItem[] {
  return items.filter(
    (item) => isFiniteNumber(item.intradayScore) && isFiniteNumber(item.nextSessionIntradayReturn)
  );
}

function getEligibleSwingItems(items: Topix100RankingItem[]): Topix100RankingItem[] {
  return items.filter((item) => isFiniteNumber(item.longScore5d) && isFiniteNumber(item.openToClose5dReturn));
}

function buildIntradayPortfolioSummaries(items: Topix100RankingItem[]): IntradaySnapshotSummary[] {
  const eligibleItems = getEligibleIntradayItems(items);
  if (eligibleItems.length === 0) {
    return [];
  }

  const longSorted = [...eligibleItems].sort((left, right) => compareByScoreDesc(left, right, 'intraday'));
  const shortSorted = [...eligibleItems].sort((left, right) => compareByScoreAsc(left, right, 'intraday'));

  return SNAPSHOT_SUMMARY_TOP_K_VALUES.flatMap((topK) => {
    if (eligibleItems.length < topK * 2) {
      return [];
    }

    const longItems = longSorted.slice(0, topK);
    const shortItems = shortSorted.slice(0, topK);
    const distinctCodes = new Set([...longItems, ...shortItems].map((item) => item.code));
    if (distinctCodes.size < topK * 2) {
      return [];
    }

    const longReturnMean =
      longItems.reduce((sum, item) => sum + (item.nextSessionIntradayReturn ?? 0), 0) / topK;
    const shortEdgeMean =
      shortItems.reduce((sum, item) => sum - (item.nextSessionIntradayReturn ?? 0), 0) / topK;
    const grossEdge = longReturnMean + shortEdgeMean;

    return [
      {
        topK,
        longReturnMean,
        shortEdgeMean,
        grossEdge,
        pairReturn: grossEdge / 2,
        realizedDateLabel: summarizeSingleDateLabel([...longItems, ...shortItems].map((item) => item.nextSessionDate)),
      },
    ];
  });
}

function buildSwingPortfolioSummaries(data: Topix100RankingResponse | undefined): SwingSnapshotSummary[] {
  const eligibleItems = getEligibleSwingItems(data?.items ?? []);
  if (eligibleItems.length === 0) {
    return [];
  }

  const benchmarkTopix = isFiniteNumber(data?.primaryBenchmarkReturn) ? data.primaryBenchmarkReturn : null;
  const benchmarkUniverse = isFiniteNumber(data?.secondaryBenchmarkReturn) ? data.secondaryBenchmarkReturn : null;
  const sorted = [...eligibleItems].sort((left, right) => compareByScoreDesc(left, right, 'swing_5d'));

  return SNAPSHOT_SUMMARY_TOP_K_VALUES.flatMap((topK) => {
    if (eligibleItems.length < topK) {
      return [];
    }

    const selectedItems = sorted.slice(0, topK);
    const selectedReturnMean =
      selectedItems.reduce((sum, item) => sum + (item.openToClose5dReturn ?? 0), 0) / topK;

    return [
      {
        topK,
        selectedReturnMean,
        excessVsTopix: benchmarkTopix !== null ? selectedReturnMean - benchmarkTopix : null,
        excessVsUniverse: benchmarkUniverse !== null ? selectedReturnMean - benchmarkUniverse : null,
        entryDateLabel: summarizeSingleDateLabel(selectedItems.map((item) => item.swingEntryDate)),
        exitDateLabel: summarizeSingleDateLabel(selectedItems.map((item) => item.swingExitDate)),
      },
    ];
  });
}

function buildSnapshotBookRoleMap(items: Topix100RankingItem[], topK: number): Map<string, SnapshotBookRole> {
  const eligibleItems = getEligibleIntradayItems(items);
  if (eligibleItems.length < topK * 2) {
    return new Map();
  }

  const longItems = [...eligibleItems].sort((left, right) => compareByScoreDesc(left, right, 'intraday')).slice(0, topK);
  const shortItems = [...eligibleItems].sort((left, right) => compareByScoreAsc(left, right, 'intraday')).slice(0, topK);
  const distinctCodes = new Set([...longItems, ...shortItems].map((item) => item.code));
  if (distinctCodes.size < topK * 2) {
    return new Map();
  }

  const roleMap = new Map<string, SnapshotBookRole>();
  longItems.forEach((item, index) => {
    roleMap.set(item.code, { side: 'long', rank: index + 1 });
  });
  shortItems.forEach((item, index) => {
    roleMap.set(item.code, { side: 'short', rank: index + 1 });
  });
  return roleMap;
}

function formatSnapshotBookRole(role: SnapshotBookRole | undefined): string {
  if (!role) {
    return '-';
  }
  return `${role.side === 'long' ? 'L' : 'S'}${role.rank}`;
}

function resolveSnapshotBookEdge(item: Topix100RankingItem, role: SnapshotBookRole | undefined): number | null {
  if (!role || !isFiniteNumber(item.nextSessionIntradayReturn)) {
    return null;
  }
  return role.side === 'long' ? item.nextSessionIntradayReturn : -item.nextSessionIntradayReturn;
}

function buildRuntimeWindowLabel(
  data: Topix100RankingResponse | undefined,
  isDailyRefit: boolean,
  scoreTrainWindowDays: number
): string {
  if (isDailyRefit) {
    if (data?.scoreSplitTrainStart && data?.scoreSplitTrainEnd) {
      return `Train = ${data.scoreSplitTrainStart} -> ${data.scoreSplitTrainEnd}`;
    }
    return `Train = trailing ${scoreTrainWindowDays} signal days`;
  }

  if (
    data?.scoreSplitTrainStart &&
    data?.scoreSplitTrainEnd &&
    data?.scoreSplitTestStart &&
    data?.scoreSplitTestEnd
  ) {
    return `Split = train ${data.scoreSplitTrainStart} -> ${data.scoreSplitTrainEnd} | test ${data.scoreSplitTestStart} -> ${data.scoreSplitTestEnd}${data.scoreSplitPartialTail ? ' (partial tail)' : ''}`;
  }

  return 'Split = unavailable';
}

function buildRuntimeScoreLabel(
  studyMode: Topix100StudyMode,
  isDailyRefit: boolean,
  scoreTrainWindowDays: number,
  scoreTestWindowDays: number,
  scoreStepDays: number
): string {
  if (isDailyRefit) {
    if (studyMode === 'swing_5d') {
      return `Score = daily fresh-fit LightGBM (trailing ${scoreTrainWindowDays} signal days)`;
    }
    return `Score = daily fresh-fit LightGBM (trailing ${scoreTrainWindowDays} signal days, cadence 1)`;
  }
  return `Score = walk-forward frozen LightGBM (train ${scoreTrainWindowDays} / test ${scoreTestWindowDays} / step ${scoreStepDays})`;
}

function buildRuntimeMetaSummary(
  data: Topix100RankingResponse | undefined,
  rankingMetric: Topix100RankingMetric,
  rankingSmaWindow: 20 | 50 | 100,
  studyMode: Topix100StudyMode
): RuntimeMetaSummary {
  const effectiveStudyMode = data?.studyMode ?? studyMode;
  const effectiveMetric = data?.rankingMetric ?? rankingMetric;
  const effectiveSmaWindow = data?.smaWindow ?? rankingSmaWindow;
  const scoreTrainWindowDays = data?.scoreTrainWindowDays ?? 756;
  const scoreTestWindowDays = data?.scoreTestWindowDays ?? 126;
  const scoreStepDays = data?.scoreStepDays ?? 126;
  const isDailyRefit = data?.scoreModelType === 'daily_refit';

  return {
    studyMode: effectiveStudyMode,
    metricLabel: getTopix100RankingMetricLabel(effectiveMetric, effectiveSmaWindow),
    studyReadItems: getStudyReadItems(effectiveMetric, effectiveStudyMode),
    runtimeLabel: buildRuntimeScoreLabel(
      effectiveStudyMode,
      isDailyRefit,
      scoreTrainWindowDays,
      scoreTestWindowDays,
      scoreStepDays
    ),
    windowLabel: buildRuntimeWindowLabel(data, isDailyRefit, scoreTrainWindowDays),
  };
}

function Topix100ResultsHeader({
  itemCount,
  data,
  runtimeMeta,
}: {
  itemCount: number;
  data: Topix100RankingResponse | undefined;
  runtimeMeta: RuntimeMetaSummary;
}) {
  const realizedLabel =
    runtimeMeta.studyMode === 'swing_5d'
      ? 'Realized = next available open -> 5th close when present'
      : 'Realized = next available open -> close when present';

  return (
    <div className="space-y-1 border-b border-border/70 px-4 py-2">
      <SectionEyebrow>Results</SectionEyebrow>
      <div className="flex flex-wrap items-center justify-between gap-x-3 gap-y-1">
        <h2 className="text-base font-semibold text-foreground">
          TOPIX100 SMA Divergence
          {itemCount > 0 ? (
            <span className="ml-2 text-sm font-normal text-muted-foreground">({itemCount})</span>
          ) : null}
        </h2>
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-muted-foreground">
          <span>{runtimeMeta.metricLabel}</span>
          {runtimeMeta.studyReadItems.map((item) => (
            <span key={item}>{item}</span>
          ))}
          <span>
            State X = {data?.shortWindowStreaks ?? 3}/{data?.longWindowStreaks ?? 53}
          </span>
          <span>{runtimeMeta.runtimeLabel}</span>
          <span>{runtimeMeta.windowLabel}</span>
          <span>{realizedLabel}</span>
          <span>{data?.date ?? '-'}</span>
        </div>
      </div>
    </div>
  );
}

function Topix100IntradaySnapshotBooksSection({
  snapshotSummaries,
}: {
  snapshotSummaries: IntradaySnapshotSummary[];
}) {
  if (snapshotSummaries.length === 0) {
    return null;
  }

  return (
    <div className="border-b border-border/70 px-4 py-3">
      <div className="flex items-center justify-between gap-3">
        <div className="space-y-1">
          <SectionEyebrow>Snapshot Books</SectionEyebrow>
          <p className="text-xs text-muted-foreground">
            Full-snapshot Top/Bottom books reconstructed from current scores and next-session realized returns.
          </p>
        </div>
        <p className="text-[11px] text-muted-foreground">Research comparison uses `Pair 50/50`.</p>
      </div>
      <div className="mt-3 grid gap-3 md:grid-cols-3">
        {snapshotSummaries.map((summary) => (
          <CompactMetric
            key={summary.topK}
            label={`Top ${summary.topK} / Bottom ${summary.topK}`}
            value={formatScore(summary.pairReturn)}
            detail={`long ${formatScore(summary.longReturnMean)} | short edge ${formatScore(summary.shortEdgeMean)} | gross ${formatScore(summary.grossEdge)} | ${summary.realizedDateLabel}`}
            tone={summary.pairReturn > 0 ? 'success' : summary.pairReturn < 0 ? 'danger' : 'neutral'}
            className="p-3"
          />
        ))}
      </div>
    </div>
  );
}

function Topix100SwingSummarySection({ summaries }: { summaries: SwingSnapshotSummary[] }) {
  if (summaries.length === 0) {
    return null;
  }

  return (
    <div className="border-b border-border/70 px-4 py-3">
      <div className="space-y-1">
        <SectionEyebrow>Swing Summary</SectionEyebrow>
        <p className="text-xs text-muted-foreground">
          Top-K long books scored on the X-date snapshot, entered on X+1 open, and closed on X+5 close.
        </p>
      </div>
      <div className="mt-3 grid gap-3 md:grid-cols-3">
        {summaries.map((summary) => {
          const primaryValue = summary.excessVsTopix ?? summary.selectedReturnMean;
          return (
            <CompactMetric
              key={summary.topK}
              label={`Top ${summary.topK}`}
              value={formatScore(primaryValue)}
              detail={`raw ${formatScore(summary.selectedReturnMean)} | vs TOPIX100 EW ${formatScore(summary.excessVsUniverse)} | ${summary.entryDateLabel} -> ${summary.exitDateLabel}`}
              tone={primaryValue > 0 ? 'success' : primaryValue < 0 ? 'danger' : 'neutral'}
              className="p-3"
            />
          );
        })}
      </div>
    </div>
  );
}

function resolveScoreLabel(studyMode: Topix100StudyMode): string {
  return studyMode === 'swing_5d' ? '5D Score' : 'ID Score';
}

function resolveReturnLabel(studyMode: Topix100StudyMode): string {
  return studyMode === 'swing_5d' ? '5D Ret' : 'Next Ret';
}

function Topix100RankingDataTable({
  items,
  studyMode,
  effectiveMetric,
  metricLabel,
  sortBy,
  sortOrder,
  onSortChange,
  onStockClick,
  snapshotBookRoleMap,
  showSnapshotBookColumns,
}: {
  items: Topix100RankingItem[];
  studyMode: Topix100StudyMode;
  effectiveMetric: Topix100RankingMetric;
  metricLabel: string;
  sortBy: Topix100RankingSortKey;
  sortOrder: SortOrder;
  onSortChange: (sortBy: Topix100RankingSortKey, sortOrder: SortOrder) => void;
  onStockClick: (code: string) => void;
  snapshotBookRoleMap: Map<string, SnapshotBookRole>;
  showSnapshotBookColumns: boolean;
}) {
  const scoreSortField: Topix100RankingSortKey = studyMode === 'swing_5d' ? 'longScore5d' : 'intradayScore';
  const returnSortField: Topix100RankingSortKey =
    studyMode === 'swing_5d' ? 'openToClose5dReturn' : 'nextSessionIntradayReturn';

  return (
    <table className="w-full text-xs">
      <thead className="sticky top-0 z-10 border-b bg-[var(--app-surface-muted)]">
        <tr>
          <th className="w-12 px-2 py-1.5 text-center text-muted-foreground">#</th>
          <SortableHeader
            label="Code"
            sortField="code"
            activeSortBy={sortBy}
            activeSortOrder={sortOrder}
            onSortChange={onSortChange}
            className="w-16 px-2 py-1.5 text-left"
          />
          <SortableHeader
            label="Company"
            sortField="companyName"
            activeSortBy={sortBy}
            activeSortOrder={sortOrder}
            onSortChange={onSortChange}
            className="px-2 py-1.5 text-left"
          />
          <SortableHeader
            label={metricLabel}
            sortField="metric"
            activeSortBy={sortBy}
            activeSortOrder={sortOrder}
            onSortChange={onSortChange}
            className="w-28 px-2 py-1.5 text-right"
            buttonClassName="justify-end"
          />
          <SortableHeader
            label="Vol Split"
            sortField="volumeBucket"
            activeSortBy={sortBy}
            activeSortOrder={sortOrder}
            onSortChange={onSortChange}
            className="w-20 px-2 py-1.5 text-left"
          />
          <SortableHeader
            label="Short"
            sortField="streakShortMode"
            activeSortBy={sortBy}
            activeSortOrder={sortOrder}
            onSortChange={onSortChange}
            className="w-20 px-2 py-1.5 text-left"
          />
          <SortableHeader
            label="Long"
            sortField="streakLongMode"
            activeSortBy={sortBy}
            activeSortOrder={sortOrder}
            onSortChange={onSortChange}
            className="w-20 px-2 py-1.5 text-left"
          />
          <SortableHeader
            label={resolveScoreLabel(studyMode)}
            sortField={scoreSortField}
            activeSortBy={sortBy}
            activeSortOrder={sortOrder}
            onSortChange={onSortChange}
            className="w-24 px-2 py-1.5 text-right"
            buttonClassName="justify-end"
          />
          {showSnapshotBookColumns ? (
            <>
              <th className="w-16 px-2 py-1.5 text-left text-muted-foreground">Book3</th>
              <th className="w-24 px-2 py-1.5 text-right text-muted-foreground">Edge3</th>
            </>
          ) : null}
          <SortableHeader
            label={resolveReturnLabel(studyMode)}
            sortField={returnSortField}
            activeSortBy={sortBy}
            activeSortOrder={sortOrder}
            onSortChange={onSortChange}
            className="w-28 px-2 py-1.5 text-right"
            buttonClassName="justify-end"
          />
          <SortableHeader
            label="Volume SMA 5/20"
            sortField="volumeSma5_20"
            activeSortBy={sortBy}
            activeSortOrder={sortOrder}
            onSortChange={onSortChange}
            className="w-28 px-2 py-1.5 text-right"
            buttonClassName="justify-end"
          />
          <SortableHeader
            label="Price"
            sortField="currentPrice"
            activeSortBy={sortBy}
            activeSortOrder={sortOrder}
            onSortChange={onSortChange}
            className="w-24 px-2 py-1.5 text-right"
            buttonClassName="justify-end"
          />
          <SortableHeader
            label="Sector"
            sortField="sector33Name"
            activeSortBy={sortBy}
            activeSortOrder={sortOrder}
            onSortChange={onSortChange}
            className="w-24 px-2 py-1.5 text-left"
          />
          <SortableHeader
            label="Volume"
            sortField="volume"
            activeSortBy={sortBy}
            activeSortOrder={sortOrder}
            onSortChange={onSortChange}
            className="w-24 px-2 py-1.5 text-right"
            buttonClassName="justify-end"
          />
        </tr>
      </thead>
      <tbody>
        {items.map((item, index) => (
          <tr
            key={item.code}
            className="cursor-pointer border-b border-border/30 transition-colors hover:bg-[var(--app-surface-muted)]"
            onClick={() => onStockClick(item.code)}
          >
            <td className="px-2 py-1.5 text-center font-medium tabular-nums">{index + 1}</td>
            <td className="px-2 py-1.5 font-medium">{item.code}</td>
            <td className="max-w-[220px] truncate px-2 py-1.5">{item.companyName}</td>
            <td className="px-2 py-1.5 text-right tabular-nums">
              {effectiveMetric === 'price_sma_20_80'
                ? formatVolumeRatio(item.priceSma20_80)
                : formatRate(item.priceVsSmaGap)}
            </td>
            <td className="px-2 py-1.5 text-muted-foreground">{item.volumeBucket ?? '-'}</td>
            <td className="px-2 py-1.5">
              <span
                className={`inline-flex rounded-full px-2 py-1 text-[11px] font-medium ${streakModeToneClass(item.streakShortMode)}`}
              >
                {item.streakShortMode ? getTopix100StreakModeLabel(item.streakShortMode) : '-'}
              </span>
            </td>
            <td className="px-2 py-1.5">
              <span
                className={`inline-flex rounded-full px-2 py-1 text-[11px] font-medium ${streakModeToneClass(item.streakLongMode)}`}
              >
                {item.streakLongMode ? getTopix100StreakModeLabel(item.streakLongMode) : '-'}
              </span>
            </td>
            <td className="px-2 py-1.5 text-right tabular-nums">{formatScore(resolveStudyScore(item, studyMode))}</td>
            {showSnapshotBookColumns ? (
              <>
                <td className="px-2 py-1.5 font-medium tabular-nums">
                  {formatSnapshotBookRole(snapshotBookRoleMap.get(item.code))}
                </td>
                <td className="px-2 py-1.5 text-right tabular-nums">
                  {formatScore(resolveSnapshotBookEdge(item, snapshotBookRoleMap.get(item.code)))}
                </td>
              </>
            ) : null}
            <td className="px-2 py-1.5 text-right tabular-nums">
              <div>{formatScore(resolveStudyReturn(item, studyMode))}</div>
              <div className="text-[10px] text-muted-foreground">
                {studyMode === 'swing_5d'
                  ? `${item.swingEntryDate ?? '-'} -> ${item.swingExitDate ?? '-'}`
                  : item.nextSessionDate ?? '-'}
              </div>
            </td>
            <td className="px-2 py-1.5 text-right tabular-nums">{formatVolumeRatio(item.volumeSma5_20)}</td>
            <td className="px-2 py-1.5 text-right tabular-nums">{formatPriceJPY(item.currentPrice)}</td>
            <td className="max-w-[120px] truncate px-2 py-1.5 text-muted-foreground">{item.sector33Name}</td>
            <td className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">
              {formatVolume(item.volume)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function getDefaultSortOrder(sortBy: Topix100RankingSortKey): SortOrder {
  switch (sortBy) {
    case 'code':
    case 'companyName':
    case 'volumeBucket':
    case 'streakShortMode':
    case 'streakLongMode':
    case 'longScore5dRank':
    case 'intradayLongRank':
    case 'intradayShortRank':
    case 'sector33Name':
      return 'asc';
    default:
      return 'desc';
  }
}

function resolveNextSortOrder(
  currentSortBy: Topix100RankingSortKey,
  currentSortOrder: SortOrder,
  nextSortBy: Topix100RankingSortKey
): SortOrder {
  if (currentSortBy === nextSortBy) {
    return currentSortOrder === 'asc' ? 'desc' : 'asc';
  }
  return getDefaultSortOrder(nextSortBy);
}

function compareNullableNumbers(
  left: number | null | undefined,
  right: number | null | undefined,
  sortOrder: SortOrder
): number {
  const leftMissing = typeof left !== 'number' || !Number.isFinite(left);
  const rightMissing = typeof right !== 'number' || !Number.isFinite(right);
  if (leftMissing && rightMissing) return 0;
  if (leftMissing) return 1;
  if (rightMissing) return -1;
  return sortOrder === 'asc' ? left - right : right - left;
}

function compareNullableStrings(
  left: string | null | undefined,
  right: string | null | undefined,
  sortOrder: SortOrder
): number {
  const leftValue = typeof left === 'string' ? left.trim() : '';
  const rightValue = typeof right === 'string' ? right.trim() : '';
  const leftMissing = leftValue.length === 0;
  const rightMissing = rightValue.length === 0;
  if (leftMissing && rightMissing) return 0;
  if (leftMissing) return 1;
  if (rightMissing) return -1;
  return sortOrder === 'asc' ? leftValue.localeCompare(rightValue) : rightValue.localeCompare(leftValue);
}

function compareItems(
  left: Topix100RankingItem,
  right: Topix100RankingItem,
  rankingMetric: Topix100RankingMetric,
  sortBy: Topix100RankingSortKey,
  sortOrder: SortOrder
): number {
  switch (sortBy) {
    case 'rank':
      return compareNullableNumbers(left.rank, right.rank, sortOrder);
    case 'code':
      return compareNullableStrings(left.code, right.code, sortOrder);
    case 'companyName':
      return compareNullableStrings(left.companyName, right.companyName, sortOrder);
    case 'metric':
      return compareNullableNumbers(
        rankingMetric === 'price_sma_20_80' ? left.priceSma20_80 : left.priceVsSmaGap,
        rankingMetric === 'price_sma_20_80' ? right.priceSma20_80 : right.priceVsSmaGap,
        sortOrder
      );
    case 'volumeBucket':
      return compareNullableStrings(left.volumeBucket, right.volumeBucket, sortOrder);
    case 'streakShortMode':
      return compareNullableStrings(left.streakShortMode, right.streakShortMode, sortOrder);
    case 'streakLongMode':
      return compareNullableStrings(left.streakLongMode, right.streakLongMode, sortOrder);
    case 'longScore5d':
      return compareNullableNumbers(left.longScore5d, right.longScore5d, sortOrder);
    case 'longScore5dRank':
      return compareNullableNumbers(left.longScore5dRank, right.longScore5dRank, sortOrder);
    case 'intradayScore':
      return compareNullableNumbers(left.intradayScore, right.intradayScore, sortOrder);
    case 'intradayLongRank':
      return compareNullableNumbers(left.intradayLongRank, right.intradayLongRank, sortOrder);
    case 'intradayShortRank':
      return compareNullableNumbers(left.intradayShortRank, right.intradayShortRank, sortOrder);
    case 'openToClose5dReturn':
      return compareNullableNumbers(left.openToClose5dReturn, right.openToClose5dReturn, sortOrder);
    case 'nextSessionIntradayReturn':
      return compareNullableNumbers(left.nextSessionIntradayReturn, right.nextSessionIntradayReturn, sortOrder);
    case 'volumeSma5_20':
      return compareNullableNumbers(left.volumeSma5_20, right.volumeSma5_20, sortOrder);
    case 'currentPrice':
      return compareNullableNumbers(left.currentPrice, right.currentPrice, sortOrder);
    case 'sector33Name':
      return compareNullableStrings(left.sector33Name, right.sector33Name, sortOrder);
    case 'volume':
      return compareNullableNumbers(left.volume, right.volume, sortOrder);
    default:
      return 0;
  }
}

function sortItems(
  items: Topix100RankingItem[],
  rankingMetric: Topix100RankingMetric,
  sortBy: Topix100RankingSortKey,
  sortOrder: SortOrder
): Topix100RankingItem[] {
  return items
    .map((item, index) => ({ item, index }))
    .sort((left, right) => {
      const comparison = compareItems(left.item, right.item, rankingMetric, sortBy, sortOrder);
      if (comparison !== 0) {
        return comparison;
      }
      return left.index - right.index;
    })
    .map(({ item }) => item);
}

function renderSortMark(active: boolean, sortOrder: SortOrder): string {
  if (!active) {
    return '↕';
  }
  return sortOrder === 'asc' ? '↑' : '↓';
}

function SortableHeader({
  label,
  sortField,
  activeSortBy,
  activeSortOrder,
  onSortChange,
  className,
  buttonClassName,
}: {
  label: string;
  sortField: Topix100RankingSortKey;
  activeSortBy: Topix100RankingSortKey;
  activeSortOrder: SortOrder;
  onSortChange: (sortBy: Topix100RankingSortKey, sortOrder: SortOrder) => void;
  className: string;
  buttonClassName?: string;
}) {
  const isActive = activeSortBy === sortField;
  const nextOrder = resolveNextSortOrder(activeSortBy, activeSortOrder, sortField);

  return (
    <th
      aria-sort={isActive ? (activeSortOrder === 'asc' ? 'ascending' : 'descending') : 'none'}
      className={className}
    >
      <button
        type="button"
        className={`inline-flex w-full items-center gap-1 text-current transition-colors hover:text-foreground ${buttonClassName ?? 'justify-start'}`}
        onClick={() => onSortChange(sortField, nextOrder)}
      >
        <span>{label}</span>
        <span aria-hidden="true" className="text-[10px] text-muted-foreground">
          {renderSortMark(isActive, activeSortOrder)}
        </span>
      </button>
    </th>
  );
}

export function Topix100RankingTable({
  data,
  isLoading,
  error,
  onStockClick,
  studyMode,
  rankingMetric,
  rankingSmaWindow,
  priceBucketFilter,
  volumeBucketFilter,
  shortModeFilter,
  longModeFilter,
  sortBy,
  sortOrder,
  onSortChange,
}: Topix100RankingTableProps) {
  const effectiveStudyMode = data?.studyMode ?? studyMode;
  const intradaySummaries =
    effectiveStudyMode === 'intraday' ? buildIntradayPortfolioSummaries(data?.items ?? []) : [];
  const swingSummaries = effectiveStudyMode === 'swing_5d' ? buildSwingPortfolioSummaries(data) : [];
  const snapshotBookRoleMap =
    effectiveStudyMode === 'intraday' ? buildSnapshotBookRoleMap(data?.items ?? [], SNAPSHOT_BOOK_TOP_K) : new Map();
  const showSnapshotBookColumns = effectiveStudyMode === 'intraday' && snapshotBookRoleMap.size > 0;
  const filteredItems = (data?.items ?? []).filter((item) =>
    matchesFilters(item, priceBucketFilter, volumeBucketFilter, shortModeFilter, longModeFilter)
  );
  const effectiveMetric = data?.rankingMetric ?? rankingMetric;
  const items = sortItems(filteredItems, effectiveMetric, sortBy, sortOrder);
  const runtimeMeta = buildRuntimeMetaSummary(data, rankingMetric, rankingSmaWindow, studyMode);

  return (
    <Surface className="flex min-h-[24rem] flex-1 flex-col overflow-hidden">
      <Topix100ResultsHeader itemCount={items.length} data={data} runtimeMeta={runtimeMeta} />
      {effectiveStudyMode === 'swing_5d' ? (
        <Topix100SwingSummarySection summaries={swingSummaries} />
      ) : (
        <Topix100IntradaySnapshotBooksSection snapshotSummaries={intradaySummaries} />
      )}

      <div className="min-h-0 flex-1 overflow-auto">
        <DataStateWrapper
          isLoading={isLoading}
          error={error}
          isEmpty={items.length === 0}
          emptyMessage="No TOPIX100 ranking data available"
          emptySubMessage="Try a different date or relax the filters."
          height="h-full min-h-[18rem]"
        >
          <Topix100RankingDataTable
            items={items}
            studyMode={effectiveStudyMode}
            effectiveMetric={effectiveMetric}
            metricLabel={runtimeMeta.metricLabel}
            sortBy={sortBy}
            sortOrder={sortOrder}
            onSortChange={onSortChange}
            onStockClick={onStockClick}
            snapshotBookRoleMap={snapshotBookRoleMap}
            showSnapshotBookColumns={showSnapshotBookColumns}
          />
        </DataStateWrapper>
      </div>
    </Surface>
  );
}
