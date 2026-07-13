import type { MarketRankingSymbolResponse } from '@trading25/contracts/types/api-response-types';
import type { StockInfoResponse } from '@/hooks/useStockInfo';
import type { ShikihoDailyOverlayProvenance } from '@/lib/shikihoDailyOverlay';
import type { ChartHeaderMarketCaps } from '@/pages/SymbolWorkbenchHeader';
import { formatMarketCap } from '@/utils/formatters';
import {
  DAILY_RANKING_VALUE_METRICS,
  type DailyRankingMetric,
  type DailyRankingMetricKey,
  DailyRankingMetricValue,
  DailyRankingRegimeChip,
  DailyRankingSignalChips,
  SectorStrengthScoreChip,
} from './dailyRankingPresentation';

interface DailyRankingSnapshotProps {
  response: MarketRankingSymbolResponse | undefined;
  isLoading: boolean;
  error: Error | null;
  onRetry: () => void;
  stockInfo: StockInfoResponse | undefined;
  latestMarketCaps: ChartHeaderMarketCaps;
  provisionalProvenance?: ShikihoDailyOverlayProvenance | null;
}

const MARKET_CODE_LABELS: Record<string, string> = {
  prime: 'Prime',
  standard: 'Standard',
  growth: 'Growth',
  '0111': 'Prime',
  '0112': 'Standard',
  '0113': 'Growth',
};

const PRIMARY_METRIC_KEYS = [
  'currentPrice',
  'changePercentage',
  'per',
  'forwardPer',
  'forecastOperatingProfitGrowthRatio',
  'pbr',
  'valueCompositeScore',
] as const satisfies readonly DailyRankingMetricKey[];

function getMetric(key: DailyRankingMetricKey): DailyRankingMetric {
  const metric = DAILY_RANKING_VALUE_METRICS.find((candidate) => candidate.key === key);
  if (!metric) throw new Error(`Unknown Daily Ranking metric: ${key}`);
  return metric;
}

const PRIMARY_METRICS = PRIMARY_METRIC_KEYS.map(getMetric);
const SECTOR_STRENGTH_METRIC = getMetric('sectorStrengthScore');
const PSR_METRIC = getMetric('psr');
const FORWARD_PSR_METRIC = getMetric('forwardPsr');
const LIQUIDITY_METRIC = getMetric('liquidityResidualZ');
const TRADING_VALUE_METRIC = getMetric('tradingValue');
const SMA5_METRIC = getMetric('sma5AboveCount5d');

function formatMarket(value: string | null | undefined, fallbackName?: string): string {
  const normalized = value?.trim() ?? '';
  if (!normalized) return fallbackName?.trim() || '-';
  return MARKET_CODE_LABELS[normalized.toLowerCase()] ?? fallbackName?.trim() ?? normalized;
}

function formatIndexMembership(value: string | null | undefined): string {
  const normalized = value?.trim();
  if (!normalized) return '-';
  return normalized.replace(/^TOPIX\s+/u, '') || normalized;
}

function SnapshotField({ label, shortLabel, value }: { label: string; shortLabel: string; value: string }) {
  return (
    <div className="flex min-w-0 items-baseline gap-1 border-l border-border/70 pl-2 first:border-l-0 first:pl-0">
      <dt title={label} className="shrink-0 text-[9px] font-medium uppercase leading-3 text-muted-foreground">
        {shortLabel}
      </dt>
      <dd className="whitespace-nowrap text-[11px] font-semibold leading-4 text-foreground">{value}</dd>
    </div>
  );
}

function SnapshotMetric({ label, title, children }: { label: string; title?: string; children: React.ReactNode }) {
  return (
    <div className="min-w-0 border-l border-border/70 px-2 py-0.5 first:border-l-0 first:pl-0">
      <dt title={title} className="truncate text-[9px] font-semibold uppercase leading-3 text-muted-foreground">
        {label}
      </dt>
      <dd className="text-xs font-semibold leading-4 tabular-nums text-foreground">{children}</dd>
    </div>
  );
}

function renderRankingMetric(item: NonNullable<MarketRankingSymbolResponse['item']>, metric: DailyRankingMetric) {
  const value =
    metric.key === 'sectorStrengthScore' ? (
      <SectorStrengthScoreChip value={item.sectorStrengthScore} />
    ) : (
      <DailyRankingMetricValue item={item} metric={metric} />
    );

  return (
    <SnapshotMetric key={metric.key} label={metric.label} title={metric.title}>
      {value}
    </SnapshotMetric>
  );
}

export function DailyRankingSnapshot({
  response,
  isLoading,
  error,
  onRetry,
  stockInfo,
  latestMarketCaps,
  provisionalProvenance = null,
}: DailyRankingSnapshotProps) {
  const item = response?.item ?? null;
  const hasRankingItem = item != null;
  const market = hasRankingItem
    ? formatMarket(item.marketCode)
    : formatMarket(stockInfo?.marketCode, stockInfo?.marketName);
  const sector33 = hasRankingItem ? item.sector33Name || '-' : stockInfo?.sector33Name || '-';
  const marketCap = hasRankingItem ? item.marketCap : latestMarketCaps.issuedShares;

  return (
    <section
      aria-label="Daily Ranking Snapshot"
      data-testid="daily-ranking-snapshot"
      className="mt-3 border-t border-border/60 pt-2"
    >
      <dl data-testid="daily-ranking-basic-info" className="flex flex-wrap items-center gap-x-2 gap-y-0.5">
        <SnapshotField label="Market" shortLabel="Mkt" value={market} />
        <SnapshotField
          label="Index Membership"
          shortLabel="Index"
          value={formatIndexMembership(stockInfo?.scaleCategory)}
        />
        <SnapshotField label="Sector 17" shortLabel="S17" value={stockInfo?.sector17Name || '-'} />
        <SnapshotField label="Sector 33" shortLabel="S33" value={sector33} />
        <SnapshotField label="Market Cap" shortLabel="MCap" value={formatMarketCap(marketCap)} />
        <SnapshotField
          label="Free-Float Market Cap"
          shortLabel="FF MCap"
          value={formatMarketCap(latestMarketCaps.freeFloat)}
        />
        <SnapshotField label="As of" shortLabel="As of" value={response?.date ?? '-'} />
      </dl>

      {provisionalProvenance ? (
        <p
          role="note"
          aria-label="Daily Ranking metrics are 四季報の当日暫定値"
          className="mt-1 text-[10px] font-medium text-amber-700 dark:text-amber-300"
        >
          四季報 15分遅延・当日暫定
        </p>
      ) : null}

      <dl
        data-testid="daily-ranking-metrics"
        className="mt-1.5 grid grid-cols-2 gap-y-1 border-t border-border/40 pt-1 sm:grid-cols-4 lg:grid-cols-7"
      >
        {isLoading ? (
          <div role="status" aria-live="polite" className="col-span-full py-1 text-xs text-muted-foreground">
            Loading Daily Ranking data…
          </div>
        ) : error ? (
          <div
            role="alert"
            className="col-span-full flex flex-wrap items-center justify-between gap-2 border-l-2 border-red-500/40 py-1 pl-2"
          >
            <div>
              <div className="text-xs font-semibold text-red-700">Unable to load Daily Ranking data</div>
              <div className="text-[11px] text-red-700/80">{error.message}</div>
            </div>
            <button
              type="button"
              onClick={onRetry}
              className="rounded border border-red-500/30 px-2 py-0.5 text-[11px] font-semibold text-red-700 hover:bg-red-500/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-500/60"
            >
              Retry
            </button>
          </div>
        ) : item ? (
          <>
            {PRIMARY_METRICS.map((metric) => renderRankingMetric(item, metric))}
            {renderRankingMetric(item, SECTOR_STRENGTH_METRIC)}
            <div data-testid="daily-ranking-psr-pair" className="min-w-0 border-l border-border/70 px-2 py-0.5">
              <div className="grid grid-cols-2 gap-1">
                <div className="min-w-0">
                  <dt className="truncate text-[9px] font-semibold uppercase leading-3 text-muted-foreground">
                    {PSR_METRIC.label}
                  </dt>
                  <dd className="text-xs font-semibold leading-4 tabular-nums text-foreground">
                    <DailyRankingMetricValue item={item} metric={PSR_METRIC} />
                  </dd>
                </div>
                <div className="min-w-0">
                  <dt className="truncate text-[9px] font-semibold uppercase leading-3 text-muted-foreground">
                    {FORWARD_PSR_METRIC.label}
                  </dt>
                  <dd className="text-xs font-semibold leading-4 tabular-nums text-foreground">
                    <DailyRankingMetricValue item={item} metric={FORWARD_PSR_METRIC} />
                  </dd>
                </div>
              </div>
            </div>
            {renderRankingMetric(item, LIQUIDITY_METRIC)}
            {renderRankingMetric(item, TRADING_VALUE_METRIC)}
            {renderRankingMetric(item, SMA5_METRIC)}
            <div data-testid="daily-ranking-regime" className="min-w-0 border-l border-border/70 px-2 py-0.5">
              <dt className="text-[9px] font-semibold uppercase leading-3 text-muted-foreground">Regime</dt>
              <dd className="leading-4">
                <DailyRankingRegimeChip item={item} />
              </dd>
            </div>
            <div data-testid="daily-ranking-signals" className="min-w-0 border-l border-border/70 px-2 py-0.5">
              <dt className="text-[9px] font-semibold uppercase leading-3 text-muted-foreground">Signals</dt>
              <dd className="leading-4">
                <DailyRankingSignalChips item={item} />
              </dd>
            </div>
          </>
        ) : (
          <div role="status" aria-live="polite" className="col-span-full py-1 text-xs text-muted-foreground">
            Daily Ranking data unavailable
          </div>
        )}
      </dl>
    </section>
  );
}
