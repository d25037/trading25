import type { MarketRankingSymbolResponse } from '@trading25/contracts/types/api-response-types';
import type { StockInfoResponse } from '@/hooks/useStockInfo';
import type { ChartHeaderMarketCaps } from '@/pages/SymbolWorkbenchHeader';
import { formatMarketCap } from '@/utils/formatters';
import {
  DAILY_RANKING_VALUE_METRICS,
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
}

const MARKET_CODE_LABELS: Record<string, string> = {
  prime: 'Prime',
  standard: 'Standard',
  growth: 'Growth',
  '0111': 'Prime',
  '0112': 'Standard',
  '0113': 'Growth',
};

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

function SnapshotField({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 border-l border-border/70 pl-3">
      <div className="text-[11px] font-medium uppercase tracking-[0.12em] text-muted-foreground">{label}</div>
      <div className="truncate text-sm font-semibold text-foreground">{value}</div>
    </div>
  );
}

function SnapshotMetric({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="min-w-0 rounded-lg bg-[var(--app-surface-muted)] px-2.5 py-2">
      <div className="text-[10px] font-semibold uppercase text-muted-foreground">{label}</div>
      <div className="mt-0.5 font-semibold tabular-nums text-foreground">{children}</div>
    </div>
  );
}

export function DailyRankingSnapshot({
  response,
  isLoading,
  error,
  onRetry,
  stockInfo,
  latestMarketCaps,
}: DailyRankingSnapshotProps) {
  const item = response?.item ?? null;
  const hasRankingItem = item != null;
  const market = hasRankingItem
    ? formatMarket(item.marketCode)
    : formatMarket(stockInfo?.marketCode, stockInfo?.marketName);
  const sector33 = hasRankingItem ? item.sector33Name || '-' : stockInfo?.sector33Name || '-';
  const marketCap = hasRankingItem ? item.marketCap : latestMarketCaps.issuedShares;

  return (
    <section data-testid="daily-ranking-snapshot" className="mt-4 border-t border-border/60 pt-3">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="text-sm font-semibold text-foreground">Daily Ranking Snapshot</h3>
        <span className="text-xs font-medium text-muted-foreground">As of {response?.date ?? '-'}</span>
      </div>

      <div
        data-testid="daily-ranking-basic-info"
        className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-3 sm:gap-3 xl:grid-cols-6"
      >
        <SnapshotField label="Market" value={market} />
        <SnapshotField label="Index Membership" value={formatIndexMembership(stockInfo?.scaleCategory)} />
        <SnapshotField label="Sector 17" value={stockInfo?.sector17Name || '-'} />
        <SnapshotField label="Sector 33" value={sector33} />
        <SnapshotField label="Market Cap" value={formatMarketCap(marketCap)} />
        <SnapshotField label="Free-Float Market Cap" value={formatMarketCap(latestMarketCaps.freeFloat)} />
      </div>

      <div
        data-testid="daily-ranking-metrics"
        className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5"
      >
        {isLoading ? (
          <div className="col-span-full py-2 text-sm text-muted-foreground">Loading Daily Ranking data...</div>
        ) : error ? (
          <div className="col-span-full flex flex-wrap items-center justify-between gap-2 rounded-lg border border-red-500/20 bg-red-500/10 px-3 py-2">
            <div>
              <div className="text-sm font-semibold text-red-700">Unable to load Daily Ranking data</div>
              <div className="text-xs text-red-700/80">{error.message}</div>
            </div>
            <button
              type="button"
              onClick={onRetry}
              className="rounded-md border border-red-500/30 px-2.5 py-1 text-xs font-semibold text-red-700 hover:bg-red-500/10"
            >
              Retry
            </button>
          </div>
        ) : item ? (
          <>
            {DAILY_RANKING_VALUE_METRICS.map((metric) => (
              <SnapshotMetric key={metric.key} label={metric.label}>
                {metric.key === 'sectorStrengthScore' ? (
                  <SectorStrengthScoreChip value={item.sectorStrengthScore} />
                ) : (
                  <DailyRankingMetricValue item={item} metric={metric} />
                )}
              </SnapshotMetric>
            ))}
            <div
              data-testid="daily-ranking-regime"
              className="col-span-2 rounded-lg bg-[var(--app-surface-muted)] px-2.5 py-2"
            >
              <div className="text-[10px] font-semibold uppercase text-muted-foreground">Regime</div>
              <div className="mt-1">
                <DailyRankingRegimeChip item={item} />
              </div>
            </div>
            <div
              data-testid="daily-ranking-signals"
              className="col-span-2 rounded-lg bg-[var(--app-surface-muted)] px-2.5 py-2"
            >
              <div className="text-[10px] font-semibold uppercase text-muted-foreground">Signals</div>
              <div className="mt-1">
                <DailyRankingSignalChips item={item} />
              </div>
            </div>
          </>
        ) : (
          <div className="col-span-full py-2 text-sm text-muted-foreground">Daily Ranking data unavailable</div>
        )}
      </div>
    </section>
  );
}
