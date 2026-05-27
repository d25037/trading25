import type { ApiLiquidityProfile, DataProvenance, ResponseDiagnostics } from '@trading25/contracts/types/api-types';
import { BookOpen, Loader2, RotateCcw, SettingsIcon, TrendingUp, Wallet } from 'lucide-react';
import { TimeframeSelector } from '@/components/Chart/TimeframeSelector';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { Button } from '@/components/ui/button';
import type { StockInfoResponse } from '@/hooks/useStockInfo';
import { cn } from '@/lib/utils';
import type { useChartStore } from '@/stores/chartStore';
import { formatMarketCap } from '@/utils/formatters';

type ChartSettings = ReturnType<typeof useChartStore.getState>['settings'];

export interface ChartRefreshFeedback {
  tone: 'success' | 'error';
  message: string;
}

export interface ChartHeaderMarketCaps {
  freeFloat: number | null;
  issuedShares: number | null;
}

export function resolveLatestMarketCaps(
  dailyValuation:
    | Array<{
        freeFloatMarketCap?: number | null;
        marketCap?: number | null;
      }>
    | null
    | undefined
): ChartHeaderMarketCaps {
  if (!dailyValuation || dailyValuation.length === 0) {
    return {
      freeFloat: null,
      issuedShares: null,
    };
  }

  const latest = dailyValuation[dailyValuation.length - 1];
  return {
    freeFloat: latest?.freeFloatMarketCap ?? null,
    issuedShares: latest?.marketCap ?? null,
  };
}

function ChartHeaderInfoField({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 border-l border-border/70 pl-3">
      <div className="text-[11px] font-medium uppercase tracking-[0.12em] text-muted-foreground">{label}</div>
      <div className="truncate text-sm font-semibold text-foreground">{value}</div>
    </div>
  );
}

function formatYenPrice(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toLocaleString('ja-JP', { maximumFractionDigits: value >= 100 ? 0 : 1 })}円`;
}

function formatSignedPercent(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return '-';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(digits)}%`;
}

function formatPercent(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toFixed(digits)}%`;
}

function formatSignedNumber(value: number | null | undefined, digits = 2): string {
  if (value == null || !Number.isFinite(value)) return '-';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(digits)}`;
}

function formatLiquidityRegime(value: string | null | undefined): string {
  switch (value) {
    case 'neutral_rerating':
      return 'Neutral Re-rating';
    case 'crowded_rerating':
      return 'Crowded Re-rating';
    case 'distribution_stress':
      return 'Stress';
    case 'stale_liquidity':
      return 'Stale';
    case 'neutral':
      return 'Neutral';
    default:
      return '-';
  }
}

function LiquidityProfileStrip({ profile }: { profile: ApiLiquidityProfile | null | undefined }) {
  if (!profile) return null;
  if (!profile.supported) {
    return (
      <div className="mt-4 border-t border-border/60 pt-3 text-xs text-muted-foreground">
        Liquidity: <span className="font-medium text-foreground">Prime model only</span>
      </div>
    );
  }

  const adv60 = profile.windows.find((item) => item.advWindow === 60);
  const adv20 = profile.windows.find((item) => item.advWindow === 20);
  const primary = adv60 ?? adv20;
  if (!primary) return null;

  return (
    <div className="mt-4 border-t border-border/60 pt-3">
      <div className="mb-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs">
        <span className="uppercase tracking-[0.14em] text-muted-foreground">Prime Liquidity</span>
        <span className="font-medium text-foreground">{profile.date ?? '-'}</span>
        <span className="text-muted-foreground">
          20d/60d {formatSignedPercent(profile.recentReturn20dPct)} / {formatSignedPercent(profile.recentReturn60dPct)}
        </span>
      </div>
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
        <ChartHeaderInfoField
          label={`Med ADV${primary.advWindow} / Free Float`}
          value={formatPercent(primary.freeFloatTradingValueRatioPct, 2)}
        />
        <ChartHeaderInfoField
          label="Liquidity Residual"
          value={`${formatSignedNumber(primary.liquidityResidualZ)} / ${formatLiquidityRegime(primary.liquidityRegime)}`}
        />
        <ChartHeaderInfoField
          label={adv20 && adv60 ? 'Med ADV20 / 60' : `Med ADV${primary.advWindow}`}
          value={
            adv20 && adv60
              ? `${formatMarketCap(adv20.averageTradingValue ?? null)} / ${formatMarketCap(
                  adv60.averageTradingValue ?? null
                )}`
              : formatMarketCap(primary.averageTradingValue ?? null)
          }
        />
      </div>
      <div className="mt-2 text-[11px] text-muted-foreground">
        流動性等価株価 Med ADV{primary.advWindow}:{' '}
        <span className="font-medium text-foreground">
          {formatYenPrice(primary.liquidityImpliedPrice)} ({formatSignedPercent(primary.liquidityImpliedPriceGapPct)})
        </span>
      </div>
    </div>
  );
}

function ChartHeaderMetaChip({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex min-w-0 items-center gap-2 text-xs">
      <span className="shrink-0 uppercase tracking-[0.14em] text-muted-foreground">{label}</span>
      <span className="min-w-0 truncate font-medium text-foreground">{value}</span>
    </div>
  );
}

function formatOptionalDate(value: string | null | undefined): string {
  if (!value) return '-';
  return value;
}

function formatList(values: string[] | null | undefined): string {
  if (!values || values.length === 0) return '-';
  return values.join(', ');
}

const MARKET_CODE_LABELS: Record<string, string> = {
  prime: 'Prime',
  standard: 'Standard',
  growth: 'Growth',
  '0111': 'Prime',
  '0112': 'Standard',
  '0113': 'Growth',
};

function formatMarketLabel(stockInfo: StockInfoResponse | undefined): string {
  if (!stockInfo) {
    return '-';
  }

  const rawMarketCode = stockInfo.marketCode?.trim() ?? '';
  const canonicalLabel = rawMarketCode ? (MARKET_CODE_LABELS[rawMarketCode.toLowerCase()] ?? '') : '';
  return canonicalLabel || stockInfo.marketName?.trim() || rawMarketCode || '-';
}

function formatScaleCategoryLabel(scaleCategory: string | null | undefined): string {
  const normalized = scaleCategory?.trim();
  if (!normalized) {
    return '-';
  }

  const shortLabel = normalized.replace(/^TOPIX\s+/u, '');
  return shortLabel || normalized;
}

function mergeUniqueStrings(...groups: Array<string[] | null | undefined>): string[] {
  const seen = new Set<string>();
  for (const group of groups) {
    for (const value of group ?? []) {
      if (value) {
        seen.add(value);
      }
    }
  }
  return [...seen];
}

function mergeWarnings(...groups: Array<ResponseDiagnostics | DataProvenance | null | undefined>): string[] {
  return mergeUniqueStrings(...groups.map((group) => group?.warnings));
}

function openCompanyPage(baseUrl: string, selectedSymbol: string | null, suffix = '') {
  if (!selectedSymbol) return;
  window.open(`${baseUrl}${selectedSymbol}${suffix}`, '_blank', 'noopener,noreferrer');
}

function ChartRefreshFeedbackBanner({ feedback }: { feedback: ChartRefreshFeedback }) {
  const toneClassName =
    feedback.tone === 'success'
      ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-700'
      : 'border-red-500/20 bg-red-500/10 text-red-700';

  return <div className={cn('rounded-xl border px-4 py-3 text-sm', toneClassName)}>{feedback.message}</div>;
}

export function ChartHeader({
  settings,
  selectedSymbol,
  stockInfo,
  latestMarketCaps,
  liquidityProfile,
  strategyName,
  matchedDate,
  signalProvenance,
  signalDiagnostics,
  fundamentalsProvenance,
  refreshFeedback,
  isRefreshing,
  onRefresh,
  onOpenMobileSettings,
}: {
  settings: ChartSettings;
  selectedSymbol: string;
  stockInfo: StockInfoResponse | undefined;
  latestMarketCaps: ChartHeaderMarketCaps;
  liquidityProfile: ApiLiquidityProfile | null | undefined;
  strategyName: string | null;
  matchedDate: string | null;
  signalProvenance: DataProvenance | null | undefined;
  signalDiagnostics: ResponseDiagnostics | null | undefined;
  fundamentalsProvenance: DataProvenance | null | undefined;
  refreshFeedback: ChartRefreshFeedback | null;
  isRefreshing: boolean;
  onRefresh: () => void;
  onOpenMobileSettings: () => void;
}) {
  const mergedLoadedDomains = mergeUniqueStrings(
    signalProvenance?.loaded_domains,
    fundamentalsProvenance?.loaded_domains
  );
  const warnings = mergeWarnings(signalProvenance, fundamentalsProvenance, signalDiagnostics);
  const marketSnapshotId = signalProvenance?.market_snapshot_id ?? fundamentalsProvenance?.market_snapshot_id ?? '-';
  let overlayLabel = '-';
  if (strategyName) {
    overlayLabel = `${strategyName} (strategy)`;
  } else if (settings.signalOverlay?.enabled) {
    overlayLabel = 'ad hoc signal overlay';
  }

  return (
    <div className="space-y-3">
      <Surface className="px-3 py-3 sm:px-5 sm:py-4">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0 space-y-3">
            <div className="flex items-center gap-2 sm:gap-3">
              <div className="hidden h-10 w-10 items-center justify-center rounded-2xl bg-[var(--app-surface-muted)] text-primary sm:flex">
                <TrendingUp className="h-5 w-5" />
              </div>
              <div className="min-w-0 flex-1">
                <SectionEyebrow>Symbol Workbench</SectionEyebrow>
                <h2 className="truncate text-lg font-semibold tracking-tight text-foreground sm:text-2xl">
                  {selectedSymbol}
                  {stockInfo?.companyName && (
                    <span className="ml-2 font-medium text-foreground">{stockInfo.companyName}</span>
                  )}
                  {settings.relativeMode && <span className="font-medium text-muted-foreground"> / TOPIX</span>}
                </h2>
              </div>
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="shrink-0 lg:hidden"
                onClick={onOpenMobileSettings}
              >
                <SettingsIcon className="mr-1 h-4 w-4" />
                設定
              </Button>
            </div>

            <div className="hidden flex-wrap gap-x-5 gap-y-2 sm:flex">
              <ChartHeaderMetaChip label="Overlay" value={overlayLabel} />
              <ChartHeaderMetaChip label="Matched Date" value={formatOptionalDate(matchedDate)} />
              <ChartHeaderMetaChip label="Market Snapshot" value={marketSnapshotId} />
              <ChartHeaderMetaChip label="Signal Domains" value={formatList(mergedLoadedDomains)} />
            </div>
            <div className="grid grid-cols-2 gap-2 text-xs sm:hidden">
              <ChartHeaderMetaChip label="Overlay" value={overlayLabel} />
              <ChartHeaderMetaChip label="Date" value={formatOptionalDate(matchedDate)} />
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2 xl:justify-end">
            <Button
              variant="outline"
              size="sm"
              className="hidden sm:inline-flex"
              onClick={() => openCompanyPage('https://shikiho.toyokeizai.net/stocks/', selectedSymbol)}
              title="四季報を開く"
            >
              <BookOpen className="mr-1 h-4 w-4" />
              四季報
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="hidden sm:inline-flex"
              onClick={() => openCompanyPage('https://www.buffett-code.com/company/', selectedSymbol, '/')}
              title="Buffett Codeを開く"
            >
              <Wallet className="mr-1 h-4 w-4" />
              B.C.
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="flex-1 sm:flex-none"
              onClick={onRefresh}
              disabled={isRefreshing}
            >
              {isRefreshing ? (
                <>
                  <Loader2 className="mr-1 h-4 w-4 animate-spin" />
                  Refreshing...
                </>
              ) : (
                <>
                  <RotateCcw className="mr-1 h-4 w-4" />
                  Stock Refresh
                </>
              )}
            </Button>
            <TimeframeSelector />
          </div>
        </div>

        <div className="mt-3 grid grid-cols-2 gap-2 sm:mt-4 sm:grid-cols-2 sm:gap-3 lg:grid-cols-3 xl:grid-cols-6">
          <ChartHeaderInfoField label="市場" value={formatMarketLabel(stockInfo)} />
          <ChartHeaderInfoField label="指数採用" value={formatScaleCategoryLabel(stockInfo?.scaleCategory)} />
          <ChartHeaderInfoField label="セクター17" value={stockInfo?.sector17Name || '-'} />
          <ChartHeaderInfoField label="セクター33" value={stockInfo?.sector33Name || '-'} />
          <ChartHeaderInfoField label="時価総額 (Free Float)" value={formatMarketCap(latestMarketCaps.freeFloat)} />
          <ChartHeaderInfoField
            label="時価総額 (発行済み株式数)"
            value={formatMarketCap(latestMarketCaps.issuedShares)}
          />
        </div>

        <LiquidityProfileStrip profile={liquidityProfile} />

        {(signalProvenance?.reference_date || fundamentalsProvenance?.reference_date || warnings.length > 0) && (
          <div className="mt-4 border-t border-border/60 pt-3 text-xs text-muted-foreground">
            <div>
              Reference Date:{' '}
              <span className="font-medium text-foreground">
                {signalProvenance?.reference_date ?? fundamentalsProvenance?.reference_date ?? '-'}
              </span>
            </div>
            {warnings.length > 0 && (
              <div className="mt-1">
                Warnings: <span className="font-medium text-foreground">{warnings.join(' | ')}</span>
              </div>
            )}
          </div>
        )}
      </Surface>

      {refreshFeedback && <ChartRefreshFeedbackBanner feedback={refreshFeedback} />}
    </div>
  );
}
