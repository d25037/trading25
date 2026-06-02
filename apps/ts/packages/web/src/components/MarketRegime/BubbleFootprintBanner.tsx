import { Link } from '@tanstack/react-router';
import type { MarketBubbleFootprintMonitor } from '@/types/marketBubbleFootprint';

interface BubbleFootprintBannerProps {
  data: MarketBubbleFootprintMonitor | null | undefined;
  isLoading?: boolean;
  errorMessage?: string | null;
}

function formatPct(value: number | null | undefined, digits = 1): string {
  return typeof value === 'number' && Number.isFinite(value) ? `${value.toFixed(digits)}%` : '-';
}

function regimeLabel(value: string): string {
  if (value === 'blowoff_watch') return 'Blowoff watch';
  if (value === 'crowded') return 'Crowded';
  if (value === 'narrowing') return 'Narrowing';
  if (value === 'normal') return 'Normal';
  return value;
}

function regimeMarkerClass(data: MarketBubbleFootprintMonitor): string {
  if (data.overallRegime === 'blowoff_watch') return 'bg-red-500 ring-red-500/20';
  if (data.nearBlowoff || data.overallRegime === 'crowded') return 'bg-amber-500 ring-amber-500/20';
  if (data.overallRegime === 'narrowing') return 'bg-sky-500 ring-sky-500/20';
  return 'bg-emerald-500 ring-emerald-500/20';
}

export function BubbleFootprintBanner({ data, isLoading, errorMessage }: BubbleFootprintBannerProps) {
  if (isLoading) {
    return (
      <div className="min-w-0 text-xs text-muted-foreground" aria-label="Market Regime">
        Loading market regime...
      </div>
    );
  }

  if (errorMessage) {
    return (
      <div className="min-w-0 text-xs text-destructive" aria-label="Market Regime">
        {errorMessage}
      </div>
    );
  }

  if (!data) return null;

  const horizonTitle = data.horizons
    .map((item) => `${item.horizon}D ${item.intensityLabel} / score ${item.score} / breadth ${formatPct(item.breadthUpPct)}`)
    .join('\n');

  return (
    <div
      className="flex min-w-0 max-w-full flex-col gap-1 overflow-hidden text-xs"
      aria-label="Market Regime"
    >
      <div className="flex min-w-0 items-center gap-2 whitespace-nowrap">
        <span className="shrink-0 font-semibold uppercase text-muted-foreground">Market Regime</span>
        <span className="flex shrink-0 items-center gap-1.5 text-sm font-semibold text-foreground">
          <span
            aria-label={`Regime marker: ${regimeLabel(data.overallRegime)}`}
            className={`h-2 w-2 rounded-full ring-4 ${regimeMarkerClass(data)}`}
          />
          {regimeLabel(data.overallRegime)}
        </span>
        <span className="shrink-0 text-muted-foreground">
          {data.date} / score {data.overallScore}
        </span>
        <Link
          to="/research/detail"
          search={{ experimentId: data.researchExperimentId }}
          className="shrink-0 font-medium text-primary hover:underline"
        >
          Footprint
        </Link>
        <Link
          to="/research/detail"
          search={{ experimentId: data.reratingExperimentId }}
          className="shrink-0 font-medium text-primary hover:underline"
        >
          Rerating
        </Link>
      </div>
      <div className="grid w-full min-w-0 grid-cols-2 gap-1.5 sm:grid-cols-4 lg:w-[29rem]" title={horizonTitle}>
        {data.horizons.map((item) => (
          <div
            key={item.horizon}
            className="flex h-9 min-w-0 flex-col justify-center rounded border border-border/60 bg-[var(--app-surface-muted)] px-1.5 leading-tight"
          >
            <span className="truncate font-semibold text-foreground">
              {item.horizon}D score {item.score}
            </span>
            <span className="truncate text-muted-foreground">breadth {formatPct(item.breadthUpPct, 0)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
