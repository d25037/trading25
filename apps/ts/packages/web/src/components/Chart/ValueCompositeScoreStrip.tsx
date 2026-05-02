import { useValueCompositeScore } from '@/hooks/useValueCompositeScore';

interface ValueCompositeScoreStripProps {
  symbol: string | null;
  enabled?: boolean;
}

function formatScore(value: number): string {
  return (value * 100).toLocaleString('en-US', {
    maximumFractionDigits: 1,
    minimumFractionDigits: 1,
  });
}

function formatComponentScore(value: number): string {
  return (value * 100).toLocaleString('en-US', {
    maximumFractionDigits: 0,
    minimumFractionDigits: 0,
  });
}

function formatRank(rank: number, universeCount: number): string {
  return `Rank ${rank.toLocaleString('en-US')} / ${universeCount.toLocaleString('en-US')}`;
}

function formatScoreMethod(method: string | null | undefined): string {
  if (method === 'prime_size_tilt') return 'Prime size tilt';
  if (method === 'standard_pbr_tilt') return 'Standard PBR tilt';
  if (method === 'equal_weight') return 'Equal weight';
  return 'Value score';
}

function resolveUnavailableMessage(reason: string | null | undefined): string | null {
  if (reason === 'forward_eps_missing') {
    return 'Value Score unavailable: forward EPS missing';
  }
  if (reason === 'bps_missing') {
    return 'Value Score unavailable: BPS missing';
  }
  return null;
}

export function ValueCompositeScoreStrip({ symbol, enabled = true }: ValueCompositeScoreStripProps) {
  const { data } = useValueCompositeScore(symbol, { enabled, forwardEpsMode: 'latest' });
  const item = data?.item;

  const unavailableMessage = resolveUnavailableMessage(data?.unsupportedReason);
  if (!data?.scoreAvailable && unavailableMessage) {
    return (
      <div
        data-testid="value-composite-score-unavailable"
        className="rounded-md border border-border/60 bg-muted/30 px-3 py-2 text-xs text-muted-foreground"
      >
        {unavailableMessage}
      </div>
    );
  }

  if (!data?.scoreAvailable || !item) {
    return null;
  }

  return (
    <div
      data-testid="value-composite-score-strip"
      className="grid gap-2 rounded-md border border-border/60 bg-muted/30 px-3 py-2 text-xs sm:grid-cols-[minmax(9rem,1fr)_auto]"
    >
      <div className="flex min-w-0 items-center gap-3">
        <div className="min-w-0">
          <div className="text-[10px] font-medium uppercase leading-none tracking-wide text-muted-foreground">
            Value Score
          </div>
          <div className="mt-1 flex flex-wrap items-baseline gap-x-2 gap-y-1">
            <span className="text-lg font-semibold leading-none text-foreground">{formatScore(item.score)}</span>
            <span className="text-xs text-muted-foreground">{formatRank(item.rank, data.universeCount)}</span>
            <span className="text-xs text-muted-foreground">{formatScoreMethod(data.scoreMethod)}</span>
          </div>
        </div>
      </div>
      <div className="grid grid-cols-3 gap-1 text-center">
        <div className="rounded bg-background/70 px-2 py-1">
          <div className="text-[10px] uppercase leading-none text-muted-foreground">Size</div>
          <div className="mt-1 font-medium text-foreground">{formatComponentScore(item.smallMarketCapScore)}</div>
        </div>
        <div className="rounded bg-background/70 px-2 py-1">
          <div className="text-[10px] uppercase leading-none text-muted-foreground">PBR</div>
          <div className="mt-1 font-medium text-foreground">{formatComponentScore(item.lowPbrScore)}</div>
        </div>
        <div className="rounded bg-background/70 px-2 py-1">
          <div className="text-[10px] uppercase leading-none text-muted-foreground">PER</div>
          <div className="mt-1 font-medium text-foreground">{formatComponentScore(item.lowForwardPerScore)}</div>
        </div>
      </div>
    </div>
  );
}
