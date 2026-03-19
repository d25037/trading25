const PREFERRED_MARKET_ORDER = ['prime', 'standard', 'growth'] as const;
const MARKET_LABELS: Record<string, string> = {
  prime: 'Prime',
  standard: 'Standard',
  growth: 'Growth',
};

export function canonicalizeMarkets(markets: readonly string[] | null | undefined): string[] {
  const normalized = (markets ?? [])
    .map((market) => market.trim().toLowerCase())
    .filter(Boolean);
  const seen = new Set<string>();
  const canonical: string[] = [];

  for (const market of PREFERRED_MARKET_ORDER) {
    if (normalized.includes(market)) {
      canonical.push(market);
      seen.add(market);
    }
  }

  for (const market of normalized) {
    if (seen.has(market)) {
      continue;
    }
    canonical.push(market);
    seen.add(market);
  }

  return canonical;
}

export function unionMarkets(marketLists: Array<readonly string[] | null | undefined>): string[] {
  const union: string[] = [];
  const seen = new Set<string>();

  for (const marketList of marketLists) {
    for (const market of canonicalizeMarkets(marketList)) {
      if (seen.has(market)) {
        continue;
      }
      union.push(market);
      seen.add(market);
    }
  }

  return canonicalizeMarkets(union);
}

export function formatMarketsLabel(markets: readonly string[] | null | undefined): string {
  const normalized = canonicalizeMarkets(markets);
  if (normalized.length === 0) {
    return 'Auto';
  }
  if (normalized.join(',') === 'prime,standard,growth') {
    return 'All Markets';
  }
  return normalized.map((market) => MARKET_LABELS[market] ?? market).join(' + ');
}
