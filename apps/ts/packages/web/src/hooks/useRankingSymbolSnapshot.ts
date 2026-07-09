import { useQuery } from '@tanstack/react-query';
import type { MarketRankingSymbolResponse } from '@trading25/contracts/types/api-response-types';
import { analyticsClient } from '@/lib/analytics-client';

export function normalizeRankingSymbol(symbol: string | null | undefined): string | null {
  const normalized = symbol?.trim().toUpperCase();
  if (!normalized) return null;
  return normalized.length === 5 && normalized.endsWith('0') ? normalized.slice(0, -1) : normalized;
}

export const rankingSymbolSnapshotKeys = {
  detail: (symbol: string) => ['ranking', 'symbol', normalizeRankingSymbol(symbol)] as const,
};

export function useRankingSymbolSnapshot(symbol: string | null | undefined) {
  const normalizedSymbol = normalizeRankingSymbol(symbol);
  return useQuery<MarketRankingSymbolResponse>({
    queryKey: ['ranking', 'symbol', normalizedSymbol],
    queryFn: () => analyticsClient.getMarketRankingSymbol(normalizedSymbol as string),
    enabled: normalizedSymbol != null,
    staleTime: 60_000,
    gcTime: 300_000,
  });
}
