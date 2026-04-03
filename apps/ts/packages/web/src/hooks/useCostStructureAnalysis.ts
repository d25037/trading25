import { useQuery } from '@tanstack/react-query';
import type { CostStructureAnalysisView } from '@trading25/api-clients/analytics';
import type { ApiCostStructureResponse } from '@trading25/contracts/types/api-types';
import { analyticsClient } from '@/lib/analytics-client';

interface UseCostStructureAnalysisOptions {
  enabled?: boolean;
  view?: CostStructureAnalysisView;
  windowQuarters?: number;
}

function fetchCostStructureAnalysis(
  symbol: string,
  view: CostStructureAnalysisView,
  windowQuarters: number
): Promise<ApiCostStructureResponse> {
  return analyticsClient.getCostStructureAnalysis({ symbol, view, windowQuarters });
}

export function useCostStructureAnalysis(symbol: string | null, options: UseCostStructureAnalysisOptions = {}) {
  const { enabled = true, view = 'recent', windowQuarters = 12 } = options;

  return useQuery({
    queryKey: ['cost-structure', symbol, view, windowQuarters],
    queryFn: () => {
      if (!symbol) throw new Error('Symbol is required');
      return fetchCostStructureAnalysis(symbol, view, windowQuarters);
    },
    enabled: !!symbol && enabled,
    staleTime: 10 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    retry: 2,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 10000),
  });
}
