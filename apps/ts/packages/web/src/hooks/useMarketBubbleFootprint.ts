import { useQuery } from '@tanstack/react-query';
import type { MarketBubbleFootprintLatestResponseContract } from '@trading25/contracts/types/api-response-types';
import { apiGet } from '@/lib/api-client';
import type {
  MarketBubbleFootprintHorizon,
  MarketBubbleFootprintLatest,
} from '@/types/marketBubbleFootprint';

interface MarketBubbleFootprintParams {
  markets?: string;
  date?: string;
}

function getIntensityLabel(horizon: MarketBubbleFootprintHorizon): string {
  if (horizon.regime === 'blowoff_watch') return 'Blowoff watch';
  if (horizon.nearBlowoff) return 'Near blowoff';
  if (horizon.regime === 'crowded') return 'Crowded';
  if (horizon.regime === 'narrowing') return 'Narrowing';
  return 'Normal';
}

function normalizeFootprint(response: MarketBubbleFootprintLatestResponseContract): MarketBubbleFootprintLatest {
  const horizons = (response.horizons ?? []).map((item) => {
    const horizon: MarketBubbleFootprintHorizon = {
      ...item,
      activeFlags: item.activeFlags ?? [],
      intensityLabel: '',
    };
    return { ...horizon, intensityLabel: getIntensityLabel(horizon) };
  });
  return { ...response, horizons };
}

export function useMarketBubbleFootprint(params: MarketBubbleFootprintParams = {}, enabled = true) {
  return useQuery({
    queryKey: ['market-bubble-footprint', params],
    queryFn: async () => {
      const response = await apiGet<MarketBubbleFootprintLatestResponseContract>(
        '/api/analytics/market-bubble-footprint/latest',
        {
          markets: params.markets ?? 'prime,standard,growth',
          date: params.date,
        }
      );
      return normalizeFootprint(response);
    },
    enabled,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
  });
}
