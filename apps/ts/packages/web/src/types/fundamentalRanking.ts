import type {
  FundamentalRankingItem,
  FundamentalRankingMetricKey,
  FundamentalRankings,
  MarketFundamentalRankingResponse,
} from '@trading25/api-clients/analytics';

export interface FundamentalRankingParams {
  limit?: number;
  markets?: string;
  metricKey?: FundamentalRankingMetricKey;
  forecastAboveRecentFyActuals?: boolean;
  forecastLookbackFyCount?: number;
}

export type {
  FundamentalRankingItem,
  FundamentalRankingMetricKey,
  FundamentalRankings,
  MarketFundamentalRankingResponse,
};
