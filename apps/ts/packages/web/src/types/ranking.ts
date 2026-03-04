/**
 * Ranking-related types for frontend
 * Re-exports from @trading25/contracts and adds frontend-specific types
 */

export type {
  MarketRankingResponse,
  RankingItem,
  Rankings,
  RankingType,
} from '@trading25/contracts/types/api-response-types';

// Frontend-specific types
export interface RankingParams {
  date?: string;
  limit?: number;
  markets?: string;
  lookbackDays?: number;
  periodDays?: number;
}
