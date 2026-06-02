import type {
  MarketBubbleFootprintHorizonContract,
  MarketBubbleFootprintLatestResponseContract,
} from '@trading25/contracts/types/api-response-types';

export type MarketBubbleRegime = 'normal' | 'narrowing' | 'crowded' | 'blowoff_watch' | string;

export interface MarketBubbleFootprintHorizon
  extends Omit<MarketBubbleFootprintHorizonContract, 'activeFlags' | 'regime'> {
  regime: MarketBubbleRegime;
  intensityLabel: string;
  activeFlags: string[];
}

export interface MarketBubbleFootprintLatest
  extends Omit<MarketBubbleFootprintLatestResponseContract, 'horizons' | 'overallRegime'> {
  overallRegime: MarketBubbleRegime;
  horizons: MarketBubbleFootprintHorizon[];
}

export type MarketBubbleFootprintMonitor = MarketBubbleFootprintLatest;
