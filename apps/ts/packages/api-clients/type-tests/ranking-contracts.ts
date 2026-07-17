import type { ApiJsonResponse, ApiQuery } from '@trading25/contracts';
import type { AnalyticsClient } from '../src/analytics/AnalyticsClient.js';
import type { MarketRankingParams } from '../src/analytics/types.js';

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Expect<Value extends true> = Value;

type RankingQuery = ApiQuery<'/api/analytics/ranking', 'get'>;
type RankingResponse = ApiJsonResponse<'/api/analytics/ranking', 'get', 200>;
type ClientRankingParams = Exclude<Parameters<AnalyticsClient['getMarketRanking']>[0], undefined>;
type ClientRankingResponse = Awaited<ReturnType<AnalyticsClient['getMarketRanking']>>;

type _RankingParams = Expect<Equal<ClientRankingParams, RankingQuery>>;
type _RankingResponse = Expect<Equal<ClientRankingResponse, RankingResponse>>;

declare const client: AnalyticsClient;
// @ts-expect-error getFundamentals does not allow callers to choose an arbitrary response type
client.getFundamentals<{ callerSelected: true }>({ symbol: '7203' });

const removedLegacyParam: MarketRankingParams = {
  // @ts-expect-error liquidityState was removed; use regimeState or riskState
  liquidityState: 'crowded_rerating',
};

void removedLegacyParam;
export type RankingClientContractAssertions = [_RankingParams, _RankingResponse];
