import type {
  FactorRegressionIndexMatch,
  PortfolioFactorRegressionIndexMatch,
  PortfolioFactorRegressionResponse,
} from '@trading25/contracts/types/api-response-types';
import type { ApiJsonResponse, ApiQuery } from '@trading25/contracts';
import type { AnalyticsClient } from '../src/analytics/AnalyticsClient.js';

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Expect<Value extends true> = Value;

type FactorQuery = ApiQuery<'/api/analytics/factor-regression/{symbol}', 'get'>;
type FactorResponse = ApiJsonResponse<'/api/analytics/factor-regression/{symbol}', 'get', 200>;
type ClientFactorParams = Parameters<AnalyticsClient['getFactorRegression']>[0];
type ClientFactorResponse = Awaited<ReturnType<AnalyticsClient['getFactorRegression']>>;

type _FactorQuery = Expect<Equal<Omit<ClientFactorParams, 'symbol'>, FactorQuery>>;
type _FactorResponse = Expect<Equal<ClientFactorResponse, FactorResponse>>;

const stockMatch: FactorRegressionIndexMatch = {
  indexCode: '0085',
  indexName: 'TOPIX-17',
  category: 'sector17',
  rSquared: 0.8,
  beta: 1.1,
};

const portfolioMatch: PortfolioFactorRegressionIndexMatch = {
  code: '0085',
  name: 'TOPIX-17',
  rSquared: 0.8,
};

declare const response: PortfolioFactorRegressionResponse;
response.sector17Matches satisfies PortfolioFactorRegressionIndexMatch[];

// @ts-expect-error stock match is not a portfolio match
const invalidPortfolioMatch: PortfolioFactorRegressionIndexMatch = stockMatch;
void portfolioMatch;
void invalidPortfolioMatch;
export type FactorRegressionClientContractAssertions = [_FactorQuery, _FactorResponse];
