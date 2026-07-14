import type {
  FactorRegressionIndexMatch,
  PortfolioFactorRegressionIndexMatch,
  PortfolioFactorRegressionResponse,
} from '@trading25/contracts/types/api-response-types';

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
