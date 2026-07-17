import type { ApiJsonBody, ApiJsonResponse, ApiOperation, ApiPathParams, ApiQuery } from '..';

type FundamentalsPath = '/api/analytics/fundamentals/{symbol}';
type ScreeningJobsPath = '/api/analytics/screening/jobs';
type PortfolioDetailPath = '/api/portfolio/{id}';
type DbSyncPath = '/api/db/sync';
type StrategyValidationPath = '/api/strategies/{strategy_name}/validate';

const fundamentalsPathParams: ApiPathParams<FundamentalsPath, 'get'> = {
  symbol: '7203',
};
const fundamentalsQuery: ApiQuery<FundamentalsPath, 'get'> = {
  periodType: 'FY',
  to: '2026-07-17',
};
const fundamentalsResponse: Pick<ApiJsonResponse<FundamentalsPath, 'get', 200>, 'asOfDate' | 'symbol'> = {
  asOfDate: '2026-07-17',
  symbol: '7203',
};

// @ts-expect-error The generated path parameter is named symbol.
const invalidFundamentalsPathParams: ApiPathParams<FundamentalsPath, 'get'> = { id: '7203' };
// @ts-expect-error Unsupported period types must stay rejected by the generated contract.
const invalidFundamentalsQuery: ApiQuery<FundamentalsPath, 'get'> = { periodType: 'Q4' };
// @ts-expect-error GET is the only supported operation for fundamentals detail.
const invalidFundamentalsOperation: ApiOperation<FundamentalsPath, 'post'> = {};
// @ts-expect-error This GET operation has no request body.
const invalidFundamentalsBody: ApiJsonBody<FundamentalsPath, 'get'> = {};
// @ts-expect-error 201 is not a generated response status for this operation.
const invalidFundamentalsStatus: ApiJsonResponse<FundamentalsPath, 'get', 201> = {};

const screeningBody: ApiJsonBody<ScreeningJobsPath, 'post'> = {
  entry_decidability: 'pre_open_decidable',
  order: 'desc',
  recentDays: 10,
  sortBy: 'matchedDate',
};
const screeningResponse: Pick<ApiJsonResponse<ScreeningJobsPath, 'post', 202>, 'job_id' | 'status'> = {
  job_id: 'screening-job-id',
  status: 'pending',
};

const invalidScreeningBody: ApiJsonBody<ScreeningJobsPath, 'post'> = {
  entry_decidability: 'pre_open_decidable',
  order: 'desc',
  recentDays: 10,
  // @ts-expect-error sortBy is constrained by the generated request schema.
  sortBy: 'score',
};
// @ts-expect-error This POST operation has no query parameters.
const invalidScreeningQuery: ApiQuery<ScreeningJobsPath, 'post'> = {};

const portfolioPathParams: ApiPathParams<PortfolioDetailPath, 'get'> = { id: 42 };
const portfolioResponse: Pick<ApiJsonResponse<PortfolioDetailPath, 'get', 200>, 'id' | 'name'> = {
  id: 42,
  name: 'Long-term holdings',
};

// @ts-expect-error Portfolio ids are generated as numbers.
const invalidPortfolioPathParams: ApiPathParams<PortfolioDetailPath, 'get'> = { id: '42' };

const dbSyncBody: ApiJsonBody<DbSyncPath, 'post'> = {
  enforceBulkForStockData: true,
  mode: 'incremental',
  resetBeforeSync: false,
};
const dbSyncResponse: Pick<ApiJsonResponse<DbSyncPath, 'post', 202>, 'jobId'> = {
  jobId: 'sync-job-id',
};

const invalidDbSyncBody: ApiJsonBody<DbSyncPath, 'post'> = {
  enforceBulkForStockData: false,
  // @ts-expect-error Unsupported sync modes must stay rejected by the generated contract.
  mode: 'full',
  resetBeforeSync: false,
};

const strategyValidationBody: ApiJsonBody<StrategyValidationPath, 'post'> = {
  config: { shared_config: { data_source: 'market' } },
};

void fundamentalsPathParams;
void fundamentalsQuery;
void fundamentalsResponse;
void invalidFundamentalsPathParams;
void invalidFundamentalsQuery;
void invalidFundamentalsOperation;
void invalidFundamentalsBody;
void invalidFundamentalsStatus;
void screeningBody;
void screeningResponse;
void invalidScreeningBody;
void invalidScreeningQuery;
void portfolioPathParams;
void portfolioResponse;
void invalidPortfolioPathParams;
void dbSyncBody;
void dbSyncResponse;
void invalidDbSyncBody;
void strategyValidationBody;
