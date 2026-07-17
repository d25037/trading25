/** Compile-only checks for public Lab hook path/query boundaries. */

import type { ApiPathParams, ApiQuery } from '@trading25/contracts';
import type {
  LabJobCancelPathParams,
  LabJobStatusPathParams,
  LabJobsQuery,
  LabOptimizeRecommendationQuery,
} from '@trading25/api-clients/backtest';
import type { labKeys, useLabJobs, useLabJobStatus, useLabOptimizeRecommendation } from './useLab';

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Expect<Value extends true> = Value;

type LabHookOperationContracts = [
  Expect<Equal<LabJobsQuery, ApiQuery<'/api/lab/jobs', 'get'>>>,
  Expect<Equal<LabJobStatusPathParams, ApiPathParams<'/api/lab/jobs/{job_id}', 'get'>>>,
  Expect<Equal<LabJobCancelPathParams, ApiPathParams<'/api/lab/jobs/{job_id}/cancel', 'post'>>>,
  Expect<Equal<LabOptimizeRecommendationQuery, ApiQuery<'/api/lab/optimize/recommendation', 'get'>>>,
  Expect<Equal<Parameters<typeof useLabJobs>[0], NonNullable<LabJobsQuery['limit']> | undefined>>,
  Expect<Equal<Parameters<typeof useLabJobStatus>[0], LabJobStatusPathParams['job_id'] | null>>,
  Expect<
    Equal<
      Parameters<typeof useLabOptimizeRecommendation>,
      [
        strategyName: LabOptimizeRecommendationQuery['strategy_name'] | null,
        targetScope?: NonNullable<LabOptimizeRecommendationQuery['target_scope']>,
        allowedCategories?: NonNullable<LabOptimizeRecommendationQuery['allowed_categories']>,
      ]
    >
  >,
  Expect<Equal<Parameters<typeof labKeys.jobs>[0], LabJobsQuery['limit']>>,
  Expect<Equal<Parameters<typeof labKeys.job>[0], LabJobStatusPathParams['job_id']>>,
];

export type { LabHookOperationContracts };
