import type { components } from '../clients/backtest/generated/bt-api-types';

type Schemas = components['schemas'];
type Assert<T extends true> = T;
type AssertFalse<T extends false> = T;
type HasKey<T, K extends PropertyKey> = K extends keyof T ? true : false;
type RemovedPolicyKey = `engine_${'policy'}`;
type RemovedVerificationSchema = `Verification${'Summary'}`;
type RemovedEngineValue = `nauti${'lus'}`;

export type _BacktestHasNoEngine = AssertFalse<HasKey<Schemas['BacktestRequest'], 'engine_family'>>;
export type _OptimizationHasNoPolicy = AssertFalse<HasKey<Schemas['OptimizationRequest'], RemovedPolicyKey>>;
export type _GenerateHasNoPolicy = AssertFalse<HasKey<Schemas['LabGenerateRequest'], RemovedPolicyKey>>;
export type _EvolveHasNoPolicy = AssertFalse<HasKey<Schemas['LabEvolveRequest'], RemovedPolicyKey>>;
export type _OptimizeHasNoPolicy = AssertFalse<HasKey<Schemas['LabOptimizeRequest'], RemovedPolicyKey>>;
export type _HasNoVerificationSchema = AssertFalse<
  RemovedVerificationSchema extends keyof Schemas ? true : false
>;
export type _RemovedEngineValueAbsent = AssertFalse<
  RemovedEngineValue extends Schemas['EngineFamily'] ? true : false
>;
export type _VectorbtRemains = Assert<'vectorbt' extends Schemas['EngineFamily'] ? true : false>;
export type _UnknownRemains = Assert<'unknown' extends Schemas['EngineFamily'] ? true : false>;
export type _FastCandidatesRemain = Assert<
  HasKey<Schemas['OptimizationJobResponse'], 'fast_candidates'>
>;
