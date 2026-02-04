/** Zod schemas for Lab result data runtime validation. */

import { z } from 'zod';
import type {
  EvolutionHistoryItem,
  GenerateResultItem,
  ImprovementItem,
  LabEvolveResult,
  LabGenerateResult,
  LabImproveResult,
  LabOptimizeResult,
  LabResultData,
  OptimizeTrialItem,
} from './types.js';

// Sub-item schemas

const GenerateResultItemSchema = z.object({
  strategy_id: z.string(),
  score: z.number(),
  sharpe_ratio: z.number(),
  calmar_ratio: z.number(),
  total_return: z.number(),
  max_drawdown: z.number(),
  win_rate: z.number(),
  trade_count: z.number(),
  entry_signals: z.array(z.string()),
  exit_signals: z.array(z.string()),
});

const EvolutionHistoryItemSchema = z.object({
  generation: z.number(),
  best_score: z.number(),
  avg_score: z.number(),
  worst_score: z.number(),
});

const OptimizeTrialItemSchema = z.object({
  trial: z.number(),
  score: z.number(),
  params: z.record(z.string(), z.unknown()),
});

const ImprovementItemSchema = z.object({
  improvement_type: z.string(),
  target: z.string(),
  signal_name: z.string(),
  changes: z.record(z.string(), z.unknown()),
  reason: z.string(),
  expected_impact: z.string(),
});

// Variant schemas

const LabGenerateResultSchema = z.object({
  lab_type: z.literal('generate'),
  results: z.array(GenerateResultItemSchema),
  total_generated: z.number(),
  saved_strategy_path: z.string().optional(),
});

const LabEvolveResultSchema = z.object({
  lab_type: z.literal('evolve'),
  best_strategy_id: z.string(),
  best_score: z.number(),
  history: z.array(EvolutionHistoryItemSchema),
  saved_strategy_path: z.string().optional(),
  saved_history_path: z.string().optional(),
});

const LabOptimizeResultSchema = z.object({
  lab_type: z.literal('optimize'),
  best_score: z.number(),
  best_params: z.record(z.string(), z.unknown()),
  total_trials: z.number(),
  history: z.array(OptimizeTrialItemSchema),
  saved_strategy_path: z.string().optional(),
  saved_history_path: z.string().optional(),
});

const LabImproveResultSchema = z.object({
  lab_type: z.literal('improve'),
  strategy_name: z.string(),
  max_drawdown: z.number(),
  max_drawdown_duration_days: z.number(),
  suggested_improvements: z.array(ImprovementItemSchema),
  improvements: z.array(ImprovementItemSchema),
  saved_strategy_path: z.string().optional(),
});

// Discriminated union schema
export const LabResultDataSchema = z.discriminatedUnion('lab_type', [
  LabGenerateResultSchema,
  LabEvolveResultSchema,
  LabOptimizeResultSchema,
  LabImproveResultSchema,
]);

// Bidirectional compile-time type checks: catches drift in either direction
type Exact<A, B> = [A] extends [B] ? ([B] extends [A] ? true : never) : never;

const _typeCheck: [
  Exact<z.infer<typeof GenerateResultItemSchema>, GenerateResultItem>,
  Exact<z.infer<typeof EvolutionHistoryItemSchema>, EvolutionHistoryItem>,
  Exact<z.infer<typeof OptimizeTrialItemSchema>, OptimizeTrialItem>,
  Exact<z.infer<typeof ImprovementItemSchema>, ImprovementItem>,
  Exact<z.infer<typeof LabGenerateResultSchema>, LabGenerateResult>,
  Exact<z.infer<typeof LabEvolveResultSchema>, LabEvolveResult>,
  Exact<z.infer<typeof LabOptimizeResultSchema>, LabOptimizeResult>,
  Exact<z.infer<typeof LabImproveResultSchema>, LabImproveResult>,
] = [true, true, true, true, true, true, true, true];
void _typeCheck;

/** Validate unknown data against the LabResultData discriminated union schema. */
export function validateLabResultData(
  data: unknown
): { success: true; data: LabResultData } | { success: false; error: string } {
  const result = LabResultDataSchema.safeParse(data);
  if (result.success) {
    return { success: true, data: result.data as LabResultData };
  }
  return {
    success: false,
    error: result.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; '),
  };
}
