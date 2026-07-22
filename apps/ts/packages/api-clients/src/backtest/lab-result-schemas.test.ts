import { describe, expect, test } from 'bun:test';
import {
  LabEvolveResultSchema,
  LabOptimizeResultSchema,
  validateLabResultData,
} from './lab-result-schemas.js';

const validGenerateResult = {
  lab_type: 'generate' as const,
  results: [
    {
      strategy_id: 'strat_001',
      score: 85.5,
      sharpe_ratio: 1.2,
      calmar_ratio: 2.1,
      total_return: 0.35,
      max_drawdown: -0.12,
      win_rate: 0.6,
      trade_count: 42,
      entry_signals: ['sma_cross'],
      exit_signals: ['rsi_overbought'],
    },
  ],
  total_generated: 100,
  saved_strategy_path: '/path/to/strategy.yaml',
};

const validEvolveResult = {
  lab_type: 'evolve' as const,
  best_strategy_id: 'evolved_001',
  best_score: 92.3,
  history: [
    { generation: 1, best_score: 80.0, avg_score: 60.0, worst_score: 30.0 },
    { generation: 2, best_score: 85.0, avg_score: 65.0, worst_score: 35.0 },
  ],
  saved_strategy_path: '/path/to/evolved.yaml',
};

const validOptimizeResult = {
  lab_type: 'optimize' as const,
  best_score: 95.0,
  best_params: { sma_period: 20, rsi_threshold: 70 },
  total_trials: 50,
  history: [
    { trial: 1, score: 70.0, params: { sma_period: 10 } },
    { trial: 2, score: 80.0, params: { sma_period: 15 } },
  ],
};

const validImproveResult = {
  lab_type: 'improve' as const,
  strategy_name: 'my_strategy',
  max_drawdown: -0.15,
  max_drawdown_duration_days: 30,
  suggested_improvements: ['Reduce whipsaws'],
  improvements: [],
};

describe('validateLabResultData', () => {
  describe('valid data', () => {
    test('generate result passes', () => {
      const result = validateLabResultData(validGenerateResult);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.lab_type).toBe('generate');
      }
    });

    test('evolve result passes', () => {
      const result = validateLabResultData(validEvolveResult);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.lab_type).toBe('evolve');
      }
    });

    test('optimize result passes', () => {
      const result = validateLabResultData(validOptimizeResult);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.lab_type).toBe('optimize');
      }
    });

    test('improve result passes', () => {
      const result = validateLabResultData(validImproveResult);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.lab_type).toBe('improve');
      }
    });

    test('generate result without optional saved_strategy_path passes', () => {
      const { saved_strategy_path: _, ...withoutPath } = validGenerateResult;
      const result = validateLabResultData(withoutPath);
      expect(result.success).toBe(true);
    });

    test('nullable saved paths and omitted optional improvement changes pass', () => {
      const {
        entry_signals: _,
        exit_signals: __,
        ...generatedItemWithoutOptionalSignals
      } = validGenerateResult.results[0];
      const results = [
        validateLabResultData({
          ...validGenerateResult,
          results: [generatedItemWithoutOptionalSignals],
          saved_strategy_path: null,
        }),
        validateLabResultData({
          ...validEvolveResult,
          saved_strategy_path: null,
          saved_history_path: null,
        }),
        validateLabResultData({
          ...validOptimizeResult,
          saved_strategy_path: null,
          saved_history_path: null,
        }),
        validateLabResultData({
          ...validImproveResult,
          saved_strategy_path: null,
          improvements: [
            {
              improvement_type: 'parameter_tune',
              target: 'entry',
              signal_name: 'sma_cross',
              reason: 'Reduce whipsaws',
              expected_impact: 'moderate',
            },
          ],
        }),
      ];

      expect(results.every((result) => result.success)).toBe(true);
    });
  });

  test('evolve result preserves fast candidates', () => {
    const parsed = LabEvolveResultSchema.parse({
      lab_type: 'evolve',
      best_strategy_id: 'candidate-1',
      best_score: 1.25,
      history: [],
      fast_candidates: [
        { candidate_id: 'evolve_0001', rank: 1, score: 1.25, metrics: null },
      ],
      saved_strategy_path: null,
      saved_history_path: null,
    });

    expect(parsed.fast_candidates?.[0]?.candidate_id).toBe('evolve_0001');
  });

  test('optimize result preserves fast candidates', () => {
    const parsed = LabOptimizeResultSchema.parse({
      lab_type: 'optimize',
      best_score: 1.25,
      best_params: {},
      total_trials: 1,
      history: [],
      fast_candidates: [
        { candidate_id: 'optimize_0001', rank: 1, score: 1.25, metrics: null },
      ],
      saved_strategy_path: null,
      saved_history_path: null,
    });

    expect(parsed.fast_candidates?.[0]?.candidate_id).toBe('optimize_0001');
  });

  describe('invalid data', () => {
    test('missing required field returns error', () => {
      const { total_generated: _, ...incomplete } = validGenerateResult;
      const result = validateLabResultData(incomplete);
      expect(result.success).toBe(false);
    });

    test('invalid lab_type returns error', () => {
      const result = validateLabResultData({ ...validGenerateResult, lab_type: 'unknown' });
      expect(result.success).toBe(false);
    });

    test('wrong nested type returns error', () => {
      const bad = {
        ...validGenerateResult,
        results: [{ ...validGenerateResult.results[0], score: 'not_a_number' }],
      };
      const result = validateLabResultData(bad);
      expect(result.success).toBe(false);
    });

    test('null returns error', () => {
      const result = validateLabResultData(null);
      expect(result.success).toBe(false);
    });

    test('undefined returns error', () => {
      const result = validateLabResultData(undefined);
      expect(result.success).toBe(false);
    });

    test('empty object returns error', () => {
      const result = validateLabResultData({});
      expect(result.success).toBe(false);
    });

    test('error message includes field path', () => {
      const bad = {
        ...validGenerateResult,
        results: [{ ...validGenerateResult.results[0], score: 'bad' }],
      };
      const result = validateLabResultData(bad);
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error).toContain('score');
      }
    });
  });
});
