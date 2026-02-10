import { describe, expect, it } from 'vitest';
import type { SignalDefinition } from '@/types/backtest';
import { mergeValidationResults, validateStrategyConfigLocally } from './strategyValidation';

const signalDefs: SignalDefinition[] = [
  {
    key: 'volume',
    name: 'Volume',
    category: 'volume',
    description: '',
    usage_hint: '',
    yaml_snippet: '',
    exit_disabled: false,
    data_requirements: [],
    fields: [
      { name: 'enabled', type: 'boolean', description: '' },
      { name: 'direction', type: 'select', description: '', options: ['surge', 'drop'] },
      { name: 'threshold', type: 'number', description: '', constraints: { gt: 0 } },
    ],
  },
];

describe('validateStrategyConfigLocally', () => {
  it('returns error for invalid signal parameter name', () => {
    const result = validateStrategyConfigLocally(
      {
        entry_filter_params: {
          volume: {
            enabled: true,
            unknown_param: 123,
          },
        },
      },
      signalDefs
    );

    expect(result.valid).toBe(false);
    expect(result.errors).toContain('entry_filter_params.volume.unknown_param is not a valid parameter name');
  });

  it('validates kelly_fraction max as 2', () => {
    const result = validateStrategyConfigLocally(
      {
        entry_filter_params: { volume: { enabled: true, direction: 'surge', threshold: 1.2 } },
        shared_config: { kelly_fraction: 2.1 },
      },
      signalDefs
    );

    expect(result.valid).toBe(false);
    expect(result.errors).toContain('shared_config.kelly_fraction must be between 0 and 2');
  });

  it('passes valid config', () => {
    const result = validateStrategyConfigLocally(
      {
        entry_filter_params: { volume: { enabled: true, direction: 'drop', threshold: 1.1 } },
        shared_config: { kelly_fraction: 2 },
      },
      signalDefs
    );

    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
  });
});

describe('mergeValidationResults', () => {
  it('merges errors and warnings from multiple sources', () => {
    const merged = mergeValidationResults(
      { valid: false, errors: ['error-a'], warnings: [] },
      { valid: true, errors: [], warnings: ['warning-a'] }
    );

    expect(merged).toEqual({
      valid: false,
      errors: ['error-a'],
      warnings: ['warning-a'],
    });
  });
});
