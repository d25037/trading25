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

  it('ignores null numeric constraints from signal reference payload', () => {
    const withNullConstraint: SignalDefinition[] = [
      {
        key: 'risk_adjusted_return',
        name: 'Risk Adjusted Return',
        category: 'fundamental',
        description: '',
        usage_hint: '',
        yaml_snippet: '',
        exit_disabled: false,
        data_requirements: [],
        fields: [
          { name: 'enabled', type: 'boolean', description: '' },
          {
            name: 'threshold',
            type: 'number',
            description: '',
            constraints: { ge: -5, le: 10, lt: null as unknown as number },
          },
        ],
      },
    ];

    const result = validateStrategyConfigLocally(
      {
        entry_filter_params: {
          risk_adjusted_return: {
            enabled: true,
            threshold: 1.0,
          },
        },
      },
      withNullConstraint
    );

    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
  });

  it('does not reject nested fundamental group key', () => {
    const result = validateStrategyConfigLocally(
      {
        entry_filter_params: {
          fundamental: {
            per: { enabled: true, threshold: 15 },
          },
        },
      },
      signalDefs
    );

    expect(result.errors).not.toContain('entry_filter_params.fundamental is not a valid signal name');
  });

  it('skips signal-name validation when signal reference is unavailable', () => {
    const result = validateStrategyConfigLocally(
      {
        entry_filter_params: {
          period_breakout: { enabled: true, period: 20 },
        },
      },
      []
    );

    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
    expect(result.warnings).toContain('Signal reference is unavailable, so parameter-name validation may be incomplete');
  });

  it('returns error when both entry and exit sections are missing', () => {
    const result = validateStrategyConfigLocally({}, signalDefs);
    expect(result.valid).toBe(false);
    expect(result.errors).toContain('entry_filter_params or exit_trigger_params is required');
  });

  it('returns error when signal section is not an object', () => {
    const result = validateStrategyConfigLocally(
      {
        entry_filter_params: 'invalid',
      },
      signalDefs
    );

    expect(result.valid).toBe(false);
    expect(result.errors).toContain('entry_filter_params must be an object');
  });

  it('returns error for unknown signal name', () => {
    const result = validateStrategyConfigLocally(
      {
        entry_filter_params: {
          unknown_signal: { enabled: true },
        },
      },
      signalDefs
    );

    expect(result.valid).toBe(false);
    expect(result.errors).toContain('entry_filter_params.unknown_signal is not a valid signal name');
  });

  it('returns error when signal config is not an object', () => {
    const result = validateStrategyConfigLocally(
      {
        entry_filter_params: {
          volume: true,
        },
      },
      signalDefs
    );

    expect(result.valid).toBe(false);
    expect(result.errors).toContain('entry_filter_params.volume must be an object');
  });

  it('returns error when nested fundamental value is not an object', () => {
    const result = validateStrategyConfigLocally(
      {
        entry_filter_params: {
          fundamental: true,
        },
      },
      signalDefs
    );

    expect(result.valid).toBe(false);
    expect(result.errors).toContain('entry_filter_params.fundamental must be an object');
  });

  it('validates primitive types and select option values', () => {
    const result = validateStrategyConfigLocally(
      {
        entry_filter_params: {
          volume: {
            enabled: 'true',
            direction: 'invalid',
            threshold: '1.0',
          },
        },
      },
      signalDefs
    );

    expect(result.valid).toBe(false);
    expect(result.errors).toContain('entry_filter_params.volume.enabled must be a boolean');
    expect(result.errors).toContain('entry_filter_params.volume.direction must be one of: surge, drop');
    expect(result.errors).toContain('entry_filter_params.volume.threshold must be a number');
  });

  it('validates numeric constraints for gt/ge/lt/le', () => {
    const constrainedDefs: SignalDefinition[] = [
      {
        key: 'constraint_signal',
        name: 'Constraint Signal',
        category: 'breakout',
        description: '',
        usage_hint: '',
        yaml_snippet: '',
        exit_disabled: false,
        data_requirements: [],
        fields: [
          { name: 'enabled', type: 'boolean', description: '' },
          { name: 'gt_only', type: 'number', description: '', constraints: { gt: 1 } },
          { name: 'ge_only', type: 'number', description: '', constraints: { ge: 1 } },
          { name: 'lt_only', type: 'number', description: '', constraints: { lt: 10 } },
          { name: 'le_only', type: 'number', description: '', constraints: { le: 10 } },
        ],
      },
    ];

    const result = validateStrategyConfigLocally(
      {
        entry_filter_params: {
          constraint_signal: {
            enabled: true,
            gt_only: 1,
            ge_only: 0.5,
            lt_only: 10,
            le_only: 11,
          },
        },
      },
      constrainedDefs
    );

    expect(result.valid).toBe(false);
    expect(result.errors).toContain('entry_filter_params.constraint_signal.gt_only must be > 1');
    expect(result.errors).toContain('entry_filter_params.constraint_signal.ge_only must be >= 1');
    expect(result.errors).toContain('entry_filter_params.constraint_signal.lt_only must be < 10');
    expect(result.errors).toContain('entry_filter_params.constraint_signal.le_only must be <= 10');
  });

  it('validates shared_config object shape and kelly type', () => {
    const nonObject = validateStrategyConfigLocally(
      {
        entry_filter_params: { volume: { enabled: true, direction: 'drop', threshold: 1.1 } },
        shared_config: 'invalid',
      },
      signalDefs
    );
    expect(nonObject.valid).toBe(false);
    expect(nonObject.errors).toContain('shared_config must be an object');

    const invalidFieldAndType = validateStrategyConfigLocally(
      {
        entry_filter_params: { volume: { enabled: true, direction: 'drop', threshold: 1.1 } },
        shared_config: { unknown_key: true, kelly_fraction: 'x' },
      },
      signalDefs
    );
    expect(invalidFieldAndType.valid).toBe(false);
    expect(invalidFieldAndType.errors).toContain('shared_config.unknown_key is not a valid parameter name');
    expect(invalidFieldAndType.errors).toContain('shared_config.kelly_fraction must be a number');
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
