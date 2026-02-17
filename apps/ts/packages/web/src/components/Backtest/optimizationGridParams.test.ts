import { describe, expect, it } from 'vitest';
import { extractGridParameterEntries, formatGridParameterValue } from './optimizationGridParams';

describe('optimizationGridParams', () => {
  it('extracts nested parameter ranges from grid yaml', () => {
    const yamlContent = `
parameter_ranges:
  entry_filter_params:
    period_breakout:
      period: [10, 20, 30]
  exit_trigger_params:
    atr_stop:
      atr_multiplier: [1.5, 2.0]
`;

    expect(extractGridParameterEntries(yamlContent)).toEqual([
      {
        path: 'entry_filter_params.period_breakout.period',
        values: [10, 20, 30],
      },
      {
        path: 'exit_trigger_params.atr_stop.atr_multiplier',
        values: [1.5, 2.0],
      },
    ]);
  });

  it('returns empty array for invalid yaml', () => {
    expect(extractGridParameterEntries('invalid: [yaml')).toEqual([]);
  });

  it('formats parameter values for display', () => {
    expect(formatGridParameterValue(undefined)).toBe('undefined');
    expect(formatGridParameterValue('prime')).toBe('"prime"');
    expect(formatGridParameterValue(5)).toBe('5');
    expect(formatGridParameterValue(true)).toBe('true');
    expect(formatGridParameterValue({ min: 1 })).toBe('{"min":1}');
  });
});
