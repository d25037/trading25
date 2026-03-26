import { describe, expect, it } from 'vitest';
import { analyzeGridParameters, extractGridParameterEntries, formatGridParameterValue } from './optimizationGridParams';

describe('optimizationGridParams', () => {
  it('extracts nested parameter ranges from grid yaml', () => {
    const yamlContent = `
parameter_ranges:
  entry_filter_params:
    period_extrema_break:
      period: [10, 20, 30]
  exit_trigger_params:
    atr_stop:
      atr_multiplier: [1.5, 2.0]
`;

    expect(extractGridParameterEntries(yamlContent)).toEqual([
      {
        path: 'parameter_ranges.entry_filter_params.period_extrema_break.period',
        values: [10, 20, 30],
      },
      {
        path: 'parameter_ranges.exit_trigger_params.atr_stop.atr_multiplier',
        values: [1.5, 2.0],
      },
    ]);
  });

  it('returns empty array for invalid yaml', () => {
    expect(extractGridParameterEntries('invalid: [yaml')).toEqual([]);
  });

  it('returns parse error details for invalid yaml', () => {
    const analysis = analyzeGridParameters('invalid: [yaml');
    expect(analysis.parseError).toContain('YAML parse error:');
    expect(analysis.hasParameterRanges).toBe(false);
    expect(analysis.paramCount).toBe(0);
    expect(analysis.combinations).toBe(0);
    expect(analysis.valid).toBe(false);
  });

  it('returns warning-shaped analysis when parameter_ranges is missing', () => {
    const analysis = analyzeGridParameters('foo: 1');
    expect(analysis.parseError).toBeNull();
    expect(analysis.hasParameterRanges).toBe(false);
    expect(analysis.entries).toEqual([]);
    expect(analysis.paramCount).toBe(0);
    expect(analysis.combinations).toBe(0);
    expect(analysis.valid).toBe(true);
    expect(analysis.readyToRun).toBe(false);
    expect(analysis.warnings[0]?.path).toBe('parameter_ranges');
  });

  it('treats non-object root as validation error', () => {
    const analysis = analyzeGridParameters('- 1\n- 2');
    expect(analysis.parseError).toBeNull();
    expect(analysis.hasParameterRanges).toBe(false);
    expect(analysis.paramCount).toBe(0);
    expect(analysis.combinations).toBe(0);
    expect(analysis.valid).toBe(false);
    expect(analysis.errors[0]?.path).toBe('$');
  });

  it('reports invalid signal shapes as validation errors', () => {
    const analysis = analyzeGridParameters(`
parameter_ranges:
  entry_filter_params:
    ratio_threshold: [1.0, 1.5, 2.0]
`);

    expect(analysis.valid).toBe(false);
    expect(analysis.readyToRun).toBe(false);
    expect(analysis.errors[0]).toEqual({
      path: 'parameter_ranges.entry_filter_params.ratio_threshold',
      message: 'Signal must be a mapping of parameter names to candidate lists.',
    });
  });

  it('formats parameter values for display', () => {
    expect(formatGridParameterValue(undefined)).toBe('undefined');
    expect(formatGridParameterValue('prime')).toBe('"prime"');
    expect(formatGridParameterValue(5)).toBe('5');
    expect(formatGridParameterValue(true)).toBe('true');
    expect(formatGridParameterValue({ min: 1 })).toBe('{"min":1}');
  });
});
