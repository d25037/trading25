import { describe, expect, it } from 'vitest';
import {
  addFundamentalSignalConfig,
  asStringArray,
  buildDefaultFundamentalConfig,
  buildSignalOptions,
  coerceNumber,
  dumpYamlObject,
  getSignalFieldDefaultValue,
  getValueAtPath,
  hasValueAtPath,
  normalizeSignalSection,
  parseYamlObject,
  removeFundamentalChildConfig,
  removeValueAtPath,
  safeDumpYaml,
  setValueAtPath,
  updateFundamentalChildConfig,
  updateFundamentalParentConfig,
  updateRegularSignalConfig,
} from './authoringUtils';

describe('authoringUtils', () => {
  it('parses object yaml and rejects non-object yaml', () => {
    expect(parseYamlObject('shared_config:\n  dataset: demo').value).toEqual({
      shared_config: { dataset: 'demo' },
    });
    expect(parseYamlObject('- item').error).toContain('Must be an object');
  });

  it('reports yaml parse failures', () => {
    const result = parseYamlObject('shared_config: [');

    expect(result.value).toBeNull();
    expect(result.error).toContain('YAML parse error:');
  });

  it('dumps yaml objects', () => {
    expect(dumpYamlObject({ shared_config: { dataset: 'demo' } })).toContain('dataset: demo');
  });

  it('falls back to json when safe yaml dump fails', () => {
    const nonYamlSafe = { fn: () => 'noop' };

    expect(safeDumpYaml(nonYamlSafe)).toBe('{}');
  });

  it('reads and checks nested paths', () => {
    const source = {
      shared_config: {
        execution_policy: {
          mode: 'standard',
        },
      },
    };

    expect(getValueAtPath(source, 'shared_config.execution_policy.mode')).toBe('standard');
    expect(getValueAtPath(source, 'shared_config.dataset')).toBeUndefined();
    expect(hasValueAtPath(source, 'shared_config.execution_policy.mode')).toBe(true);
    expect(hasValueAtPath(source, 'shared_config.dataset')).toBe(false);
  });

  it('normalizes non-object signal sections and visual compatibility checks', () => {
    expect(normalizeSignalSection(null)).toEqual({});
  });

  it('sets nested values without mutating the source', () => {
    const source = {
      shared_config: {
        dataset: 'default',
      },
    };

    const next = setValueAtPath(source, 'shared_config.execution_policy.mode', 'next_session_round_trip');

    expect(next).toEqual({
      shared_config: {
        dataset: 'default',
        execution_policy: {
          mode: 'next_session_round_trip',
        },
      },
    });
    expect(source).toEqual({
      shared_config: {
        dataset: 'default',
      },
    });
  });

  it('removes nested values and prunes empty parents', () => {
    const source = {
      shared_config: {
        execution_policy: {
          mode: 'standard',
        },
      },
    };

    expect(removeValueAtPath(source, 'shared_config.execution_policy.mode')).toEqual({});
    expect(removeValueAtPath(source, 'shared_config.missing')).toEqual(source);
  });

  it('filters string arrays and coerces numbers', () => {
    expect(asStringArray(['7203', 1234, '6758'])).toEqual(['7203', '6758']);
    expect(asStringArray('7203')).toEqual([]);
    expect(coerceNumber(' 1.5 ')).toBe(1.5);
    expect(coerceNumber('')).toBeNull();
    expect(coerceNumber('abc')).toBeNull();
  });

  it('resolves default signal field values', () => {
    expect(getSignalFieldDefaultValue({ type: 'boolean', default: undefined } as never)).toBe(false);
    expect(getSignalFieldDefaultValue({ type: 'number', default: undefined } as never)).toBe(0);
    expect(getSignalFieldDefaultValue({ type: 'string', default: undefined } as never)).toBe('');
    expect(getSignalFieldDefaultValue({ type: 'number', default: 5 } as never)).toBe(5);
  });

  it('builds signal options and updates signal configs', () => {
    const regularDefinitions = [
      {
        signal_type: 'volume_ratio_above',
        category: 'volume',
        exit_disabled: false,
      },
      {
        signal_type: 'entry_only',
        category: 'volume',
        exit_disabled: true,
      },
    ] as never;

    expect(
      buildSignalOptions(
        { volume_ratio_above: { enabled: true } },
        [{ key: 'volume', label: 'Volume' }],
        regularDefinitions,
        'exit_trigger_params'
      )
    ).toEqual([]);

    expect(
      updateRegularSignalConfig(
        { volume_ratio_above: { enabled: true } },
        'volume_ratio_above',
        { name: 'ratio_threshold' } as never,
        2
      )
    ).toEqual({ enabled: true, ratio_threshold: 2 });
  });

  it('builds and updates fundamental signal configs', () => {
    const parentFields = [
      { name: 'enabled', type: 'boolean', default: true },
      { name: 'period_type', type: 'select', default: 'FY' },
    ] as never;
    const defaultFundamental = buildDefaultFundamentalConfig(parentFields);
    const definition = {
      fields: [
        { name: 'enabled', type: 'boolean', default: true },
        { name: 'period_type', type: 'select', default: 'FY' },
        { name: 'threshold', type: 'number', default: 0.2 },
      ],
    } as never;

    expect(defaultFundamental).toEqual({ enabled: true, period_type: 'FY' });
    expect(
      addFundamentalSignalConfig({}, 'forward_eps_growth', definition, ['enabled', 'period_type'], defaultFundamental)
    ).toEqual({
      enabled: true,
      period_type: 'FY',
      forward_eps_growth: {
        enabled: true,
        threshold: 0.2,
      },
    });

    expect(updateFundamentalParentConfig({}, { name: 'period_type' } as never, 'Q', defaultFundamental)).toEqual({
      enabled: true,
      period_type: 'Q',
    });

    expect(
      updateFundamentalChildConfig(
        { enabled: true, period_type: 'FY', forward_eps_growth: { enabled: true } },
        'forward_eps_growth',
        { name: 'threshold' } as never,
        0.3,
        defaultFundamental
      )
    ).toEqual({
      enabled: true,
      period_type: 'FY',
      forward_eps_growth: {
        enabled: true,
        threshold: 0.3,
      },
    });

    expect(
      removeFundamentalChildConfig(
        { enabled: true, period_type: 'FY', forward_eps_growth: { enabled: true } },
        'forward_eps_growth',
        ['enabled', 'period_type']
      )
    ).toEqual({
      nextFundamental: {
        enabled: true,
        period_type: 'FY',
      },
      shouldRemoveSection: true,
    });
  });
});
