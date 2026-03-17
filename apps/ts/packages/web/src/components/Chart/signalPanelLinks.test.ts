import type { SignalConfig } from '@/stores/chartStore';
import type { SignalDefinition } from '@/types/backtest';
import { describe, expect, it } from 'vitest';
import { buildSignalPanelLinks } from './signalPanelLinks';

function makeSignal(type: string, enabled = true): SignalConfig {
  return {
    type,
    enabled,
    mode: 'entry',
    params: {},
  };
}

function makeDefinition(key: string, dataRequirements: string[]): SignalDefinition {
  return {
    key,
    signal_type: key.replace(/^fundamental_/, ''),
    name: key,
    category: 'test',
    description: '',
    usage_hint: '',
    fields: [],
    yaml_snippet: '',
    exit_disabled: false,
    data_requirements: dataRequirements,
    chart: {
      supported: true,
      supported_modes: ['entry', 'exit'],
      supports_relative_mode: true,
      requires_benchmark: false,
      requires_sector_data: false,
      requires_margin_data: false,
      requires_statements_data: false,
    },
  };
}

describe('buildSignalPanelLinks', () => {
  it('maps signal requirements to panel links', () => {
    const result = buildSignalPanelLinks({
      signals: [
        makeSignal('rsi_threshold'),
        makeSignal('volume_ratio_above'),
        makeSignal('per'),
        makeSignal('margin'),
        makeSignal('sector_strength_ranking'),
        makeSignal('beta'),
      ],
      definitions: [
        makeDefinition('rsi_threshold', ['ohlc']),
        makeDefinition('volume_ratio_above', ['volume']),
        makeDefinition('fundamental_per', ['statements:EPS']),
        makeDefinition('margin', ['margin']),
        makeDefinition('sector_strength_ranking', ['sector']),
        makeDefinition('beta', ['benchmark']),
      ],
    });

    expect(result.ppo.signalTypes).toEqual(['rsi_threshold']);
    expect(result.ppo.requirements).toEqual(['ohlc']);
    expect(result.riskAdjustedReturn.signalTypes).toEqual(['rsi_threshold']);
    expect(result.riskAdjustedReturn.requirements).toEqual(['ohlc']);

    expect(result.volumeComparison.signalTypes).toEqual(['volume_ratio_above']);
    expect(result.volumeComparison.requirements).toEqual(['volume']);

    expect(result.tradingValueMA.signalTypes).toEqual(['volume_ratio_above']);
    expect(result.tradingValueMA.requirements).toEqual(['volume']);

    expect(result.fundamentals.signalTypes).toEqual(['per']);
    expect(result.fundamentals.requirements).toEqual(['statements:EPS']);

    expect(result.fundamentalsHistory.signalTypes).toEqual(['per']);
    expect(result.fundamentalsHistory.requirements).toEqual(['statements:EPS']);

    expect(result.marginPressure.signalTypes).toEqual(['margin']);
    expect(result.marginPressure.requirements).toEqual(['margin']);

    expect(result.factorRegression.signalTypes).toEqual(['beta', 'sector_strength_ranking']);
    expect(result.factorRegression.requirements).toEqual(['benchmark', 'sector']);
  });

  it('ignores unknown and disabled signals', () => {
    const result = buildSignalPanelLinks({
      signals: [makeSignal('unknown_signal'), makeSignal('volume_ratio_above', false)],
      definitions: [makeDefinition('volume_ratio_above', ['volume'])],
    });

    expect(result.ppo.signalTypes).toEqual([]);
    expect(result.riskAdjustedReturn.signalTypes).toEqual([]);
    expect(result.volumeComparison.signalTypes).toEqual([]);
    expect(result.tradingValueMA.signalTypes).toEqual([]);
    expect(result.fundamentals.signalTypes).toEqual([]);
    expect(result.fundamentalsHistory.signalTypes).toEqual([]);
    expect(result.marginPressure.signalTypes).toEqual([]);
    expect(result.factorRegression.signalTypes).toEqual([]);
  });

  it('returns empty links when signal list or definitions are empty', () => {
    const withNoSignals = buildSignalPanelLinks({
      signals: [],
      definitions: [makeDefinition('rsi_threshold', ['ohlc'])],
    });
    const withNoDefinitions = buildSignalPanelLinks({
      signals: [makeSignal('rsi_threshold')],
      definitions: [],
    });

    expect(withNoSignals.ppo.signalTypes).toEqual([]);
    expect(withNoSignals.riskAdjustedReturn.signalTypes).toEqual([]);
    expect(withNoDefinitions.ppo.signalTypes).toEqual([]);
    expect(withNoDefinitions.riskAdjustedReturn.signalTypes).toEqual([]);
  });
});
