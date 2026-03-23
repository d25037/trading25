import { describe, expect, it } from 'vitest';
import type { SignalDefinition } from '@/types/backtest';
import {
  buildDefaultDocumentAdvancedOnlyPaths,
  buildVisualAdvancedOnlyPaths,
  canVisualizeDefaultDocument,
  canVisualizeStrategyConfig,
  deriveFundamentalParentFieldNames,
} from './authoringDocumentUtils';

describe('authoringDocumentUtils', () => {
  it('checks strategy visual compatibility', () => {
    expect(canVisualizeStrategyConfig({ shared_config: [] })).toContain('shared_config');
    expect(canVisualizeStrategyConfig({ entry_filter_params: [] })).toContain('entry_filter_params');
    expect(canVisualizeStrategyConfig({ exit_trigger_params: [] })).toContain('exit_trigger_params');
    expect(canVisualizeStrategyConfig({ shared_config: {} })).toBeNull();
  });

  it('derives fundamental parent fields and strategy advanced-only paths', () => {
    const wrappedFundamentalDefinition = {
      key: 'fundamental_forward_eps_growth',
      yaml_snippet: `entry_filter_params:
  fundamental:
    enabled: true
    period_type: FY
    use_adjusted: true
    forward_eps_growth:
      enabled: true
      threshold: 0.2`,
      fields: [{ name: 'enabled' }, { name: 'period_type' }, { name: 'use_adjusted' }, { name: 'threshold' }],
    } as unknown as SignalDefinition;

    const rootFundamentalDefinition = {
      ...wrappedFundamentalDefinition,
      yaml_snippet: `fundamental:
  enabled: true
  period_type: FY
  use_adjusted: true
  forward_eps_growth:
    enabled: true
    threshold: 0.2`,
    } as SignalDefinition;

    expect(deriveFundamentalParentFieldNames([wrappedFundamentalDefinition])).toEqual([
      'enabled',
      'period_type',
      'use_adjusted',
    ]);
    expect(deriveFundamentalParentFieldNames([rootFundamentalDefinition])).toEqual([
      'enabled',
      'period_type',
      'use_adjusted',
    ]);
    expect(
      deriveFundamentalParentFieldNames([
        wrappedFundamentalDefinition,
        rootFundamentalDefinition,
        { ...wrappedFundamentalDefinition, yaml_snippet: 'entry_filter_params: {}' },
      ])
    ).toEqual(['enabled', 'period_type', 'use_adjusted']);
    expect(
      deriveFundamentalParentFieldNames([{ ...wrappedFundamentalDefinition, yaml_snippet: 'entry_filter_params: {}' }])
    ).toEqual(['enabled', 'period_type', 'use_adjusted']);

    const regularDefinitions = new Map([
      ['volume_ratio_above', { signal_type: 'volume_ratio_above' } as unknown as SignalDefinition],
    ]);
    const fundamentalDefinitions = new Map([['forward_eps_growth', wrappedFundamentalDefinition]]);

    expect(
      buildVisualAdvancedOnlyPaths(
        {
          custom_block: true,
          entry_filter_params: {
            unsupported_signal: {},
            fundamental: {
              enabled: true,
              rogue: false,
            },
          },
          exit_trigger_params: {
            volume_ratio_above: [],
          },
        },
        regularDefinitions,
        fundamentalDefinitions,
        ['enabled', 'period_type', 'use_adjusted'],
        new Set(['entry_filter_params', 'exit_trigger_params'])
      )
    ).toEqual([
      'custom_block',
      'entry_filter_params.fundamental.rogue',
      'entry_filter_params.unsupported_signal',
      'exit_trigger_params.volume_ratio_above',
    ]);
  });

  it('checks default document visual compatibility and advanced-only paths', () => {
    expect(canVisualizeDefaultDocument({})).toContain("default.yaml must contain a 'default' object");
    expect(canVisualizeDefaultDocument({ default: { execution: [] } })).toContain('default.execution');
    expect(canVisualizeDefaultDocument({ default: { parameters: [] } })).toContain('default.parameters');
    expect(canVisualizeDefaultDocument({ default: { parameters: { shared_config: [] } } })).toContain(
      'default.parameters.shared_config'
    );
    expect(
      canVisualizeDefaultDocument({
        default: {
          execution: {},
          parameters: {
            shared_config: {},
          },
        },
      })
    ).toBeNull();

    expect(
      buildDefaultDocumentAdvancedOnlyPaths({
        metadata: true,
        default: {
          mode: 'legacy',
          execution: {
            cash: 1000,
          },
          parameters: {
            shared_config: {
              dataset: 'demo',
            },
            extra: true,
          },
        },
      })
    ).toEqual(['default.mode', 'default.parameters.extra', 'metadata']);
  });
});
