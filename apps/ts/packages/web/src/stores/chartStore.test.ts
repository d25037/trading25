import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { defaultSettings, useChartStore } from './chartStore';

const resetChartStore = () => {
  useChartStore.setState({
    settings: structuredClone(defaultSettings),
    presets: [],
    activePresetId: null,
  });
};

describe('chartStore', () => {
  beforeEach(() => {
    const randomUUID = vi
      .fn()
      .mockReturnValueOnce('preset-1')
      .mockReturnValueOnce('preset-2')
      .mockReturnValue('preset-3');
    vi.stubGlobal('crypto', { randomUUID });
    useChartStore.persist?.clearStorage?.();
    resetChartStore();
  });

  afterEach(() => {
    useChartStore.persist?.clearStorage?.();
    vi.unstubAllGlobals();
  });

  it('updates settings and toggles indicators', () => {
    const { updateSettings, toggleIndicator, toggleRelativeMode, setDisplayTimeframe } = useChartStore.getState();

    updateSettings({ showVolume: false, chartType: 'line', showFundamentalsPanel: false });
    toggleIndicator('sma');
    toggleRelativeMode();
    setDisplayTimeframe('weekly');

    const state = useChartStore.getState();
    expect(state.settings.showVolume).toBe(false);
    expect(state.settings.showFundamentalsPanel).toBe(false);
    expect(state.settings.chartType).toBe('line');
    expect(state.settings.indicators.sma.enabled).toBe(true);
    expect(state.settings.relativeMode).toBe(true);
    expect(state.settings.displayTimeframe).toBe('weekly');
  });

  it('toggles indicator back to original state', () => {
    const { toggleIndicator } = useChartStore.getState();

    expect(useChartStore.getState().settings.indicators.ppo.enabled).toBe(true);
    toggleIndicator('ppo');
    expect(useChartStore.getState().settings.indicators.ppo.enabled).toBe(false);
    toggleIndicator('ppo');
    expect(useChartStore.getState().settings.indicators.ppo.enabled).toBe(true);
  });

  it('updates indicator settings', () => {
    const { updateIndicatorSettings } = useChartStore.getState();

    updateIndicatorSettings('sma', { period: 50 });
    expect(useChartStore.getState().settings.indicators.sma.period).toBe(50);
    expect(useChartStore.getState().settings.indicators.sma.enabled).toBe(false);

    updateIndicatorSettings('vwema', { enabled: true, period: 34 });
    expect(useChartStore.getState().settings.indicators.vwema.enabled).toBe(true);
    expect(useChartStore.getState().settings.indicators.vwema.period).toBe(34);

    updateIndicatorSettings('macd', { fast: 8, slow: 21 });
    expect(useChartStore.getState().settings.indicators.macd.fast).toBe(8);
    expect(useChartStore.getState().settings.indicators.macd.slow).toBe(21);
    expect(useChartStore.getState().settings.indicators.macd.signal).toBe(9);
  });

  it('updates volume comparison settings', () => {
    const { updateVolumeComparison } = useChartStore.getState();

    updateVolumeComparison({ shortPeriod: 10, higherMultiplier: 2.0 });

    const vc = useChartStore.getState().settings.volumeComparison;
    expect(vc.shortPeriod).toBe(10);
    expect(vc.higherMultiplier).toBe(2.0);
    expect(vc.longPeriod).toBe(100);
    expect(vc.lowerMultiplier).toBe(1.0);
  });

  it('updates trading value MA settings', () => {
    const { updateTradingValueMA } = useChartStore.getState();

    updateTradingValueMA({ period: 25 });

    expect(useChartStore.getState().settings.tradingValueMA.period).toBe(25);
  });

  it('manages signal overlay settings and signals', () => {
    const { toggleSignalOverlay, addSignal, updateSignal, toggleSignal, removeSignal } = useChartStore.getState();

    toggleSignalOverlay();
    expect(useChartStore.getState().settings.signalOverlay.enabled).toBe(true);

    addSignal({ type: 'volume_ratio_above', mode: 'entry', params: { ratio_threshold: 1.5 } });
    expect(useChartStore.getState().settings.signalOverlay.signals).toEqual([
      { type: 'volume_ratio_above', mode: 'entry', params: { ratio_threshold: 1.5 }, enabled: true },
    ]);

    addSignal({ type: 'volume_ratio_above', mode: 'exit', params: { ratio_threshold: 2.0 } });
    expect(useChartStore.getState().settings.signalOverlay.signals).toHaveLength(1);

    updateSignal('volume_ratio_above', { mode: 'exit', params: { ratio_threshold: 2.0 } });
    expect(useChartStore.getState().settings.signalOverlay.signals[0]).toMatchObject({
      type: 'volume_ratio_above',
      mode: 'exit',
      params: { ratio_threshold: 2.0 },
      enabled: true,
    });

    toggleSignal('volume_ratio_above');
    expect(useChartStore.getState().settings.signalOverlay.signals[0]?.enabled).toBe(false);

    removeSignal('volume_ratio_above');
    expect(useChartStore.getState().settings.signalOverlay.signals).toEqual([]);
  });

  it('creates, updates, duplicates, and deletes presets', () => {
    const { createPreset, updateSettings, updatePreset, renamePreset, duplicatePreset, loadPreset, deletePreset } =
      useChartStore.getState();

    const presetId = createPreset('Alpha');
    updateSettings({ visibleBars: 200 });
    updatePreset(presetId);
    renamePreset(presetId, 'Alpha+');

    const duplicateId = duplicatePreset(presetId, 'Alpha Copy');
    loadPreset(duplicateId);
    deletePreset(presetId);

    const state = useChartStore.getState();
    expect(state.presets).toHaveLength(1);
    expect(state.presets[0]?.name).toBe('Alpha Copy');
    expect(state.activePresetId).toBe(duplicateId);
    expect(state.settings.visibleBars).toBe(200);
  });

  it('creates preset with current settings snapshot', () => {
    const { updateSettings, createPreset } = useChartStore.getState();

    updateSettings({
      showVolume: false,
      chartType: 'area',
      showFundamentalsPanel: false,
      showFundamentalsHistoryPanel: false,
      showCostStructurePanel: false,
      showMarginPressurePanel: false,
      showFactorRegressionPanel: false,
    });
    const id = createPreset('Custom');

    const preset = useChartStore.getState().presets.find((p) => p.id === id);
    expect(preset?.settings.showVolume).toBe(false);
    expect(preset?.settings.chartType).toBe('area');
    expect(preset?.settings.showFundamentalsPanel).toBe(false);
    expect(preset?.settings.showFundamentalsHistoryPanel).toBe(false);
    expect(preset?.settings.showCostStructurePanel).toBe(false);
    expect(preset?.settings.showMarginPressurePanel).toBe(false);
    expect(preset?.settings.showFactorRegressionPanel).toBe(false);
    expect(useChartStore.getState().activePresetId).toBe(id);
  });

  it('loadPreset restores settings', () => {
    const { createPreset, updateSettings, loadPreset } = useChartStore.getState();

    const id = createPreset('Saved');
    updateSettings({
      chartType: 'line',
      showVolume: false,
      showFundamentalsPanel: false,
      showFundamentalsHistoryPanel: false,
      showCostStructurePanel: false,
      showMarginPressurePanel: false,
      showFactorRegressionPanel: false,
    });

    expect(useChartStore.getState().settings.chartType).toBe('line');
    expect(useChartStore.getState().settings.showFundamentalsPanel).toBe(false);

    loadPreset(id);

    expect(useChartStore.getState().settings.chartType).toBe('candlestick');
    expect(useChartStore.getState().settings.showVolume).toBe(true);
    expect(useChartStore.getState().settings.showFundamentalsPanel).toBe(true);
    expect(useChartStore.getState().settings.showFundamentalsHistoryPanel).toBe(true);
    expect(useChartStore.getState().settings.showCostStructurePanel).toBe(true);
    expect(useChartStore.getState().settings.showMarginPressurePanel).toBe(true);
    expect(useChartStore.getState().settings.showFactorRegressionPanel).toBe(true);
  });

  it('loadPreset does nothing for non-existent preset', () => {
    const { loadPreset } = useChartStore.getState();

    loadPreset('nonexistent');

    expect(useChartStore.getState().settings).toEqual(defaultSettings);
    expect(useChartStore.getState().activePresetId).toBeNull();
  });

  it('duplicatePreset returns empty string for non-existent preset', () => {
    const { duplicatePreset } = useChartStore.getState();

    const result = duplicatePreset('nonexistent', 'Copy');

    expect(result).toBe('');
    expect(useChartStore.getState().presets).toHaveLength(0);
  });

  it('deletePreset clears activePresetId when deleting active preset', () => {
    const { createPreset, deletePreset } = useChartStore.getState();

    const id = createPreset('Active');
    expect(useChartStore.getState().activePresetId).toBe(id);

    deletePreset(id);

    expect(useChartStore.getState().activePresetId).toBeNull();
  });

  it('deletePreset preserves activePresetId when deleting non-active preset', () => {
    const { createPreset, deletePreset } = useChartStore.getState();

    createPreset('First');
    const secondId = createPreset('Second');

    expect(useChartStore.getState().activePresetId).toBe(secondId);

    deletePreset('preset-1');

    expect(useChartStore.getState().activePresetId).toBe(secondId);
    expect(useChartStore.getState().presets).toHaveLength(1);
  });

  it('rehydrates legacy persisted settings with missing nested fields safely', async () => {
    localStorage.setItem(
      'trading25-chart-store',
      JSON.stringify({
        state: {
          selectedSymbol: '7203',
          settings: {
            displayTimeframe: 'weekly',
            indicators: {
              sma: { enabled: true, period: 50 },
            },
          },
          presets: [
            {
              id: 'legacy-preset',
              name: 'Legacy',
              settings: { showVolume: false },
              createdAt: 1,
              updatedAt: 2,
            },
          ],
          activePresetId: 'legacy-preset',
        },
        version: 0,
      })
    );

    await useChartStore.persist?.rehydrate();

    const state = useChartStore.getState();
    expect('selectedSymbol' in state).toBe(false);
    expect(state.settings.displayTimeframe).toBe('weekly');
    expect(state.settings.indicators.sma.enabled).toBe(true);
    expect(state.settings.indicators.sma.period).toBe(50);
    expect(state.settings.indicators.ema.period).toBe(defaultSettings.indicators.ema.period);
    expect(state.settings.indicators.vwema.period).toBe(defaultSettings.indicators.vwema.period);
    expect(state.settings.tradingValueMA.period).toBe(defaultSettings.tradingValueMA.period);
    expect(state.settings.accumulationFlow).toEqual(defaultSettings.accumulationFlow);
    expect(state.settings.showCMF).toBe(defaultSettings.showCMF);
    expect(state.settings.showChaikinOscillator).toBe(defaultSettings.showChaikinOscillator);
    expect(state.settings.showOBVFlowScore).toBe(defaultSettings.showOBVFlowScore);
    expect(state.settings.showFundamentalsPanel).toBe(defaultSettings.showFundamentalsPanel);
    expect(state.settings.showFundamentalsHistoryPanel).toBe(defaultSettings.showFundamentalsHistoryPanel);
    expect(state.settings.showCostStructurePanel).toBe(defaultSettings.showCostStructurePanel);
    expect(state.settings.showMarginPressurePanel).toBe(defaultSettings.showMarginPressurePanel);
    expect(state.settings.showFactorRegressionPanel).toBe(defaultSettings.showFactorRegressionPanel);
    expect(state.settings.fundamentalsMetricOrder).toEqual(defaultSettings.fundamentalsMetricOrder);
    expect(state.settings.fundamentalsMetricVisibility).toEqual(defaultSettings.fundamentalsMetricVisibility);
    expect(state.settings.fundamentalsHistoryMetricOrder).toEqual(defaultSettings.fundamentalsHistoryMetricOrder);
    expect(state.settings.fundamentalsHistoryMetricVisibility).toEqual(
      defaultSettings.fundamentalsHistoryMetricVisibility
    );
    expect(state.settings.signalOverlay.enabled).toBe(false);
    expect(state.settings.signalOverlay.signals).toEqual([]);
    expect(state.presets[0]?.settings.tradingValueMA.period).toBe(defaultSettings.tradingValueMA.period);
    expect(state.activePresetId).toBe('legacy-preset');
  });

  it('sanitizes invalid persisted scalar types during rehydrate', async () => {
    localStorage.setItem(
      'trading25-chart-store',
      JSON.stringify({
        state: {
          selectedSymbol: { code: '7203' },
          settings: {
            displayTimeframe: 123,
            chartType: 'invalid-type',
            showPPOChart: 'true',
            showFundamentalsPanel: 'false',
            visibleBars: '250',
            indicators: {
              ppo: {
                enabled: 'yes',
                fast: '12',
              },
            },
            signalOverlay: {
              enabled: 'true',
              signals: [{ type: 'foo', mode: 'entry', params: { period: 5 }, enabled: 'yes' }],
            },
          },
        },
        version: 0,
      })
    );

    await useChartStore.persist?.rehydrate();

    const state = useChartStore.getState();
    expect('selectedSymbol' in state).toBe(false);
    expect(state.settings.displayTimeframe).toBe(defaultSettings.displayTimeframe);
    expect(state.settings.chartType).toBe(defaultSettings.chartType);
    expect(state.settings.showPPOChart).toBe(defaultSettings.showPPOChart);
    expect(state.settings.showCMF).toBe(defaultSettings.showCMF);
    expect(state.settings.showChaikinOscillator).toBe(defaultSettings.showChaikinOscillator);
    expect(state.settings.showOBVFlowScore).toBe(defaultSettings.showOBVFlowScore);
    expect(state.settings.showFundamentalsPanel).toBe(defaultSettings.showFundamentalsPanel);
    expect(state.settings.visibleBars).toBe(defaultSettings.visibleBars);
    expect(state.settings.indicators.ppo.enabled).toBe(defaultSettings.indicators.ppo.enabled);
    expect(state.settings.indicators.ppo.fast).toBe(defaultSettings.indicators.ppo.fast);
    expect(state.settings.fundamentalsMetricOrder).toEqual(defaultSettings.fundamentalsMetricOrder);
    expect(state.settings.fundamentalsMetricVisibility).toEqual(defaultSettings.fundamentalsMetricVisibility);
    expect(state.settings.fundamentalsHistoryMetricOrder).toEqual(defaultSettings.fundamentalsHistoryMetricOrder);
    expect(state.settings.fundamentalsHistoryMetricVisibility).toEqual(
      defaultSettings.fundamentalsHistoryMetricVisibility
    );
    expect(state.settings.signalOverlay.enabled).toBe(defaultSettings.signalOverlay.enabled);
    expect(state.settings.signalOverlay.signals).toHaveLength(1);
    expect(state.settings.signalOverlay.signals[0]?.enabled).toBe(true);
  });

  it('rehydrates persisted vwema settings safely', async () => {
    localStorage.setItem(
      'trading25-chart-store',
      JSON.stringify({
        state: {
          settings: {
            indicators: {
              vwema: {
                enabled: true,
                period: 34,
              },
            },
          },
        },
        version: 0,
      })
    );

    await useChartStore.persist?.rehydrate();

    const settings = useChartStore.getState().settings;
    expect(settings.indicators.vwema.enabled).toBe(true);
    expect(settings.indicators.vwema.period).toBe(34);
    expect(settings.indicators.sma.period).toBe(defaultSettings.indicators.sma.period);
  });

  it('defaults panel visibility flags to true', () => {
    const settings = useChartStore.getState().settings;
    expect(settings.showFundamentalsPanel).toBe(true);
    expect(settings.showFundamentalsHistoryPanel).toBe(true);
    expect(settings.showCostStructurePanel).toBe(true);
    expect(settings.showMarginPressurePanel).toBe(true);
    expect(settings.showFactorRegressionPanel).toBe(true);
  });

  it('defaults fundamentals panel order', () => {
    const settings = useChartStore.getState().settings;
    expect(settings.fundamentalsPanelOrder).toEqual([
      'fundamentals',
      'fundamentalsHistory',
      'costStructure',
      'marginPressure',
      'factorRegression',
    ]);
    expect(settings.fundamentalsMetricOrder).toEqual(defaultSettings.fundamentalsMetricOrder);
    expect(settings.fundamentalsMetricVisibility).toEqual(defaultSettings.fundamentalsMetricVisibility);
    expect(settings.fundamentalsHistoryMetricOrder).toEqual(defaultSettings.fundamentalsHistoryMetricOrder);
    expect(settings.fundamentalsHistoryMetricVisibility).toEqual(defaultSettings.fundamentalsHistoryMetricVisibility);
  });

  it('defaults risk adjusted return chart settings', () => {
    const settings = useChartStore.getState().settings;
    expect(settings.showRiskAdjustedReturnChart).toBe(false);
    expect(settings.riskAdjustedReturn).toEqual({
      lookbackPeriod: 60,
      ratioType: 'sortino',
      threshold: 1.0,
      condition: 'above',
    });
  });

  it('defaults recent return chart settings', () => {
    const settings = useChartStore.getState().settings;
    expect(settings.showRecentReturnChart).toBe(false);
    expect(settings.recentReturn).toEqual({
      shortPeriod: 20,
      longPeriod: 60,
    });
  });

  it('defaults accumulation-flow chart settings', () => {
    const settings = useChartStore.getState().settings;
    expect(settings.showCMF).toBe(false);
    expect(settings.showChaikinOscillator).toBe(false);
    expect(settings.showOBVFlowScore).toBe(false);
    expect(settings.accumulationFlow).toEqual({
      cmfPeriod: 20,
      chaikinFastPeriod: 3,
      chaikinSlowPeriod: 10,
      obvLookbackPeriod: 20,
    });
  });

  it('sanitizes invalid persisted accumulation-flow settings during rehydrate', async () => {
    localStorage.setItem(
      'trading25-chart-store',
      JSON.stringify({
        state: {
          settings: {
            showCMF: 'true',
            showChaikinOscillator: 'false',
            showOBVFlowScore: 1,
            accumulationFlow: {
              cmfPeriod: 'x',
              chaikinFastPeriod: 9,
              chaikinSlowPeriod: 3,
              obvLookbackPeriod: 0,
            },
          },
        },
        version: 0,
      })
    );

    await useChartStore.persist?.rehydrate();

    const settings = useChartStore.getState().settings;
    expect(settings.showCMF).toBe(defaultSettings.showCMF);
    expect(settings.showChaikinOscillator).toBe(defaultSettings.showChaikinOscillator);
    expect(settings.showOBVFlowScore).toBe(defaultSettings.showOBVFlowScore);
    expect(settings.accumulationFlow).toEqual({
      cmfPeriod: defaultSettings.accumulationFlow.cmfPeriod,
      chaikinFastPeriod: 9,
      chaikinSlowPeriod: 10,
      obvLookbackPeriod: 1,
    });
  });

  it('sanitizes invalid persisted risk adjusted return settings during rehydrate', async () => {
    localStorage.setItem(
      'trading25-chart-store',
      JSON.stringify({
        state: {
          settings: {
            showRiskAdjustedReturnChart: 'true',
            riskAdjustedReturn: {
              lookbackPeriod: 'x',
              ratioType: 'invalid',
              threshold: 'invalid',
              condition: 'invalid',
            },
          },
        },
        version: 0,
      })
    );

    await useChartStore.persist?.rehydrate();

    const settings = useChartStore.getState().settings;
    expect(settings.showRiskAdjustedReturnChart).toBe(defaultSettings.showRiskAdjustedReturnChart);
    expect(settings.riskAdjustedReturn).toEqual(defaultSettings.riskAdjustedReturn);
  });

  it('sanitizes invalid persisted recent return settings during rehydrate', async () => {
    localStorage.setItem(
      'trading25-chart-store',
      JSON.stringify({
        state: {
          settings: {
            showRecentReturnChart: 'true',
            recentReturn: {
              shortPeriod: 'x',
              longPeriod: 0,
            },
          },
        },
        version: 0,
      })
    );

    await useChartStore.persist?.rehydrate();

    const settings = useChartStore.getState().settings;
    expect(settings.showRecentReturnChart).toBe(defaultSettings.showRecentReturnChart);
    expect(settings.recentReturn).toEqual({
      shortPeriod: defaultSettings.recentReturn.shortPeriod,
      longPeriod: 1,
    });
  });

  it('sanitizes persisted fundamentals panel order during rehydrate', async () => {
    localStorage.setItem(
      'trading25-chart-store',
      JSON.stringify({
        state: {
          settings: {
            fundamentalsPanelOrder: ['marginPressure', 'invalid', 'marginPressure'],
            fundamentalsMetricOrder: ['eps', 'invalid', 'eps'],
            fundamentalsMetricVisibility: {
              eps: false,
              per: 'yes',
            },
            fundamentalsHistoryMetricOrder: ['roe', 'invalid', 'roe'],
            fundamentalsHistoryMetricVisibility: {
              roe: false,
              eps: 'no',
            },
          },
        },
        version: 0,
      })
    );

    await useChartStore.persist?.rehydrate();

    const settings = useChartStore.getState().settings;
    expect(settings.fundamentalsPanelOrder).toEqual([
      'marginPressure',
      'fundamentals',
      'fundamentalsHistory',
      'costStructure',
      'factorRegression',
    ]);
    expect(settings.fundamentalsMetricOrder).toEqual([
      'eps',
      ...defaultSettings.fundamentalsMetricOrder.filter((metricId) => metricId !== 'eps'),
    ]);
    expect(settings.fundamentalsMetricVisibility.eps).toBe(false);
    expect(settings.fundamentalsMetricVisibility.per).toBe(defaultSettings.fundamentalsMetricVisibility.per);
    expect(settings.fundamentalsHistoryMetricOrder).toEqual([
      'roe',
      ...defaultSettings.fundamentalsHistoryMetricOrder.filter((metricId) => metricId !== 'roe'),
    ]);
    expect(settings.fundamentalsHistoryMetricVisibility.roe).toBe(false);
    expect(settings.fundamentalsHistoryMetricVisibility.eps).toBe(
      defaultSettings.fundamentalsHistoryMetricVisibility.eps
    );
  });
});
