import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { defaultSettings, useChartStore } from './chartStore';

const resetChartStore = () => {
  useChartStore.setState({
    selectedSymbol: null,
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

  it('sets selected symbol', () => {
    const { setSelectedSymbol } = useChartStore.getState();

    setSelectedSymbol('7203');

    expect(useChartStore.getState().selectedSymbol).toBe('7203');
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
      showMarginPressurePanel: false,
      showFactorRegressionPanel: false,
    });
    const id = createPreset('Custom');

    const preset = useChartStore.getState().presets.find((p) => p.id === id);
    expect(preset?.settings.showVolume).toBe(false);
    expect(preset?.settings.chartType).toBe('area');
    expect(preset?.settings.showFundamentalsPanel).toBe(false);
    expect(preset?.settings.showFundamentalsHistoryPanel).toBe(false);
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
    expect(state.selectedSymbol).toBe('7203');
    expect(state.settings.displayTimeframe).toBe('weekly');
    expect(state.settings.indicators.sma.enabled).toBe(true);
    expect(state.settings.indicators.sma.period).toBe(50);
    expect(state.settings.indicators.ema.period).toBe(defaultSettings.indicators.ema.period);
    expect(state.settings.tradingValueMA.period).toBe(defaultSettings.tradingValueMA.period);
    expect(state.settings.showFundamentalsPanel).toBe(defaultSettings.showFundamentalsPanel);
    expect(state.settings.showFundamentalsHistoryPanel).toBe(defaultSettings.showFundamentalsHistoryPanel);
    expect(state.settings.showMarginPressurePanel).toBe(defaultSettings.showMarginPressurePanel);
    expect(state.settings.showFactorRegressionPanel).toBe(defaultSettings.showFactorRegressionPanel);
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
    expect(state.selectedSymbol).toBeNull();
    expect(state.settings.displayTimeframe).toBe(defaultSettings.displayTimeframe);
    expect(state.settings.chartType).toBe(defaultSettings.chartType);
    expect(state.settings.showPPOChart).toBe(defaultSettings.showPPOChart);
    expect(state.settings.showFundamentalsPanel).toBe(defaultSettings.showFundamentalsPanel);
    expect(state.settings.visibleBars).toBe(defaultSettings.visibleBars);
    expect(state.settings.indicators.ppo.enabled).toBe(defaultSettings.indicators.ppo.enabled);
    expect(state.settings.indicators.ppo.fast).toBe(defaultSettings.indicators.ppo.fast);
    expect(state.settings.signalOverlay.enabled).toBe(defaultSettings.signalOverlay.enabled);
    expect(state.settings.signalOverlay.signals).toHaveLength(1);
    expect(state.settings.signalOverlay.signals[0]?.enabled).toBe(true);
  });

  it('defaults panel visibility flags to true', () => {
    const settings = useChartStore.getState().settings;
    expect(settings.showFundamentalsPanel).toBe(true);
    expect(settings.showFundamentalsHistoryPanel).toBe(true);
    expect(settings.showMarginPressurePanel).toBe(true);
    expect(settings.showFactorRegressionPanel).toBe(true);
  });
});
