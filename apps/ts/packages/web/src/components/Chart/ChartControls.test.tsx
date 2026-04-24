import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  DEFAULT_FUNDAMENTAL_METRIC_ORDER,
  DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
} from '@/constants/fundamentalMetrics';
import {
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER,
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
} from '@/constants/fundamentalsHistoryMetrics';
import { ChartControls } from './ChartControls';

let selectedSymbol: string | null = null;
const mockOnSelectSymbol = vi.fn();

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
    },
  },
});

function TestWrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}

const mockUseSignalReference = vi.fn();
const mockUseStockSearch = vi.fn();

const SEARCH_RESULT = {
  code: '7203',
  companyName: 'Toyota Motor',
  marketName: 'Prime',
  sector33Name: '輸送用機器',
};

const mockChartStore = {
  settings: {
    timeframe: '1D' as const,
    displayTimeframe: 'daily' as const,
    chartType: 'candlestick' as const,
    showVolume: true,
    showPPOChart: false,
    showVolumeComparison: false,
    showTradingValueMA: false,
    showRecentReturnChart: false,
    showCMF: false,
    showChaikinOscillator: false,
    showOBVFlowScore: false,
    showRiskAdjustedReturnChart: false,
    showFundamentalsPanel: true,
    showFundamentalsHistoryPanel: true,
    showCostStructurePanel: true,
    showMarginPressurePanel: true,
    showFactorRegressionPanel: true,
    fundamentalsPanelOrder: [
      'fundamentals',
      'fundamentalsHistory',
      'costStructure',
      'marginPressure',
      'factorRegression',
    ],
    fundamentalsMetricOrder: [...DEFAULT_FUNDAMENTAL_METRIC_ORDER],
    fundamentalsMetricVisibility: { ...DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY },
    fundamentalsHistoryMetricOrder: [...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER],
    fundamentalsHistoryMetricVisibility: { ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY },
    visibleBars: 30,
    relativeMode: false,
    indicators: {
      sma: { enabled: false, period: 20 },
      ema: { enabled: false, period: 12 },
      vwema: { enabled: false, period: 20 },
      macd: { enabled: false, fast: 12, slow: 26, signal: 9 },
      ppo: { enabled: false, fast: 12, slow: 26, signal: 9 },
      atrSupport: { enabled: false, period: 20, multiplier: 3.0 },
      nBarSupport: { enabled: false, period: 60 },
      bollinger: { enabled: false, period: 20, deviation: 2.0 },
    },
    volumeComparison: {
      shortPeriod: 20,
      longPeriod: 100,
      lowerMultiplier: 1.0,
      higherMultiplier: 1.5,
    },
    tradingValueMA: {
      period: 15,
    },
    recentReturn: {
      shortPeriod: 20,
      longPeriod: 60,
    },
    accumulationFlow: {
      cmfPeriod: 20,
      chaikinFastPeriod: 3,
      chaikinSlowPeriod: 10,
      obvLookbackPeriod: 20,
    },
    riskAdjustedReturn: {
      lookbackPeriod: 60,
      ratioType: 'sortino' as const,
      threshold: 1.0,
      condition: 'above' as const,
    },
    signalOverlay: {
      enabled: false,
      signals: [] as Array<{
        type: string;
        enabled: boolean;
        mode: 'entry' | 'exit';
        params: Record<string, number | string | boolean>;
      }>,
    },
  },
  toggleSignalOverlay: vi.fn(),
  addSignal: vi.fn(),
  removeSignal: vi.fn(),
  updateSignal: vi.fn(),
  toggleSignal: vi.fn(),
  presets: [] as Array<{ id: string; name: string; settings: unknown; createdAt: number; updatedAt: number }>,
  activePresetId: null as string | null,
  createPreset: vi.fn(),
  updatePreset: vi.fn(),
  deletePreset: vi.fn(),
  loadPreset: vi.fn(),
  renamePreset: vi.fn(),
  duplicatePreset: vi.fn(),
  updateSettings: vi.fn(),
  toggleIndicator: vi.fn(),
  toggleRelativeMode: vi.fn(),
  setDisplayTimeframe: vi.fn(),
  updateIndicatorSettings: vi.fn(),
  updateVolumeComparison: vi.fn(),
  updateTradingValueMA: vi.fn(),
};

vi.mock('@/stores/chartStore', () => ({
  useChartStore: () => mockChartStore,
}));

vi.mock('@/hooks/useBacktest', () => ({
  useSignalReference: () => mockUseSignalReference(),
}));

vi.mock('@/hooks/useStockSearch', () => ({
  useStockSearch: (...args: unknown[]) => mockUseStockSearch(...args),
}));

vi.mock('@/components/Chart/SignalMarkers', () => ({
  SignalOverlayControls: () => <div>Signal Overlay Controls</div>,
}));

function renderChartControls() {
  return render(<ChartControls selectedSymbol={selectedSymbol} onSelectSymbol={mockOnSelectSymbol} />, {
    wrapper: TestWrapper,
  });
}

describe('ChartControls', () => {
  beforeEach(() => {
    vi.useRealTimers();
    selectedSymbol = null;
    mockOnSelectSymbol.mockReset();
    mockChartStore.settings.showFundamentalsPanel = true;
    mockChartStore.settings.showFundamentalsHistoryPanel = true;
    mockChartStore.settings.showCostStructurePanel = true;
    mockChartStore.settings.showMarginPressurePanel = true;
    mockChartStore.settings.showFactorRegressionPanel = true;
    mockChartStore.settings.fundamentalsPanelOrder = [
      'fundamentals',
      'fundamentalsHistory',
      'costStructure',
      'marginPressure',
      'factorRegression',
    ];
    mockChartStore.settings.fundamentalsMetricOrder = [...DEFAULT_FUNDAMENTAL_METRIC_ORDER];
    mockChartStore.settings.fundamentalsMetricVisibility = { ...DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY };
    mockChartStore.settings.fundamentalsHistoryMetricOrder = [...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER];
    mockChartStore.settings.fundamentalsHistoryMetricVisibility = {
      ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
    };
    mockChartStore.settings.signalOverlay.signals = [];
    mockChartStore.settings.showRecentReturnChart = false;
    mockChartStore.settings.recentReturn = {
      shortPeriod: 20,
      longPeriod: 60,
    };
    mockChartStore.settings.showCMF = false;
    mockChartStore.settings.showChaikinOscillator = false;
    mockChartStore.settings.showOBVFlowScore = false;
    mockChartStore.settings.accumulationFlow = {
      cmfPeriod: 20,
      chaikinFastPeriod: 3,
      chaikinSlowPeriod: 10,
      obvLookbackPeriod: 20,
    };
    mockChartStore.updateSettings = vi.fn();
    mockChartStore.toggleRelativeMode = vi.fn();
    mockUseSignalReference.mockReturnValue({ data: undefined, error: null });
    mockUseStockSearch.mockImplementation((query: string) => ({
      data: query ? { results: [SEARCH_RESULT] } : { results: [] },
      isLoading: false,
    }));
  });

  it('renders symbol search input and search button', () => {
    renderChartControls();

    expect(screen.getByPlaceholderText('銘柄コードまたは会社名で検索...')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /検索/i })).toBeInTheDocument();
  });

  it('uses non-password-search input attributes for symbol search', () => {
    renderChartControls();

    const input = screen.getByPlaceholderText('銘柄コードまたは会社名で検索...');
    const form = input.closest('form');

    expect(input).toHaveAttribute('type', 'search');
    expect(input).toHaveAttribute('name', 'symbol-search');
    expect(input).toHaveAttribute('autocomplete', 'off');
    expect(input).toHaveAttribute('inputmode', 'search');
    expect(input).toHaveAttribute('enterkeyhint', 'search');
    expect(input).toHaveAttribute('data-form-type', 'other');
    expect(input).toHaveAttribute('data-lpignore', 'true');
    expect(input).toHaveAttribute('data-1p-ignore', 'true');
    expect(form).toHaveAttribute('autocomplete', 'off');
  });

  it('submits symbol when form is submitted', async () => {
    const user = userEvent.setup();

    renderChartControls();

    const input = screen.getByPlaceholderText('銘柄コードまたは会社名で検索...');
    await user.type(input, '7203');
    await user.click(screen.getByRole('button', { name: /検索/i }));

    expect(mockOnSelectSymbol).toHaveBeenCalledWith('7203');
  });

  it('shows current symbol when selected', () => {
    selectedSymbol = '7203';

    renderChartControls();

    expect(screen.getByText('選択中: 7203')).toBeInTheDocument();
  });

  it('opens chart settings dialog and toggles volume setting', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Chart Settings' }));
    await user.click(screen.getByRole('switch', { name: /show volume/i }));

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showVolume: false });
  });

  it('opens chart settings dialog and toggles relative mode', async () => {
    const user = userEvent.setup();
    mockChartStore.toggleRelativeMode = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Chart Settings' }));
    await user.click(screen.getByRole('switch', { name: /relative to topix/i }));

    expect(mockChartStore.toggleRelativeMode).toHaveBeenCalled();
  });

  it('opens chart settings dialog and renders visible bars control', async () => {
    const user = userEvent.setup();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Chart Settings' }));
    expect(screen.getByText('Visible Bars')).toBeInTheDocument();
  });

  it('updates visible bars from chart settings', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Chart Settings' }));
    const dialog = screen.getByRole('dialog');
    await user.click(within(dialog).getByRole('combobox'));
    await user.click(screen.getByRole('option', { name: '60 bars' }));

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ visibleBars: 60 });
  });

  it('opens panel layout dialog and toggles fundamentals panel visibility', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Panel Layout' }));
    await user.click(screen.getByRole('switch', { name: /^Fundamentals$/i }));

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showFundamentalsPanel: false });
  });

  it('moves panel order down from panel layout dialog', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Panel Layout' }));
    const [firstDownButton] = screen.getAllByRole('button', { name: /^Down$/ });
    expect(firstDownButton).toBeDefined();
    if (!firstDownButton) return;
    await user.click(firstDownButton);

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      fundamentalsPanelOrder: [
        'fundamentalsHistory',
        'fundamentals',
        'costStructure',
        'marginPressure',
        'factorRegression',
      ],
    });
  });

  it('opens panel layout dialog and toggles cost structure panel visibility', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Panel Layout' }));
    await user.click(screen.getByRole('switch', { name: /^Cost Structure$/i }));

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showCostStructurePanel: false });
  });

  it('guards against stale panel-layout reorder actions', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Panel Layout' }));
    const [upButton] = screen.getAllByRole('button', { name: /^Up$/ });
    const [downButton] = screen.getAllByRole('button', { name: /^Down$/ });
    expect(upButton).toBeDefined();
    expect(downButton).toBeDefined();
    if (!upButton || !downButton) return;

    mockChartStore.settings.fundamentalsPanelOrder = [];
    fireEvent.click(downButton);
    expect(mockChartStore.updateSettings).not.toHaveBeenCalled();

    mockChartStore.settings.fundamentalsPanelOrder = [
      'fundamentals',
      'fundamentalsHistory',
      'costStructure',
      'marginPressure',
      'factorRegression',
    ];
    upButton.removeAttribute('disabled');
    fireEvent.click(upButton);
    expect(mockChartStore.updateSettings).not.toHaveBeenCalled();

    mockChartStore.settings.fundamentalsPanelOrder = [
      'fundamentals',
      undefined as unknown as 'fundamentalsHistory',
      'costStructure',
      'marginPressure',
      'factorRegression',
    ];
    fireEvent.click(downButton);
    expect(mockChartStore.updateSettings).not.toHaveBeenCalled();
  });

  it('opens fundamental metrics dialog and toggles metric visibility', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Fundamental Metrics' }));
    await user.click(screen.getByRole('switch', { name: /^PER$/i }));

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      fundamentalsMetricVisibility: {
        ...DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
        per: false,
      },
    });
  });

  it('moves fundamental metric order down from fundamental metrics dialog', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Fundamental Metrics' }));
    const [firstDownButton] = screen.getAllByRole('button', { name: /^Down$/ });
    expect(firstDownButton).toBeDefined();
    if (!firstDownButton) return;
    await user.click(firstDownButton);

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      fundamentalsMetricOrder: ['pbr', 'per', ...DEFAULT_FUNDAMENTAL_METRIC_ORDER.slice(2)],
    });
  });

  it('guards against stale fundamental metric reorder actions', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();
    const [firstFundamentalMetric, ...remainingFundamentalMetrics] = DEFAULT_FUNDAMENTAL_METRIC_ORDER;

    if (!firstFundamentalMetric) {
      throw new Error('DEFAULT_FUNDAMENTAL_METRIC_ORDER must not be empty');
    }

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Fundamental Metrics' }));
    const [upButton] = screen.getAllByRole('button', { name: /^Up$/ });
    const [downButton] = screen.getAllByRole('button', { name: /^Down$/ });
    expect(upButton).toBeDefined();
    expect(downButton).toBeDefined();
    if (!upButton || !downButton) return;

    mockChartStore.settings.fundamentalsMetricOrder = [];
    fireEvent.click(downButton);
    expect(mockChartStore.updateSettings).not.toHaveBeenCalled();

    mockChartStore.settings.fundamentalsMetricOrder = [...DEFAULT_FUNDAMENTAL_METRIC_ORDER];
    upButton.removeAttribute('disabled');
    fireEvent.click(upButton);
    expect(mockChartStore.updateSettings).not.toHaveBeenCalled();

    mockChartStore.settings.fundamentalsMetricOrder = [
      firstFundamentalMetric,
      undefined as unknown as (typeof DEFAULT_FUNDAMENTAL_METRIC_ORDER)[number],
      ...remainingFundamentalMetrics.slice(1),
    ];
    fireEvent.click(downButton);
    expect(mockChartStore.updateSettings).not.toHaveBeenCalled();
  });

  it('opens FY history metrics dialog and toggles metric visibility', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'FY History Metrics' }));
    await user.click(screen.getByRole('switch', { name: /^EPS$/i }));

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      fundamentalsHistoryMetricVisibility: {
        ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
        eps: false,
      },
    });
  });

  it('moves FY history metric order down from FY history metrics dialog', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'FY History Metrics' }));
    const [firstDownButton] = screen.getAllByRole('button', { name: /^Down$/ });
    expect(firstDownButton).toBeDefined();
    if (!firstDownButton) return;
    await user.click(firstDownButton);

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      fundamentalsHistoryMetricOrder: ['forecastEps', 'eps', ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER.slice(2)],
    });
  });

  it('guards against stale FY history metric reorder actions', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();
    const [firstHistoryMetric, ...remainingHistoryMetrics] = DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER;

    if (!firstHistoryMetric) {
      throw new Error('DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER must not be empty');
    }

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'FY History Metrics' }));
    const [upButton] = screen.getAllByRole('button', { name: /^Up$/ });
    const [downButton] = screen.getAllByRole('button', { name: /^Down$/ });
    expect(upButton).toBeDefined();
    expect(downButton).toBeDefined();
    if (!upButton || !downButton) return;

    mockChartStore.settings.fundamentalsHistoryMetricOrder = [];
    fireEvent.click(downButton);
    expect(mockChartStore.updateSettings).not.toHaveBeenCalled();

    mockChartStore.settings.fundamentalsHistoryMetricOrder = [...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER];
    upButton.removeAttribute('disabled');
    fireEvent.click(upButton);
    expect(mockChartStore.updateSettings).not.toHaveBeenCalled();

    mockChartStore.settings.fundamentalsHistoryMetricOrder = [
      firstHistoryMetric,
      undefined as unknown as (typeof DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER)[number],
      ...remainingHistoryMetrics.slice(1),
    ];
    fireEvent.click(downButton);
    expect(mockChartStore.updateSettings).not.toHaveBeenCalled();
  });

  it('opens sub-chart indicators dialog and toggles risk adjusted return', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Sub-Chart Indicators' }));
    await user.click(screen.getByRole('switch', { name: /risk adjusted return/i }));

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showRiskAdjustedReturnChart: true });
  });

  it('toggles recent return sub-chart', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Sub-Chart Indicators' }));
    await user.click(screen.getByRole('switch', { name: /recent return/i }));

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showRecentReturnChart: true });
  });

  it('updates sub-chart indicator numeric settings', async () => {
    const user = userEvent.setup();
    mockChartStore.settings.showRiskAdjustedReturnChart = true;
    mockChartStore.settings.showRecentReturnChart = true;
    mockChartStore.settings.showVolumeComparison = true;
    mockChartStore.settings.showTradingValueMA = true;
    mockChartStore.settings.showCMF = true;
    mockChartStore.settings.showChaikinOscillator = true;
    mockChartStore.settings.showOBVFlowScore = true;
    mockChartStore.updateVolumeComparison = vi.fn();
    mockChartStore.updateTradingValueMA = vi.fn();
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Sub-Chart Indicators' }));

    fireEvent.change(screen.getByLabelText('Lookback'), { target: { value: '80' } });
    fireEvent.change(screen.getByLabelText('Threshold'), { target: { value: '1.5' } });
    fireEvent.change(screen.getByLabelText('Short Lookback'), { target: { value: '15' } });
    fireEvent.change(screen.getByLabelText('Long Lookback'), { target: { value: '55' } });
    fireEvent.change(screen.getByLabelText('Short Period'), { target: { value: '25' } });
    fireEvent.change(screen.getByLabelText('Long Period'), { target: { value: '120' } });
    fireEvent.change(screen.getByLabelText('Lower Mult.'), { target: { value: '1.2' } });
    fireEvent.change(screen.getByLabelText('Higher Mult.'), { target: { value: '1.8' } });
    fireEvent.change(screen.getByLabelText('CMF Period'), { target: { value: '21' } });
    fireEvent.change(screen.getByLabelText('Chaikin Fast'), { target: { value: '4' } });
    fireEvent.change(screen.getByLabelText('Chaikin Slow'), { target: { value: '12' } });
    fireEvent.change(screen.getByLabelText('OBV Score Lookback'), { target: { value: '34' } });
    fireEvent.change(screen.getByLabelText('Period'), { target: { value: '18' } });

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      riskAdjustedReturn: expect.objectContaining({ lookbackPeriod: 80 }),
    });
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      riskAdjustedReturn: expect.objectContaining({ threshold: 1.5 }),
    });
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      recentReturn: expect.objectContaining({ shortPeriod: 15 }),
    });
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      recentReturn: expect.objectContaining({ longPeriod: 55 }),
    });
    expect(mockChartStore.updateVolumeComparison).toHaveBeenCalledWith({ shortPeriod: 25 });
    expect(mockChartStore.updateVolumeComparison).toHaveBeenCalledWith({ longPeriod: 120 });
    expect(mockChartStore.updateVolumeComparison).toHaveBeenCalledWith({ lowerMultiplier: 1.2 });
    expect(mockChartStore.updateVolumeComparison).toHaveBeenCalledWith({ higherMultiplier: 1.8 });
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      accumulationFlow: expect.objectContaining({ cmfPeriod: 21 }),
    });
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      accumulationFlow: expect.objectContaining({ chaikinFastPeriod: 4, chaikinSlowPeriod: 10 }),
    });
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      accumulationFlow: expect.objectContaining({ chaikinSlowPeriod: 12 }),
    });
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      accumulationFlow: expect.objectContaining({ obvLookbackPeriod: 34 }),
    });
    expect(mockChartStore.updateTradingValueMA).toHaveBeenCalledWith({ period: 18 });
  });

  it('updates ratio selects and toggles extra sub-chart panels', async () => {
    const user = userEvent.setup();
    mockChartStore.settings.showRiskAdjustedReturnChart = true;
    mockChartStore.settings.showVolumeComparison = false;
    mockChartStore.settings.showTradingValueMA = false;
    mockChartStore.updateSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Sub-Chart Indicators' }));

    const dialog = screen.getByRole('dialog');
    const [ratioTypeSelect, conditionSelect] = within(dialog).getAllByRole('combobox');
    expect(ratioTypeSelect).toBeDefined();
    expect(conditionSelect).toBeDefined();
    if (!ratioTypeSelect || !conditionSelect) return;

    await user.click(ratioTypeSelect);
    await user.click(screen.getByRole('option', { name: 'sharpe' }));
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      riskAdjustedReturn: expect.objectContaining({ ratioType: 'sharpe' }),
    });

    await user.click(conditionSelect);
    await user.click(screen.getByRole('option', { name: 'below' }));
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      riskAdjustedReturn: expect.objectContaining({ condition: 'below' }),
    });

    await user.click(screen.getByRole('switch', { name: /volume comparison/i }));
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showVolumeComparison: true });

    await user.click(screen.getByRole('switch', { name: /^cmf$/i }));
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showCMF: true });

    await user.click(screen.getByRole('switch', { name: /chaikin oscillator/i }));
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showChaikinOscillator: true });

    await user.click(screen.getByRole('switch', { name: /obv flow score/i }));
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showOBVFlowScore: true });

    await user.click(screen.getByRole('switch', { name: /trading value ma/i }));
    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showTradingValueMA: true });
  });

  it('opens and closes the signal overlay dialog', async () => {
    const user = userEvent.setup();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Signal Overlay' }));
    expect(screen.getByText('Signal Overlay Controls')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Close' }));
    await waitFor(() => expect(screen.queryByText('Signal Overlay Controls')).not.toBeInTheDocument());
  });

  it('opens overlay indicators dialog and toggles VWEMA', async () => {
    const user = userEvent.setup();
    mockChartStore.updateIndicatorSettings = vi.fn();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Overlay Indicators' }));
    await user.click(screen.getByRole('switch', { name: /vwema/i }));

    expect(mockChartStore.updateIndicatorSettings).toHaveBeenCalledWith('vwema', { enabled: true });
  });

  it('shows signal metadata in sub-chart indicators when reference API is available', async () => {
    const user = userEvent.setup();
    mockChartStore.settings.signalOverlay.signals = [
      { type: 'volume_ratio_above', enabled: true, mode: 'entry', params: {} },
    ];
    mockUseSignalReference.mockReturnValue({
      data: {
        signals: [
          {
            key: 'volume_ratio_above',
            name: 'volume_ratio_above',
            category: 'volume',
            description: '',
            usage_hint: '',
            fields: [],
            yaml_snippet: '',
            exit_disabled: false,
            data_requirements: ['volume'],
          },
        ],
        categories: [],
        total: 1,
      },
      error: null,
    });

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Sub-Chart Indicators' }));
    expect(screen.getAllByText('Signal req: volume | Signals: volume_ratio_above').length).toBeGreaterThan(0);
  });

  it('suppresses signal metadata when reference API fails', async () => {
    const user = userEvent.setup();
    mockChartStore.settings.signalOverlay.signals = [
      { type: 'volume_ratio_above', enabled: true, mode: 'entry', params: {} },
    ];
    mockUseSignalReference.mockReturnValue({
      data: undefined,
      error: new Error('failed to load'),
    });

    renderChartControls();

    await user.click(screen.getByRole('button', { name: 'Sub-Chart Indicators' }));
    expect(screen.queryByText(/Signal req:/i)).not.toBeInTheDocument();
  });

  it('renders search suggestions and selects stock by click', async () => {
    const user = userEvent.setup();

    renderChartControls();

    const input = screen.getByPlaceholderText('銘柄コードまたは会社名で検索...');
    await user.type(input, '7203');
    expect(await screen.findByText('Toyota Motor')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Toyota Motor/i }));

    expect(mockOnSelectSymbol).toHaveBeenCalledWith('7203');
  });

  it('ignores blank symbol submissions', async () => {
    const user = userEvent.setup();

    renderChartControls();

    await user.click(screen.getByRole('button', { name: /検索/i }));

    expect(mockOnSelectSymbol).not.toHaveBeenCalled();
  });

  it('supports keyboard navigation in search suggestions', async () => {
    const user = userEvent.setup();

    renderChartControls();

    const input = screen.getByPlaceholderText('銘柄コードまたは会社名で検索...');
    await user.type(input, '7');
    expect(await screen.findByText('Toyota Motor')).toBeInTheDocument();

    fireEvent.keyDown(input, { key: 'ArrowDown' });
    fireEvent.keyDown(input, { key: 'Enter' });

    expect(mockOnSelectSymbol).toHaveBeenCalledWith('7203');
  });

  it('closes search suggestions on escape and outside click', async () => {
    const user = userEvent.setup();

    renderChartControls();

    const input = screen.getByPlaceholderText('銘柄コードまたは会社名で検索...');
    await user.type(input, '7');
    expect(await screen.findByText('Toyota Motor')).toBeInTheDocument();
    fireEvent.keyDown(input, { key: 'Escape' });
    await waitFor(() => expect(screen.queryByText('Toyota Motor')).not.toBeInTheDocument());

    await user.type(input, '2');
    expect(await screen.findByText('Toyota Motor')).toBeInTheDocument();
    fireEvent.mouseDown(document.body);
    await waitFor(() => expect(screen.queryByText('Toyota Motor')).not.toBeInTheDocument());
  });
});
