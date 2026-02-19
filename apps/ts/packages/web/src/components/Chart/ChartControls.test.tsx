import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ChartControls } from './ChartControls';

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
  selectedSymbol: null as string | null,
  settings: {
    timeframe: '1D' as const,
    displayTimeframe: 'daily' as const,
    chartType: 'candlestick' as const,
    showVolume: true,
    showPPOChart: false,
    showVolumeComparison: false,
    showTradingValueMA: false,
    showRiskAdjustedReturnChart: false,
    showFundamentalsPanel: true,
    showFundamentalsHistoryPanel: true,
    showMarginPressurePanel: true,
    showFactorRegressionPanel: true,
    fundamentalsPanelOrder: ['fundamentals', 'fundamentalsHistory', 'marginPressure', 'factorRegression'],
    visibleBars: 30,
    relativeMode: false,
    indicators: {
      sma: { enabled: false, period: 20 },
      ema: { enabled: false, period: 12 },
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
  setSelectedSymbol: vi.fn(),
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

describe('ChartControls', () => {
  beforeEach(() => {
    vi.useRealTimers();
    mockChartStore.selectedSymbol = null;
    mockChartStore.settings.showFundamentalsPanel = true;
    mockChartStore.settings.showFundamentalsHistoryPanel = true;
    mockChartStore.settings.showMarginPressurePanel = true;
    mockChartStore.settings.showFactorRegressionPanel = true;
    mockChartStore.settings.fundamentalsPanelOrder = [
      'fundamentals',
      'fundamentalsHistory',
      'marginPressure',
      'factorRegression',
    ];
    mockChartStore.settings.signalOverlay.signals = [];
    mockChartStore.setSelectedSymbol = vi.fn();
    mockChartStore.updateSettings = vi.fn();
    mockChartStore.toggleRelativeMode = vi.fn();
    mockUseSignalReference.mockReturnValue({ data: undefined, error: null });
    mockUseStockSearch.mockImplementation((query: string) => ({
      data: query ? { results: [SEARCH_RESULT] } : { results: [] },
      isLoading: false,
    }));
  });

  it('renders symbol search input and search button', () => {
    render(<ChartControls />, { wrapper: TestWrapper });

    expect(screen.getByPlaceholderText('銘柄コードまたは会社名で検索...')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /検索/i })).toBeInTheDocument();
  });

  it('uses non-password-search input attributes for symbol search', () => {
    render(<ChartControls />, { wrapper: TestWrapper });

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
    mockChartStore.setSelectedSymbol = vi.fn();

    render(<ChartControls />, { wrapper: TestWrapper });

    const input = screen.getByPlaceholderText('銘柄コードまたは会社名で検索...');
    await user.type(input, '7203');
    await user.click(screen.getByRole('button', { name: /検索/i }));

    expect(mockChartStore.setSelectedSymbol).toHaveBeenCalledWith('7203');
  });

  it('shows current symbol when selected', () => {
    mockChartStore.selectedSymbol = '7203';

    render(<ChartControls />, { wrapper: TestWrapper });

    expect(screen.getByText('選択中: 7203')).toBeInTheDocument();
  });

  it('opens chart settings dialog and toggles volume setting', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    render(<ChartControls />, { wrapper: TestWrapper });

    await user.click(screen.getByRole('button', { name: 'Chart Settings' }));
    await user.click(screen.getByRole('switch', { name: /show volume/i }));

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showVolume: false });
  });

  it('opens chart settings dialog and toggles relative mode', async () => {
    const user = userEvent.setup();
    mockChartStore.toggleRelativeMode = vi.fn();

    render(<ChartControls />, { wrapper: TestWrapper });

    await user.click(screen.getByRole('button', { name: 'Chart Settings' }));
    await user.click(screen.getByRole('switch', { name: /relative to topix/i }));

    expect(mockChartStore.toggleRelativeMode).toHaveBeenCalled();
  });

  it('opens chart settings dialog and renders visible bars control', async () => {
    const user = userEvent.setup();

    render(<ChartControls />, { wrapper: TestWrapper });

    await user.click(screen.getByRole('button', { name: 'Chart Settings' }));
    expect(screen.getByText('Visible Bars')).toBeInTheDocument();
  });

  it('opens panel layout dialog and toggles fundamentals panel visibility', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    render(<ChartControls />, { wrapper: TestWrapper });

    await user.click(screen.getByRole('button', { name: 'Panel Layout' }));
    await user.click(screen.getByRole('switch', { name: /^Fundamentals$/i }));

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showFundamentalsPanel: false });
  });

  it('moves panel order down from panel layout dialog', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    render(<ChartControls />, { wrapper: TestWrapper });

    await user.click(screen.getByRole('button', { name: 'Panel Layout' }));
    const [firstDownButton] = screen.getAllByRole('button', { name: /^Down$/ });
    expect(firstDownButton).toBeDefined();
    if (!firstDownButton) return;
    await user.click(firstDownButton);

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({
      fundamentalsPanelOrder: ['fundamentalsHistory', 'fundamentals', 'marginPressure', 'factorRegression'],
    });
  });

  it('opens sub-chart indicators dialog and toggles risk adjusted return', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    render(<ChartControls />, { wrapper: TestWrapper });

    await user.click(screen.getByRole('button', { name: 'Sub-Chart Indicators' }));
    await user.click(screen.getByRole('switch', { name: /risk adjusted return/i }));

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showRiskAdjustedReturnChart: true });
  });

  it('shows signal metadata in sub-chart indicators when reference API is available', async () => {
    const user = userEvent.setup();
    mockChartStore.settings.signalOverlay.signals = [{ type: 'volume', enabled: true, mode: 'entry', params: {} }];
    mockUseSignalReference.mockReturnValue({
      data: {
        signals: [
          {
            key: 'volume',
            name: 'volume',
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

    render(<ChartControls />, { wrapper: TestWrapper });

    await user.click(screen.getByRole('button', { name: 'Sub-Chart Indicators' }));
    expect(screen.getAllByText('Signal req: volume | Signals: volume').length).toBeGreaterThan(0);
  });

  it('suppresses signal metadata when reference API fails', async () => {
    const user = userEvent.setup();
    mockChartStore.settings.signalOverlay.signals = [{ type: 'volume', enabled: true, mode: 'entry', params: {} }];
    mockUseSignalReference.mockReturnValue({
      data: undefined,
      error: new Error('failed to load'),
    });

    render(<ChartControls />, { wrapper: TestWrapper });

    await user.click(screen.getByRole('button', { name: 'Sub-Chart Indicators' }));
    expect(screen.queryByText(/Signal req:/i)).not.toBeInTheDocument();
  });

  it('renders search suggestions and selects stock by click', async () => {
    const user = userEvent.setup();
    mockChartStore.setSelectedSymbol = vi.fn();

    render(<ChartControls />, { wrapper: TestWrapper });

    const input = screen.getByPlaceholderText('銘柄コードまたは会社名で検索...');
    await user.type(input, '7203');
    expect(await screen.findByText('Toyota Motor')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Toyota Motor/i }));

    expect(mockChartStore.setSelectedSymbol).toHaveBeenCalledWith('7203');
  });

  it('supports keyboard navigation in search suggestions', async () => {
    const user = userEvent.setup();
    mockChartStore.setSelectedSymbol = vi.fn();

    render(<ChartControls />, { wrapper: TestWrapper });

    const input = screen.getByPlaceholderText('銘柄コードまたは会社名で検索...');
    await user.type(input, '7');
    expect(await screen.findByText('Toyota Motor')).toBeInTheDocument();

    fireEvent.keyDown(input, { key: 'ArrowDown' });
    fireEvent.keyDown(input, { key: 'Enter' });

    expect(mockChartStore.setSelectedSymbol).toHaveBeenCalledWith('7203');
  });

  it('closes search suggestions on escape and outside click', async () => {
    const user = userEvent.setup();

    render(<ChartControls />, { wrapper: TestWrapper });

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
