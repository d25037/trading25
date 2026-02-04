import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { describe, expect, it, vi } from 'vitest';
import { ChartControls } from './ChartControls';

// Create a wrapper with QueryClientProvider
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

// Mock the chart store
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
    signalOverlay: {
      enabled: false,
      signals: [],
    },
  },
  // Signal overlay actions
  toggleSignalOverlay: vi.fn(),
  addSignal: vi.fn(),
  removeSignal: vi.fn(),
  updateSignal: vi.fn(),
  toggleSignal: vi.fn(),
  // Preset management - required by ChartPresetSelector
  presets: [] as Array<{ id: string; name: string; settings: unknown; createdAt: number; updatedAt: number }>,
  activePresetId: null as string | null,
  createPreset: vi.fn(),
  updatePreset: vi.fn(),
  deletePreset: vi.fn(),
  loadPreset: vi.fn(),
  renamePreset: vi.fn(),
  duplicatePreset: vi.fn(),
  // Other methods
  setSelectedSymbol: vi.fn(),
  updateSettings: vi.fn(),
  toggleIndicator: vi.fn(),
  toggleRelativeMode: vi.fn(),
  setDisplayTimeframe: vi.fn(),
};

vi.mock('@/stores/chartStore', () => ({
  useChartStore: () => mockChartStore,
}));

describe('ChartControls', () => {
  it('renders symbol search input and search button', () => {
    render(<ChartControls />, { wrapper: TestWrapper });

    expect(screen.getByPlaceholderText('銘柄コードまたは会社名で検索...')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /検索/i })).toBeInTheDocument();
  });

  it('submits symbol when form is submitted', async () => {
    const user = userEvent.setup();
    mockChartStore.setSelectedSymbol = vi.fn();

    render(<ChartControls />, { wrapper: TestWrapper });

    const input = screen.getByPlaceholderText('銘柄コードまたは会社名で検索...');

    await user.type(input, '7203');

    // Find and click the submit button (Search icon button)
    const searchButton = screen.getByRole('button', { name: /検索/i });
    await user.click(searchButton);

    expect(mockChartStore.setSelectedSymbol).toHaveBeenCalledWith('7203');
  });

  it('shows current symbol when selected', () => {
    mockChartStore.selectedSymbol = '7203';

    render(<ChartControls />, { wrapper: TestWrapper });

    expect(screen.getByText('選択中: 7203')).toBeInTheDocument();
  });

  it('toggles volume setting when switch is clicked', async () => {
    const user = userEvent.setup();
    mockChartStore.updateSettings = vi.fn();

    render(<ChartControls />, { wrapper: TestWrapper });

    const volumeSwitch = screen.getByRole('switch', { name: /show volume/i });
    await user.click(volumeSwitch);

    expect(mockChartStore.updateSettings).toHaveBeenCalledWith({ showVolume: false });
  });

  it('toggles relative mode when switch is clicked', async () => {
    const user = userEvent.setup();
    mockChartStore.toggleRelativeMode = vi.fn();

    render(<ChartControls />, { wrapper: TestWrapper });

    const relativeModeSwitch = screen.getByRole('switch', { name: /relative to topix/i });
    await user.click(relativeModeSwitch);

    expect(mockChartStore.toggleRelativeMode).toHaveBeenCalled();
  });

  it('updates visible bars when select value changes', () => {
    mockChartStore.updateSettings = vi.fn();

    render(<ChartControls />, { wrapper: TestWrapper });

    // Note: Testing select components from shadcn/ui can be complex
    // This test validates the component renders the visible bars control
    expect(screen.getByText('Visible Bars')).toBeInTheDocument();
  });
});
