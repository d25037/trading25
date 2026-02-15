import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { CHART_COLORS } from '@/lib/constants';
import { RiskAdjustedReturnChart } from './RiskAdjustedReturnChart';

const mockChartStore = {
  settings: {
    visibleBars: 120,
  },
};

vi.mock('@/stores/chartStore', () => ({
  useChartStore: () => mockChartStore,
}));

const { mockCreateChart, mockChart, mockRatioSeries, mockThresholdSeries, mockTimeScale, mockLineSeries } =
  vi.hoisted(() => {
    const mockRatioSeries = {
      setData: vi.fn(),
      applyOptions: vi.fn(),
    };
    const mockThresholdSeries = {
      setData: vi.fn(),
      applyOptions: vi.fn(),
    };
    const mockTimeScale = {
      setVisibleLogicalRange: vi.fn(),
    };
    const mockChart = {
      addSeries: vi.fn(),
      timeScale: vi.fn(() => mockTimeScale),
      applyOptions: vi.fn(),
      remove: vi.fn(),
    };
    const mockCreateChart = vi.fn(() => mockChart);
    const mockLineSeries = 'LineSeries';

    return {
      mockCreateChart,
      mockChart,
      mockRatioSeries,
      mockThresholdSeries,
      mockTimeScale,
      mockLineSeries,
    };
  });

vi.mock('lightweight-charts', () => ({
  createChart: mockCreateChart,
  LineSeries: mockLineSeries,
}));

const sampleData = [
  { time: '2024-01-01', value: 0.8 },
  { time: '2024-01-02', value: 1.2 },
];

describe('RiskAdjustedReturnChart', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockChart.addSeries.mockReset().mockReturnValueOnce(mockRatioSeries).mockReturnValueOnce(mockThresholdSeries);
  });

  it('creates chart and renders ratio/threshold metadata', () => {
    render(
      <RiskAdjustedReturnChart
        data={sampleData}
        lookbackPeriod={60}
        ratioType="sortino"
        threshold={1}
        condition="above"
      />
    );

    expect(mockCreateChart).toHaveBeenCalled();
    expect(mockChart.addSeries).toHaveBeenCalledTimes(2);
    expect(mockRatioSeries.setData).toHaveBeenCalledWith([
      { time: '2024-01-01', value: 0.8 },
      { time: '2024-01-02', value: 1.2 },
    ]);
    expect(mockThresholdSeries.setData).toHaveBeenCalledWith([
      { time: '2024-01-01', value: 1 },
      { time: '2024-01-02', value: 1 },
    ]);
    expect(mockTimeScale.setVisibleLogicalRange).toHaveBeenCalledWith({ from: 0, to: 1.5 });
    expect(screen.getByText('Risk Adjusted Return')).toBeInTheDocument();
    expect(screen.getByText('60')).toBeInTheDocument();
    expect(screen.getByText('sortino')).toBeInTheDocument();
    expect(screen.getByText('above 1.00')).toBeInTheDocument();
    expect(screen.getByText('1.20')).toBeInTheDocument();
  });

  it('updates threshold color based on condition', () => {
    const { rerender } = render(
      <RiskAdjustedReturnChart
        data={sampleData}
        lookbackPeriod={60}
        ratioType="sortino"
        threshold={1}
        condition="above"
      />
    );

    expect(mockThresholdSeries.applyOptions).toHaveBeenCalledWith({ color: CHART_COLORS.UP });

    rerender(
      <RiskAdjustedReturnChart
        data={sampleData}
        lookbackPeriod={60}
        ratioType="sortino"
        threshold={1}
        condition="below"
      />
    );

    expect(mockThresholdSeries.applyOptions).toHaveBeenLastCalledWith({ color: CHART_COLORS.DOWN });
    expect(screen.getByText('below 1.00')).toBeInTheDocument();
  });

  it('clears chart data when data is empty', () => {
    render(
      <RiskAdjustedReturnChart
        data={[]}
        lookbackPeriod={30}
        ratioType="sharpe"
        threshold={0.5}
        condition="above"
      />
    );

    expect(mockRatioSeries.setData).toHaveBeenCalledWith([]);
    expect(mockThresholdSeries.setData).toHaveBeenCalledWith([]);
  });

  it('falls back to empty threshold line when time labels are falsy', () => {
    render(
      <RiskAdjustedReturnChart
        data={[{ time: '', value: 1.0 }]}
        lookbackPeriod={20}
        ratioType="sharpe"
        threshold={0.5}
        condition="above"
      />
    );

    expect(mockThresholdSeries.setData).toHaveBeenCalledWith([]);
  });

  it('applies resize options when resize observer callback runs', () => {
    let resizeCallback: (() => void) | undefined;

    class MockResizeObserver {
      observe = vi.fn();
      disconnect = vi.fn();

      constructor(callback: () => void) {
        resizeCallback = callback;
      }
    }

    vi.stubGlobal('ResizeObserver', MockResizeObserver as unknown as typeof ResizeObserver);

    const { container } = render(
      <RiskAdjustedReturnChart
        data={sampleData}
        lookbackPeriod={60}
        ratioType="sortino"
        threshold={1}
        condition="above"
      />
    );

    const chartContainer = container.querySelector('.flex-1.h-full') as HTMLDivElement;
    Object.defineProperty(chartContainer, 'clientWidth', { configurable: true, value: 320 });
    Object.defineProperty(chartContainer, 'clientHeight', { configurable: true, value: 160 });

    if (resizeCallback) {
      resizeCallback();
    }

    expect(mockChart.applyOptions).toHaveBeenCalledWith({ width: 320, height: 160 });

    vi.unstubAllGlobals();
  });

  it('cleans up chart on unmount', () => {
    const { unmount } = render(
      <RiskAdjustedReturnChart
        data={sampleData}
        lookbackPeriod={60}
        ratioType="sortino"
        threshold={1}
        condition="above"
      />
    );

    unmount();
    expect(mockChart.remove).toHaveBeenCalled();
  });
});
