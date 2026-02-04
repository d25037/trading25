import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { MarginVolumeRatioData } from '@/types/chart';
import { MarginRatioChart } from './MarginRatioChart';

// Mock lightweight-charts with vi.hoisted for ESM module support
const {
  mockSeries,
  mockTimeScale,
  mockChart,
  mockCreateChart,
  mockCandlestickSeries,
  mockHistogramSeries,
  mockLineSeries,
} = vi.hoisted(() => {
  const mockSeries = {
    setData: vi.fn(),
  };

  const mockTimeScale = {
    setVisibleRange: vi.fn(),
  };

  const mockChart = {
    addSeries: vi.fn(() => mockSeries),
    timeScale: vi.fn(() => mockTimeScale),
    applyOptions: vi.fn(),
    remove: vi.fn(),
  };

  const mockCreateChart = vi.fn(() => mockChart);
  const mockCandlestickSeries = 'CandlestickSeries';
  const mockHistogramSeries = 'HistogramSeries';
  const mockLineSeries = 'LineSeries';

  return {
    mockSeries,
    mockTimeScale,
    mockChart,
    mockCreateChart,
    mockCandlestickSeries,
    mockHistogramSeries,
    mockLineSeries,
  };
});

vi.mock('lightweight-charts', () => ({
  createChart: mockCreateChart,
  CandlestickSeries: mockCandlestickSeries,
  HistogramSeries: mockHistogramSeries,
  LineSeries: mockLineSeries,
}));

const mockMarginData: MarginVolumeRatioData[] = [
  {
    date: '2024-01-01',
    ratio: 2.5,
    marginVolume: 1000,
    weeklyAvgVolume: 400,
  },
  {
    date: '2024-01-02',
    ratio: 3.0,
    marginVolume: 1200,
    weeklyAvgVolume: 400,
  },
];

const mockLongData: MarginVolumeRatioData[] = [
  {
    date: '2024-01-01',
    ratio: 2.0,
    marginVolume: 800,
    weeklyAvgVolume: 400,
  },
  {
    date: '2024-01-02',
    ratio: 2.5,
    marginVolume: 1000,
    weeklyAvgVolume: 400,
  },
];

const mockShortData: MarginVolumeRatioData[] = [
  {
    date: '2024-01-01',
    ratio: 0.5,
    marginVolume: 200,
    weeklyAvgVolume: 400,
  },
  {
    date: '2024-01-02',
    ratio: 0.5,
    marginVolume: 200,
    weeklyAvgVolume: 400,
  },
];

describe('MarginRatioChart', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders chart container for ratio type', () => {
    render(<MarginRatioChart data={mockMarginData} title="Test Ratio Chart" type="ratio" />);

    const chartContainer = document.querySelector('div');
    expect(chartContainer).toBeInTheDocument();
  });

  it('renders chart container for comparison type', () => {
    render(
      <MarginRatioChart
        data={[]}
        title="Test Comparison Chart"
        longData={mockLongData}
        shortData={mockShortData}
        type="comparison"
      />
    );

    const chartContainer = document.querySelector('div');
    expect(chartContainer).toBeInTheDocument();
  });

  it('shows no data message when ratio data is empty', () => {
    render(<MarginRatioChart data={[]} title="Test Chart" type="ratio" />);

    expect(screen.getByText('No margin ratio data available')).toBeInTheDocument();
  });

  it('shows no data message when comparison data is empty', () => {
    render(<MarginRatioChart data={[]} title="Test Chart" longData={[]} shortData={[]} type="comparison" />);

    expect(screen.getByText('No margin ratio data available')).toBeInTheDocument();
  });

  it('does not show no data message when comparison has valid data', () => {
    render(
      <MarginRatioChart
        data={[]}
        title="Test Chart"
        longData={mockLongData}
        shortData={mockShortData}
        type="comparison"
      />
    );

    expect(screen.queryByText('No margin ratio data available')).not.toBeInTheDocument();
  });

  it('creates chart when component mounts', () => {
    render(<MarginRatioChart data={mockMarginData} title="Test Chart" type="ratio" />);

    expect(mockCreateChart).toHaveBeenCalledWith(
      expect.any(HTMLElement),
      expect.objectContaining({
        height: 280,
        timeScale: expect.objectContaining({
          timeVisible: true,
          secondsVisible: false,
        }),
      })
    );
  });

  it('adds line series for ratio type', () => {
    render(<MarginRatioChart data={mockMarginData} title="Test Ratio Chart" type="ratio" />);

    expect(mockChart.addSeries).toHaveBeenCalledWith(mockLineSeries, {
      color: '#059669',
      lineWidth: 2,
      title: 'Test Ratio Chart',
    });
  });

  it('adds two line series for comparison type', () => {
    render(
      <MarginRatioChart
        data={[]}
        title="Test Comparison Chart"
        longData={mockLongData}
        shortData={mockShortData}
        type="comparison"
      />
    );

    expect(mockChart.addSeries).toHaveBeenCalledTimes(2);
    expect(mockChart.addSeries).toHaveBeenCalledWith(mockLineSeries, {
      color: '#2563eb',
      lineWidth: 2,
      title: '買い残',
    });
    expect(mockChart.addSeries).toHaveBeenCalledWith(mockLineSeries, {
      color: '#dc2626',
      lineWidth: 2,
      title: '売り残',
    });
  });

  it('sets data for ratio chart', () => {
    render(<MarginRatioChart data={mockMarginData} title="Test Chart" type="ratio" />);

    expect(mockSeries.setData).toHaveBeenCalledWith([
      { time: '2024-01-01', value: 2.5 },
      { time: '2024-01-02', value: 3.0 },
    ]);
  });

  it('sets 6-month visible range when data is provided', () => {
    render(<MarginRatioChart data={mockMarginData} title="Test Chart" type="ratio" />);

    expect(mockTimeScale.setVisibleRange).toHaveBeenCalledWith({
      from: expect.any(Number),
      to: '2024-01-02',
    });
  });

  it('cleans up chart when component unmounts', () => {
    const { unmount } = render(<MarginRatioChart data={mockMarginData} title="Test Chart" type="ratio" />);

    unmount();

    expect(mockChart.remove).toHaveBeenCalled();
  });
});
