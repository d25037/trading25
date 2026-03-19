import { render, screen } from '@testing-library/react';
import { createChart } from 'lightweight-charts';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { LinePricePoint } from './LinePriceChart';
import { LinePriceChart } from './LinePriceChart';
import { CHART_DIMENSIONS } from '@/lib/constants';

const mockSetData = vi.fn();
const mockFitContent = vi.fn();
const mockApplyOptions = vi.fn();
const mockRemove = vi.fn();

vi.mock('lightweight-charts', () => ({
  createChart: vi.fn(() => ({
    addSeries: vi.fn(() => ({
      setData: mockSetData,
    })),
    timeScale: vi.fn(() => ({
      fitContent: mockFitContent,
    })),
    applyOptions: mockApplyOptions,
    remove: mockRemove,
  })),
  LineSeries: 'LineSeries',
}));

function stubResizeObserver(): { trigger: () => void } {
  let resizeCallback: (() => void) | null = null;

  class ResizeObserverMock {
    observe = vi.fn();
    disconnect = vi.fn();

    constructor(callback: () => void) {
      resizeCallback = callback;
    }
  }

  vi.stubGlobal('ResizeObserver', ResizeObserverMock);
  return {
    trigger: () => resizeCallback?.(),
  };
}

const sampleData: LinePricePoint[] = [
  { time: '2026-02-10', value: 39000 },
  { time: '2026-02-11', value: 39200 },
];

describe('LinePriceChart', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    Object.defineProperty(HTMLElement.prototype, 'clientWidth', {
      configurable: true,
      get: () => 640,
    });
    Object.defineProperty(HTMLElement.prototype, 'clientHeight', {
      configurable: true,
      get: () => 400,
    });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('shows no data message when data is empty', () => {
    render(<LinePriceChart data={[]} />);

    expect(screen.getByText('No chart data available')).toBeInTheDocument();
    expect(mockSetData).toHaveBeenCalledWith([]);
  });

  it('sets line data and fits content when data is provided', () => {
    render(<LinePriceChart data={sampleData} />);

    expect(screen.queryByText('No chart data available')).not.toBeInTheDocument();
    expect(mockSetData).toHaveBeenCalledWith(sampleData);
    expect(mockFitContent).toHaveBeenCalled();
  });

  it('uses the default height when the container height is zero', () => {
    Object.defineProperty(HTMLElement.prototype, 'clientHeight', {
      configurable: true,
      get: () => 0,
    });

    render(<LinePriceChart data={sampleData} />);

    expect(vi.mocked(createChart).mock.calls[0]?.[1]).toMatchObject({
      width: 640,
      height: CHART_DIMENSIONS.DEFAULT_HEIGHT,
    });
  });

  it('resizes the chart when the container grows', () => {
    const resize = stubResizeObserver();

    render(<LinePriceChart data={sampleData} />);
    resize.trigger();

    expect(mockApplyOptions).toHaveBeenCalledWith({ width: 640, height: 400 });
  });

  it('skips resize updates when the container height is below the minimum', () => {
    const resize = stubResizeObserver();
    Object.defineProperty(HTMLElement.prototype, 'clientHeight', {
      configurable: true,
      get: () => CHART_DIMENSIONS.MIN_HEIGHT - 1,
    });

    render(<LinePriceChart data={sampleData} />);
    mockApplyOptions.mockClear();
    resize.trigger();

    expect(mockApplyOptions).not.toHaveBeenCalled();
  });

  it('ignores resize callbacks after the chart is removed', () => {
    const resize = stubResizeObserver();
    const { unmount } = render(<LinePriceChart data={sampleData} />);

    mockApplyOptions.mockClear();
    unmount();
    resize.trigger();

    expect(mockApplyOptions).not.toHaveBeenCalled();
  });

  it('removes the chart on unmount', () => {
    const { unmount } = render(<LinePriceChart data={sampleData} />);

    unmount();

    expect(mockRemove).toHaveBeenCalled();
  });
});
