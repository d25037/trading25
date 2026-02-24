import '@testing-library/jest-dom';
import { vi } from 'vitest';

// Mock window.open for tests (only in browser environment)
if (typeof window !== 'undefined') {
  Object.defineProperty(window, 'open', {
    writable: true,
    value: vi.fn(),
  });
}

// Mock lightweight-charts
vi.mock('lightweight-charts', () => ({
  createChart: vi.fn(() => ({
    addSeries: vi.fn(() => ({
      setData: vi.fn(),
      priceScale: vi.fn(() => ({
        applyOptions: vi.fn(),
      })),
    })),
    timeScale: vi.fn(() => ({
      setVisibleRange: vi.fn(),
    })),
    applyOptions: vi.fn(),
    remove: vi.fn(),
    removeSeries: vi.fn(),
  })),
  CandlestickSeries: 'CandlestickSeries',
  HistogramSeries: 'HistogramSeries',
  LineSeries: 'LineSeries',
}));

// Mock ResizeObserver (only in browser environment)
if (typeof global !== 'undefined') {
  class MockResizeObserver {
    observe = vi.fn();
    unobserve = vi.fn();
    disconnect = vi.fn();
  }
  global.ResizeObserver = MockResizeObserver as unknown as typeof ResizeObserver;
}

// Radix Select relies on pointer capture APIs that are not implemented in happy-dom.
if (!Element.prototype.hasPointerCapture) {
  Element.prototype.hasPointerCapture = () => false;
}
if (!Element.prototype.setPointerCapture) {
  Element.prototype.setPointerCapture = () => {};
}
if (!Element.prototype.releasePointerCapture) {
  Element.prototype.releasePointerCapture = () => {};
}
