import { renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { apiPost } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { btMarginKeys, useBtMarginIndicators } from './useBtMarginIndicators';

vi.mock('@/lib/api-client', () => ({
  apiPost: vi.fn(),
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

const mockApiPost = apiPost as ReturnType<typeof vi.fn>;

// ===== btMarginKeys Tests =====

describe('btMarginKeys', () => {
  it('should generate correct compute key', () => {
    const key = btMarginKeys.compute('7203', 15);
    expect(key).toEqual(['bt-margin', '7203', 15]);
  });
});

// ===== useBtMarginIndicators Tests =====

describe('useBtMarginIndicators', () => {
  it('should not call API when symbol is null', () => {
    const { wrapper } = createTestWrapper();
    renderHook(() => useBtMarginIndicators(null), { wrapper });
    expect(mockApiPost).not.toHaveBeenCalled();
  });

  it('should call API with correct request body', async () => {
    mockApiPost.mockResolvedValueOnce({
      stock_code: '7203',
      indicators: {
        margin_long_pressure: [],
        margin_flow_pressure: [],
        margin_turnover_days: [],
      },
    });

    const { wrapper } = createTestWrapper();
    renderHook(() => useBtMarginIndicators('7203'), { wrapper });

    await waitFor(() => {
      expect(mockApiPost).toHaveBeenCalledWith('/api/indicators/margin', {
        stock_code: '7203',
        indicators: ['margin_long_pressure', 'margin_flow_pressure', 'margin_turnover_days'],
        average_period: 15,
      });
    });
  });

  it('should use custom period', async () => {
    mockApiPost.mockResolvedValueOnce({
      stock_code: '7203',
      indicators: {
        margin_long_pressure: [],
        margin_flow_pressure: [],
        margin_turnover_days: [],
      },
    });

    const { wrapper } = createTestWrapper();
    renderHook(() => useBtMarginIndicators('7203', 30), { wrapper });

    await waitFor(() => {
      expect(mockApiPost).toHaveBeenCalledWith(
        '/api/indicators/margin',
        expect.objectContaining({
          average_period: 30,
        })
      );
    });
  });

  it('should transform response correctly', async () => {
    mockApiPost.mockResolvedValueOnce({
      stock_code: '7203',
      indicators: {
        margin_long_pressure: [
          { date: '2024-01-01', pressure: 0.5, longVol: 200000, shortVol: 50000, avgVolume: 100000 },
        ],
        margin_flow_pressure: [
          {
            date: '2024-01-01',
            flowPressure: 0.1,
            currentNetMargin: 150000,
            previousNetMargin: 140000,
            avgVolume: 100000,
          },
        ],
        margin_turnover_days: [{ date: '2024-01-01', turnoverDays: 2.5, longVol: 200000, avgVolume: 100000 }],
      },
    });

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useBtMarginIndicators('7203'), { wrapper });

    await waitFor(() => {
      expect(result.current.data).toBeDefined();
    });

    const data = result.current.data;
    expect(data).toBeDefined();
    expect(data?.symbol).toBe('7203');
    expect(data?.averagePeriod).toBe(15);

    expect(data?.longPressure).toHaveLength(1);
    expect(data?.longPressure?.[0]).toEqual({
      date: '2024-01-01',
      pressure: 0.5,
      longVol: 200000,
      shortVol: 50000,
      avgVolume: 100000,
    });

    expect(data?.flowPressure).toHaveLength(1);
    expect(data?.flowPressure?.[0]).toEqual({
      date: '2024-01-01',
      flowPressure: 0.1,
      currentNetMargin: 150000,
      previousNetMargin: 140000,
      avgVolume: 100000,
    });

    expect(data?.turnoverDays).toHaveLength(1);
    expect(data?.turnoverDays?.[0]).toEqual({
      date: '2024-01-01',
      turnoverDays: 2.5,
      longVol: 200000,
      avgVolume: 100000,
    });
  });

  it('should handle empty indicator arrays', async () => {
    mockApiPost.mockResolvedValueOnce({
      stock_code: '7203',
      indicators: {},
    });

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useBtMarginIndicators('7203'), { wrapper });

    await waitFor(() => {
      expect(result.current.data).toBeDefined();
    });

    expect(result.current.data?.longPressure).toEqual([]);
    expect(result.current.data?.flowPressure).toEqual([]);
    expect(result.current.data?.turnoverDays).toEqual([]);
  });

  it('should handle API error', async () => {
    mockApiPost.mockRejectedValueOnce(new Error('Network error'));

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useBtMarginIndicators('7203'), { wrapper });

    await waitFor(() => {
      expect(result.current.error).toBeDefined();
    });

    expect(result.current.data).toBeUndefined();
  });
});
