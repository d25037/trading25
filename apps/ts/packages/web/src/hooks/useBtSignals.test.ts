import { renderHook, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { useSignalReference } from '@/hooks/useBacktest';
import { apiPost } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import type { SignalOverlaySettings } from '@/stores/chartStore';
import { btSignalKeys, buildSignalSpecs, useBtSignals } from './useBtSignals';

vi.mock('@/lib/api-client', () => ({
  apiPost: vi.fn(),
}));

vi.mock('@/hooks/useBacktest', () => ({
  useSignalReference: vi.fn(),
}));

const mockApiPost = vi.mocked(apiPost);
const mockUseSignalReference = vi.mocked(useSignalReference);

const manualSettings: SignalOverlaySettings = {
  enabled: true,
  signals: [
    {
      type: 'buy_and_hold',
      params: {},
      mode: 'entry',
      enabled: true,
    },
    {
      type: 'rsi_threshold',
      params: { threshold: 70, condition: 'above' },
      mode: 'exit',
      enabled: false,
    },
  ],
};

const relativeModeSettings: SignalOverlaySettings = {
  enabled: true,
  signals: [
    {
      type: 'buy_and_hold',
      params: {},
      mode: 'entry',
      enabled: true,
    },
    {
      type: 'trading_value',
      params: { period: 15, threshold_value: 100000000, direction: 'above' },
      mode: 'entry',
      enabled: true,
    },
  ],
};

describe('btSignalKeys', () => {
  it('builds stable query keys for manual and strategy overlays', () => {
    expect(btSignalKeys.compute('7203', 'daily', '{"specs":[],"relativeMode":false}', null)).toEqual([
      'bt-signals',
      'compute',
      '7203',
      'daily',
      '{"specs":[],"relativeMode":false}',
      'manual',
    ]);
    expect(
      btSignalKeys.compute('7203', 'weekly', '{"specs":[],"relativeMode":true}', 'production/demo')
    ).toEqual([
      'bt-signals',
      'compute',
      '7203',
      'weekly',
      '{"specs":[],"relativeMode":true}',
      'production/demo',
    ]);
  });
});

describe('buildSignalSpecs', () => {
  it('returns enabled signals only', () => {
    expect(buildSignalSpecs(manualSettings)).toEqual([
      {
        type: 'buy_and_hold',
        params: {},
        mode: 'entry',
      },
    ]);
  });

  it('returns empty specs when settings are absent', () => {
    expect(buildSignalSpecs(undefined)).toEqual([]);
  });
});

describe('useBtSignals', () => {
  beforeEach(() => {
    mockApiPost.mockReset();
    mockUseSignalReference.mockReset();
    mockUseSignalReference.mockReturnValue({ data: null } as never);
  });

  it('does not call the API when there is no runnable overlay', () => {
    const { wrapper } = createTestWrapper();
    renderHook(() => useBtSignals(null, 'daily', manualSettings, null, false), { wrapper });
    expect(mockApiPost).not.toHaveBeenCalled();
  });

  it('posts manual signals and maps successful markers in chronological order', async () => {
    mockApiPost.mockResolvedValueOnce({
      stock_code: '7203',
      timeframe: 'daily',
      strategy_name: null,
      signals: {
        buy_and_hold: {
          label: 'Buy & Hold',
          mode: 'entry',
          trigger_dates: ['2025-01-03', '2025-01-01'],
          count: 2,
        },
        rsi_threshold: {
          label: 'RSI',
          mode: 'exit',
          trigger_dates: ['2025-01-02'],
          count: 1,
          error: 'ignored',
        },
      },
      provenance: {
        source_kind: 'market',
        loaded_domains: ['stock_data'],
      },
      diagnostics: {},
    });

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useBtSignals('7203', 'daily', manualSettings, null, false), { wrapper });

    await waitFor(() => {
      expect(result.current.response).not.toBeNull();
    });

    expect(mockApiPost).toHaveBeenCalledWith('/api/signals/compute', {
      stock_code: '7203',
      source: 'market',
      timeframe: 'daily',
      signals: [
        {
          type: 'buy_and_hold',
          params: {},
          mode: 'entry',
        },
      ],
    });
    expect(result.current.markers).toEqual([
      {
        time: '2025-01-01',
        position: 'belowBar',
        color: '#26a69a',
        shape: 'arrowUp',
        text: 'Buy & Hold',
        size: 1,
      },
      {
        time: '2025-01-03',
        position: 'belowBar',
        color: '#26a69a',
        shape: 'arrowUp',
        text: 'Buy & Hold',
        size: 1,
      },
    ]);
  });

  it('filters unsupported manual signals in relative mode using signal reference metadata', async () => {
    mockUseSignalReference.mockReturnValue({
      data: {
        signals: [
          {
            signal_type: 'buy_and_hold',
            chart: { supports_relative_mode: true },
          },
          {
            signal_type: 'trading_value',
            chart: { supports_relative_mode: false },
          },
        ],
      },
    } as never);
    mockApiPost.mockResolvedValueOnce({
      stock_code: '7203',
      timeframe: 'daily',
      strategy_name: null,
      signals: {
        buy_and_hold: {
          label: 'Buy & Hold',
          mode: 'entry',
          trigger_dates: ['2025-01-03'],
          count: 1,
        },
      },
      provenance: {
        source_kind: 'market',
        loaded_domains: ['stock_data'],
      },
      diagnostics: {},
    });

    const { wrapper } = createTestWrapper();
    renderHook(() => useBtSignals('7203', 'daily', relativeModeSettings, null, true), { wrapper });

    await waitFor(() => {
      expect(mockApiPost).toHaveBeenCalledTimes(1);
    });

    expect(mockApiPost).toHaveBeenCalledWith('/api/signals/compute', {
      stock_code: '7203',
      source: 'market',
      timeframe: 'daily',
      signals: [
        {
          type: 'buy_and_hold',
          params: {},
          mode: 'entry',
        },
      ],
    });
  });

  it('refetches manual overlays when relative mode changes', async () => {
    mockUseSignalReference.mockReturnValue({ data: null } as never);
    mockApiPost
      .mockResolvedValueOnce({
        stock_code: '7203',
        timeframe: 'daily',
        strategy_name: null,
        signals: {},
        provenance: { source_kind: 'market', loaded_domains: ['stock_data'] },
        diagnostics: {},
      })
      .mockResolvedValueOnce({
        stock_code: '7203',
        timeframe: 'daily',
        strategy_name: null,
        signals: {},
        provenance: { source_kind: 'market', loaded_domains: ['stock_data'] },
        diagnostics: {},
      });

    const { wrapper } = createTestWrapper();
    const { rerender } = renderHook(
      ({ relativeMode }) => useBtSignals('7203', 'daily', relativeModeSettings, null, relativeMode),
      {
        wrapper,
        initialProps: { relativeMode: false },
      }
    );

    await waitFor(() => {
      expect(mockApiPost).toHaveBeenCalledTimes(1);
    });

    rerender({ relativeMode: true });

    await waitFor(() => {
      expect(mockApiPost).toHaveBeenCalledTimes(2);
    });

    expect(mockApiPost.mock.calls[0]?.[1]).toMatchObject({
      signals: [
        { type: 'buy_and_hold' },
        { type: 'trading_value' },
      ],
    });
    expect(mockApiPost.mock.calls[1]?.[1]).toMatchObject({
      signals: [{ type: 'buy_and_hold' }],
    });
  });

  it('posts strategy overlays and maps combined entry and exit markers', async () => {
    mockApiPost.mockResolvedValueOnce({
      stock_code: '7203',
      timeframe: 'weekly',
      strategy_name: 'production/demo',
      signals: {},
      combined_entry: {
        label: 'Demo entry',
        mode: 'entry',
        trigger_dates: ['2025-01-10'],
        count: 1,
      },
      combined_exit: {
        label: 'Demo exit',
        mode: 'exit',
        trigger_dates: ['2025-01-17'],
        count: 1,
      },
      provenance: {
        source_kind: 'market',
        loaded_domains: ['stock_data', 'statements'],
        strategy_name: 'production/demo',
      },
      diagnostics: {},
    });

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(
      () => useBtSignals('7203', 'weekly', { enabled: false, signals: [] }, 'production/demo', true),
      { wrapper }
    );

    await waitFor(() => {
      expect(result.current.response?.strategy_name).toBe('production/demo');
    });

    expect(mockApiPost).toHaveBeenCalledWith('/api/signals/compute', {
      stock_code: '7203',
      source: 'market',
      timeframe: 'weekly',
      strategy_name: 'production/demo',
    });
    expect(result.current.markers).toEqual([
      {
        time: '2025-01-10',
        position: 'belowBar',
        color: '#26a69a',
        shape: 'arrowUp',
        text: 'Demo entry',
        size: 1,
      },
      {
        time: '2025-01-17',
        position: 'aboveBar',
        color: '#ef5350',
        shape: 'arrowDown',
        text: 'Demo exit',
        size: 1,
      },
    ]);
  });
});
