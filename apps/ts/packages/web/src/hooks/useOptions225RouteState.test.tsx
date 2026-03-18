import { act, renderHook } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { useMigrateOptions225RouteState, useOptions225RouteState } from './useOptions225RouteState';

const routeSearchState = {
  options225: {} as Record<string, unknown>,
};

const mockNavigate = vi.fn(
  (options: {
    to: '/options-225';
    search: Record<string, unknown> | ((current: Record<string, unknown>) => Record<string, unknown>);
  }) => {
    routeSearchState.options225 =
      typeof options.search === 'function' ? options.search(routeSearchState.options225) : options.search;
    return Promise.resolve();
  }
);

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mockNavigate,
}));

vi.mock('@/router', () => ({
  options225Route: { useSearch: () => routeSearchState.options225 },
}));

describe('useOptions225RouteState', () => {
  beforeEach(() => {
    routeSearchState.options225 = {};
    mockNavigate.mockClear();
  });

  it('reads defaults and preserves sequential updates in route search', () => {
    const { result } = renderHook(() => useOptions225RouteState());

    expect(result.current.date).toBeNull();
    expect(result.current.putCall).toBe('all');
    expect(result.current.contractMonth).toBeNull();
    expect(result.current.strikeMin).toBeNull();
    expect(result.current.strikeMax).toBeNull();
    expect(result.current.sortBy).toBe('openInterest');
    expect(result.current.order).toBe('desc');

    act(() => {
      result.current.setDate('2026-03-18');
      result.current.setPutCall('put');
      result.current.setContractMonth('2026-04');
      result.current.setStrikeRange(34000, 36000);
      result.current.setSort('volume', 'asc');
    });

    expect(routeSearchState.options225).toEqual({
      date: '2026-03-18',
      putCall: 'put',
      contractMonth: '2026-04',
      strikeMin: 34000,
      strikeMax: 36000,
      sortBy: 'volume',
      order: 'asc',
    });
    expect(mockNavigate).toHaveBeenCalledTimes(5);
  });

  it('reads normalized search values from the route', () => {
    routeSearchState.options225 = {
      date: '2026-03-18',
      putCall: 'call',
      contractMonth: '2026-05',
      strikeMin: 35000,
      strikeMax: 35500,
      sortBy: 'wholeDayClose',
      order: 'asc',
    };

    const { result } = renderHook(() => useOptions225RouteState());

    expect(result.current.date).toBe('2026-03-18');
    expect(result.current.putCall).toBe('call');
    expect(result.current.contractMonth).toBe('2026-05');
    expect(result.current.strikeMin).toBe(35000);
    expect(result.current.strikeMax).toBe(35500);
    expect(result.current.sortBy).toBe('wholeDayClose');
    expect(result.current.order).toBe('asc');
  });

  it('keeps migrate hook as a no-op', () => {
    renderHook(() => useMigrateOptions225RouteState());

    expect(mockNavigate).not.toHaveBeenCalled();
  });
});
