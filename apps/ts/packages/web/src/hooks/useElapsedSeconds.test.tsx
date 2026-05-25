import { act, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { useElapsedSeconds } from './useElapsedSeconds';

describe('useElapsedSeconds', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('tracks elapsed seconds while active', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2024-03-15T09:30:05Z'));

    const { result } = renderHook(() => useElapsedSeconds(true, '2024-03-15T09:30:00Z'));

    expect(result.current).toBe(5);

    act(() => {
      vi.advanceTimersByTime(2000);
    });

    expect(result.current).toBe(7);
  });

  it('resets when inactive or missing a start time', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2024-03-15T09:30:05Z'));

    const { result, rerender } = renderHook(({ isActive, startTime }) => useElapsedSeconds(isActive, startTime), {
      initialProps: {
        isActive: true,
        startTime: '2024-03-15T09:30:00Z' as string | null,
      },
    });

    expect(result.current).toBe(5);

    rerender({ isActive: false, startTime: '2024-03-15T09:30:00Z' });
    expect(result.current).toBe(0);

    rerender({ isActive: true, startTime: null });
    expect(result.current).toBe(0);
  });

  it('returns zero for invalid or future start times', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2024-03-15T09:30:05Z'));

    const { result, rerender } = renderHook(({ startTime }) => useElapsedSeconds(true, startTime), {
      initialProps: {
        startTime: 'not-a-date',
      },
    });

    expect(result.current).toBe(0);

    rerender({ startTime: '2024-03-15T09:30:10Z' });
    expect(result.current).toBe(0);
  });
});
