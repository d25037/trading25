import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useLabSSE } from './useLabSSE';

vi.mock('@/utils/logger', () => ({
  logger: {
    debug: vi.fn(),
    error: vi.fn(),
  },
}));

// Mock EventSource
class MockEventSource {
  static instances: MockEventSource[] = [];
  url: string;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  readyState = 0;
  closed = false;

  constructor(url: string) {
    this.url = url;
    MockEventSource.instances.push(this);
  }

  close() {
    this.closed = true;
    this.readyState = 2;
  }

  simulateOpen() {
    this.readyState = 1;
    this.onopen?.();
  }

  simulateMessage(data: Record<string, unknown>) {
    this.onmessage?.({ data: JSON.stringify(data) });
  }

  simulateError() {
    this.onerror?.();
  }
}

describe('useLabSSE', () => {
  beforeEach(() => {
    MockEventSource.instances = [];
    vi.stubGlobal('EventSource', MockEventSource);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('returns initial state when jobId is null', () => {
    const { result } = renderHook(() => useLabSSE(null));

    expect(result.current).toEqual({
      progress: null,
      message: null,
      status: null,
      isConnected: false,
    });
    expect(MockEventSource.instances).toHaveLength(0);
  });

  it('connects to SSE when jobId is provided', () => {
    const { result } = renderHook(() => useLabSSE('job-123'));

    expect(MockEventSource.instances).toHaveLength(1);
    expect(MockEventSource.instances[0]?.url).toBe('/api/lab/jobs/job-123/stream');

    // Simulate open
    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
    });

    expect(result.current.isConnected).toBe(true);
  });

  it('updates state on SSE message', () => {
    const { result } = renderHook(() => useLabSSE('job-123'));

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
    });

    act(() => {
      MockEventSource.instances[0]?.simulateMessage({
        status: 'running',
        progress: 0.5,
        message: 'Processing...',
      });
    });

    expect(result.current).toEqual({
      progress: 0.5,
      message: 'Processing...',
      status: 'running',
      isConnected: true,
    });
  });

  it('closes connection on terminal status', () => {
    const { result } = renderHook(() => useLabSSE('job-123'));

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
    });

    act(() => {
      MockEventSource.instances[0]?.simulateMessage({
        status: 'completed',
        progress: 1.0,
        message: 'Done',
      });
    });

    expect(result.current.status).toBe('completed');
    expect(result.current.isConnected).toBe(false);
    expect(MockEventSource.instances[0]?.closed).toBe(true);
  });

  it('closes connection on failed status', () => {
    const { result } = renderHook(() => useLabSSE('job-123'));

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
      MockEventSource.instances[0]?.simulateMessage({ status: 'failed' });
    });

    expect(result.current.status).toBe('failed');
    expect(result.current.isConnected).toBe(false);
  });

  it('closes connection on cancelled status', () => {
    const { result } = renderHook(() => useLabSSE('job-123'));

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
      MockEventSource.instances[0]?.simulateMessage({ status: 'cancelled' });
    });

    expect(result.current.status).toBe('cancelled');
    expect(result.current.isConnected).toBe(false);
  });

  it('retries on error up to MAX_RETRIES', () => {
    renderHook(() => useLabSSE('job-123'));

    expect(MockEventSource.instances).toHaveLength(1);

    // First error - should retry after 1s
    act(() => {
      MockEventSource.instances[0]?.simulateError();
    });
    expect(MockEventSource.instances[0]?.closed).toBe(true);

    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(MockEventSource.instances).toHaveLength(2);

    // Second error - should retry after 2s
    act(() => {
      MockEventSource.instances[1]?.simulateError();
    });

    act(() => {
      vi.advanceTimersByTime(2000);
    });
    expect(MockEventSource.instances).toHaveLength(3);

    // Third error - should retry after 3s
    act(() => {
      MockEventSource.instances[2]?.simulateError();
    });

    act(() => {
      vi.advanceTimersByTime(3000);
    });
    expect(MockEventSource.instances).toHaveLength(4);
  });

  it('stops retrying after MAX_RETRIES exceeded', () => {
    const { result } = renderHook(() => useLabSSE('job-123'));

    // Trigger 4 errors (MAX_RETRIES = 3, so 4th should not reconnect)
    for (let i = 0; i < 3; i++) {
      act(() => {
        const lastInstance = MockEventSource.instances[MockEventSource.instances.length - 1];
        lastInstance?.simulateError();
      });
      act(() => {
        vi.advanceTimersByTime((i + 1) * 1000);
      });
    }

    const countBeforeFinal = MockEventSource.instances.length;

    // 4th error - should NOT retry
    act(() => {
      const lastInstance = MockEventSource.instances[MockEventSource.instances.length - 1];
      lastInstance?.simulateError();
    });

    act(() => {
      vi.advanceTimersByTime(10000);
    });

    expect(MockEventSource.instances).toHaveLength(countBeforeFinal);
    expect(result.current.isConnected).toBe(false);
  });

  it('cleans up on unmount', () => {
    const { unmount } = renderHook(() => useLabSSE('job-123'));

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
    });

    unmount();

    expect(MockEventSource.instances[0]?.closed).toBe(true);
  });

  it('resets state when jobId changes to null', () => {
    const { result, rerender } = renderHook(({ jobId }) => useLabSSE(jobId), {
      initialProps: { jobId: 'job-123' as string | null },
    });

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
      MockEventSource.instances[0]?.simulateMessage({
        status: 'running',
        progress: 0.5,
        message: 'Working',
      });
    });

    expect(result.current.status).toBe('running');

    rerender({ jobId: null });

    expect(result.current).toEqual({
      progress: null,
      message: null,
      status: null,
      isConnected: false,
    });
    expect(MockEventSource.instances[0]?.closed).toBe(true);
  });

  it('reconnects when jobId changes', () => {
    const { rerender } = renderHook(({ jobId }) => useLabSSE(jobId), {
      initialProps: { jobId: 'job-1' as string | null },
    });

    expect(MockEventSource.instances).toHaveLength(1);
    expect(MockEventSource.instances[0]?.url).toBe('/api/lab/jobs/job-1/stream');

    rerender({ jobId: 'job-2' });

    expect(MockEventSource.instances[0]?.closed).toBe(true);
    expect(MockEventSource.instances).toHaveLength(2);
    expect(MockEventSource.instances[1]?.url).toBe('/api/lab/jobs/job-2/stream');
  });

  it('handles invalid JSON in message gracefully', () => {
    renderHook(() => useLabSSE('job-123'));

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
    });

    // Send invalid JSON
    act(() => {
      MockEventSource.instances[0]?.onmessage?.({ data: 'invalid-json' });
    });

    // Should not crash - logger.error should be called
  });

  it('clears pending reconnect timer on cleanup', () => {
    const { unmount } = renderHook(() => useLabSSE('job-123'));

    // Trigger error to start reconnect timer
    act(() => {
      MockEventSource.instances[0]?.simulateError();
    });

    // Unmount before timer fires
    unmount();

    // Timer should be cleared - no new EventSource created
    const countBefore = MockEventSource.instances.length;
    act(() => {
      vi.advanceTimersByTime(10000);
    });
    expect(MockEventSource.instances).toHaveLength(countBefore);
  });

  it('encodes jobId with special characters', () => {
    renderHook(() => useLabSSE('job/with spaces'));

    expect(MockEventSource.instances[0]?.url).toBe('/api/lab/jobs/job%2Fwith%20spaces/stream');
  });

  it('handles message with null fields', () => {
    const { result } = renderHook(() => useLabSSE('job-123'));

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
      MockEventSource.instances[0]?.simulateMessage({
        status: 'running',
      });
    });

    expect(result.current).toEqual({
      progress: null,
      message: null,
      status: 'running',
      isConnected: true,
    });
  });
});
