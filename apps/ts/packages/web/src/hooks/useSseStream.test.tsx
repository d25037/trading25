import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { MockEventSource } from '@/test-utils/mockEventSource';
import { useSseStream } from './useSseStream';

describe('useSseStream', () => {
  beforeEach(() => {
    MockEventSource.reset();
    vi.stubGlobal('EventSource', MockEventSource);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('stays disconnected when url is null', () => {
    const { result } = renderHook(() => useSseStream({ url: null }));

    expect(result.current.isConnected).toBe(false);
    expect(MockEventSource.instances).toHaveLength(0);
  });

  it('connects and dispatches default and named events', () => {
    const messages: string[] = [];
    const namedEvents: Array<[string, string]> = [];
    const { result } = renderHook(() =>
      useSseStream({
        url: '/api/jobs/job-1/stream',
        eventNames: ['snapshot', 'job'],
        onMessage: (rawData) => messages.push(rawData),
        onEvent: (eventName, rawData) => namedEvents.push([eventName, rawData]),
      })
    );

    expect(MockEventSource.instances[0]?.url).toBe('/api/jobs/job-1/stream');

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
      MockEventSource.instances[0]?.simulateMessage('default-payload');
      MockEventSource.instances[0]?.simulateNamedMessage('snapshot', 'snapshot-payload');
      MockEventSource.instances[0]?.simulateNamedMessage('job', 'job-payload');
    });

    expect(result.current.isConnected).toBe(true);
    expect(messages).toEqual(['default-payload']);
    expect(namedEvents).toEqual([
      ['snapshot', 'snapshot-payload'],
      ['job', 'job-payload'],
    ]);
  });

  it('lets handlers close the current stream', () => {
    const { result } = renderHook(() =>
      useSseStream({
        url: '/api/jobs/job-1/stream',
        onMessage: (_rawData, controls) => controls.close(),
      })
    );

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
      MockEventSource.instances[0]?.simulateMessage('terminal');
    });

    expect(result.current.isConnected).toBe(false);
    expect(MockEventSource.instances[0]?.closed).toBe(true);
  });

  it('retries errors up to the configured maximum and resets when url changes', () => {
    const onMaxRetriesExceeded = vi.fn();
    const { rerender } = renderHook(
      ({ url }) =>
        useSseStream({
          url,
          maxRetries: 1,
          eventNames: ['job'],
          onMaxRetriesExceeded,
        }),
      {
        initialProps: { url: '/api/jobs/job-1/stream' },
      }
    );

    act(() => {
      MockEventSource.instances[0]?.simulateError();
      vi.advanceTimersByTime(1000);
    });

    expect(MockEventSource.instances).toHaveLength(2);

    act(() => {
      MockEventSource.instances[1]?.simulateError();
    });

    expect(onMaxRetriesExceeded).toHaveBeenCalledOnce();

    rerender({ url: '/api/jobs/job-2/stream' });

    act(() => {
      MockEventSource.instances[2]?.simulateError();
      vi.advanceTimersByTime(1000);
    });

    expect(MockEventSource.instances[2]?.url).toBe('/api/jobs/job-2/stream');
    expect(MockEventSource.instances[3]?.url).toBe('/api/jobs/job-2/stream');
  });
});
