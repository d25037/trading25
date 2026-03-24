import { act, renderHook } from '@testing-library/react';
import type { UIEvent } from 'react';
import { describe, expect, it } from 'vitest';
import { useVirtualizedRows } from './useVirtualizedRows';

describe('useVirtualizedRows', () => {
  it('returns all items when virtualization is disabled', () => {
    const items = ['a', 'b', 'c'];
    const { result } = renderHook(() =>
      useVirtualizedRows(items, {
        enabled: false,
        rowHeight: 20,
        viewportHeight: 100,
      })
    );

    expect(result.current.visibleItems).toEqual(items);
    expect(result.current.startIndex).toBe(0);
    expect(result.current.endIndex).toBe(3);
    expect(result.current.paddingTop).toBe(0);
    expect(result.current.paddingBottom).toBe(0);
    expect(result.current.onScroll).toBeUndefined();
  });

  it('updates visible range on scroll when virtualization is enabled', () => {
    const items = Array.from({ length: 200 }, (_, i) => i);
    const { result } = renderHook(() =>
      useVirtualizedRows(items, {
        enabled: true,
        rowHeight: 10,
        viewportHeight: 50,
        overscan: 1,
      })
    );

    expect(result.current.startIndex).toBe(0);
    expect(result.current.endIndex).toBe(6);
    expect(result.current.visibleItems).toEqual([0, 1, 2, 3, 4, 5]);

    act(() => {
      result.current.onScroll?.({
        currentTarget: { scrollTop: 100 },
      } as UIEvent<HTMLElement>);
    });

    expect(result.current.startIndex).toBe(9);
    expect(result.current.endIndex).toBe(16);
    expect(result.current.visibleItems).toEqual([9, 10, 11, 12, 13, 14, 15]);
    expect(result.current.paddingTop).toBe(90);
    expect(result.current.paddingBottom).toBe(1840);
  });

  it('keeps rows visible when item count shrinks after deep scroll', () => {
    const options = {
      enabled: true,
      rowHeight: 10,
      viewportHeight: 50,
      overscan: 1,
    } as const;
    const initialItems = Array.from({ length: 200 }, (_, i) => i);
    const { result, rerender } = renderHook(
      ({ items }) =>
        useVirtualizedRows(items, {
          enabled: options.enabled,
          rowHeight: options.rowHeight,
          viewportHeight: options.viewportHeight,
          overscan: options.overscan,
        }),
      { initialProps: { items: initialItems } }
    );

    act(() => {
      result.current.onScroll?.({
        currentTarget: { scrollTop: 1900 },
      } as UIEvent<HTMLElement>);
    });

    const nextItems = Array.from({ length: 20 }, (_, i) => i);
    rerender({ items: nextItems });

    expect(result.current.startIndex).toBe(15);
    expect(result.current.endIndex).toBe(20);
    expect(result.current.visibleItems).toEqual([15, 16, 17, 18, 19]);
    expect(result.current.paddingTop).toBe(150);
    expect(result.current.paddingBottom).toBe(0);
  });
});
