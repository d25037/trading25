import { type UIEvent, useMemo, useState } from 'react';

interface UseVirtualizedRowsOptions {
  enabled: boolean;
  rowHeight: number;
  viewportHeight: number;
  overscan?: number;
}

interface VirtualizedRowsResult<TItem> {
  visibleItems: TItem[];
  startIndex: number;
  endIndex: number;
  paddingTop: number;
  paddingBottom: number;
  onScroll: ((event: UIEvent<HTMLElement>) => void) | undefined;
}

export function useVirtualizedRows<TItem>(
  items: TItem[],
  { enabled, rowHeight, viewportHeight, overscan = 8 }: UseVirtualizedRowsOptions
): VirtualizedRowsResult<TItem> {
  const [scrollTop, setScrollTop] = useState(0);

  const virtualRange = useMemo(() => {
    if (!enabled) {
      return {
        startIndex: 0,
        endIndex: items.length,
        paddingTop: 0,
        paddingBottom: 0,
      };
    }

    const visibleCount = Math.ceil(viewportHeight / rowHeight);
    const firstVisibleIndex = Math.floor(scrollTop / rowHeight);
    const maxStartIndex = Math.max(0, items.length - visibleCount);
    const startIndex = Math.min(maxStartIndex, Math.max(0, firstVisibleIndex - overscan));
    const endIndex = Math.min(items.length, Math.max(startIndex, firstVisibleIndex + visibleCount + overscan));

    return {
      startIndex,
      endIndex,
      paddingTop: startIndex * rowHeight,
      paddingBottom: Math.max(0, (items.length - endIndex) * rowHeight),
    };
  }, [enabled, items.length, overscan, rowHeight, scrollTop, viewportHeight]);

  const visibleItems = useMemo(
    () => items.slice(virtualRange.startIndex, virtualRange.endIndex),
    [items, virtualRange.endIndex, virtualRange.startIndex]
  );

  const onScroll = enabled ? (event: UIEvent<HTMLElement>) => setScrollTop(event.currentTarget.scrollTop) : undefined;

  return {
    visibleItems,
    startIndex: virtualRange.startIndex,
    endIndex: virtualRange.endIndex,
    paddingTop: virtualRange.paddingTop,
    paddingBottom: virtualRange.paddingBottom,
    onScroll,
  };
}
