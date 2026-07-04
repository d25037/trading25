import { Info } from 'lucide-react';
import {
  type CSSProperties,
  type ReactNode,
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type WheelEvent,
} from 'react';
import { createPortal } from 'react-dom';
import { cn } from '@/lib/utils';

interface InfoPopoverProps {
  ariaLabel: string;
  children: ReactNode;
  className?: string;
  contentClassName?: string;
  side?: 'left' | 'right';
}

interface PopoverStyle {
  left: number;
  width: number;
  maxHeight: number;
  top?: number;
  bottom?: number;
}

const VIEWPORT_PADDING_PX = 12;
const POPOVER_GAP_PX = 8;
const POPOVER_MAX_WIDTH_PX = 384;
const MIN_PREFERRED_HEIGHT_PX = 180;
const MIN_USABLE_HEIGHT_PX = 64;

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

function getMaxHeight(availableHeight: number): number {
  return Math.max(MIN_USABLE_HEIGHT_PX, Math.floor(availableHeight));
}

export function InfoPopover({ ariaLabel, children, className, contentClassName, side = 'right' }: InfoPopoverProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [popoverStyle, setPopoverStyle] = useState<PopoverStyle | null>(null);
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const contentRef = useRef<HTMLDivElement | null>(null);

  const updatePosition = useCallback(() => {
    const trigger = triggerRef.current;
    if (!trigger) return;

    const triggerRect = trigger.getBoundingClientRect();
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    const width = Math.min(POPOVER_MAX_WIDTH_PX, Math.max(0, viewportWidth - VIEWPORT_PADDING_PX * 2));
    const preferredLeft = side === 'right' ? triggerRect.right - width : triggerRect.left;
    const left = clamp(preferredLeft, VIEWPORT_PADDING_PX, viewportWidth - width - VIEWPORT_PADDING_PX);
    const belowTop = triggerRect.bottom + POPOVER_GAP_PX;
    const availableBelow = viewportHeight - belowTop - VIEWPORT_PADDING_PX;
    const availableAbove = triggerRect.top - VIEWPORT_PADDING_PX - POPOVER_GAP_PX;

    if (availableBelow < MIN_PREFERRED_HEIGHT_PX && availableAbove > availableBelow) {
      setPopoverStyle({
        left,
        width,
        bottom: viewportHeight - triggerRect.top + POPOVER_GAP_PX,
        maxHeight: getMaxHeight(availableAbove),
      });
      return;
    }

    setPopoverStyle({
      left,
      width,
      top: belowTop,
      maxHeight: getMaxHeight(availableBelow),
    });
  }, [side]);

  useLayoutEffect(() => {
    if (!isOpen) return;
    updatePosition();
  }, [isOpen, updatePosition]);

  useEffect(() => {
    if (!isOpen) return;

    const handlePointerDown = (event: PointerEvent) => {
      const target = event.target as Node;
      if (!triggerRef.current?.contains(target) && !contentRef.current?.contains(target)) {
        setIsOpen(false);
      }
    };
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault();
        event.stopPropagation();
        event.stopImmediatePropagation();
        setIsOpen(false);
      }
    };
    const handleViewportChange = () => updatePosition();

    document.addEventListener('pointerdown', handlePointerDown, true);
    window.addEventListener('keydown', handleKeyDown, true);
    window.addEventListener('resize', handleViewportChange);
    window.addEventListener('scroll', handleViewportChange, true);
    return () => {
      document.removeEventListener('pointerdown', handlePointerDown, true);
      window.removeEventListener('keydown', handleKeyDown, true);
      window.removeEventListener('resize', handleViewportChange);
      window.removeEventListener('scroll', handleViewportChange, true);
    };
  }, [isOpen, updatePosition]);

  const handleContentWheel = useCallback((event: WheelEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.scrollTop += event.deltaY;
  }, []);

  const content =
    isOpen && popoverStyle
      ? createPortal(
          <div
            ref={contentRef}
            role="dialog"
            aria-label={ariaLabel}
            style={
              {
                left: popoverStyle.left,
                width: popoverStyle.width,
                maxHeight: popoverStyle.maxHeight,
                top: popoverStyle.top,
                bottom: popoverStyle.bottom,
              } satisfies CSSProperties
            }
            onWheelCapture={handleContentWheel}
            className={cn(
              'pointer-events-auto fixed z-[70] overscroll-contain overflow-y-auto rounded-md border border-border/70 bg-popover p-3 text-popover-foreground shadow-lg',
              contentClassName
            )}
          >
            {children}
          </div>,
          document.body
        )
      : null;

  return (
    <div className={cn('relative inline-flex', className)}>
      <button
        ref={triggerRef}
        type="button"
        aria-expanded={isOpen}
        aria-label={ariaLabel}
        onClick={() => setIsOpen((current) => !current)}
        className="app-interactive inline-flex h-6 w-6 items-center justify-center rounded-md border border-border/70 text-muted-foreground hover:bg-[var(--app-surface-muted)] hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/60"
      >
        <Info className="h-3.5 w-3.5" aria-hidden="true" />
      </button>
      {content}
    </div>
  );
}
