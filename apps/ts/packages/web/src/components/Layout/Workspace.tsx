import type { LucideIcon } from 'lucide-react';
import { type HTMLAttributes, startTransition } from 'react';
import { cn } from '@/lib/utils';

export interface SegmentedTabItem<T extends string> {
  value: T;
  label: string;
  icon?: LucideIcon;
  disabled?: boolean;
}

export interface NavRailItem<T extends string> {
  value: T;
  label: string;
  icon: LucideIcon;
  disabled?: boolean;
}

interface SegmentedTabsProps<T extends string> {
  items: readonly SegmentedTabItem<T>[];
  value: T;
  onChange: (value: T) => void;
  className?: string;
  itemClassName?: string;
}

interface ModeSwitcherPanelProps<T extends string> extends SegmentedTabsProps<T> {
  label: string;
}

interface NavRailProps<T extends string> {
  items: readonly NavRailItem<T>[];
  value: T;
  onChange: (value: T) => void;
  className?: string;
}

export function Surface({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        'rounded-xl border border-border/70 bg-card/84 text-card-foreground shadow-sm shadow-black/5',
        className
      )}
      {...props}
    />
  );
}

export function SectionEyebrow({ className, ...props }: HTMLAttributes<HTMLParagraphElement>) {
  return (
    <p
      className={cn('text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground', className)}
      {...props}
    />
  );
}

export function SegmentedTabs<T extends string>({
  items,
  value,
  onChange,
  className,
  itemClassName,
}: SegmentedTabsProps<T>) {
  return (
    <div className={cn('flex flex-wrap gap-2', className)}>
      {items.map((item) => {
        const Icon = item.icon;
        const isActive = value === item.value;

        return (
          <button
            key={item.value}
            type="button"
            disabled={item.disabled}
            aria-pressed={isActive}
            onClick={() => {
              if (item.disabled || isActive) {
                return;
              }

              startTransition(() => onChange(item.value));
            }}
            className={cn(
              'inline-flex items-center gap-2 rounded-lg border px-3 py-2 text-sm font-medium transition-all duration-150',
              isActive
                ? 'border-primary/20 bg-primary/12 text-primary shadow-sm shadow-primary/10'
                : 'border-transparent bg-background/70 text-muted-foreground hover:border-border/60 hover:bg-accent/70 hover:text-foreground',
              item.disabled && 'cursor-not-allowed opacity-50',
              itemClassName
            )}
          >
            {Icon ? <Icon className="h-4 w-4" /> : null}
            <span>{item.label}</span>
          </button>
        );
      })}
    </div>
  );
}

export function ModeSwitcherPanel<T extends string>({
  label,
  items,
  value,
  onChange,
  className,
  itemClassName,
}: ModeSwitcherPanelProps<T>) {
  return (
    <Surface className={cn('p-3', className)}>
      <SectionEyebrow className="mb-3">{label}</SectionEyebrow>
      <SegmentedTabs items={items} value={value} onChange={onChange} itemClassName={itemClassName} />
    </Surface>
  );
}

export function SplitLayout({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return <div className={cn('flex min-h-0 flex-1', className)} {...props} />;
}

export function SplitSidebar({ className, ...props }: HTMLAttributes<HTMLElement>) {
  return <aside className={cn('shrink-0', className)} {...props} />;
}

export function SplitMain({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return <div className={cn('flex min-h-0 min-w-0 flex-1 flex-col', className)} {...props} />;
}

export function NavRail<T extends string>({ items, value, onChange, className }: NavRailProps<T>) {
  return (
    <nav className={cn('flex flex-col gap-1', className)}>
      {items.map((item) => {
        const Icon = item.icon;
        const isActive = value === item.value;

        return (
          <button
            key={item.value}
            type="button"
            disabled={item.disabled}
            aria-pressed={isActive}
            onClick={() => {
              if (item.disabled || isActive) {
                return;
              }

              startTransition(() => onChange(item.value));
            }}
            className={cn(
              'flex items-center gap-2 rounded-md px-3 py-2 text-sm transition-colors',
              isActive
                ? 'bg-primary/10 font-medium text-primary'
                : 'text-muted-foreground hover:bg-accent hover:text-foreground',
              item.disabled && 'cursor-not-allowed opacity-50'
            )}
          >
            <Icon className="h-4 w-4 shrink-0" />
            <span>{item.label}</span>
          </button>
        );
      })}
    </nav>
  );
}
