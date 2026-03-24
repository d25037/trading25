import type { LucideIcon } from 'lucide-react';
import { type HTMLAttributes, type ReactNode, startTransition } from 'react';
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

type MetricTone = 'neutral' | 'accent' | 'success' | 'warning' | 'danger';

interface PageIntroProps extends HTMLAttributes<HTMLElement> {
  eyebrow?: string;
  title: string;
  description: string;
  meta?: ReactNode;
  aside?: ReactNode;
}

interface SectionHeadingProps extends HTMLAttributes<HTMLDivElement> {
  eyebrow?: string;
  title: string;
  description?: string;
  actions?: ReactNode;
}

interface CompactMetricProps extends HTMLAttributes<HTMLDivElement> {
  label: string;
  value: string;
  detail?: string;
  tone?: MetricTone;
}

export interface PageIntroMetaItem {
  label: string;
  value: string;
}

function getMetricToneClasses(tone: MetricTone): string {
  switch (tone) {
    case 'accent':
      return 'border-primary/18 bg-primary/10 text-primary';
    case 'success':
      return 'border-emerald-500/18 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300';
    case 'warning':
      return 'border-amber-500/18 bg-amber-500/10 text-amber-700 dark:text-amber-300';
    case 'danger':
      return 'border-red-500/18 bg-red-500/10 text-red-700 dark:text-red-300';
    default:
      return 'border-border/70 bg-[var(--app-surface-muted)] text-foreground';
  }
}

export function Surface({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={cn('app-panel rounded-2xl text-card-foreground', className)} {...props} />
  );
}

export function SectionEyebrow({ className, ...props }: HTMLAttributes<HTMLParagraphElement>) {
  return (
    <p
      className={cn('text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground', className)}
      {...props}
    />
  );
}

export function SectionHeading({
  eyebrow,
  title,
  description,
  actions,
  className,
  ...props
}: SectionHeadingProps) {
  return (
    <div className={cn('flex flex-col gap-3 md:flex-row md:items-end md:justify-between', className)} {...props}>
      <div className="space-y-2">
        {eyebrow ? <SectionEyebrow>{eyebrow}</SectionEyebrow> : null}
        <div className="space-y-1">
          <h2 className="text-xl font-semibold tracking-tight text-foreground">{title}</h2>
          {description ? <p className="max-w-2xl text-sm text-muted-foreground">{description}</p> : null}
        </div>
      </div>
      {actions ? <div className="shrink-0">{actions}</div> : null}
    </div>
  );
}

export function PageIntro({ eyebrow, title, description, meta, aside, className, ...props }: PageIntroProps) {
  return (
    <section className={cn('app-panel rounded-[28px] px-5 py-5 sm:px-6 sm:py-6', className)} {...props}>
      <div className={cn('flex flex-col gap-6', aside && 'xl:flex-row xl:items-start xl:justify-between')}>
        <div className="max-w-3xl space-y-4">
          {eyebrow ? <SectionEyebrow>{eyebrow}</SectionEyebrow> : null}
          <div className="space-y-3">
            <h1 className="text-3xl font-semibold tracking-tight text-foreground sm:text-4xl">{title}</h1>
            <p className="max-w-2xl text-sm leading-6 text-muted-foreground sm:text-[15px]">{description}</p>
          </div>
          {meta ? <div>{meta}</div> : null}
        </div>
        {aside ? <div className="xl:w-[32rem]">{aside}</div> : null}
      </div>
    </section>
  );
}

export function PageIntroMetaList({
  items,
  className,
  ...props
}: HTMLAttributes<HTMLDListElement> & { items: readonly PageIntroMetaItem[] }) {
  return (
    <dl className={cn('flex flex-wrap gap-x-6 gap-y-3', className)} {...props}>
      {items.map((item) => (
        <div key={item.label} className="min-w-[9rem] border-l border-border/70 pl-3">
          <dt className="text-[11px] font-medium uppercase tracking-[0.14em] text-muted-foreground">{item.label}</dt>
          <dd className="mt-1 text-sm font-medium text-foreground">{item.value}</dd>
        </div>
      ))}
    </dl>
  );
}

export function CompactMetric({ label, value, detail, tone = 'neutral', className, ...props }: CompactMetricProps) {
  return (
    <div className={cn('app-panel-muted rounded-2xl p-4', getMetricToneClasses(tone), className)} {...props}>
      <p className="text-[11px] font-medium uppercase tracking-[0.16em] opacity-80">{label}</p>
      <p className="mt-3 text-xl font-semibold leading-tight tracking-tight tabular-nums">{value}</p>
      {detail ? <p className="mt-2 text-xs opacity-80">{detail}</p> : null}
    </div>
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
            data-state={isActive ? 'active' : 'inactive'}
            onClick={() => {
              if (item.disabled || isActive) {
                return;
              }

              startTransition(() => onChange(item.value));
            }}
            className={cn(
              'app-interactive inline-flex items-center gap-2 rounded-xl border px-3 py-2.5 text-sm font-medium',
              isActive
                ? 'border-border/70 bg-[var(--app-surface-emphasis)] text-foreground shadow-sm'
                : 'border-transparent bg-transparent text-muted-foreground hover:border-border/60 hover:bg-[var(--app-surface-muted)] hover:text-foreground',
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
    <Surface className={cn('p-4', className)}>
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
            data-state={isActive ? 'active' : 'inactive'}
            onClick={() => {
              if (item.disabled || isActive) {
                return;
              }

              startTransition(() => onChange(item.value));
            }}
            className={cn(
              'app-interactive flex items-center gap-2 rounded-xl px-3 py-2.5 text-sm',
              isActive
                ? 'bg-[var(--app-surface-emphasis)] font-medium text-foreground shadow-sm'
                : 'text-muted-foreground hover:bg-[var(--app-surface-muted)] hover:text-foreground',
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
