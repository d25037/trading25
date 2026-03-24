import type { LucideIcon } from 'lucide-react';
import type { ReactNode } from 'react';
import { cn } from '@/lib/utils';

export interface SummaryMetricItem {
  icon: LucideIcon;
  label: string;
  value: ReactNode;
  meta?: ReactNode;
  tone?: 'default' | 'positive' | 'negative' | 'warning';
}

interface SummaryMetricsProps {
  items: SummaryMetricItem[];
  className?: string;
  columns?: 2 | 3 | 4;
}

const columnsClassName: Record<NonNullable<SummaryMetricsProps['columns']>, string> = {
  2: 'sm:grid-cols-2',
  3: 'sm:grid-cols-2 xl:grid-cols-3',
  4: 'sm:grid-cols-2 xl:grid-cols-4',
};

const toneClassName: Record<NonNullable<SummaryMetricItem['tone']>, string> = {
  default: 'text-foreground',
  positive: 'text-green-600 dark:text-green-400',
  negative: 'text-red-600 dark:text-red-400',
  warning: 'text-warning',
};

export function SummaryMetrics({ items, className, columns = 4 }: SummaryMetricsProps) {
  return (
    <div className={cn('mb-4 grid grid-cols-1 gap-3', columnsClassName[columns], className)}>
      {items.map((item) => {
        const Icon = item.icon;
        return (
          <section
            key={item.label}
            className="rounded-xl border border-border/70 bg-card/90 px-4 py-3 shadow-sm shadow-black/5"
          >
            <div className="flex items-start gap-3">
              <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-accent text-muted-foreground">
                <Icon className="h-4 w-4" />
              </div>
              <div className="min-w-0 flex-1">
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                  {item.label}
                </p>
                <p
                  className={cn(
                    'mt-1 text-sm font-semibold leading-5 tabular-nums',
                    toneClassName[item.tone ?? 'default']
                  )}
                >
                  {item.value}
                </p>
                {item.meta ? <p className="mt-1 truncate text-xs text-muted-foreground">{item.meta}</p> : null}
              </div>
            </div>
          </section>
        );
      })}
    </div>
  );
}
