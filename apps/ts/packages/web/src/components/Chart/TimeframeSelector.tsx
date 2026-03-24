import { cn } from '@/lib/utils';
import { type DisplayTimeframe, useChartStore } from '@/stores/chartStore';

const timeframeOptions: { value: DisplayTimeframe; label: string }[] = [
  { value: 'daily', label: 'Daily' },
  { value: 'weekly', label: 'Weekly' },
  { value: 'monthly', label: 'Monthly' },
];

export function TimeframeSelector() {
  const { settings, setDisplayTimeframe } = useChartStore();
  const currentTimeframe = settings.displayTimeframe;

  return (
    <div className="flex gap-2">
      {timeframeOptions.map((option) => (
        <button
          key={option.value}
          type="button"
          aria-pressed={currentTimeframe === option.value}
          onClick={() => setDisplayTimeframe(option.value)}
          className={cn(
            'inline-flex items-center rounded-xl border px-3 py-2 text-sm font-medium transition-colors',
            currentTimeframe === option.value
              ? 'border-border/70 bg-[var(--app-surface-emphasis)] text-foreground shadow-sm'
              : 'border-transparent bg-transparent text-muted-foreground hover:border-border/60 hover:bg-[var(--app-surface-muted)] hover:text-foreground'
          )}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}
