import { Button } from '@/components/ui/button';
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
        <Button
          key={option.value}
          variant={currentTimeframe === option.value ? 'default' : 'outline'}
          size="sm"
          onClick={() => setDisplayTimeframe(option.value)}
          className={cn(
            'transition-all duration-200',
            currentTimeframe === option.value
              ? 'gradient-primary text-white shadow-lg'
              : 'glass-panel hover:bg-accent/50'
          )}
        >
          {option.label}
        </Button>
      ))}
    </div>
  );
}
