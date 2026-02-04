import { Loader2 } from 'lucide-react';
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import type { StrategyMetadata } from '@/types/backtest';

interface StrategySelectorProps {
  strategies: StrategyMetadata[] | undefined;
  isLoading: boolean;
  value: string | null;
  onChange: (value: string) => void;
  disabled?: boolean;
}

export function StrategySelector({ strategies, isLoading, value, onChange, disabled }: StrategySelectorProps) {
  if (isLoading) {
    return (
      <div className="flex items-center gap-2 h-10 px-3 border border-input rounded-md">
        <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
        <span className="text-sm text-muted-foreground">Loading strategies...</span>
      </div>
    );
  }

  if (!strategies || strategies.length === 0) {
    return (
      <div className="flex items-center h-10 px-3 border border-input rounded-md">
        <span className="text-sm text-muted-foreground">No strategies available</span>
      </div>
    );
  }

  // Group strategies by category
  const grouped = strategies.reduce(
    (acc, s) => {
      const cat = s.category || 'other';
      if (!acc[cat]) acc[cat] = [];
      acc[cat].push(s);
      return acc;
    },
    {} as Record<string, StrategyMetadata[]>
  );

  // Sort strategies within each category by last_modified descending
  for (const cat of Object.keys(grouped)) {
    grouped[cat]?.sort((a, b) => {
      const aTime = a.last_modified ? new Date(a.last_modified).getTime() : 0;
      const bTime = b.last_modified ? new Date(b.last_modified).getTime() : 0;
      return bTime - aTime;
    });
  }

  // Sort categories: production first, then alphabetical
  const sortedCategories = Object.keys(grouped).sort((a, b) => {
    if (a === 'production') return -1;
    if (b === 'production') return 1;
    return a.localeCompare(b);
  });

  return (
    <Select value={value ?? undefined} onValueChange={onChange} disabled={disabled}>
      <SelectTrigger className="w-full">
        <SelectValue placeholder="Select a strategy" />
      </SelectTrigger>
      <SelectContent>
        {sortedCategories.map((category) => {
          const categoryStrategies = grouped[category] ?? [];
          return (
            <SelectGroup key={category}>
              <SelectLabel className="capitalize">{category}</SelectLabel>
              {categoryStrategies.map((strategy) => (
                <SelectItem key={strategy.name} value={strategy.name}>
                  {strategy.display_name || strategy.name}
                </SelectItem>
              ))}
            </SelectGroup>
          );
        })}
      </SelectContent>
    </Select>
  );
}
