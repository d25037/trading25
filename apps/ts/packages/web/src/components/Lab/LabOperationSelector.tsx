import { Dna, Lightbulb, Shuffle, Target } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { LabType } from '@/types/backtest';

const operations: { id: LabType; label: string; icon: typeof Shuffle; description: string }[] = [
  { id: 'generate', label: 'Generate', icon: Shuffle, description: 'Random strategy generation' },
  { id: 'evolve', label: 'Evolve', icon: Dna, description: 'GA evolution' },
  { id: 'optimize', label: 'Optimize', icon: Target, description: 'Optuna optimization' },
  { id: 'improve', label: 'Improve', icon: Lightbulb, description: 'AI improvement' },
];

interface LabOperationSelectorProps {
  value: LabType;
  onChange: (type: LabType) => void;
  disabled?: boolean;
}

export function LabOperationSelector({ value, onChange, disabled }: LabOperationSelectorProps) {
  return (
    <div className="flex gap-1 rounded-lg bg-muted p-1">
      {operations.map((op) => {
        const Icon = op.icon;
        const isActive = value === op.id;
        return (
          <button
            key={op.id}
            type="button"
            disabled={disabled}
            className={cn(
              'flex flex-1 items-center justify-center gap-1.5 rounded-md px-3 py-2 text-sm transition-colors',
              isActive
                ? 'bg-background text-foreground shadow-sm font-medium'
                : 'text-muted-foreground hover:text-foreground',
              disabled && 'opacity-50 cursor-not-allowed'
            )}
            onClick={() => onChange(op.id)}
            title={op.description}
          >
            <Icon className="h-3.5 w-3.5" />
            {op.label}
          </button>
        );
      })}
    </div>
  );
}
