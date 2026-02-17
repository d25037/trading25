import { ArrowLeftRight, Loader2 } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useMoveStrategy } from '@/hooks/useBacktest';
import type { StrategyMoveTargetCategory } from '@/types/backtest';
import { MANAGED_STRATEGY_CATEGORIES } from './strategyCategoryOrder';

function getCategoryLabel(category: StrategyMoveTargetCategory): string {
  return category.charAt(0).toUpperCase() + category.slice(1);
}

const GROUP_OPTIONS: Array<{ value: StrategyMoveTargetCategory; label: string }> = MANAGED_STRATEGY_CATEGORIES.map(
  (category) => ({
    value: category,
    label: getCategoryLabel(category),
  })
);

function getDefaultTargetCategory(currentCategory: string): StrategyMoveTargetCategory {
  const firstAvailable = GROUP_OPTIONS.find((option) => option.value !== currentCategory);
  return firstAvailable?.value ?? 'experimental';
}

interface MoveGroupDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  strategyName: string;
  currentCategory: string;
  onSuccess?: (newStrategyName: string) => void;
}

export function MoveGroupDialog({
  open,
  onOpenChange,
  strategyName,
  currentCategory,
  onSuccess,
}: MoveGroupDialogProps) {
  const moveStrategy = useMoveStrategy();
  const [targetCategory, setTargetCategory] = useState<StrategyMoveTargetCategory>(
    getDefaultTargetCategory(currentCategory)
  );

  useEffect(() => {
    if (open) {
      setTargetCategory(getDefaultTargetCategory(currentCategory));
    }
  }, [open, currentCategory]);

  const availableOptions = useMemo(
    () => GROUP_OPTIONS.filter((option) => option.value !== currentCategory),
    [currentCategory]
  );

  const handleMove = () => {
    moveStrategy.mutate(
      { name: strategyName, request: { target_category: targetCategory } },
      {
        onSuccess: (data) => {
          onOpenChange(false);
          onSuccess?.(data.new_strategy_name);
        },
      }
    );
  };

  const isValidTarget = availableOptions.some((option) => option.value === targetCategory);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <ArrowLeftRight className="h-5 w-5" />
            Move Strategy Group
          </DialogTitle>
          <DialogDescription>
            Move <span className="font-semibold text-foreground">{strategyName}</span> from{' '}
            <span className="font-semibold text-foreground">{currentCategory}</span> to another group.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-2 py-4">
          <Label htmlFor="target-category">Target Group</Label>
          <Select
            value={targetCategory}
            onValueChange={(value) => setTargetCategory(value as StrategyMoveTargetCategory)}
          >
            <SelectTrigger id="target-category">
              <SelectValue placeholder="Select target group" />
            </SelectTrigger>
            <SelectContent>
              {availableOptions.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={handleMove} disabled={moveStrategy.isPending || !isValidTarget}>
            {moveStrategy.isPending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Moving...
              </>
            ) : (
              'Move'
            )}
          </Button>
        </DialogFooter>
        {moveStrategy.isError && <p className="text-sm text-destructive">Error: {moveStrategy.error.message}</p>}
      </DialogContent>
    </Dialog>
  );
}
