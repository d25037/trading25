import { Copy, Loader2 } from 'lucide-react';
import { useState } from 'react';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useDuplicateStrategy } from '@/hooks/useBacktest';

interface DuplicateDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  strategyName: string;
  onSuccess?: (newStrategyName: string) => void;
}

export function DuplicateDialog({ open, onOpenChange, strategyName, onSuccess }: DuplicateDialogProps) {
  const [newName, setNewName] = useState('');
  const duplicateStrategy = useDuplicateStrategy();

  const handleDuplicate = () => {
    if (!newName.trim()) return;

    duplicateStrategy.mutate(
      { name: strategyName, request: { new_name: newName.trim() } },
      {
        onSuccess: (data) => {
          onOpenChange(false);
          setNewName('');
          onSuccess?.(data.new_strategy_name);
        },
      }
    );
  };

  const handleOpenChange = (open: boolean) => {
    if (!open) {
      setNewName('');
    }
    onOpenChange(open);
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Copy className="h-5 w-5" />
            Duplicate Strategy
          </DialogTitle>
          <DialogDescription>
            Create a copy of <span className="font-semibold text-foreground">{strategyName}</span>. The new strategy
            will be saved in the experimental category.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label htmlFor="new-name">New Strategy Name</Label>
            <Input
              id="new-name"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              placeholder="e.g., my_strategy_v2"
            />
            <p className="text-xs text-muted-foreground">
              Do not include category prefix. The strategy will be saved as experimental/{newName || 'new_name'}
            </p>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => handleOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={handleDuplicate} disabled={duplicateStrategy.isPending || !newName.trim()}>
            {duplicateStrategy.isPending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Duplicating...
              </>
            ) : (
              'Duplicate'
            )}
          </Button>
        </DialogFooter>
        {duplicateStrategy.isError && (
          <p className="text-sm text-destructive">Error: {duplicateStrategy.error.message}</p>
        )}
      </DialogContent>
    </Dialog>
  );
}
