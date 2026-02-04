import { Edit, Loader2 } from 'lucide-react';
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
import { useRenameStrategy } from '@/hooks/useBacktest';

interface RenameDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  strategyName: string;
  onSuccess?: (newStrategyName: string) => void;
}

export function RenameDialog({ open, onOpenChange, strategyName, onSuccess }: RenameDialogProps) {
  const [newName, setNewName] = useState('');
  const renameStrategy = useRenameStrategy();

  // Extract current name without category prefix
  const currentNameWithoutPrefix = strategyName.includes('/') ? (strategyName.split('/').pop() ?? '') : strategyName;

  const handleRename = () => {
    const trimmedName = newName.trim();
    if (!trimmedName) return;
    if (trimmedName === currentNameWithoutPrefix) return;

    renameStrategy.mutate(
      { name: strategyName, request: { new_name: trimmedName } },
      {
        onSuccess: (data) => {
          onOpenChange(false);
          setNewName('');
          onSuccess?.(data.new_name);
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

  const isValid = newName.trim() && newName.trim() !== currentNameWithoutPrefix;

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Edit className="h-5 w-5" />
            Rename Strategy
          </DialogTitle>
          <DialogDescription>
            Rename <span className="font-semibold text-foreground">{strategyName}</span>. The strategy will remain in
            the experimental category.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label htmlFor="new-name">New Strategy Name</Label>
            <Input
              id="new-name"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              placeholder={currentNameWithoutPrefix}
            />
            <p className="text-xs text-muted-foreground">
              Do not include category prefix. The strategy will be saved as experimental/
              {newName || currentNameWithoutPrefix}
            </p>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => handleOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={handleRename} disabled={renameStrategy.isPending || !isValid}>
            {renameStrategy.isPending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Renaming...
              </>
            ) : (
              'Rename'
            )}
          </Button>
        </DialogFooter>
        {renameStrategy.isError && <p className="text-sm text-destructive">Error: {renameStrategy.error.message}</p>}
      </DialogContent>
    </Dialog>
  );
}
