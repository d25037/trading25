import { Loader2, Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { useDeleteDataset } from '@/hooks/useDataset';

interface DatasetDeleteDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  datasetName: string;
}

export function DatasetDeleteDialog({ open, onOpenChange, datasetName }: DatasetDeleteDialogProps) {
  const deleteDataset = useDeleteDataset();

  const handleDelete = () => {
    deleteDataset.mutate(datasetName, {
      onSuccess: () => {
        onOpenChange(false);
      },
    });
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Trash2 className="h-5 w-5 text-destructive" />
            データセット削除
          </DialogTitle>
          <DialogDescription>
            <span className="font-semibold text-foreground">{datasetName}</span>{' '}
            を削除しますか？この操作は取り消せません。
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            キャンセル
          </Button>
          <Button variant="destructive" onClick={handleDelete} disabled={deleteDataset.isPending}>
            {deleteDataset.isPending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                削除中...
              </>
            ) : (
              '削除'
            )}
          </Button>
        </DialogFooter>
        {deleteDataset.isError && <p className="text-sm text-destructive">Error: {deleteDataset.error.message}</p>}
      </DialogContent>
    </Dialog>
  );
}
