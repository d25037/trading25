import { Copy, Edit2, MoreVertical, Plus, Save, Trash2 } from 'lucide-react';
import { useCallback, useState } from 'react';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { cn } from '@/lib/utils';
import { type ChartPreset, useChartStore } from '@/stores/chartStore';

type DialogMode = 'create' | 'rename' | 'duplicate' | 'delete' | null;

export function ChartPresetSelector() {
  const {
    presets,
    activePresetId,
    createPreset,
    updatePreset,
    deletePreset,
    loadPreset,
    renamePreset,
    duplicatePreset,
  } = useChartStore();

  const [dialogMode, setDialogMode] = useState<DialogMode>(null);
  const [presetName, setPresetName] = useState('');
  const [targetPreset, setTargetPreset] = useState<ChartPreset | null>(null);
  const [showMenu, setShowMenu] = useState(false);

  const activePreset = presets.find((p) => p.id === activePresetId);

  const handleCreate = useCallback(() => {
    if (presetName.trim()) {
      createPreset(presetName.trim());
      setPresetName('');
      setDialogMode(null);
    }
  }, [presetName, createPreset]);

  const handleRename = useCallback(() => {
    if (targetPreset && presetName.trim()) {
      renamePreset(targetPreset.id, presetName.trim());
      setPresetName('');
      setTargetPreset(null);
      setDialogMode(null);
    }
  }, [targetPreset, presetName, renamePreset]);

  const handleDuplicate = useCallback(() => {
    if (targetPreset && presetName.trim()) {
      duplicatePreset(targetPreset.id, presetName.trim());
      setPresetName('');
      setTargetPreset(null);
      setDialogMode(null);
    }
  }, [targetPreset, presetName, duplicatePreset]);

  const handleDelete = useCallback(() => {
    if (targetPreset) {
      deletePreset(targetPreset.id);
      setTargetPreset(null);
      setDialogMode(null);
    }
  }, [targetPreset, deletePreset]);

  const handleSave = useCallback(() => {
    if (activePresetId) {
      updatePreset(activePresetId);
    }
  }, [activePresetId, updatePreset]);

  const openRenameDialog = (preset: ChartPreset) => {
    setTargetPreset(preset);
    setPresetName(preset.name);
    setDialogMode('rename');
    setShowMenu(false);
  };

  const openDuplicateDialog = (preset: ChartPreset) => {
    setTargetPreset(preset);
    setPresetName(`${preset.name} (copy)`);
    setDialogMode('duplicate');
    setShowMenu(false);
  };

  const openDeleteDialog = (preset: ChartPreset) => {
    setTargetPreset(preset);
    setDialogMode('delete');
    setShowMenu(false);
  };

  return (
    <div className="glass-panel rounded-lg p-3 space-y-2">
      <div className="flex items-center gap-2">
        <div className="gradient-primary rounded p-1.5">
          <Save className="h-3.5 w-3.5 text-white" />
        </div>
        <h3 className="text-sm font-semibold text-foreground">Presets</h3>
      </div>

      <div className="space-y-2">
        {/* Preset selector */}
        <div className="flex items-center gap-1.5">
          <Select
            value={activePresetId ?? 'none'}
            onValueChange={(value) => {
              if (value !== 'none') {
                loadPreset(value);
              }
            }}
          >
            <SelectTrigger className="flex-1 h-8 text-xs glass-panel border-border/30 focus:border-primary/50">
              <SelectValue placeholder="Select preset..." />
            </SelectTrigger>
            <SelectContent className="bg-background/95 backdrop-blur-md border-border shadow-xl">
              <SelectItem value="none" className="text-xs text-muted-foreground">
                (No preset)
              </SelectItem>
              {presets.map((preset) => (
                <SelectItem key={preset.id} value={preset.id} className="text-xs">
                  {preset.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          {/* Save current settings to active preset */}
          <Button
            type="button"
            variant="outline"
            size="icon"
            className="h-8 w-8 shrink-0"
            disabled={!activePresetId}
            onClick={handleSave}
            title="Save to preset"
          >
            <Save className="h-3.5 w-3.5" />
          </Button>

          {/* Create new preset */}
          <Button
            type="button"
            variant="outline"
            size="icon"
            className="h-8 w-8 shrink-0"
            onClick={() => {
              setPresetName('');
              setDialogMode('create');
            }}
            title="Create preset"
          >
            <Plus className="h-3.5 w-3.5" />
          </Button>

          {/* More actions menu */}
          <div className="relative">
            <Button
              type="button"
              variant="outline"
              size="icon"
              className="h-8 w-8 shrink-0"
              disabled={!activePresetId}
              onClick={() => setShowMenu(!showMenu)}
              title="More actions"
            >
              <MoreVertical className="h-3.5 w-3.5" />
            </Button>

            {showMenu && activePreset && (
              <>
                {/* Backdrop to close menu */}
                <button
                  type="button"
                  className="fixed inset-0 z-40 cursor-default"
                  onClick={() => setShowMenu(false)}
                  aria-label="Close menu"
                />
                <div className="absolute right-0 top-full mt-1 z-50 min-w-[140px] rounded-md border border-border bg-background/95 backdrop-blur-md shadow-lg">
                  <button
                    type="button"
                    className="flex w-full items-center gap-2 px-3 py-2 text-xs hover:bg-accent/50 transition-colors"
                    onClick={() => openRenameDialog(activePreset)}
                  >
                    <Edit2 className="h-3.5 w-3.5" />
                    Rename
                  </button>
                  <button
                    type="button"
                    className="flex w-full items-center gap-2 px-3 py-2 text-xs hover:bg-accent/50 transition-colors"
                    onClick={() => openDuplicateDialog(activePreset)}
                  >
                    <Copy className="h-3.5 w-3.5" />
                    Duplicate
                  </button>
                  <button
                    type="button"
                    className={cn(
                      'flex w-full items-center gap-2 px-3 py-2 text-xs hover:bg-destructive/10 transition-colors',
                      'text-destructive'
                    )}
                    onClick={() => openDeleteDialog(activePreset)}
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                    Delete
                  </button>
                </div>
              </>
            )}
          </div>
        </div>

        {/* Active preset indicator */}
        {activePreset && (
          <div className="flex items-center gap-1.5 px-2 py-1 rounded bg-primary/5 border border-primary/20">
            <Save className="h-3 w-3 text-primary" />
            <span className="text-xs text-primary truncate">{activePreset.name}</span>
          </div>
        )}
      </div>

      {/* Create Dialog */}
      <Dialog open={dialogMode === 'create'} onOpenChange={(open) => !open && setDialogMode(null)}>
        <DialogContent className="sm:max-w-[400px]">
          <DialogHeader>
            <DialogTitle>Create Preset</DialogTitle>
            <DialogDescription>Save current settings as a new preset.</DialogDescription>
          </DialogHeader>
          <div className="space-y-2 py-4">
            <Label htmlFor="preset-name" className="text-sm">
              Preset Name
            </Label>
            <Input
              id="preset-name"
              value={presetName}
              onChange={(e) => setPresetName(e.target.value)}
              placeholder="My Preset"
              onKeyDown={(e) => {
                if (e.key === 'Enter') handleCreate();
              }}
            />
          </div>
          <DialogFooter>
            <DialogClose asChild>
              <Button variant="outline">Cancel</Button>
            </DialogClose>
            <Button onClick={handleCreate} disabled={!presetName.trim()}>
              Create
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Rename Dialog */}
      <Dialog open={dialogMode === 'rename'} onOpenChange={(open) => !open && setDialogMode(null)}>
        <DialogContent className="sm:max-w-[400px]">
          <DialogHeader>
            <DialogTitle>Rename Preset</DialogTitle>
            <DialogDescription>Enter a new name for the preset.</DialogDescription>
          </DialogHeader>
          <div className="space-y-2 py-4">
            <Label htmlFor="rename-preset-name" className="text-sm">
              Preset Name
            </Label>
            <Input
              id="rename-preset-name"
              value={presetName}
              onChange={(e) => setPresetName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') handleRename();
              }}
            />
          </div>
          <DialogFooter>
            <DialogClose asChild>
              <Button variant="outline">Cancel</Button>
            </DialogClose>
            <Button onClick={handleRename} disabled={!presetName.trim()}>
              Rename
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Duplicate Dialog */}
      <Dialog open={dialogMode === 'duplicate'} onOpenChange={(open) => !open && setDialogMode(null)}>
        <DialogContent className="sm:max-w-[400px]">
          <DialogHeader>
            <DialogTitle>Duplicate Preset</DialogTitle>
            <DialogDescription>Create a copy of &quot;{targetPreset?.name}&quot;.</DialogDescription>
          </DialogHeader>
          <div className="space-y-2 py-4">
            <Label htmlFor="duplicate-preset-name" className="text-sm">
              New Preset Name
            </Label>
            <Input
              id="duplicate-preset-name"
              value={presetName}
              onChange={(e) => setPresetName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') handleDuplicate();
              }}
            />
          </div>
          <DialogFooter>
            <DialogClose asChild>
              <Button variant="outline">Cancel</Button>
            </DialogClose>
            <Button onClick={handleDuplicate} disabled={!presetName.trim()}>
              Duplicate
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Dialog */}
      <Dialog open={dialogMode === 'delete'} onOpenChange={(open) => !open && setDialogMode(null)}>
        <DialogContent className="sm:max-w-[400px]">
          <DialogHeader>
            <DialogTitle>Delete Preset</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete &quot;{targetPreset?.name}&quot;? This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <DialogClose asChild>
              <Button variant="outline">Cancel</Button>
            </DialogClose>
            <Button variant="destructive" onClick={handleDelete}>
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
