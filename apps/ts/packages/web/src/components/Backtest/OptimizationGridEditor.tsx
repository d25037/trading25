import { Loader2, Save, Trash2 } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
import { MonacoYamlEditor } from '@/components/Editor/MonacoYamlEditor';
import { Button } from '@/components/ui/button';
import { useDeleteOptimizationGrid, useOptimizationGridConfig, useSaveOptimizationGrid } from '@/hooks/useOptimization';

const TEMPLATE_YAML = `# Parameter ranges for optimization grid search
# Each parameter should be a list of values to try
parameter_ranges:
  entry_filter_params:
    period_breakout:
      period: [10, 15, 20, 25, 30]
  exit_trigger_params:
    atr_stop:
      atr_multiplier: [1.5, 2.0, 2.5, 3.0]
`;

interface OptimizationGridEditorProps {
  strategyName: string;
}

export function OptimizationGridEditor({ strategyName }: OptimizationGridEditorProps) {
  // Extract basename for grid config lookup
  const basename = strategyName.split('/').pop() ?? strategyName;

  const { data: gridConfig, isLoading, isError } = useOptimizationGridConfig(basename);
  const saveGrid = useSaveOptimizationGrid();
  const deleteGrid = useDeleteOptimizationGrid();

  const [content, setContent] = useState('');
  const [isDirty, setIsDirty] = useState(false);

  useEffect(() => {
    if (gridConfig) {
      setContent(gridConfig.content);
      setIsDirty(false);
    } else if (isError) {
      setContent(TEMPLATE_YAML);
      setIsDirty(false);
    }
  }, [gridConfig, isError]);

  const handleContentChange = useCallback((value: string) => {
    setContent(value);
    setIsDirty(true);
  }, []);

  const handleSave = useCallback(() => {
    saveGrid.mutate(
      { strategy: basename, request: { content } },
      {
        onSuccess: () => {
          setIsDirty(false);
        },
      }
    );
  }, [basename, content, saveGrid]);

  const handleDelete = useCallback(() => {
    deleteGrid.mutate(basename, {
      onSuccess: () => {
        setContent(TEMPLATE_YAML);
        setIsDirty(false);
      },
    });
  }, [basename, deleteGrid]);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-32">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  const savedInfo = gridConfig ? `${gridConfig.param_count} params, ${gridConfig.combinations} combinations` : null;

  const lastSaveInfo = saveGrid.data
    ? `${saveGrid.data.param_count} params, ${saveGrid.data.combinations} combinations`
    : null;

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <div>
          <h4 className="text-sm font-medium">Grid Configuration</h4>
          {(lastSaveInfo || savedInfo) && (
            <p className="text-xs text-muted-foreground mt-0.5">{lastSaveInfo || savedInfo}</p>
          )}
        </div>
        <div className="flex gap-2">
          {gridConfig && (
            <Button
              variant="outline"
              size="sm"
              onClick={handleDelete}
              disabled={deleteGrid.isPending}
              className="text-destructive hover:text-destructive"
            >
              <Trash2 className="h-4 w-4 mr-1" />
              Delete
            </Button>
          )}
          <Button size="sm" onClick={handleSave} disabled={!isDirty || saveGrid.isPending}>
            {saveGrid.isPending ? <Loader2 className="h-4 w-4 mr-1 animate-spin" /> : <Save className="h-4 w-4 mr-1" />}
            Save
          </Button>
        </div>
      </div>

      <MonacoYamlEditor value={content} onChange={handleContentChange} height="320px" />

      {saveGrid.isError && (
        <div className="rounded-md bg-red-500/10 p-2 text-sm text-red-500">{saveGrid.error.message}</div>
      )}

      {deleteGrid.isError && (
        <div className="rounded-md bg-red-500/10 p-2 text-sm text-red-500">{deleteGrid.error.message}</div>
      )}

      {!gridConfig && !isError && (
        <p className="text-xs text-muted-foreground">
          No grid config exists for this strategy. Edit the template above and save to create one.
        </p>
      )}
    </div>
  );
}
