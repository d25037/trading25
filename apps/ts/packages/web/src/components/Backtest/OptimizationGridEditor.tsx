import { AlertCircle, CheckCircle2, Edit, Info, Loader2, RotateCcw, Save, Trash2 } from 'lucide-react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { MonacoYamlEditor } from '@/components/Editor/MonacoYamlEditor';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { useDeleteOptimizationGrid, useOptimizationGridConfig, useSaveOptimizationGrid } from '@/hooks/useOptimization';
import { cn } from '@/lib/utils';
import { analyzeGridParameters, type GridParameterAnalysis } from './optimizationGridParams';
import { SignalReferencePanel } from './SignalReferencePanel';

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

type ValidationTone = 'error' | 'warning' | 'success';

interface ValidationState {
  tone: ValidationTone;
  message: string;
}

interface EditorActionsProps {
  canDelete: boolean;
  isDirty: boolean;
  isSavePending: boolean;
  isDeletePending: boolean;
  hasParseError: boolean;
  onReset: () => void;
  onDelete: () => void;
  onSave: () => void;
}

interface SummaryCardsProps {
  paramCount: number;
  combinations: number;
  savedInfo: string;
  isDirty: boolean;
}

interface EditorDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  basename: string;
  content: string;
  analysis: GridParameterAnalysis;
  validationState: ValidationState;
  savedInfo: string;
  isDirty: boolean;
  canDelete: boolean;
  isSavePending: boolean;
  isDeletePending: boolean;
  saveErrorMessage: string | null;
  deleteErrorMessage: string | null;
  onContentChange: (value: string) => void;
  onCopySnippet: (snippet: string) => void;
  onReset: () => void;
  onDelete: () => void;
  onSave: () => void;
}

const VALIDATION_TONE_CLASS: Record<ValidationTone, string> = {
  error: 'bg-destructive/10 text-destructive',
  warning: 'bg-yellow-500/10 text-yellow-700',
  success: 'bg-green-500/10 text-green-700',
};

const VALIDATION_TONE_ICON = {
  error: AlertCircle,
  warning: Info,
  success: CheckCircle2,
} as const;

function buildValidationState(analysis: GridParameterAnalysis): ValidationState {
  if (analysis.parseError) {
    return { tone: 'error', message: analysis.parseError };
  }

  if (!analysis.hasParameterRanges) {
    return {
      tone: 'warning',
      message: 'Missing "parameter_ranges" key. Saving is allowed, but optimization combinations will be 0.',
    };
  }

  if (analysis.paramCount === 0) {
    return {
      tone: 'warning',
      message: 'No parameter arrays found under "parameter_ranges". Add list values like period: [10, 20, 30].',
    };
  }

  return {
    tone: 'success',
    message: `Ready: ${analysis.paramCount} parameters, ${analysis.combinations} combinations detected.`,
  };
}

function LoadingState() {
  return (
    <div className="flex items-center justify-center h-32">
      <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
    </div>
  );
}

function ValidationBanner({ validationState }: { validationState: ValidationState }) {
  const Icon = VALIDATION_TONE_ICON[validationState.tone];

  return (
    <div className={cn('flex items-start gap-2 rounded-md p-3 text-sm', VALIDATION_TONE_CLASS[validationState.tone])}>
      <Icon className="h-4 w-4 mt-0.5 shrink-0" />
      <span>{validationState.message}</span>
    </div>
  );
}

function ErrorBanner({ message }: { message: string }) {
  return (
    <div className="flex items-start gap-2 rounded-md bg-destructive/10 p-3 text-sm text-destructive">
      <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
      <span>{message}</span>
    </div>
  );
}

function EditorActions({
  canDelete,
  isDirty,
  isSavePending,
  isDeletePending,
  hasParseError,
  onReset,
  onDelete,
  onSave,
}: EditorActionsProps) {
  const isBusy = isSavePending || isDeletePending;

  return (
    <div className="flex flex-wrap gap-2 justify-end">
      <Button variant="outline" size="sm" onClick={onReset} disabled={!isDirty || isBusy}>
        <RotateCcw className="h-4 w-4 mr-1" />
        Reset
      </Button>
      {canDelete && (
        <Button
          variant="outline"
          size="sm"
          onClick={onDelete}
          disabled={isBusy}
          className="text-destructive hover:text-destructive"
        >
          <Trash2 className="h-4 w-4 mr-1" />
          Delete
        </Button>
      )}
      <Button size="sm" onClick={onSave} disabled={!isDirty || hasParseError || isBusy}>
        {isSavePending ? <Loader2 className="h-4 w-4 mr-1 animate-spin" /> : <Save className="h-4 w-4 mr-1" />}
        Save
      </Button>
    </div>
  );
}

function SummaryCards({ paramCount, combinations, savedInfo, isDirty }: SummaryCardsProps) {
  return (
    <div className="grid gap-2 md:grid-cols-3">
      <div className="rounded-md border bg-muted/20 p-2">
        <p className="text-[11px] uppercase tracking-wide text-muted-foreground">Current</p>
        <p className="text-sm font-medium">
          {paramCount} params / {combinations} combos
        </p>
      </div>
      <div className="rounded-md border bg-muted/20 p-2">
        <p className="text-[11px] uppercase tracking-wide text-muted-foreground">Saved</p>
        <p className="text-sm font-medium">{savedInfo}</p>
      </div>
      <div className="rounded-md border bg-muted/20 p-2">
        <p className="text-[11px] uppercase tracking-wide text-muted-foreground">State</p>
        <p className={cn('text-sm font-medium', isDirty ? 'text-amber-600' : 'text-muted-foreground')}>
          {isDirty ? 'Unsaved changes' : 'Synced'}
        </p>
      </div>
    </div>
  );
}

function EditorDialog({
  open,
  onOpenChange,
  basename,
  content,
  analysis,
  validationState,
  savedInfo,
  isDirty,
  canDelete,
  isSavePending,
  isDeletePending,
  saveErrorMessage,
  deleteErrorMessage,
  onContentChange,
  onCopySnippet,
  onReset,
  onDelete,
  onSave,
}: EditorDialogProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-7xl max-h-[92vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>Optimization Grid Editor: {basename}</DialogTitle>
          <DialogDescription>
            Edit optimization ranges and use the signal reference panel for available signal definitions.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 min-h-0 overflow-hidden space-y-3">
          <SummaryCards
            paramCount={analysis.paramCount}
            combinations={analysis.combinations}
            savedInfo={savedInfo}
            isDirty={isDirty}
          />

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 h-[560px]">
            <div className="lg:col-span-2 flex flex-col min-h-0">
              <div className="flex-1 min-h-0">
                <MonacoYamlEditor value={content} onChange={onContentChange} height="460px" />
              </div>

              <div className="mt-3 space-y-2">
                <ValidationBanner validationState={validationState} />
                {saveErrorMessage && <ErrorBanner message={saveErrorMessage} />}
                {deleteErrorMessage && <ErrorBanner message={deleteErrorMessage} />}
              </div>
            </div>

            <div className="lg:col-span-1 border rounded-md overflow-hidden min-h-0">
              <SignalReferencePanel onCopySnippet={onCopySnippet} />
            </div>
          </div>
        </div>

        <DialogFooter className="gap-2 sm:justify-between">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
          <EditorActions
            canDelete={canDelete}
            isDirty={isDirty}
            isSavePending={isSavePending}
            isDeletePending={isDeletePending}
            hasParseError={Boolean(analysis.parseError)}
            onReset={onReset}
            onDelete={onDelete}
            onSave={onSave}
          />
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export function OptimizationGridEditor({ strategyName }: OptimizationGridEditorProps) {
  const basename = strategyName.split('/').pop() ?? strategyName;

  const { data: gridConfig, isLoading, isError } = useOptimizationGridConfig(basename);
  const saveGrid = useSaveOptimizationGrid();
  const deleteGrid = useDeleteOptimizationGrid();

  const [content, setContent] = useState('');
  const [baselineContent, setBaselineContent] = useState('');
  const [hasPersistedConfig, setHasPersistedConfig] = useState(false);
  const [isDirty, setIsDirty] = useState(false);
  const [isEditorOpen, setIsEditorOpen] = useState(false);

  useEffect(() => {
    if (gridConfig) {
      setContent(gridConfig.content);
      setBaselineContent(gridConfig.content);
      setHasPersistedConfig(true);
      setIsDirty(false);
      return;
    }

    if (isError) {
      setContent(TEMPLATE_YAML);
      setBaselineContent(TEMPLATE_YAML);
      setHasPersistedConfig(false);
      setIsDirty(false);
    }
  }, [gridConfig, isError]);

  const analysis = useMemo(() => analyzeGridParameters(content), [content]);
  const validationState = useMemo(() => buildValidationState(analysis), [analysis]);

  const applyContent = useCallback(
    (nextContent: string) => {
      setContent(nextContent);
      setIsDirty(nextContent !== baselineContent);
    },
    [baselineContent]
  );

  const handleCopySnippet = useCallback(
    (snippet: string) => {
      const nextContent = content.trim() ? `${content.trim()}\n\n${snippet}` : snippet;
      applyContent(nextContent);
    },
    [applyContent, content]
  );

  const handleSave = useCallback(() => {
    saveGrid.mutate(
      { strategy: basename, request: { content } },
      {
        onSuccess: () => {
          setBaselineContent(content);
          setHasPersistedConfig(true);
          setIsDirty(false);
        },
      }
    );
  }, [basename, content, saveGrid]);

  const handleDelete = useCallback(() => {
    deleteGrid.mutate(basename, {
      onSuccess: () => {
        setContent(TEMPLATE_YAML);
        setBaselineContent(TEMPLATE_YAML);
        setHasPersistedConfig(false);
        setIsDirty(false);
      },
    });
  }, [basename, deleteGrid]);

  const handleReset = useCallback(() => {
    setContent(baselineContent);
    setIsDirty(false);
  }, [baselineContent]);

  if (isLoading) {
    return <LoadingState />;
  }

  const savedInfo = gridConfig
    ? `${gridConfig.param_count} params, ${gridConfig.combinations} combinations`
    : 'Not saved';

  const lastSaveInfo =
    saveGrid.data && saveGrid.data.strategy_name === basename
      ? `${saveGrid.data.param_count} params, ${saveGrid.data.combinations} combinations`
      : null;

  const displaySavedInfo = lastSaveInfo || savedInfo;

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div className="space-y-1">
          <h4 className="text-sm font-semibold">Optimization Grid</h4>
          <p className="text-xs text-muted-foreground">
            Open the editor popup to modify grid YAML while browsing signal definitions.
          </p>
          <p className="text-xs font-mono text-muted-foreground">strategy: {basename}</p>
        </div>

        <Button size="sm" variant="outline" onClick={() => setIsEditorOpen(true)}>
          <Edit className="h-4 w-4 mr-1" />
          Open Editor
        </Button>
      </div>

      <SummaryCards
        paramCount={analysis.paramCount}
        combinations={analysis.combinations}
        savedInfo={displaySavedInfo}
        isDirty={isDirty}
      />

      {!hasPersistedConfig && (
        <p className="text-xs text-muted-foreground">
          No saved grid config exists yet. Use <span className="font-medium">Open Editor</span> and save to create one.
        </p>
      )}

      <EditorDialog
        open={isEditorOpen}
        onOpenChange={setIsEditorOpen}
        basename={basename}
        content={content}
        analysis={analysis}
        validationState={validationState}
        savedInfo={displaySavedInfo}
        isDirty={isDirty}
        canDelete={hasPersistedConfig}
        isSavePending={saveGrid.isPending}
        isDeletePending={deleteGrid.isPending}
        saveErrorMessage={saveGrid.isError ? saveGrid.error.message : null}
        deleteErrorMessage={deleteGrid.isError ? deleteGrid.error.message : null}
        onContentChange={applyContent}
        onCopySnippet={handleCopySnippet}
        onReset={handleReset}
        onDelete={handleDelete}
        onSave={handleSave}
      />
    </div>
  );
}
