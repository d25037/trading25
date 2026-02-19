import { AlertCircle, CheckCircle2, Info, Loader2, RotateCcw, Save, Trash2 } from 'lucide-react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { MonacoYamlEditor } from '@/components/Editor/MonacoYamlEditor';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { useDeleteOptimizationGrid, useOptimizationGridConfig, useSaveOptimizationGrid } from '@/hooks/useOptimization';
import { analyzeGridParameters, formatGridParameterValue, type GridParameterAnalysis } from './optimizationGridParams';

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

const PRESET_TEMPLATES = [
  {
    id: 'starter',
    label: 'Starter',
    description: 'Minimal entry + exit ranges',
    content: TEMPLATE_YAML,
  },
  {
    id: 'breakout-heavy',
    label: 'Breakout Focus',
    description: 'Period/volume breakout combinations',
    content: `parameter_ranges:
  entry_filter_params:
    period_breakout:
      period: [10, 15, 20, 30]
    volume_breakout:
      period: [10, 20]
      ratio: [1.1, 1.3, 1.5]
  exit_trigger_params:
    atr_stop:
      atr_multiplier: [1.8, 2.2, 2.8]
`,
  },
  {
    id: 'balanced',
    label: 'Balanced',
    description: 'Momentum + fundamental + risk control',
    content: `parameter_ranges:
  entry_filter_params:
    rsi:
      period: [10, 14]
      lower: [25, 30, 35]
    forward_eps_growth:
      threshold: [0.05, 0.1, 0.15]
  exit_trigger_params:
    take_profit:
      pct: [0.08, 0.12, 0.16]
    atr_stop:
      atr_multiplier: [1.5, 2.0, 2.5]
`,
  },
] as const;

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

interface GridHelperPanelProps {
  analysis: GridParameterAnalysis;
  isBusy: boolean;
  onApplyPreset: (presetContent: string) => void;
}

interface EditorWorkspaceProps {
  content: string;
  analysis: GridParameterAnalysis;
  validationState: ValidationState;
  isBusy: boolean;
  saveErrorMessage: string | null;
  deleteErrorMessage: string | null;
  onContentChange: (value: string) => void;
  onApplyPreset: (presetContent: string) => void;
}

interface OptimizationGridEditorViewProps {
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
  onReset: () => void;
  onDelete: () => void;
  onSave: () => void;
  onApplyPreset: (presetContent: string) => void;
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
    <div className="flex flex-wrap gap-2">
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

function GridHelperPanel({ analysis, isBusy, onApplyPreset }: GridHelperPanelProps) {
  return (
    <div className="border rounded-md overflow-hidden min-h-0 flex flex-col">
      <div className="border-b p-3">
        <h5 className="text-sm font-medium">Grid Helper</h5>
        <p className="text-xs text-muted-foreground mt-1">Apply presets and inspect detected parameter paths.</p>
      </div>
      <div className="flex-1 overflow-y-auto p-3 space-y-4">
        <section className="space-y-2">
          <p className="text-xs font-medium text-muted-foreground">Presets</p>
          {PRESET_TEMPLATES.map((preset) => (
            <button
              key={preset.id}
              type="button"
              className="w-full rounded-md border p-2 text-left hover:bg-muted/30 transition-colors"
              onClick={() => onApplyPreset(preset.content)}
              disabled={isBusy}
            >
              <p className="text-sm font-medium">{preset.label}</p>
              <p className="text-xs text-muted-foreground">{preset.description}</p>
            </button>
          ))}
        </section>

        <section className="space-y-2">
          <p className="text-xs font-medium text-muted-foreground">Detected Parameters ({analysis.paramCount})</p>
          {analysis.entries.length > 0 ? (
            <ul className="space-y-1">
              {analysis.entries.map((entry) => (
                <li key={entry.path} className="rounded bg-muted/30 px-2 py-1 text-xs">
                  <p className="font-mono break-all">{entry.path}</p>
                  <p className="text-muted-foreground break-all">
                    [{entry.values.map((value) => formatGridParameterValue(value)).join(', ')}]
                  </p>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-xs text-muted-foreground">
              Parameter lists are not detected yet. Add arrays under <code>parameter_ranges</code>.
            </p>
          )}
        </section>
      </div>
    </div>
  );
}

function EditorWorkspace({
  content,
  analysis,
  validationState,
  isBusy,
  saveErrorMessage,
  deleteErrorMessage,
  onContentChange,
  onApplyPreset,
}: EditorWorkspaceProps) {
  return (
    <div className="grid grid-cols-1 xl:grid-cols-3 gap-4 min-h-[460px]">
      <div className="xl:col-span-2 flex flex-col min-h-0">
        <div className="flex-1 min-h-0">
          <MonacoYamlEditor value={content} onChange={onContentChange} height="380px" />
        </div>

        <div className="mt-3 space-y-2">
          <ValidationBanner validationState={validationState} />
          {saveErrorMessage && <ErrorBanner message={saveErrorMessage} />}
          {deleteErrorMessage && <ErrorBanner message={deleteErrorMessage} />}
        </div>
      </div>

      <GridHelperPanel analysis={analysis} isBusy={isBusy} onApplyPreset={onApplyPreset} />
    </div>
  );
}

function OptimizationGridEditorView({
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
  onReset,
  onDelete,
  onSave,
  onApplyPreset,
}: OptimizationGridEditorViewProps) {
  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div className="space-y-1">
          <h4 className="text-sm font-semibold">Optimization Grid Editor</h4>
          <p className="text-xs text-muted-foreground">
            Edit optimization parameter ranges and validate combinations before running jobs.
          </p>
          <p className="text-xs font-mono text-muted-foreground">strategy: {basename}</p>
        </div>
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
      </div>

      <SummaryCards
        paramCount={analysis.paramCount}
        combinations={analysis.combinations}
        savedInfo={savedInfo}
        isDirty={isDirty}
      />

      <EditorWorkspace
        content={content}
        analysis={analysis}
        validationState={validationState}
        isBusy={isSavePending || isDeletePending}
        saveErrorMessage={saveErrorMessage}
        deleteErrorMessage={deleteErrorMessage}
        onContentChange={onContentChange}
        onApplyPreset={onApplyPreset}
      />
    </div>
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

  const handleContentChange = useCallback(
    (value: string) => {
      setContent(value);
      setIsDirty(value !== baselineContent);
    },
    [baselineContent]
  );

  const analysis = useMemo(() => analyzeGridParameters(content), [content]);
  const validationState = useMemo(() => buildValidationState(analysis), [analysis]);

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

  const handleApplyPreset = useCallback(
    (presetContent: string) => {
      setContent(presetContent);
      setIsDirty(presetContent !== baselineContent);
    },
    [baselineContent]
  );

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

  return (
    <OptimizationGridEditorView
      basename={basename}
      content={content}
      analysis={analysis}
      validationState={validationState}
      savedInfo={lastSaveInfo || savedInfo}
      isDirty={isDirty}
      canDelete={hasPersistedConfig}
      isSavePending={saveGrid.isPending}
      isDeletePending={deleteGrid.isPending}
      saveErrorMessage={saveGrid.isError ? saveGrid.error.message : null}
      deleteErrorMessage={deleteGrid.isError ? deleteGrid.error.message : null}
      onContentChange={handleContentChange}
      onReset={handleReset}
      onDelete={handleDelete}
      onSave={handleSave}
      onApplyPreset={handleApplyPreset}
    />
  );
}
